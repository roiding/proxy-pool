package domain

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/netip"
	"strings"
)

const IdentityVersion = 1

type IdentityKey struct {
	Tenant            string
	Transport         Transport
	Host              string
	Port              int
	CredentialVersion string
	Session           SessionMode
}

const fieldSep = "\x1f"

func (k IdentityKey) Canonical() string {
	return strings.Join([]string{
		fmt.Sprintf("v%d", IdentityVersion),
		k.Tenant,
		string(k.Transport),
		k.Host,
		fmt.Sprintf("%d", k.Port),
		k.CredentialVersion,
		string(k.Session),
	}, fieldSep)
}

func hmacHex(secret, domainSep string, parts ...string) string {
	mac := hmac.New(sha256.New, []byte(domainSep+"\x00"+secret))
	for i, p := range parts {
		if i > 0 {
			mac.Write([]byte(fieldSep))
		}
		mac.Write([]byte(p))
	}
	return hex.EncodeToString(mac.Sum(nil))
}

func ResourceID(secret string, k IdentityKey) string {
	return fmt.Sprintf("v%d-%s", IdentityVersion,
		hmacHex(secret, "proxy-pool/resource-identity/v1", k.Canonical()))
}

// CanonicalHost normalizes a connection host for identity purposes: literal
// IPs are parsed and IPv4-mapped IPv6 addresses are unmapped, hostnames are
// lowercased (trailing dot removed). It must stay in sync with the v7
// migration's legacy-CD fingerprint mapping.
func CanonicalHost(host string) string {
	h := strings.Trim(strings.TrimSpace(host), "[]")
	if ip, err := netip.ParseAddr(h); err == nil {
		ip = ip.Unmap().WithZone("")
		return ip.String()
	}
	return strings.ToLower(strings.TrimSuffix(h, "."))
}

// ConnectionFingerprint identifies the physical upstream connection:
// transport, normalized host, port, and the real upstream credentials.
// Project, source, tenant, and the local session policy are deliberately
// excluded — only an actual upstream session credential change may affect
// the identity, and cooldowns must survive address aliases.
func ConnectionFingerprint(secret string, t Transport, host string, port int,
	username, password string) string {
	cred := CredentialVersion(secret, username, password)
	return hmacHex(secret, "proxy-pool/connection-fingerprint/v1",
		"v1", string(t), CanonicalHost(host), fmt.Sprintf("%d", port), cred)
}

func CredentialVersion(secret, username, password string) string {
	if username == "" && password == "" {
		return ""
	}
	return "v1-" + hmacHex(secret, "proxy-pool/credential-version/v1",
		username, password)[:32]
}

func OfferID(secret, sourceID, recordID, generation string) string {
	return "off_" + hmacHex(secret, "proxy-pool/source-offer/v1",
		sourceID, recordID, generation)[:32]
}

func ContextKey(credentialVersion string, session SessionMode) string {
	return credentialVersion + "|" + string(session)
}

// nonPublicV4/nonPublicV6 enumerate every non-global range the pool must
// never dial or import: IsPrivate alone misses CGNAT (the cloud metadata
// endpoint lives in 100.64.0.0/10), benchmark, reserved, and documentation
// space. Membership checks run against the unmapped address.
var nonPublicV4 = mustPrefixes(
	"0.0.0.0/8",       // "this network" including 0.0.0.0
	"10.0.0.0/8",      // private
	"100.64.0.0/10",   // CGNAT: cloud metadata endpoints
	"127.0.0.0/8",     // loopback
	"169.254.0.0/16",  // link-local
	"172.16.0.0/12",   // private
	"192.0.0.0/24",    // IETF protocol assignments
	"192.0.2.0/24",    // TEST-NET-1
	"192.168.0.0/16",  // private
	"198.18.0.0/15",   // benchmarking
	"198.51.100.0/24", // TEST-NET-2
	"203.0.113.0/24",  // TEST-NET-3
	"224.0.0.0/4",     // multicast
	"240.0.0.0/4",     // reserved, includes the 255.255.255.255 broadcast
)

var nonPublicV6 = mustPrefixes(
	"::/128",        // unspecified
	"::1/128",       // loopback
	"::ffff:0:0/96", // IPv4-mapped (callers should unmap first)
	"2001:db8::/32", // documentation
	"fc00::/7",      // unique local
	"fe80::/10",     // link-local
	"ff00::/8",      // multicast
)

func mustPrefixes(specs ...string) []netip.Prefix {
	out := make([]netip.Prefix, 0, len(specs))
	for _, s := range specs {
		p, err := netip.ParsePrefix(s)
		if err != nil {
			panic(fmt.Sprintf("built-in non-public prefix %q: %v", s, err))
		}
		out = append(out, p)
	}
	return out
}

// AddrAllowed rejects non-public address ranges unless explicitly allowed.
// It is applied to resolved connection targets and imported literal IPs.
// IPv4-mapped IPv6 addresses are unmapped before classification, and the
// deny list covers CGNAT, benchmark, reserved, broadcast, documentation,
// and ordinary private/loopback/link-local/multicast space.
func AddrAllowed(ip netip.Addr, allowPrivate bool) bool {
	if !ip.IsValid() {
		return false
	}
	if allowPrivate {
		return true
	}
	ip = ip.Unmap()
	prefixes := nonPublicV4
	if ip.Is6() {
		prefixes = nonPublicV6
	}
	for _, p := range prefixes {
		if p.Contains(ip) {
			return false
		}
	}
	return true
}
