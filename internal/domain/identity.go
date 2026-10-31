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

// AddrAllowed rejects non-public address ranges unless explicitly allowed.
// It is applied to resolved connection targets and imported literal IPs.
func AddrAllowed(ip netip.Addr, allowPrivate bool) bool {
	if !ip.IsValid() {
		return false
	}
	if allowPrivate {
		return true
	}
	return !ip.IsLoopback() && !ip.IsPrivate() && !ip.IsLinkLocalUnicast() &&
		!ip.IsLinkLocalMulticast() && !ip.IsMulticast() && !ip.IsUnspecified()
}
