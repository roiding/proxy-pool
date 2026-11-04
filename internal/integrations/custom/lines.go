package custom

import (
	"fmt"
	"net/netip"
	"net/url"
	"strconv"
	"strings"

	"proxypool/internal/domain"
)

type LinesOptions struct {
	DefaultTransport domain.Transport
	AllowPrivate     bool
}

const (
	// MaxCredentialBytes is the shared bound for text URI and JSON import
	// usernames and passwords. Values are measured as UTF-8 bytes.
	MaxCredentialBytes = 256
	// MaxHostBytes is the shared DNS hostname bound for text and JSON imports.
	// IPv6 literals are not subject to this character limit.
	MaxHostBytes = 253
)

type LineError struct {
	Line  int    `json:"line"`
	Input string `json:"input"`
	Error string `json:"error"`
}

// ParseLines parses the lines_v1 custom import format:
// [http://|socks5://][user:pass@]host:port, one proxy per line, '#'
// comments allowed.
func ParseLines(opts LinesOptions, content string) ([]domain.NormalizedCandidate, []LineError) {
	var out []domain.NormalizedCandidate
	var errs []LineError
	for i, raw := range strings.Split(content, "\n") {
		lineNo := i + 1
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		conn, err := parseLine(opts, line)
		if err != nil {
			// Line and a generic reason identify the rejected item. Original
			// input is intentionally omitted because every shape can carry
			// authentication material (including unclosed userinfo).
			errs = append(errs, LineError{Line: lineNo, Error: err.Error()})
			continue
		}
		out = append(out, domain.NormalizedCandidate{
			Conn:    conn,
			Kind:    domain.KindStatic,
			Session: domain.SessionStatic,
		})
	}
	return out, errs
}

func parseLine(opts LinesOptions, line string) (domain.Connection, error) {
	if opts.DefaultTransport == "" {
		opts.DefaultTransport = domain.TransportHTTP
	}
	if !strings.Contains(line, "://") {
		line = string(opts.DefaultTransport) + "://" + line
	}
	u, err := url.Parse(line)
	if err != nil {
		// URL parse errors can contain the complete userinfo. Never return
		// attacker-controlled parse detail from a durable import diagnostic.
		return domain.Connection{}, fmt.Errorf("invalid proxy URI")
	}
	switch domain.Transport(u.Scheme) {
	case domain.TransportHTTP, domain.TransportSOCKS5:
	default:
		return domain.Connection{}, fmt.Errorf("unsupported transport; use http or socks5")
	}
	host, err := NormalizeHost(u.Hostname(), opts.AllowPrivate)
	if err != nil {
		return domain.Connection{}, err
	}
	portStr := u.Port()
	if portStr == "" {
		return domain.Connection{}, fmt.Errorf("missing port")
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return domain.Connection{}, fmt.Errorf("invalid port")
	}
	conn := domain.Connection{Transport: domain.Transport(u.Scheme), Host: host, Port: port}
	if u.User != nil {
		conn.Username = u.User.Username()
		if len(conn.Username) > MaxCredentialBytes {
			return domain.Connection{}, fmt.Errorf(
				"username exceeds %d bytes", MaxCredentialBytes)
		}
		conn.Password, _ = u.User.Password()
		if len(conn.Password) > MaxCredentialBytes {
			return domain.Connection{}, fmt.Errorf(
				"password exceeds %d bytes", MaxCredentialBytes)
		}
	}
	return conn, nil
}

// NormalizeHost validates and canonicalizes one import host. The text and
// JSON import parsers share it so a URI-looking, control-bearing, overlong, or
// malformed hostname cannot be accepted only because a format differs.
func NormalizeHost(raw string, allowPrivate bool) (string, error) {
	host := strings.TrimSpace(raw)
	if strings.HasPrefix(host, "[") || strings.HasSuffix(host, "]") {
		if !strings.HasPrefix(host, "[") || !strings.HasSuffix(host, "]") {
			return "", fmt.Errorf("invalid host")
		}
		host = host[1 : len(host)-1]
	}
	if host == "" {
		return "", fmt.Errorf("missing host")
	}
	if len(host) > MaxHostBytes {
		return "", fmt.Errorf("host exceeds %d bytes", MaxHostBytes)
	}
	if strings.ContainsFunc(host, func(r rune) bool {
		return r <= ' ' || r == 0x7f || strings.ContainsRune("@/\\?#[]", r)
	}) {
		return "", fmt.Errorf("invalid host")
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		if ip.Zone() != "" {
			return "", fmt.Errorf("invalid host")
		}
		ip = ip.Unmap()
		if !domain.AddrAllowed(ip, allowPrivate) {
			return "", fmt.Errorf("address %s rejected by address policy", ip.String())
		}
		return ip.String(), nil
	}
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	if !validHostname(host) {
		return "", fmt.Errorf("invalid host")
	}
	return host, nil
}

func validHostname(host string) bool {
	if host == "" || len(host) > MaxHostBytes {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 ||
			label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, r := range label {
			if !('a' <= r && r <= 'z') && !('0' <= r && r <= '9') && r != '-' {
				return false
			}
		}
	}
	return true
}
