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
			errs = append(errs, LineError{Line: lineNo, Input: redactLine(line), Error: err.Error()})
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
		return domain.Connection{}, fmt.Errorf("parse URI: %w", err)
	}
	switch domain.Transport(u.Scheme) {
	case domain.TransportHTTP, domain.TransportSOCKS5:
	default:
		return domain.Connection{}, fmt.Errorf("unsupported transport %q", u.Scheme)
	}
	host := u.Hostname()
	if host == "" {
		return domain.Connection{}, fmt.Errorf("missing host")
	}
	portStr := u.Port()
	if portStr == "" {
		return domain.Connection{}, fmt.Errorf("missing port")
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return domain.Connection{}, fmt.Errorf("invalid port %q", portStr)
	}
	host = strings.ToLower(host)
	if ip, err := netip.ParseAddr(host); err == nil {
		if !domain.AddrAllowed(ip, opts.AllowPrivate) {
			return domain.Connection{}, fmt.Errorf("address %s rejected by address policy", host)
		}
	}
	conn := domain.Connection{Transport: domain.Transport(u.Scheme), Host: host, Port: port}
	if u.User != nil {
		conn.Username = u.User.Username()
		conn.Password, _ = u.User.Password()
	}
	return conn, nil
}

func redactLine(line string) string {
	if i := strings.Index(line, "@"); i >= 0 {
		line = line[i:]
	}
	if len(line) > 120 {
		line = line[:120] + "..."
	}
	return line
}
