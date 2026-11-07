package sites

import (
	"bytes"
	"encoding/json"
	"iter"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/encoding/simplifiedchinese"
	"proxypool/internal/domain"
	"proxypool/internal/integrations/custom"
)

func trimUTF8BOM(body []byte) []byte { return bytes.TrimPrefix(body, []byte{0xef, 0xbb, 0xbf}) }
func decodePage(body []byte) ([]byte, error) {
	if len(body) > 4<<20 {
		return nil, &Error{Kind: KindFormat, Detail: "parser body exceeds its byte limit"}
	}
	lower := strings.ToLower(string(body))
	gbk := strings.Contains(lower, "charset=gbk") || strings.Contains(lower, `charset="gbk"`) || strings.Contains(lower, "charset=gb2312") || strings.Contains(lower, `charset="gb2312"`) || strings.Contains(lower, "charset='gbk'") || strings.Contains(lower, "charset='gb2312'")
	if gbk {
		decoded, err := simplifiedchinese.GBK.NewDecoder().Bytes(body)
		if err != nil || !utf8.Valid(decoded) || bytes.Contains(decoded, []byte("\ufffd")) {
			return nil, &Error{Kind: KindFormat, Detail: "invalid GBK page encoding"}
		}
		return decoded, nil
	}
	if !utf8.Valid(body) {
		return nil, &Error{Kind: KindFormat, Detail: "page is not valid UTF-8 or declared GBK"}
	}
	return body, nil
}

type boundedLine struct {
	text    string
	tooLong bool
}

// Iterator avoids allocating millions of row objects for newline-heavy input.
func splitBoundedLines(body []byte, limit int) iter.Seq2[int, boundedLine] {
	return func(yield func(int, boundedLine) bool) {
		for index := 0; len(body) > 0; index++ {
			end := bytes.IndexByte(body, '\n')
			if end < 0 {
				end = len(body)
			}
			row := body[:end]
			long := len(row) > limit
			if long {
				row = row[:limit]
			}
			if !yield(index, boundedLine{text: strings.TrimSuffix(string(row), "\r"), tooLong: long}) {
				return
			}
			if end == len(body) {
				return
			}
			body = body[end+1:]
		}
	}
}
func blockedPageReason(body []byte) (bool, string) {
	lower := strings.ToLower(string(body))
	if !strings.Contains(lower, "<html") && !strings.Contains(lower, "<!doctype") {
		return false, ""
	}
	// Error titles are not normal proxy-list decorations, even with an IP in a table.
	if start := strings.Index(lower, "<title>"); start >= 0 {
		if end := strings.Index(lower[start:], "</title>"); end >= 0 {
			title := lower[start : start+end]
			for _, marker := range []string{"service unavailable", "internal server error", "bad gateway", "not found", "access denied", "forbidden", "error 5", "error 4"} {
				if strings.Contains(title, marker) {
					return true, "source returned an HTML error page"
				}
			}
		}
	}
	for _, marker := range []string{"just a moment...", "cf-chl-", "challenge-platform", "attention required! | cloudflare", "verify you are human", "access denied", "rate limit exceeded", "too many requests", "domain is for sale", "domain for sale", "域名出售", "访问受限"} {
		if strings.Contains(lower, marker) {
			return true, "source returned an access, challenge or parked-domain page"
		}
	}
	// A normal proxy page can mention a captcha in its help/footer. Only
	// recognize a captcha/login form when no proxy list container exists.
	if !strings.Contains(lower, "<table") && !strings.Contains(lower, "<textarea") && !strings.Contains(lower, `role="row"`) && strings.Contains(lower, "<form") && (strings.Contains(lower, "captcha") || strings.Contains(lower, "验证码") || strings.Contains(lower, `type="password"`)) {
		return true, "source returned a login or captcha page"
	}
	return false, ""
}
func responseEnvelopeError(body []byte) *Error {
	var object map[string]json.RawMessage
	if json.Unmarshal(body, &object) != nil || object == nil {
		return nil
	}
	for _, key := range []string{"code", "status"} {
		if raw, ok := object[key]; ok {
			if n, ok := jsonInt(raw); ok && n != 0 && n != 200 {
				e := &Error{Kind: KindFormat, Code: "SOURCE_FORMAT_ERROR", Status: 502, Detail: "vendor JSON response signalled an error"}
				if n == 401 || n == 403 {
					e.Kind, e.Code = KindAccess, "SOURCE_AUTH_FAILED"
				}
				if n == 429 {
					e.Kind, e.Code = KindAccess, "SOURCE_RATE_LIMITED"
				}
				return e
			}
		}
	}
	if raw, ok := object["success"]; ok && strings.TrimSpace(string(raw)) == "false" {
		return &Error{Kind: KindFormat, Code: "SOURCE_FORMAT_ERROR", Status: 502, Detail: "vendor JSON response signalled failure"}
	}
	return nil
}
func parseLines(opts ParseOptions, body []byte) (parsedPage, error) {
	if bytes.Contains(body, []byte("<")) || bytes.Contains(body, []byte(">")) {
		return parsedPage{}, &Error{Kind: KindFormat, Detail: "line source returned markup rather than proxy rows"}
	}
	p := newPageBuilder(opts)
	p.out.recognized = true
	records := 0
	for index, line := range splitBoundedLines(body, opts.maxRowBytes()) {
		text := strings.TrimSpace(line.text)
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		records++
		if records > opts.maxRecords() {
			p.out.truncated = true
			break
		}
		if line.tooLong {
			p.reject(index+1, "", "line exceeds row byte limit")
			continue
		}
		conn, err := parseProxyLine(opts, text)
		if err != nil {
			p.reject(index+1, "", err.Error())
			continue
		}
		if opts.Protocol != "" && conn.Transport != opts.Protocol {
			p.rejectProtocol(index + 1)
			continue
		}
		p.accept(index+1, "", Entry{Conn: conn, Claims: map[string]string{"protocol": string(conn.Transport)}})
	}
	return p.out, nil
}
func parseProxyLine(opts ParseOptions, line string) (domain.Connection, error) {
	transport := opts.Protocol
	if transport == "" {
		transport = opts.DefaultTransport
	}
	if transport == "" {
		transport = domain.TransportHTTP
	}
	if transport != domain.TransportHTTP && transport != domain.TransportSOCKS5 {
		return domain.Connection{}, &Error{Kind: KindFormat, Detail: "unsupported default transport"}
	}
	if !strings.Contains(line, "://") {
		line = string(transport) + "://" + line
	}
	u, err := url.Parse(line)
	if err != nil || u.Opaque != "" || u.Host == "" || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return domain.Connection{}, &Error{Kind: KindFormat, Detail: "invalid proxy URI shape"}
	}
	if u.Scheme != "http" && u.Scheme != "socks5" {
		return domain.Connection{}, &Error{Kind: KindFormat, Detail: "unsupported transport; use HTTP or SOCKS5"}
	}
	host, err := custom.NormalizeHost(u.Hostname(), opts.AllowPrivate)
	if err != nil {
		return domain.Connection{}, err
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil || port < 1 || port > 65535 {
		return domain.Connection{}, &Error{Kind: KindFormat, Detail: "invalid or missing port"}
	}
	conn := domain.Connection{Transport: domain.Transport(u.Scheme), Host: host, Port: port}
	if u.User != nil {
		conn.Username = u.User.Username()
		conn.Password, _ = u.User.Password()
		if custom.ValidateCredentials(conn.Username, conn.Password) != nil {
			return domain.Connection{}, &Error{Kind: KindFormat, Detail: "invalid or oversized proxy credentials"}
		}
	}
	return conn, nil
}
