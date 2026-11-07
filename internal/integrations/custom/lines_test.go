package custom

import (
	"strings"
	"testing"

	"proxypool/internal/domain"
)

func TestParseLinesForms(t *testing.T) {
	opts := LinesOptions{DefaultTransport: domain.TransportHTTP, AllowPrivate: true}
	cands, errs := ParseLines(opts, "# comment\n\n1.2.3.4:8080\nhttp://user:p%40ss@5.6.7.8:3128\nsocks5://2.3.4.5:1080\n")
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if len(cands) != 3 {
		t.Fatalf("got %d candidates, want 2", len(cands))
	}
	if cands[0].Conn.Host != "1.2.3.4" || cands[0].Conn.Port != 8080 ||
		cands[0].Conn.Transport != domain.TransportHTTP {
		t.Fatalf("bare host:port parsed wrong: %+v", cands[0].Conn)
	}
	if cands[1].Conn.Username != "user" || cands[1].Conn.Password != "p@ss" {
		t.Fatalf("credentials decoded wrong: %+v", cands[1].Conn)
	}
	if cands[2].Conn.Transport != domain.TransportSOCKS5 ||
		cands[2].Conn.Host != "2.3.4.5" || cands[2].Conn.Port != 1080 {
		t.Fatalf("socks5 line parsed wrong: %+v", cands[2].Conn)
	}
}

func TestParseLinesCredentialBoundsAndRedaction(t *testing.T) {
	opts := LinesOptions{DefaultTransport: domain.TransportHTTP, AllowPrivate: false}
	lines := []string{
		"http://audit-user:audit-password-%ZZ@8.8.8.8:8080",
		"http://audit-user:" + strings.Repeat("p", MaxCredentialBytes+1) + "@8.8.8.8:8080",
	}
	_, errs := ParseLines(opts, strings.Join(lines, "\n"))
	if len(errs) != 2 {
		t.Fatalf("expected two rejected lines, got %d: %v", len(errs), errs)
	}
	for _, item := range errs {
		if strings.Contains(item.Error, "audit-user") ||
			strings.Contains(item.Error, "audit-password") ||
			strings.Contains(item.Input, "audit-user") ||
			strings.Contains(item.Input, "audit-password") {
			t.Fatalf("parser diagnostic exposed URI credentials: %+v", item)
		}
	}
}

func TestParseLinesRejectsPrivateAndInvalid(t *testing.T) {
	opts := LinesOptions{DefaultTransport: domain.TransportHTTP, AllowPrivate: false}
	cands, errs := ParseLines(opts, "127.0.0.1:8080\n1.2.3.4\nftp://1.2.3.4:21\n")
	if len(cands) != 0 {
		t.Fatalf("expected no candidates, got %d", len(cands))
	}
	if len(errs) != 3 {
		t.Fatalf("expected 3 rejected lines, got %d: %v", len(errs), errs)
	}
	if want := []string{"address policy", "missing port", "unsupported transport"}; true {
		for i, w := range want {
			if !contains(errs[i].Error, w) {
				t.Fatalf("error %d = %q, want containing %q", i, errs[i].Error, w)
			}
		}
	}
}

func TestHostNormalizationDoesNotStripZonesOrGuessMalformedIPv4(t *testing.T) {
	for _, host := range []string{"[fe80::1%en0]", "001.2.3.4", "999.1.1.1", "1.2.3.4."} {
		if normalized, err := NormalizeHost(host, true); err == nil {
			t.Fatalf("invalid/ambiguous IP %q normalized to %q", host, normalized)
		}
	}
	for host, want := range map[string]string{"Proxy.Example.": "proxy.example", "[::ffff:8.8.8.8]": "8.8.8.8"} {
		if got, err := NormalizeHost(host, false); err != nil || got != want {
			t.Fatalf("legal host lost: %q => %q %v", host, got, err)
		}
	}
}

func TestCredentialControlsCannotEnterConnectionIdentity(t *testing.T) {
	for _, raw := range []string{"http://first%1Fsecond:secret@8.8.8.8:8080", "http://first:secret%00@8.8.8.8:8080", "http://first:secret%FF@8.8.8.8:8080"} {
		rows, rejected := ParseLines(LinesOptions{}, raw)
		if len(rows) != 0 || len(rejected) != 1 || rejected[0].Input != "" || strings.Contains(rejected[0].Error, "secret") {
			t.Fatalf("invalid credential accepted or echoed: %+v %+v", rows, rejected)
		}
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (haystack == needle ||
		len(needle) == 0 || indexOf(haystack, needle) >= 0)
}

func indexOf(h, n string) int {
	for i := 0; i+len(n) <= len(h); i++ {
		if h[i:i+len(n)] == n {
			return i
		}
	}
	return -1
}
