package custom

import (
	"testing"

	"proxypool/internal/domain"
)

func TestParseLinesForms(t *testing.T) {
	opts := LinesOptions{DefaultTransport: domain.TransportHTTP, AllowPrivate: true}
	cands, errs := ParseLines(opts, "# comment\n\n1.2.3.4:8080\nhttp://user:p%40ss@5.6.7.8:3128\n")
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if len(cands) != 2 {
		t.Fatalf("got %d candidates, want 2", len(cands))
	}
	if cands[0].Conn.Host != "1.2.3.4" || cands[0].Conn.Port != 8080 ||
		cands[0].Conn.Transport != domain.TransportHTTP {
		t.Fatalf("bare host:port parsed wrong: %+v", cands[0].Conn)
	}
	if cands[1].Conn.Username != "user" || cands[1].Conn.Password != "p@ss" {
		t.Fatalf("credentials decoded wrong: %+v", cands[1].Conn)
	}
}

func TestParseLinesRejectsPrivateAndInvalid(t *testing.T) {
	opts := LinesOptions{DefaultTransport: domain.TransportHTTP, AllowPrivate: false}
	cands, errs := ParseLines(opts, "127.0.0.1:8080\n1.2.3.4\nsocks5://1.2.3.4:1080\n")
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
