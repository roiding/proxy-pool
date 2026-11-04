package domain

import "testing"

func TestConnectionFingerprintNormalizesAddressAliases(t *testing.T) {
	const secret = "fingerprint-secret-0123456789"
	base := func(host string) string {
		return ConnectionFingerprint(secret, TransportHTTP, host, 8080, "", "")
	}
	if base("127.0.0.1") != base("::ffff:127.0.0.1") {
		t.Fatal("IPv4-mapped IPv6 must collapse onto the IPv4 identity")
	}
	if base("Example.COM.") != base("example.com") {
		t.Fatal("host case and trailing dot must not change the identity")
	}
	if base("127.0.0.1") == base("127.0.0.2") {
		t.Fatal("different addresses must stay distinct")
	}
	if base("2001:DB8::1") != base("2001:db8::1") {
		t.Fatal("IPv6 fingerprints must be canonical lowercase")
	}
}

func TestConnectionFingerprintIgnoresLocalContext(t *testing.T) {
	const secret = "fingerprint-secret-0123456789"
	plain := ConnectionFingerprint(secret, TransportHTTP, "10.0.0.1", 8080, "", "")
	creds := ConnectionFingerprint(secret, TransportHTTP, "10.0.0.1", 8080, "u", "p")
	if plain == creds {
		t.Fatal("upstream credentials are part of the connection identity")
	}
	if ConnectionFingerprint("other-secret-0123456789", TransportHTTP, "10.0.0.1", 8080, "", "") == plain {
		t.Fatal("identity secret must change the fingerprint")
	}
}

func TestEvidenceScopeHashBindsAllParts(t *testing.T) {
	base := ValidationScope{Mode: "tls", TargetHostPort: "example.com:443",
		Profile: "https_basic_v1", SecurityHash: "h1"}
	if EvidenceScopeHash(base) != EvidenceScopeHash(base) {
		t.Fatal("scope hash must be deterministic")
	}
	for _, mutate := range []func(*ValidationScope){
		func(s *ValidationScope) { s.Mode = "basic" },
		func(s *ValidationScope) { s.TargetHostPort = "example.org:443" },
		func(s *ValidationScope) { s.Profile = "other" },
		func(s *ValidationScope) { s.SecurityHash = "h2" },
	} {
		other := base
		mutate(&other)
		if EvidenceScopeHash(other) == EvidenceScopeHash(base) {
			t.Fatal("every scope component must change the hash")
		}
	}
}
