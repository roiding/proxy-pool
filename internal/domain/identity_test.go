package domain

import "testing"

func TestResourceIDDeterministicAndSensitive(t *testing.T) {
	base := IdentityKey{Tenant: "default", Transport: TransportHTTP, Host: "1.2.3.4", Port: 8080}
	other := base
	other.CredentialVersion = "v1-abc"
	if ResourceID("secret", base) != ResourceID("secret", base) {
		t.Fatal("same identity must map to the same resource id")
	}
	if ResourceID("secret", base) == ResourceID("secret", other) {
		t.Fatal("credential version must change the resource id")
	}
	if ResourceID("secret", base) == ResourceID("other-secret", base) {
		t.Fatal("identity secret must change the resource id")
	}
}

func TestCredentialVersionEmptyWithoutMaterial(t *testing.T) {
	if CredentialVersion("secret", "", "") != "" {
		t.Fatal("no credentials must produce empty version")
	}
	if CredentialVersion("secret", "u", "p") == "" {
		t.Fatal("credentials must produce a version")
	}
}
