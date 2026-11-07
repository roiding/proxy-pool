package domain

import "testing"

func TestUnspecifiedSourceSelectionUsesProjectAllowlist(t *testing.T) {
	p := Project{AllowedSourceIDs: []string{"private-a", "public-b"}}
	got := p.FilterAllowedSources(nil)
	if len(got) != 2 || got[0] != "private-a" || got[1] != "public-b" {
		t.Fatalf("unspecified selection denied authorized sources: %v", got)
	}
	got[0] = "changed"
	if p.AllowedSourceIDs[0] != "private-a" {
		t.Fatal("returned scope aliases project configuration")
	}
	if denied := p.FilterAllowedSources([]string{}); denied == nil || len(denied) != 0 {
		t.Fatal("explicit empty selection became unrestricted")
	}
	if denied := (Project{AllowedSourceIDs: []string{}}).FilterAllowedSources(nil); denied == nil || len(denied) != 0 {
		t.Fatal("empty project allowlist became unrestricted")
	}
}
