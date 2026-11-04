package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"proxypool/internal/application"
	"proxypool/internal/storage/sqlite"
)

func TestSourceConfigTypedNestingAndFlatRejection(t *testing.T) {
	cases := []struct {
		name   string
		config string
		want   string
	}{
		{
			name:   "provider",
			config: `{"provider":{"protocol":"http","fetch_count":20}}`,
			want:   application.AdapterProvider,
		},
		{
			name:   "site",
			config: `{"site":{"site_id":"ip89","max_pages":1}}`,
			want:   application.AdapterSite,
		},
		{
			name:   "static",
			config: `{"static":{"url":"https://example.com/proxies.txt","default_transport":"http"}}`,
			want:   application.AdapterStatic,
		},
		{
			name:   "custom",
			config: `{"default_transport":"http","max_pool_leases":1}`,
			want:   application.AdapterCustom,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, derr := decodeSourceConfig(json.RawMessage(tc.config))
			if derr != nil {
				t.Fatalf("typed config rejected: %v", derr)
			}
			if got := inferAdapter(cfg); got != tc.want {
				t.Fatalf("inferred adapter=%s want=%s", got, tc.want)
			}
		})
	}
	for _, flat := range []string{
		`{"protocol":"http"}`,
		`{"site_id":"ip89"}`,
		`{"url":"https://example.com/proxies.txt"}`,
	} {
		if _, derr := decodeSourceConfig(json.RawMessage(flat)); derr == nil ||
			derr.Status != http.StatusBadRequest {
			t.Fatalf("flat compatibility config was accepted: %s => %+v", flat, derr)
		}
	}
}

func TestSourceConfigCannotCarryTwoAdapterShapes(t *testing.T) {
	// A dual-shape payload must be rejected before persistence; otherwise
	// adapter inference would silently pick one and leave the other dead.
	var body struct {
		Config json.RawMessage `json:"config"`
	}
	raw := `{"config":{"provider":{"protocol":"http"},"static":{"url":"https://example.com/list"}}}`
	if err := json.Unmarshal([]byte(raw), &body); err != nil {
		t.Fatal(err)
	}
	cfg, derr := decodeSourceConfig(body.Config)
	if derr != nil {
		t.Fatalf("decode: %v", derr)
	}
	if got := inferAdapter(cfg); got != application.AdapterProvider {
		t.Fatalf("fixture inference=%s", got)
	}
	if err := validateAdapterConfigShape(application.AdapterProvider, cfg); err == nil {
		t.Fatal("dual adapter shapes must be rejected")
	}
}

func TestDeleteSourceV1UsesIngestCleanup(t *testing.T) {
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "api-source-delete.db"), 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store := sqlite.New(db)
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	cfg := testConfig()
	ingest := application.NewIngestService(store, cfg.IdentitySecret, true)
	if _, derr := ingest.CreateSource(context.Background(), "delete-test",
		application.SourceConfig{}, true); derr != nil {
		t.Fatal(derr)
	}
	s := &Server{cfg: cfg, log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		ingest: ingest, store: store}
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/admin/sources/delete-test", nil)
	req.SetPathValue("id", "delete-test")
	rec := httptest.NewRecorder()
	s.handleDeleteSourceV1(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("delete failed: status=%d body=%s", rec.Code, rec.Body.String())
	}
	if _, err := store.GetSource(context.Background(), "delete-test"); err == nil {
		t.Fatal("source still exists after API delete")
	}
}
