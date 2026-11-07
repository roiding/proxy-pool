package application

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"proxypool/internal/domain"
	"proxypool/internal/storage/sqlite"
)

func newTestUploadSource(t *testing.T) (*IngestService, string) {
	t.Helper()
	svc := newTestIngest(t)
	if _, derr := svc.CreateSource(context.Background(), "imports",
		SourceConfig{MaxPoolLeases: 2}, true); derr != nil {
		t.Fatal(derr)
	}
	return svc, "imports"
}

func TestImportBatchTextAndJSONShareIdentity(t *testing.T) {
	svc, src := newTestUploadSource(t)
	ctx := context.Background()
	var derr *domain.Error
	_, derr = svc.ImportBatch(ctx, ImportRequest{
		SourceID:    src,
		UploaderID:  "admin",
		ContentType: "text/plain",
		Body:        []byte("http://user:p%40ss@203.0.113.10:8080\n"),
	})
	if derr != nil {
		t.Fatal(derr)
	}
	js, derr := svc.ImportBatch(ctx, ImportRequest{
		SourceID:    src,
		UploaderID:  "admin",
		ContentType: "application/json",
		Body: []byte(`{"items":[{"connection":{"transport":"http","host":"203.0.113.10",
			"port":8080,"username":"user","password":"p@ss"}}]}`),
	})
	if derr != nil {
		t.Fatal(derr)
	}
	if js.Added != 0 || js.Duplicate != 1 {
		t.Fatalf("JSON raw creds must dedupe against encoded text URI: %+v", js)
	}
	cands, err := svc.store.SelectCandidates(ctx, sqlite.SelectionFilters{
		Profile: "basic", Now: time.Now(), UsefulCutoff: time.Now().Add(-time.Minute), Limit: 10,
	})
	if err != nil || len(cands) != 1 {
		t.Fatalf("expected one deduped resource: %v %d", err, len(cands))
	}
	if got := cands[0].Resource.Conn; got.Username != "user" || got.Password != "p@ss" {
		t.Fatalf("encoded URI userinfo must decode to raw credentials: %+v", got)
	}
}

func TestImportBatchCountsAddedDuplicateUpdated(t *testing.T) {
	svc, src := newTestUploadSource(t)
	ctx := context.Background()
	body := []byte(`{"items":[
		{"connection":{"transport":"http","host":"203.0.113.11","port":8080}},
		{"connection":{"transport":"socks5","host":"203.0.113.12","port":1080}}]}`)
	first, derr := svc.ImportBatch(ctx, ImportRequest{
		SourceID: src, UploaderID: "admin", ContentType: "application/json", Body: body,
	})
	if derr != nil || first.Added != 2 {
		t.Fatalf("first import: %+v err=%v", first, derr)
	}
	same, derr := svc.ImportBatch(ctx, ImportRequest{
		SourceID: src, UploaderID: "admin", ContentType: "application/json", Body: body,
	})
	if derr != nil || same.Added != 0 || same.Updated != 0 || same.Duplicate != 2 {
		t.Fatalf("identical re-import must be duplicates: %+v err=%v", same, derr)
	}
	changed := []byte(`{"items":[
		{"connection":{"transport":"http","host":"203.0.113.11","port":8080},
		 "provider_expires_at":"2026-12-01T00:00:00Z"},
		{"connection":{"transport":"socks5","host":"203.0.113.12","port":1080}}]}`)
	merged, derr := svc.ImportBatch(ctx, ImportRequest{
		SourceID: src, UploaderID: "admin", ContentType: "application/json", Body: changed,
	})
	if derr != nil || merged.Added != 0 || merged.Updated != 1 || merged.Duplicate != 1 {
		t.Fatalf("expiry change must update only that offer: %+v err=%v", merged, derr)
	}
	cands, err := svc.store.SelectCandidates(ctx, sqlite.SelectionFilters{
		Profile: "basic", Now: time.Now(), UsefulCutoff: time.Now().Add(-time.Minute), Limit: 10,
	})
	if err != nil || len(cands) != 2 {
		t.Fatalf("expected two resources: %v %d", err, len(cands))
	}
	var sawExpiry bool
	for _, c := range cands {
		if c.Offer.ProviderExpiresAt == nil && c.Offer.Claims["uploaded_expires_at"] == "2026-12-01T00:00:00Z" {
			sawExpiry = true
		}
	}
	if !sawExpiry {
		t.Fatal("uploaded expiry metadata was not retained without an automatic deadline")
	}
}

func TestImportBatchOptionalIdempotencyKey(t *testing.T) {
	svc, src := newTestUploadSource(t)
	ctx := context.Background()
	body := []byte("http://203.0.113.20:8080\n")
	first, derr := svc.ImportBatch(ctx, ImportRequest{
		SourceID: src, UploaderID: "uk_first", ContentType: "text/plain", Body: body,
		IdempotencyKey: "batch-1",
	})
	if derr != nil || !first.Applied || first.Added != 1 {
		t.Fatalf("first keyed import: %+v err=%v", first, derr)
	}
	replay, derr := svc.ImportBatch(ctx, ImportRequest{
		SourceID: src, UploaderID: "uk_first", ContentType: "text/plain", Body: body,
		IdempotencyKey: "batch-1",
	})
	if derr != nil {
		t.Fatal(derr)
	}
	if replay.Applied {
		t.Fatal("same key and content must replay, not re-apply")
	}
	if replay.ImportID != first.ImportID || replay.Added != first.Added ||
		replay.Rejected != first.Rejected {
		t.Fatalf("replay must return the stored result: %+v vs %+v", replay, first)
	}
	conflict, derr := svc.ImportBatch(ctx, ImportRequest{
		SourceID: src, UploaderID: "uk_first", ContentType: "text/plain",
		Body: []byte("http://203.0.113.21:8080\n"), IdempotencyKey: "batch-1",
	})
	if derr == nil || derr.Code != "IDEMPOTENCY_KEY_CONFLICT" || derr.Status != 409 {
		t.Fatalf("same key with different content must conflict: %+v err=%v", conflict, derr)
	}
	otherSource, derr := svc.ImportBatch(ctx, ImportRequest{
		SourceID: src, UploaderID: "uk_second", ContentType: "text/plain", Body: body,
		IdempotencyKey: "batch-1",
	})
	if derr != nil || !otherSource.Applied {
		t.Fatalf("idempotency scope must include the uploader identity: %+v err=%v",
			otherSource, derr)
	}
	unkeyed, derr := svc.ImportBatch(ctx, ImportRequest{
		SourceID: src, UploaderID: "uk_second", ContentType: "text/plain", Body: body,
	})
	if derr != nil || !unkeyed.Applied || unkeyed.ImportID == "" {
		t.Fatalf("unkeyed imports must apply durably: %+v err=%v", unkeyed, derr)
	}
	if unkeyed.ImportID == otherSource.ImportID {
		t.Fatal("separate unkeyed batches must get distinct import ids")
	}
}

func TestImportBatchConcurrentSameKeyAppliesOnce(t *testing.T) {
	svc, src := newTestUploadSource(t)
	ctx := context.Background()
	body := []byte("http://203.0.113.30:8080\n")
	const workers = 8
	var wg sync.WaitGroup
	results := make(chan ImportBatchResult, workers)
	errs := make(chan *domain.Error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// A busy store answers 503; the documented retry contract is
			// to resend the same batch with the same Idempotency-Key.
			for attempt := 0; attempt < 20; attempt++ {
				res, derr := svc.ImportBatch(ctx, ImportRequest{
					SourceID: src, UploaderID: "uk_race", ContentType: "text/plain", Body: body,
					IdempotencyKey: "race-1",
				})
				if derr == nil {
					results <- res
					return
				}
				if derr.Code == "IMPORT_STORE_BUSY" {
					time.Sleep(50 * time.Millisecond)
					continue
				}
				errs <- derr
				return
			}
			errs <- domain.E("IMPORT_STORE_BUSY", 503, "retries exhausted")
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	applied := 0
	ids := map[string]bool{}
	for res := range results {
		ids[res.ImportID] = true
		if res.Applied {
			applied++
		}
	}
	for derr := range errs {
		t.Fatalf("concurrent keyed retries must not fail: %v", derr)
	}
	if applied != 1 || len(ids) != 1 {
		t.Fatalf("exactly one apply expected: applied=%d ids=%v", applied, ids)
	}
	stats, err := svc.store.Stats(ctx)
	if err != nil || stats.Resources != 1 || stats.EnabledOffers != 1 {
		t.Fatalf("inventory must hold exactly one resource: stats=%+v err=%v", stats, err)
	}
}

func TestImportBatchStructuralErrorsRejectWholeBatch(t *testing.T) {
	svc, src := newTestUploadSource(t)
	ctx := context.Background()
	cases := []struct {
		name        string
		contentType string
		body        string
	}{
		{"bad json", "application/json", `{"items": [`},
		{"missing items", "application/json", `{"proxies": []}`},
		{"unknown top field", "application/json", `{"items": [], "extra": 1}`},
		{"secret unknown top field", "application/json", `{"audit-private-marker":true,"items":[]}`},
		{"malformed media type", "application/json; charset", `{"items":[]}`},
		{"trailing garbage", "application/json", `{"items": []} garbage`},
		{"second json value", "application/json", `{"items": []} {"items": []}`},
		{"empty text", "text/plain", "   \n  "},
	}
	for _, tc := range cases {
		res, derr := svc.ImportBatch(ctx, ImportRequest{
			SourceID: src, UploaderID: "admin", ContentType: tc.contentType,
			Body: []byte(tc.body), IdempotencyKey: "structural-" + tc.name,
		})
		if derr == nil || derr.Status != 400 {
			t.Fatalf("%s must be a 400 structural error: %+v err=%v", tc.name, res, derr)
		}
		if strings.Contains(derr.Message, "audit-private-marker") {
			t.Fatalf("%s echoed attacker-supplied field names: %v", tc.name, derr)
		}
		stats, err := svc.store.Stats(ctx)
		if err != nil || stats.Resources != 0 || stats.EnabledOffers != 0 {
			t.Fatalf("%s must not change inventory: stats=%+v err=%v", tc.name, stats, err)
		}
		if _, derr := svc.GetImportBatch(ctx, "admin", src,
			"imp_doesnotexist"); derr == nil || derr.Status != 404 {
			t.Fatalf("rejected batch must not be recorded: err=%v", derr)
		}
	}
}

func TestImportBatchTextCredentialBoundsAndRedaction(t *testing.T) {
	svc, src := newTestUploadSource(t)
	res, derr := svc.ImportBatch(context.Background(), ImportRequest{
		SourceID: src, UploaderID: "admin", ContentType: "text/plain",
		Body: []byte("http://audit-user:audit-password-%ZZ@203.0.113.84:8080\n" +
			"http://audit-user:" + strings.Repeat("p", maxImportCredentialLen+1) + "@203.0.113.85:8080\n"),
	})
	if derr != nil {
		t.Fatal(derr)
	}
	if res.Added != 0 || res.Rejected != 2 || len(res.Errors) != 2 {
		t.Fatalf("text credential bounds/redaction result: %+v", res)
	}
	wire, _ := json.Marshal(res)
	for _, secret := range []string{"audit-user", "audit-password", strings.Repeat("p", 100)} {
		if strings.Contains(string(wire), secret) {
			t.Fatalf("durable text error exposed credential material")
		}
	}
}

func TestImportBatchTextItemLimitIsStructural(t *testing.T) {
	svc, src := newTestUploadSource(t)
	ctx := context.Background()
	lines := make([]string, MaxImportItems+1)
	for i := range lines {
		lines[i] = fmt.Sprintf("http://203.0.113.%d:8080", i%250+1)
	}
	body := []byte(strings.Join(lines, "\n"))
	res, derr := svc.ImportBatch(ctx, ImportRequest{
		SourceID: src, UploaderID: "admin", ContentType: "text/plain", Body: body,
	})
	if derr == nil || derr.Status != 413 || derr.Code != "IMPORT_TOO_MANY_ITEMS" {
		t.Fatalf("over-limit text batch must be a 413 structural reject: %+v err=%v", res, derr)
	}
	stats, err := svc.store.Stats(ctx)
	if err != nil || stats.Resources != 0 {
		t.Fatalf("over-limit text batch must not change inventory: %+v err=%v", stats, err)
	}
}

func TestImportBatchCredentialBoundsAndErrorSanitization(t *testing.T) {
	svc, src := newTestUploadSource(t)
	ctx := context.Background()
	longPassword := strings.Repeat("x", 257)
	longUsername := strings.Repeat("u", 257)
	body := []byte(`{"items":[
		{"connection":{"transport":"http","host":"203.0.113.41","port":8080,
		  "username":"u","password":"short"}},
		{"connection":{"transport":"http","host":"203.0.113.42","port":8080,
		  "username":"u","password":"` + longPassword + `"}},
		{"connection":{"transport":"http","host":"203.0.113.43","port":8080,
		  "username":"` + longUsername + `","password":"p"}},
		{"connection":{"transport":"\u0009ftp\u000a","host":"203.0.113.44","port":8080}},
		{"connection":{"transport":"http","host":"203.0.113.45","port":8080},
		 "provider_expires_at":"not-a-time\t"}]}`)
	res, derr := svc.ImportBatch(ctx, ImportRequest{
		SourceID: src, UploaderID: "admin", ContentType: "application/json", Body: body,
	})
	if derr != nil {
		t.Fatal(derr)
	}
	if res.Added != 1 || res.Rejected != 4 {
		t.Fatalf("bounds must reject items without blocking legal ones: %+v", res)
	}
	wire, _ := json.Marshal(res)
	if strings.Contains(string(wire), strings.Repeat("x", 257)) ||
		strings.Contains(string(wire), strings.Repeat("u", 257)) {
		t.Fatal("error output echoed an unbounded credential value")
	}
	for _, errItem := range res.Errors {
		if strings.ContainsAny(errItem.Error, "\t\n\r") || strings.Contains(errItem.Error, "ftp") ||
			strings.Contains(errItem.Error, "not-a-time") {
			t.Fatalf("error text echoed raw input: %q", errItem.Error)
		}
	}
}

func TestImportBatchIdempotencyExpiresWithRetention(t *testing.T) {
	svc, src := newTestUploadSource(t)
	ctx := context.Background()
	base := time.Now()
	svc.Now = func() time.Time { return base }
	body := []byte("http://203.0.113.46:8080\n")
	first, derr := svc.ImportBatch(ctx, ImportRequest{
		SourceID: src, UploaderID: "uk_age", ContentType: "text/plain", Body: body,
		IdempotencyKey: "aging",
	})
	if derr != nil || !first.Applied {
		t.Fatalf("first keyed import: %+v err=%v", first, derr)
	}
	svc.Now = func() time.Time { return base.Add(8 * 24 * time.Hour) }
	replay, derr := svc.ImportBatch(ctx, ImportRequest{
		SourceID: src, UploaderID: "uk_age", ContentType: "text/plain", Body: body,
		IdempotencyKey: "aging",
	})
	if derr != nil {
		t.Fatal(derr)
	}
	if !replay.Applied || replay.ImportID == first.ImportID {
		t.Fatalf("beyond the retention window the key must allow a fresh batch: %+v", replay)
	}
	if replay.Added != 0 || replay.Duplicate != 1 {
		t.Fatalf("re-applied content is a duplicate of live inventory: %+v", replay)
	}
	if _, derr := svc.GetImportBatch(ctx, "uk_age", src, first.ImportID); derr == nil ||
		derr.Status != 404 {
		t.Fatalf("expired batch result must read as gone: err=%v", derr)
	}
	got, derr := svc.GetImportBatch(ctx, "uk_age", src, replay.ImportID)
	if derr != nil || got.ImportID != replay.ImportID {
		t.Fatalf("fresh result must be readable: %+v err=%v", got, derr)
	}
}

func TestImportBatchItemErrorsAreBoundedAndSanitized(t *testing.T) {
	svc, src := newTestUploadSource(t)
	ctx := context.Background()
	var items []string
	items = append(items, `{"connection":{"transport":"http","host":"203.0.113.40","port":8080}}`)
	secret := "secretpw"
	for i := 0; i < sqlite.MaxImportStoredErrors+40; i++ {
		items = append(items, fmt.Sprintf(
			`{"connection":{"transport":"ftp","host":"203.0.113.%d","port":1,
			  "username":"u","password":%q}}`, i%250+1, secret))
	}
	body := []byte(`{"items":[` + strings.Join(items, ",") + `]}`)
	res, derr := svc.ImportBatch(ctx, ImportRequest{
		SourceID: src, UploaderID: "admin", ContentType: "application/json", Body: body,
	})
	if derr != nil {
		t.Fatal(derr)
	}
	if res.Added != 1 || res.Rejected != sqlite.MaxImportStoredErrors+40 {
		t.Fatalf("legal items must append and rejected count must be complete: %+v", res)
	}
	if len(res.Errors) != sqlite.MaxImportStoredErrors {
		t.Fatalf("error array must be bounded: %d", len(res.Errors))
	}
	wire, _ := json.Marshal(res)
	if strings.Contains(string(wire), secret) {
		t.Fatal("error output leaked the password")
	}
	if res.Rejected != 140 {
		t.Fatalf("unexpected rejected count: %d", res.Rejected)
	}
}

func TestImportBatchIPv6NamesAndExpiry(t *testing.T) {
	svc, src := newTestUploadSource(t)
	ctx := context.Background()
	body := []byte(`{"items":[
		{"connection":{"transport":"socks5","host":"2001:db8::1","port":1080},
		 "provider_expires_at":"2026-12-31T23:59:59Z"},
		{"connection":{"transport":"http","host":"proxy.example.net","port":8080},
		 "provider_expires_at":null},
		{"connection":{"transport":"http","host":"203.0.113.50","port":8080},
		 "provider_expires_at":"not-a-time"}]}`)
	res, derr := svc.ImportBatch(ctx, ImportRequest{
		SourceID: src, UploaderID: "admin", ContentType: "application/json", Body: body,
	})
	if derr != nil {
		t.Fatal(derr)
	}
	if res.Added != 2 || res.Rejected != 1 || len(res.Errors) != 1 {
		t.Fatalf("mixed batch result wrong: %+v", res)
	}
	cands, err := svc.store.SelectCandidates(ctx, sqlite.SelectionFilters{
		Profile: "basic", Now: time.Now(), UsefulCutoff: time.Now().Add(-time.Minute), Limit: 10,
	})
	if err != nil || len(cands) != 2 {
		t.Fatalf("expected two resources: %v %d", err, len(cands))
	}
	var v6, named bool
	for _, c := range cands {
		switch c.Resource.Conn.Host {
		case "2001:db8::1":
			v6 = true
		case "proxy.example.net":
			named = true
		}
	}
	if !v6 || !named {
		t.Fatalf("IPv6 and name hosts must both import: %+v", cands)
	}
}

func TestImportBatchPreservesHealthIncarnationAndCDOnDuplicates(t *testing.T) {
	svc, src := newTestUploadSource(t)
	ctx := context.Background()
	body := []byte("http://203.0.113.60:8080\n")
	first, derr := svc.ImportBatch(ctx, ImportRequest{
		SourceID: src, UploaderID: "admin", ContentType: "text/plain", Body: body,
	})
	if derr != nil || first.Added != 1 {
		t.Fatalf("first import: %+v err=%v", first, derr)
	}
	cands, err := svc.store.SelectCandidates(ctx, sqlite.SelectionFilters{
		Profile: "basic", Now: time.Now(), UsefulCutoff: time.Now().Add(-time.Minute), Limit: 1,
	})
	if err != nil || len(cands) != 1 {
		t.Fatalf("select candidate: %v", err)
	}
	cand := cands[0]
	now := time.Now()
	if err := svc.store.UpdateHealth(ctx, sqlite.HealthUpdate{
		ResourceID: cand.Resource.ID,
		ContextKey: cand.Resource.CredentialVersion + "|" + string(cand.Resource.Session),
		Profile:    "basic",
		Outcome:    "PASS",
		Now:        now,
	}); err != nil {
		t.Fatal(err)
	}
	replay, derr := svc.ImportBatch(ctx, ImportRequest{
		SourceID: src, UploaderID: "admin", ContentType: "text/plain", Body: body,
	})
	if derr != nil || replay.Duplicate != 1 || replay.Added != 0 {
		t.Fatalf("duplicate import: %+v err=%v", replay, derr)
	}
	after, err := svc.store.SelectCandidates(ctx, sqlite.SelectionFilters{
		Profile: "basic", Now: time.Now(), UsefulCutoff: time.Now().Add(-time.Minute), Limit: 1,
	})
	if err != nil || len(after) != 1 {
		t.Fatalf("reselect: %v", err)
	}
	if after[0].Incarnation != cand.Incarnation {
		t.Fatal("duplicate import must not recreate the resource incarnation")
	}
	if after[0].LastOutcome != "PASS" {
		t.Fatalf("duplicate import must preserve health: %+v", after[0])
	}
	if got := after[0].Resource.Conn; got.Username != "" || got.Password != "" {
		t.Fatalf("no credentials expected: %+v", got)
	}
}

func TestImportBatchResultScopedToUploader(t *testing.T) {
	svc, src := newTestUploadSource(t)
	ctx := context.Background()
	res, derr := svc.ImportBatch(ctx, ImportRequest{
		SourceID: src, UploaderID: "uk_owner", ContentType: "text/plain",
		Body: []byte("http://203.0.113.70:8080\n"),
	})
	if derr != nil {
		t.Fatal(derr)
	}
	if _, derr := svc.GetImportBatch(ctx, "uk_other", src, res.ImportID); derr == nil ||
		derr.Status != 404 {
		t.Fatalf("another uploader must not see this batch: err=%v", derr)
	}
	got, derr := svc.GetImportBatch(ctx, "uk_owner", src, res.ImportID)
	if derr != nil || got.ImportID != res.ImportID {
		t.Fatalf("owner read: %+v err=%v", got, derr)
	}
	admin, derr := svc.GetImportBatch(ctx, "", src, res.ImportID)
	if derr != nil || admin.ImportID != res.ImportID {
		t.Fatalf("admin read: %+v err=%v", admin, derr)
	}
}

func TestImportBatchGuards(t *testing.T) {
	svc, src := newTestUploadSource(t)
	ctx := context.Background()
	if _, derr := svc.ImportBatch(ctx, ImportRequest{
		SourceID: src, ContentType: "text/plain", Body: []byte("http://203.0.113.80:8080\n"),
	}); derr == nil || derr.Status != 400 {
		t.Fatal("missing uploader identity must be rejected")
	}
	if _, derr := svc.ImportBatch(ctx, ImportRequest{
		SourceID: src, UploaderID: "admin", ContentType: "text/xml",
		Body: []byte("<x/>"),
	}); derr == nil || derr.Status != 400 {
		t.Fatal("unsupported content type must be rejected")
	}
	if _, derr := svc.ImportBatch(ctx, ImportRequest{
		SourceID: src, UploaderID: "admin", ContentType: "text/plain",
		Body: []byte("http://203.0.113.81:8080\n"), IdempotencyKey: strings.Repeat("k", 129),
	}); derr == nil || derr.Status != 400 {
		t.Fatal("overlong idempotency key must be rejected")
	}
	if _, derr := svc.ImportBatch(ctx, ImportRequest{
		SourceID: "nope", UploaderID: "admin", ContentType: "text/plain",
		Body: []byte("http://203.0.113.82:8080\n"),
	}); derr == nil || derr.Status != 404 {
		t.Fatal("unknown source must 404")
	}
	// A provider channel must never accept persistent imports.
	if _, derr := svc.CreateSource(ctx, "prov-import",
		SourceConfig{Provider: &ProviderConfig{Protocol: "http"}}, true); derr != nil {
		t.Fatal(derr)
	}
	if _, derr := svc.ImportBatch(ctx, ImportRequest{
		SourceID: "prov-import", UploaderID: "admin", ContentType: "text/plain",
		Body: []byte("http://203.0.113.83:8080\n"),
	}); derr == nil || derr.Status != 400 {
		t.Fatal("non-upload channels must reject imports")
	}
}

func TestImportBatchOverLimitItemsRejected(t *testing.T) {
	svc, src := newTestUploadSource(t)
	ctx := context.Background()
	items := make([]string, MaxImportItems+1)
	for i := range items {
		items[i] = fmt.Sprintf(`{"connection":{"transport":"http","host":"203.0.113.%d","port":8080}}`,
			i%250+1)
	}
	body := []byte(`{"items":[` + strings.Join(items, ",") + `]}`)
	res, derr := svc.ImportBatch(ctx, ImportRequest{
		SourceID: src, UploaderID: "admin", ContentType: "application/json", Body: body,
	})
	if derr == nil || derr.Status != 413 || derr.Code != "IMPORT_TOO_MANY_ITEMS" {
		t.Fatalf("over-limit batch must be a 413 structural reject: %+v err=%v", res, derr)
	}
	stats, err := svc.store.Stats(ctx)
	if err != nil || stats.Resources != 0 {
		t.Fatalf("over-limit batch must not change inventory: %+v err=%v", stats, err)
	}
}
