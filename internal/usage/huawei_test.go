package usage

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"
)

const (
	fixtureDomain   = "f86ab3d6d3d948e6a6180b568d7d0c6c"
	fixtureInstance = "6ad8ba01a83ac34d79fbe3a2"
	fixturePackage  = "564b02f8-dfe9-4ddd-9579-9cad92f5921e"
)

type huaweiFixtureTransport func(*http.Request) (*http.Response, error)

func (f huaweiFixtureTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func huaweiFixtureResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}
}

func TestHuaweiSigningReferenceVector(t *testing.T) {
	body := []byte(`{"free_resource_ids":["fixture"]}`)
	req, err := http.NewRequest("POST", "https://bss.myhuaweicloud.com/v2/payments/free-resources/usages/details/query", strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	signHuaweiRequest(req, body, "reference-access", "reference-secret", time.Date(2026, 11, 8, 6, 0, 0, 0, time.UTC))
	want := "SDK-HMAC-SHA256 Access=reference-access, SignedHeaders=host;x-sdk-date, Signature=0247e3de385794fefb2e6d56cb830a3d48b58a6651248fea0c55ac42fe8d72c5"
	if req.Header.Get("Authorization") != want {
		t.Fatal("signature disagrees with independently calculated Huawei canonical-request vector")
	}
}

func TestHuaweiReaderUsesCurrentSettingsAndKeepsLastSnapshotOnFailure(t *testing.T) {
	now := time.Date(2026, 11, 8, 6, 0, 0, 0, time.UTC)
	svc, err := New(testUsageDB(t), Options{
		MasterKey: "0123456789abcdef0123456789abcdef", Now: func() time.Time { return now },
		MonthlyTotalGB: 1024, ThresholdPercent: 80, CheckTime: "09:00", Timezone: "Asia/Shanghai",
	})
	if err != nil {
		t.Fatal(err)
	}
	var patch SettingsUpdate
	if err := json.Unmarshal([]byte(`{"huawei_access_key_id":"fixture-ak","huawei_secret_access_key":"fixture-sk","huawei_region":"ap-southeast-3","huawei_resource_id":"`+fixtureInstance+`"}`), &patch); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.UpdateSettings(context.Background(), patch); err != nil {
		t.Fatal(err)
	}
	reader := svc.reader.(*huaweiReader)
	deny := false
	reader.client.Transport = huaweiFixtureTransport(func(req *http.Request) (*http.Response, error) {
		if !strings.HasPrefix(req.Header.Get("Authorization"), "SDK-HMAC-SHA256 Access=fixture-ak,") {
			t.Fatal("request did not use saved credentials")
		}
		if req.URL.Host == "iam.myhuaweicloud.com" {
			if req.Method != "GET" || req.URL.Query().Get("name") != "ap-southeast-3" {
				t.Fatal("incorrect region lookup")
			}
			return huaweiFixtureResponse(200, `{"projects":[{"name":"ap-southeast-3","parent_id":"`+fixtureDomain+`"}]}`), nil
		}
		if req.URL.Host == "rms.myhuaweicloud.com" {
			if req.Method != "GET" || req.URL.Query().Get("type") != "hcss.l-instance" ||
				req.URL.Query().Get("region_id") != "ap-southeast-3" || req.URL.Query().Get("id") != fixtureInstance {
				t.Fatal("incorrect RMS resource query")
			}
			return huaweiFixtureResponse(200, `{"resources":[{"id":"`+fixtureInstance+`","region_id":"ap-southeast-3","state":"Normal","properties":{"resources":[{"logical_resource_type":"huaweicloudinternal_cbc_freeresource","physical_resource_id":"`+fixturePackage+`"}]}}],"page_info":{"next_marker":null}}`), nil
		}
		if req.URL.Host != "bss.myhuaweicloud.com" || req.Method != "POST" ||
			req.URL.Path != "/v2/payments/free-resources/usages/details/query" {
			t.Fatalf("unexpected cloud operation: %s %s", req.Method, req.URL)
		}
		var body struct {
			IDs []string `json:"free_resource_ids"`
		}
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if len(body.IDs) != 1 || body.IDs[0] != fixturePackage {
			t.Fatal("incorrect package ID")
		}
		if deny {
			return huaweiFixtureResponse(403, `{"error_code":"CBC.0155","auth_action":"bss:bill:view","error_msg":"fixture-sk"}`), nil
		}
		return huaweiFixtureResponse(200, `{"free_resources":[
			{"free_resource_id":"`+fixturePackage+`","start_time":"2026-10-21T12:00:00Z","end_time":"2026-11-21T16:00:00Z","amount":1023.543,"original_amount":1024,"measure_id":10}
		]}`), nil
	})
	ctx := context.Background()
	if err := svc.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	status, err := svc.Status(ctx)
	if err != nil || status.ReaderStatus != "active" || status.Stale || status.Snapshot == nil ||
		math.Abs(status.Snapshot.UsedGB-0.457) > 1e-9 || status.Snapshot.TotalGB != 1024 {
		t.Fatalf("unexpected snapshot: %+v, %v", status, err)
	}
	if status.Snapshot.PeriodStart != "2026-10-21T12:00:00Z" {
		t.Fatal("used calendar month instead of actual package period")
	}
	deny = true
	if err := svc.RunOnce(ctx); err == nil || !strings.Contains(err.Error(), "bss:bill:view") {
		t.Fatalf("missing actionable permission error: %v", err)
	}
	status, err = svc.Status(ctx)
	if err != nil || !status.Stale || status.ReaderStatus != "error" || status.Snapshot == nil ||
		math.Abs(status.Snapshot.UsedGB-0.457) > 1e-9 || strings.Contains(status.LastError, "fixture-sk") {
		t.Fatalf("failed read lost data or leaked secret: %+v, %v", status, err)
	}
}

func TestHuaweiPackageRejectsMissingAmbiguousAndInvalidData(t *testing.T) {
	now := time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC)
	valid := `{"free_resource_id":"` + fixturePackage + `","start_time":"2026-10-21T12:00:00Z","end_time":"2026-11-21T16:00:00Z","amount":"1023.5","original_amount":"1024","measure_id":10}`
	cases := map[string]string{
		"empty":           `{"free_resources":[]}`,
		"ambiguous":       `{"free_resources":[` + valid + `,` + valid + `]}`,
		"missing amount":  `{"free_resources":[` + strings.Replace(valid, `"amount":"1023.5",`, "", 1) + `]}`,
		"negative amount": `{"free_resources":[` + strings.Replace(valid, "1023.5", "-1", 1) + `]}`,
		"missing unit":    `{"free_resources":[` + strings.Replace(valid, `"measure_id":10`, `"measure_id":null`, 1) + `]}`,
		"wrong unit":      `{"free_resources":[` + strings.Replace(valid, `"measure_id":10`, `"measure_id":13`, 1) + `]}`,
		"bad quota":       `{"free_resources":[` + strings.Replace(valid, "1023.5", "1025", 1) + `]}`,
		"wrong resource":  `{"free_resources":[` + strings.Replace(valid, fixturePackage, "another", 1) + `]}`,
		"expired":         `{"free_resources":[` + strings.Replace(valid, "2026-11-21", "2026-10-22", 1) + `]}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			var packages huaweiPackages
			if err := json.Unmarshal([]byte(body), &packages); err != nil {
				t.Fatal(err)
			}
			if _, err := packages.snapshot(fixturePackage, now); err == nil {
				t.Fatal("accepted invalid quota")
			}
		})
	}
	var packages huaweiPackages
	_ = json.Unmarshal([]byte(`{"free_resources":[`+valid+`]}`), &packages)
	if _, err := packages.snapshot(fixturePackage, now); err != nil {
		t.Fatal(err)
	}
}

func TestHuaweiReaderUsesConfiguredPackageWithoutRMS(t *testing.T) {
	now := time.Date(2026, 11, 8, 6, 0, 0, 0, time.UTC)
	r := &huaweiReader{
		load: func(context.Context) (settings, error) {
			return settings{
				HuaweiAK: "fixture-ak", HuaweiSK: "fixture-sk",
				HuaweiRegion: "ap-southeast-3", HuaweiPackage: fixturePackage,
			}, nil
		},
		now: func() time.Time { return now },
	}
	r.client = &http.Client{Transport: huaweiFixtureTransport(func(req *http.Request) (*http.Response, error) {
		if req.URL.Host != "bss.myhuaweicloud.com" {
			t.Fatalf("direct package ID unexpectedly queried %s", req.URL.Host)
		}
		return huaweiFixtureResponse(200, `{"free_resources":[
			{"free_resource_id":"`+fixturePackage+`","start_time":"2026-10-21T12:00:00Z","end_time":"2026-11-21T16:00:00Z","amount":1023.543,"original_amount":1024,"measure_id":10}
		]}`), nil
	})}
	snapshot, err := r.ReadMonthlyUsage(context.Background())
	if err != nil || snapshot.RemainingGB == nil || *snapshot.RemainingGB != 1023.543 ||
		!strings.Contains(snapshot.Note, fixturePackage) {
		t.Fatalf("unexpected direct package result: %+v, %v", snapshot, err)
	}
}

func TestHuaweiPackageDiscoveryRejectsWrongRegionAndMultipleResources(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"wrong region", `{"resources":[{"id":"` + fixtureInstance + `","region_id":"cn-north-4","state":"Normal","properties":{"resources":[{"logical_resource_type":"huaweicloudinternal_cbc_freeresource","physical_resource_id":"` + fixturePackage + `"}]}}]}`},
		{"multiple resources", `{"resources":[{"id":"` + fixtureInstance + `","region_id":"ap-southeast-3","state":"Normal","properties":{"resources":[{"logical_resource_type":"huaweicloudinternal_cbc_freeresource","physical_resource_id":"` + fixturePackage + `"},{"logical_resource_type":"huaweicloudinternal_cbc_freeresource","physical_resource_id":"other"}]}}],"page_info":{}}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &huaweiReader{now: time.Now, client: &http.Client{Transport: huaweiFixtureTransport(func(req *http.Request) (*http.Response, error) {
				if req.URL.Host == "iam.myhuaweicloud.com" {
					return huaweiFixtureResponse(200, `{"projects":[{"name":"ap-southeast-3","parent_id":"`+fixtureDomain+`"}]}`), nil
				}
				return huaweiFixtureResponse(200, tc.body), nil
			})}}
			st := settings{HuaweiAK: "ak", HuaweiSK: "sk", HuaweiRegion: "ap-southeast-3", HuaweiResource: fixtureInstance}
			if id, err := r.discoverPackage(context.Background(), st); err == nil || id != "" {
				t.Fatalf("accepted unsafe discovery result: %q, %v", id, err)
			}
		})
	}
}

func TestHuaweiErrorsNeverEchoCloudResponseSecrets(t *testing.T) {
	r := &huaweiReader{now: time.Now, client: &http.Client{Transport: huaweiFixtureTransport(func(*http.Request) (*http.Response, error) {
		return huaweiFixtureResponse(403, `{"error_code":"fixture-secret","error_msg":"fixture-ak fixture-secret"}`), nil
	})}}
	err := r.request(context.Background(), settings{HuaweiAK: "fixture-ak", HuaweiSK: "fixture-secret"}, "https://iam.myhuaweicloud.com/v3/projects", nil, &struct{}{})
	if err == nil || strings.Contains(err.Error(), "fixture-secret") || strings.Contains(err.Error(), "fixture-ak") {
		t.Fatalf("unsafe cloud error: %v", err)
	}
}

func TestThresholdUsesCloudQuotaAndActualCycleForDeduplication(t *testing.T) {
	now := time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC)
	svc, err := New(testUsageDB(t), Options{MonthlyTotalGB: 1024, ThresholdPercent: 80, CheckTime: "09:00", Timezone: "UTC", Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	snap := Snapshot{PeriodStart: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC), PeriodEnd: time.Date(2026, 11, 21, 16, 0, 0, 0, time.UTC), UsedGB: 800, TotalGB: 1000, Source: "fixture"}
	if err := svc.evaluateThreshold(ctx, now, snap); err != nil {
		t.Fatal(err)
	}
	// The same non-calendar cycle crosses a month boundary without rearming.
	if err := svc.evaluateThreshold(ctx, now.AddDate(0, 1, 0), snap); err != nil {
		t.Fatal(err)
	}
	var count int
	_ = svc.db.QueryRow(`SELECT COUNT(*) FROM usage_notifications`).Scan(&count)
	if count != 1 {
		t.Fatalf("cloud quota or cycle dedup ignored: %d", count)
	}
	// A new cycle in the same calendar month must rearm.
	snap.PeriodStart = snap.PeriodStart.Add(24 * time.Hour)
	if err := svc.evaluateThreshold(ctx, now, snap); err != nil {
		t.Fatal(err)
	}
	_ = svc.db.QueryRow(`SELECT COUNT(*) FROM usage_notifications`).Scan(&count)
	if count != 2 {
		t.Fatalf("new cycle did not rearm: %d", count)
	}
}

func TestDailyCheckSuccessExactlyAtSlotDoesNotRepeat(t *testing.T) {
	now := time.Date(2026, 11, 8, 9, 0, 0, 0, time.UTC)
	svc, err := New(testUsageDB(t), Options{CheckTime: "09:00", Timezone: "UTC", Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := svc.recordStatusAttempt(ctx, now); err != nil {
		t.Fatal(err)
	}
	if err := svc.recordStatusSuccess(ctx, now); err != nil {
		t.Fatal(err)
	}
	clock, _ := time.Parse("15:04", "09:00")
	if next := svc.nextScheduledAfterLastSuccess(ctx, time.UTC, clock); !next.Equal(now.AddDate(0, 0, 1)) {
		t.Fatalf("daily run repeated: %v", next)
	}
}

func TestThresholdDeduplicationSupportsPackageOnlyConfiguration(t *testing.T) {
	now := time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC)
	svc, err := New(testUsageDB(t), Options{
		MasterKey:      "0123456789abcdef0123456789abcdef",
		MonthlyTotalGB: 1024, ThresholdPercent: 80, CheckTime: "09:00", Timezone: "UTC",
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	var patch SettingsUpdate
	if err := json.Unmarshal([]byte(`{"huawei_access_key_id":"fixture-ak","huawei_secret_access_key":"fixture-sk","huawei_region":"ap-southeast-3","huawei_package_id":"`+fixturePackage+`"}`), &patch); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.UpdateSettings(context.Background(), patch); err != nil {
		t.Fatal(err)
	}
	snapshot := Snapshot{
		PeriodStart: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC),
		PeriodEnd:   time.Date(2026, 11, 21, 16, 0, 0, 0, time.UTC),
		UsedGB:      800, TotalGB: 1000, Source: "fixture",
	}
	if err := svc.evaluateThreshold(context.Background(), now, snapshot); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := svc.db.QueryRow(`SELECT COUNT(*) FROM usage_notifications`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("package-only threshold was not recorded once: count=%d err=%v", count, err)
	}
}
