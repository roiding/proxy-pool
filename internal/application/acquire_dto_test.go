package application

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"proxypool/internal/domain"
	"proxypool/internal/validation"
)

func TestProxyURIFromEncodesCredentialsAndIPv6(t *testing.T) {
	cases := []struct {
		name string
		conn domain.Connection
		want string
	}{
		{name: "anonymous", conn: domain.Connection{Transport: domain.TransportHTTP,
			Host: "203.0.113.10", Port: 8080},
			want: "http://203.0.113.10:8080"},
		{name: "reserved characters", conn: domain.Connection{Transport: domain.TransportHTTP,
			Host: "203.0.113.10", Port: 8080, Username: "u ser:@/%", Password: "p@ss:/% ?#"},
			want: "http://u%20ser%3A%40%2F%25:p%40ss%3A%2F%25%20%3F%23@203.0.113.10:8080"},
		{name: "empty password stays explicit", conn: domain.Connection{Transport: domain.TransportSOCKS5,
			Host: "203.0.113.11", Port: 1080, Username: "user"},
			want: "socks5://user:@203.0.113.11:1080"},
		{name: "ipv6 host is bracketed", conn: domain.Connection{Transport: domain.TransportHTTP,
			Host: "2001:db8::1", Port: 8080, Username: "user", Password: "p"},
			want: "http://user:p@[2001:db8::1]:8080"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := proxyURIFrom(tc.conn); got != tc.want {
				t.Fatalf("proxy_uri mismatch: got=%s want=%s", got, tc.want)
			}
		})
	}
}

func TestDeliveryEvidenceIsUTCAndIndependentOfLease(t *testing.T) {
	completed := time.Date(2026, 11, 2, 10, 0, 0, 0, time.FixedZone("TEST", 8*3600))
	ev := deliveryEvidence(domain.Evidence{CompletedAt: completed}, 30*time.Second)
	if ev.CompletedAt.Location() != time.UTC {
		t.Fatalf("completed_at must be delivered as UTC: %s", ev.CompletedAt)
	}
	if ev.ValidUntil == nil || !ev.ValidUntil.Equal(time.Date(2026, 11, 2, 2, 0, 30, 0, time.UTC)) {
		t.Fatalf("valid_until must be completed_at plus the proof age: %v", ev.ValidUntil)
	}
	raw, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	var wire struct {
		CompletedAt string `json:"completed_at"`
		ValidUntil  string `json:"valid_until"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	if wire.CompletedAt != "2026-11-02T02:00:00Z" || wire.ValidUntil != "2026-11-02T02:00:30Z" {
		t.Fatalf("wire timestamps must be UTC RFC3339: %+v", wire)
	}
}

func TestConnectionJSONKeepsExplicitEmptyPassword(t *testing.T) {
	authenticated, err := json.Marshal(domain.Connection{Transport: domain.TransportHTTP,
		Host: "203.0.113.10", Port: 8080, Username: "user"})
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(authenticated, &fields); err != nil {
		t.Fatal(err)
	}
	if _, present := fields["password"]; !present {
		t.Fatalf("authenticated connection must show its explicit empty password: %s", authenticated)
	}
	anonymous, err := json.Marshal(domain.Connection{Transport: domain.TransportHTTP,
		Host: "203.0.113.10", Port: 8080})
	if err != nil {
		t.Fatal(err)
	}
	anonymousFields := map[string]json.RawMessage{}
	if err := json.Unmarshal(anonymous, &anonymousFields); err != nil {
		t.Fatal(err)
	}
	if _, present := anonymousFields["username"]; present {
		t.Fatalf("anonymous connection must not invent credentials: %s", anonymous)
	}
	var back domain.Connection
	if err := json.Unmarshal(authenticated, &back); err != nil {
		t.Fatal(err)
	}
	if back.Username != "user" || back.Password != "" || back.Port != 8080 {
		t.Fatalf("connection round-trip lost data: %+v", back)
	}
}

func TestAcquireShortfallReasonsSeparateInventoryBudgetAndValidation(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*acquireRun)
		want  string
	}{
		{
			name: "empty inventory",
			want: "NO_ELIGIBLE_CANDIDATES",
		},
		{
			name: "exhausted inventory",
			setup: func(run *acquireRun) {
				run.sawCandidate.Store(true)
			},
			want: "NO_ELIGIBLE_CANDIDATES",
		},
		{
			name: "capacity shortage",
			setup: func(run *acquireRun) {
				run.sawCandidate.Store(true)
				run.capacitySkip.Add(1)
			},
			want: "NO_ELIGIBLE_CANDIDATES",
		},
		{
			name: "request budget",
			setup: func(run *acquireRun) {
				run.sawCandidate.Store(true)
				run.recordFailure(validation.FailBudget)
			},
			want: "VALIDATION_BUDGET_EXHAUSTED",
		},
		{
			name: "judge failure",
			setup: func(run *acquireRun) {
				run.sawCandidate.Store(true)
				run.recordFailure(validation.FailJudge)
			},
			want: "VALIDATION_INFRA_UNAVAILABLE",
		},
		{
			name: "target failure",
			setup: func(run *acquireRun) {
				run.sawCandidate.Store(true)
				run.recordFailure(validation.FailTarget)
			},
			want: "VALIDATION_INFRA_UNAVAILABLE",
		},
		{
			name: "proxy failure",
			setup: func(run *acquireRun) {
				run.sawCandidate.Store(true)
				run.recordFailure(validation.FailProxy)
			},
			want: "NO_VALIDATED_RESOURCES",
		},
		{
			name: "budget outranks inconclusive failure",
			setup: func(run *acquireRun) {
				run.sawCandidate.Store(true)
				run.recordFailure(validation.FailJudge)
				run.recordFailure(validation.FailBudget)
			},
			want: "VALIDATION_BUDGET_EXHAUSTED",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			run := &acquireRun{}
			if test.setup != nil {
				test.setup(run)
			}
			if got := run.zeroReason(); got != test.want {
				t.Errorf("zero reason = %q, want %q", got, test.want)
			}
			if got := run.shortfallReason(); got != test.want {
				t.Errorf("partial reason = %q, want %q", got, test.want)
			}
		})
	}
}

func TestRequestContextEndedErrorIsNarrow(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	expiredCtx, expire := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer expire()

	tests := []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{name: "active context", ctx: context.Background(), err: context.Canceled},
		{name: "canceled context", ctx: canceledCtx, err: fmt.Errorf("query: %w", context.Canceled), want: true},
		{name: "expired context", ctx: expiredCtx, err: context.DeadlineExceeded, want: true},
		{name: "canceled rollback race", ctx: canceledCtx, err: fmt.Errorf("insert: %w", sql.ErrTxDone), want: true},
		{name: "genuine trigger error", ctx: canceledCtx, err: errors.New("audit trigger failed")},
		{name: "different store error", ctx: canceledCtx, err: errors.New("database disk image is malformed")},
		{name: "nil error", ctx: canceledCtx},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := requestContextEndedError(test.ctx, test.err); got != test.want {
				t.Fatalf("requestContextEndedError(%v, %v) = %v, want %v",
					test.ctx.Err(), test.err, got, test.want)
			}
		})
	}
}
