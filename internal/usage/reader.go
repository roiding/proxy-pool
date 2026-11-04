package usage

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"
)

// readerUnavailableNote is the honest status shown in the admin console:
// no official Flexus L traffic-package API has been verified yet, so no real
// Huawei reader is built. Configuring credentials does not enable billing
// changes or resource access of any kind.
const readerUnavailableNote = "华为云官方流量口径尚未核实：暂无可用读取器，" +
	"不会产生任何用量数据或自动停服行为。填写凭证不会授权资源创建、修改、删除或付费。"

var ErrUsageReaderUnavailable = errors.New(readerUnavailableNote)

// Snapshot is one usage sample. Source labels the data's origin so an
// estimate can never masquerade as an official billing value.
type Snapshot struct {
	PeriodStart time.Time
	PeriodEnd   time.Time
	AsOf        time.Time
	UsedGB      float64
	TotalGB     float64
	RemainingGB *float64
	Source      string
	Note        string
}

// UsageReader produces usage samples. Tests provide mocks; production has
// no reader until the external verification gate (plan §13.3) passes.
type UsageReader interface {
	ReadMonthlyUsage(ctx context.Context) (Snapshot, error)
}

type unavailableReader struct{}

func (unavailableReader) ReadMonthlyUsage(context.Context) (Snapshot, error) {
	return Snapshot{}, ErrUsageReaderUnavailable
}

// validateSample rejects unusable reader output before it can become a
// snapshot: a malformed sample must leave the previous data (marked stale)
// untouched instead of writing zeros or nonsense over real history.
func validateSample(snap Snapshot, now time.Time) error {
	if math.IsNaN(snap.UsedGB) || math.IsInf(snap.UsedGB, 0) || snap.UsedGB < 0 {
		return fmt.Errorf("usage reader returned invalid used value %v", snap.UsedGB)
	}
	if math.IsNaN(snap.TotalGB) || math.IsInf(snap.TotalGB, 0) || snap.TotalGB < 0 {
		return fmt.Errorf("usage reader returned invalid total value %v", snap.TotalGB)
	}
	if snap.RemainingGB != nil &&
		(math.IsNaN(*snap.RemainingGB) || math.IsInf(*snap.RemainingGB, 0) || *snap.RemainingGB < 0) {
		return fmt.Errorf("usage reader returned invalid remaining value %v", *snap.RemainingGB)
	}
	if snap.Source == "" {
		return errors.New("usage reader did not label the data source")
	}
	if snap.AsOf.IsZero() {
		return errors.New("usage reader returned no data timestamp")
	}
	if snap.AsOf.After(now.Add(24 * time.Hour)) {
		return fmt.Errorf("usage reader returned a future data timestamp %s", snap.AsOf.Format(time.RFC3339))
	}
	if snap.PeriodStart.IsZero() || snap.PeriodEnd.IsZero() || !snap.PeriodStart.Before(snap.PeriodEnd) {
		return errors.New("usage reader returned an invalid billing period")
	}
	return nil
}
