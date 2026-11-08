package usage

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"
)

const readerUnavailableNote = "华为云套餐读取未配置：请填写 AK/SK、区域和 L 实例 ID，流量包 ID 可自动识别。"

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

// UsageReader produces usage samples. Production reads the Flexus L quota;
// tests may supply independent fixture readers.
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
