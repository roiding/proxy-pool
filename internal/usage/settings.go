package usage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Optional fields distinguish absent, explicit null (clear), and a value.
type OptionalString struct {
	Present bool
	Null    bool
	Value   string
}

func (f *OptionalString) UnmarshalJSON(data []byte) error {
	f.Present = true
	if string(data) == "null" {
		f.Null = true
		return nil
	}
	if err := unmarshalString(data, &f.Value); err != nil {
		return err
	}
	return nil
}

type OptionalBool struct {
	Present bool
	Null    bool
	Value   bool
}

func (f *OptionalBool) UnmarshalJSON(data []byte) error {
	f.Present = true
	if string(data) == "null" {
		f.Null = true
		return nil
	}
	return unmarshalBool(data, &f.Value)
}

type OptionalInt64 struct {
	Present bool
	Null    bool
	Value   int64
}

func (f *OptionalInt64) UnmarshalJSON(data []byte) error {
	f.Present = true
	if string(data) == "null" {
		f.Null = true
		return nil
	}
	return unmarshalInt64(data, &f.Value)
}

// SettingsUpdate is the admin-facing patch. Optional* fields are values, not
// pointers, so an explicit JSON null reaches UnmarshalJSON: absent = keep,
// null = clear, "" = keep for secrets, a value = replace. This is the same
// tri-state contract the project source-scope endpoints use.
type SettingsUpdate struct {
	BarkURL        OptionalString `json:"bark_url"`
	BarkEnabled    OptionalBool   `json:"bark_enabled"`
	HuaweiAK       OptionalString `json:"huawei_access_key_id"`
	HuaweiSK       OptionalString `json:"huawei_secret_access_key"`
	HuaweiRegion   OptionalString `json:"huawei_region"`
	HuaweiResource OptionalString `json:"huawei_resource_id"`
	HuaweiPackage  OptionalString `json:"huawei_package_id"`
	MonthlyTotalGB OptionalInt64  `json:"monthly_total_gb"`
	ThresholdPct   OptionalInt64  `json:"threshold_percent"`
	CheckTime      OptionalString `json:"check_time"`
	Timezone       OptionalString `json:"timezone"`
}

// settings is the decrypted working set.
type settings struct {
	BarkURL        string
	BarkEnabled    bool
	HuaweiAK       string
	HuaweiSK       string
	HuaweiRegion   string
	HuaweiResource string
	HuaweiPackage  string
	MonthlyTotalGB int64
	ThresholdPct   int64
	CheckTime      string
	Timezone       string
}

const (
	keyBarkURL        = "bark_url"
	keyBarkEnabled    = "bark_enabled"
	keyHuaweiAK       = "huawei_access_key_id"
	keyHuaweiSK       = "huawei_secret_access_key"
	keyHuaweiRegion   = "huawei_region"
	keyHuaweiResource = "huawei_resource_id"
	keyHuaweiPackage  = "huawei_package_id"
	keyMonthlyTotalGB = "monthly_total_gb"
	keyThresholdPct   = "threshold_percent"
	keyCheckTime      = "check_time"
	keyTimezone       = "timezone"

	// clearedSuffix marks an explicit admin clear. The sentinel must outlive
	// bootstrap seeding: deleting the value alone would let deployment config
	// silently restore a credential on the next restart.
	clearedSuffix = ":cleared"
)

var secretKeys = map[string]bool{keyBarkURL: true, keyHuaweiAK: true, keyHuaweiSK: true}

// SettingsView is the masked admin read model. Secrets never leave the
// server: the API reports presence and a masked hint only.
type SettingsView struct {
	Bark struct {
		Configured bool   `json:"configured"`
		URLHint    string `json:"url_hint,omitempty"`
		Enabled    bool   `json:"enabled"`
	} `json:"bark"`
	Huawei struct {
		Configured bool   `json:"configured"`
		AkHint     string `json:"access_key_hint,omitempty"`
		Region     string `json:"region,omitempty"`
		ResourceID string `json:"resource_id,omitempty"`
		PackageID  string `json:"package_id,omitempty"`
	} `json:"huawei"`
	MonthlyTotalGB  int64  `json:"monthly_total_gb"`
	ThresholdPct    int64  `json:"threshold_percent"`
	CheckTime       string `json:"check_time"`
	Timezone        string `json:"timezone"`
	SecretsWritable bool   `json:"secrets_writable"`
	ReaderStatus    string `json:"reader_status"`
	ReaderNote      string `json:"reader_note"`
}

func maskSecret(v string) string {
	if v == "" {
		return ""
	}
	if len(v) <= 4 {
		return "••••"
	}
	return "••••" + v[len(v)-2:]
}

// maskBarkURL keeps the scheme and host so operators can spot a wrong host,
// but hides the path that carries the device key.
func maskBarkURL(raw string) string {
	u, err := parseBarkURL(raw)
	if err != nil {
		return "(configured, invalid)"
	}
	return u.Scheme + "://" + u.Host + "/<device-key>"
}

func (s *Service) loadSettings(ctx context.Context) (settings, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT key, value, secret FROM usage_settings`)
	if err != nil {
		return settings{}, err
	}
	defer rows.Close()
	st := settings{
		BarkEnabled:    s.defaults.BarkEnabled,
		MonthlyTotalGB: s.defaults.MonthlyTotalGB,
		ThresholdPct:   s.defaults.ThresholdPercent,
		CheckTime:      s.defaults.CheckTime,
		Timezone:       s.defaults.Timezone,
		HuaweiRegion:   s.defaults.HuaweiRegion,
		HuaweiResource: s.defaults.HuaweiResourceID,
		HuaweiPackage:  s.defaults.HuaweiPackageID,
	}
	cleared := map[string]bool{}
	for rows.Next() {
		var key, value string
		var secret bool
		if err := rows.Scan(&key, &value, &secret); err != nil {
			return settings{}, err
		}
		if strings.HasSuffix(key, clearedSuffix) {
			cleared[strings.TrimSuffix(key, clearedSuffix)] = true
			continue
		}
		if secret {
			if s.box == nil {
				return settings{}, errors.New(
					"usage: stored secret " + key + " requires secret_encryption_key in the deployment config; it is not set")
			}
			plain, err := s.box.open(key, value)
			if err != nil {
				return settings{}, err
			}
			value = plain
		}
		switch key {
		case keyBarkURL:
			st.BarkURL = value
		case keyBarkEnabled:
			st.BarkEnabled = value == "true"
		case keyHuaweiAK:
			st.HuaweiAK = value
		case keyHuaweiSK:
			st.HuaweiSK = value
		case keyHuaweiRegion:
			st.HuaweiRegion = value
		case keyHuaweiResource:
			st.HuaweiResource = value
		case keyHuaweiPackage:
			st.HuaweiPackage = value
		case keyMonthlyTotalGB:
			st.MonthlyTotalGB = parseInt64OrDefault(value, st.MonthlyTotalGB)
		case keyThresholdPct:
			st.ThresholdPct = parseInt64OrDefault(value, st.ThresholdPct)
		case keyCheckTime:
			st.CheckTime = value
		case keyTimezone:
			st.Timezone = value
		}
	}
	if err := rows.Err(); err != nil {
		return settings{}, err
	}
	// Defaults are only a fallback. An explicit clear suppresses both the
	// stored value (removed in the same transaction) and the bootstrap value
	// for as long as the sentinel exists.
	for key := range cleared {
		switch key {
		case keyBarkURL:
			st.BarkURL = ""
		case keyHuaweiAK:
			st.HuaweiAK = ""
		case keyHuaweiSK:
			st.HuaweiSK = ""
		case keyHuaweiRegion:
			st.HuaweiRegion = ""
		case keyHuaweiResource:
			st.HuaweiResource = ""
		case keyHuaweiPackage:
			st.HuaweiPackage = ""
		case keyCheckTime:
			st.CheckTime = ""
		case keyTimezone:
			st.Timezone = ""
		}
	}
	return st, nil
}

func (s *Service) settingCleared(ctx context.Context, key string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM usage_settings WHERE key = ?`,
		key+clearedSuffix).Scan(&n)
	return n > 0, err
}

func (s *Service) putSetting(ctx context.Context, key, value string, secret bool, now time.Time) error {
	stored := value
	if secret {
		sealed, err := s.box.seal(key, value)
		if err != nil {
			return err
		}
		stored = sealed
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO usage_settings(key, value, secret, updated_at) VALUES(?,?,?,?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value, secret = excluded.secret,
		  updated_at = excluded.updated_at`,
		key, stored, boolInt(secret), fmtTime(now))
	return err
}

func (s *Service) deleteSetting(ctx context.Context, key string, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM usage_settings WHERE key = ?`, key)
	return err
}

// UpdateSettings applies the patch in one transaction: either every field of
// the patch lands or none does. Only values that actually change are written,
// so a re-submitted secret never re-encrypts (and never trips storage
// triggers for unchanged data). A failure rolls the whole patch back.
func (s *Service) UpdateSettings(ctx context.Context, u SettingsUpdate) (SettingsView, error) {
	if err := s.requireBox(u.storesSecrets()); err != nil {
		return SettingsView{}, err
	}
	current, err := s.loadSettings(ctx)
	if err != nil {
		return SettingsView{}, err
	}
	next := current
	// Secrets: absent = keep, "" = keep, null = clear, value = replace.
	applySecret := func(cur *string, f OptionalString) {
		if !f.Present {
			return
		}
		if f.Null {
			*cur = ""
			return
		}
		if f.Value != "" {
			*cur = f.Value
		}
	}
	// Plain strings: absent = keep, null or "" = clear, value = replace.
	applyPlain := func(cur *string, f OptionalString) {
		if !f.Present {
			return
		}
		if f.Null {
			*cur = ""
			return
		}
		if f.Value != "" {
			*cur = f.Value
		}
	}
	applySecret(&next.BarkURL, u.BarkURL)
	applySecret(&next.HuaweiAK, u.HuaweiAK)
	applySecret(&next.HuaweiSK, u.HuaweiSK)
	applyPlain(&next.HuaweiRegion, u.HuaweiRegion)
	applyPlain(&next.HuaweiResource, u.HuaweiResource)
	applyPlain(&next.HuaweiPackage, u.HuaweiPackage)
	applyPlain(&next.CheckTime, u.CheckTime)
	applyPlain(&next.Timezone, u.Timezone)
	if u.BarkEnabled.Present && !u.BarkEnabled.Null {
		next.BarkEnabled = u.BarkEnabled.Value
	}
	if u.MonthlyTotalGB.Present && !u.MonthlyTotalGB.Null {
		next.MonthlyTotalGB = u.MonthlyTotalGB.Value
	}
	if u.ThresholdPct.Present && !u.ThresholdPct.Null {
		next.ThresholdPct = u.ThresholdPct.Value
	}
	if next.BarkURL != "" && next.BarkURL != current.BarkURL {
		parsed, err := parseBarkURL(next.BarkURL)
		if err != nil {
			return SettingsView{}, err
		}
		next.BarkURL = parsed.String()
	}
	if next.MonthlyTotalGB < 0 {
		return SettingsView{}, errors.New("usage: monthly_total_gb must not be negative")
	}
	if next.ThresholdPct < 1 || next.ThresholdPct > 100 {
		return SettingsView{}, errors.New("usage: threshold_percent must be between 1 and 100")
	}
	if _, err := checkTimeSpec(next.CheckTime, next.Timezone); err != nil {
		return SettingsView{}, err
	}
	now := s.Now()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SettingsView{}, err
	}
	defer tx.Rollback()
	write := func(key string, value string, secret bool) error {
		if value == "" {
			// Replace the value with a persistent clear sentinel. The
			// sentinel is not exposed by loadSettings and blocks bootstrap
			// seeding forever until an admin supplies a new value.
			_, err := tx.ExecContext(ctx, `
				INSERT INTO usage_settings(key, value, secret, updated_at) VALUES(?,?,?,?)
				ON CONFLICT(key) DO UPDATE SET value = excluded.value, secret = excluded.secret,
				  updated_at = excluded.updated_at`,
				key+clearedSuffix, "", 0, fmtTime(now))
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, `DELETE FROM usage_settings WHERE key = ?`, key)
			return err
		}
		stored, err := s.sealFor(key, value, secret)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM usage_settings WHERE key = ?`,
			key+clearedSuffix); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO usage_settings(key, value, secret, updated_at) VALUES(?,?,?,?)
			ON CONFLICT(key) DO UPDATE SET value = excluded.value, secret = excluded.secret,
			  updated_at = excluded.updated_at`,
			key, stored, boolInt(secret), fmtTime(now))
		return err
	}
	changed := map[string]bool{}
	if u.BarkURL.Present && next.BarkURL != current.BarkURL {
		changed[keyBarkURL] = true
	}
	if u.BarkEnabled.Present && !u.BarkEnabled.Null && next.BarkEnabled != current.BarkEnabled {
		changed[keyBarkEnabled] = true
	}
	if u.HuaweiAK.Present && next.HuaweiAK != current.HuaweiAK {
		changed[keyHuaweiAK] = true
	}
	if u.HuaweiSK.Present && next.HuaweiSK != current.HuaweiSK {
		changed[keyHuaweiSK] = true
	}
	if u.HuaweiRegion.Present && next.HuaweiRegion != current.HuaweiRegion {
		changed[keyHuaweiRegion] = true
	}
	if u.HuaweiResource.Present && next.HuaweiResource != current.HuaweiResource {
		changed[keyHuaweiResource] = true
	}
	if u.HuaweiPackage.Present && next.HuaweiPackage != current.HuaweiPackage {
		changed[keyHuaweiPackage] = true
	}
	if u.MonthlyTotalGB.Present && !u.MonthlyTotalGB.Null && next.MonthlyTotalGB != current.MonthlyTotalGB {
		changed[keyMonthlyTotalGB] = true
	}
	if u.ThresholdPct.Present && !u.ThresholdPct.Null && next.ThresholdPct != current.ThresholdPct {
		changed[keyThresholdPct] = true
	}
	if u.CheckTime.Present && next.CheckTime != current.CheckTime {
		changed[keyCheckTime] = true
	}
	if u.Timezone.Present && next.Timezone != current.Timezone {
		changed[keyTimezone] = true
	}
	for key, secret := range map[string]bool{
		keyBarkURL: true, keyHuaweiAK: true, keyHuaweiSK: true,
		keyBarkEnabled: false, keyHuaweiRegion: false, keyHuaweiResource: false,
		keyHuaweiPackage: false, keyMonthlyTotalGB: false, keyThresholdPct: false,
		keyCheckTime: false, keyTimezone: false,
	} {
		if !changed[key] {
			continue
		}
		if err := write(key, settingString(next, key), secret); err != nil {
			return SettingsView{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return SettingsView{}, err
	}
	return s.Settings(ctx)
}

func (u SettingsUpdate) storesSecrets() bool {
	return (u.BarkURL.Present && !u.BarkURL.Null && u.BarkURL.Value != "") ||
		(u.HuaweiAK.Present && !u.HuaweiAK.Null && u.HuaweiAK.Value != "") ||
		(u.HuaweiSK.Present && !u.HuaweiSK.Null && u.HuaweiSK.Value != "")
}

func (s *Service) sealFor(key, value string, secret bool) (string, error) {
	if !secret {
		return value, nil
	}
	return s.box.seal(key, value)
}

func settingString(st settings, key string) string {
	switch key {
	case keyBarkURL:
		return st.BarkURL
	case keyBarkEnabled:
		return boolStr(st.BarkEnabled)
	case keyHuaweiAK:
		return st.HuaweiAK
	case keyHuaweiSK:
		return st.HuaweiSK
	case keyHuaweiRegion:
		return st.HuaweiRegion
	case keyHuaweiResource:
		return st.HuaweiResource
	case keyHuaweiPackage:
		return st.HuaweiPackage
	case keyMonthlyTotalGB:
		return fmtInt(st.MonthlyTotalGB)
	case keyThresholdPct:
		return fmtInt(st.ThresholdPct)
	case keyCheckTime:
		return st.CheckTime
	case keyTimezone:
		return st.Timezone
	}
	return ""
}

// Settings returns the masked view.
func (s *Service) Settings(ctx context.Context) (SettingsView, error) {
	st, err := s.loadSettings(ctx)
	if err != nil {
		return SettingsView{}, err
	}
	var view SettingsView
	view.Bark.Configured = st.BarkURL != ""
	view.Bark.URLHint = maskBarkURL(st.BarkURL)
	view.Bark.Enabled = st.BarkEnabled
	view.Huawei.Configured = st.HuaweiAK != "" && st.HuaweiSK != ""
	view.Huawei.AkHint = maskSecret(st.HuaweiAK)
	view.Huawei.Region = st.HuaweiRegion
	view.Huawei.ResourceID = st.HuaweiResource
	view.Huawei.PackageID = st.HuaweiPackage
	view.MonthlyTotalGB = st.MonthlyTotalGB
	view.ThresholdPct = st.ThresholdPct
	view.CheckTime = st.CheckTime
	view.Timezone = st.Timezone
	view.SecretsWritable = s.masterKeySet
	view.ReaderStatus, view.ReaderNote = s.readerView(st)
	return view, nil
}

func (s *Service) readerView(st settings) (string, string) {
	if s.defaults.Reader != nil {
		return "configured", "已接入用量读取器。"
	}
	if err := huaweiConfigurationError(st); err != nil {
		return "unconfigured", err.Error()
	}
	return "configured", huaweiReaderNote
}

func (s *Service) requireBox(needed bool) error {
	if needed && s.box == nil {
		return errors.New("usage: secret_encryption_key is not configured; secret settings cannot be stored")
	}
	return nil
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func fmtInt(v int64) string { return fmt.Sprintf("%d", v) }

func parseInt64OrDefault(v string, def int64) int64 {
	var out int64
	if _, err := fmt.Sscanf(v, "%d", &out); err != nil {
		return def
	}
	return out
}

func checkTimeSpec(clock, tz string) (time.Time, error) {
	if clock == "" {
		return time.Time{}, errors.New("usage: check_time is required (HH:MM)")
	}
	t, err := time.Parse("15:04", clock)
	if err != nil {
		return time.Time{}, fmt.Errorf("usage: check_time %q must be HH:MM", clock)
	}
	if tz != "" {
		if _, err := time.LoadLocation(tz); err != nil {
			return time.Time{}, fmt.Errorf("usage: timezone %q is not a valid IANA zone", tz)
		}
	}
	return t, nil
}
