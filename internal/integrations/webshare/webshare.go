// Package webshare reads already-assigned direct proxies. It does not buy,
// replace, download a credential-bearing URL, or allocate proxies on demand.
// Contract: https://apidocs.webshare.io/proxy-list/list
package webshare

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"proxypool/internal/domain"
	"proxypool/internal/integrations/custom"
	"proxypool/internal/integrations/sourcehttp"
)

const (
	DefaultEndpoint = "https://proxy.webshare.io/api/v2/proxy/list/"
	MaxPageBytes    = 512 << 10
	MaxPages        = 20
	MaxPageSize     = 100
	MaxRecords      = MaxPages * MaxPageSize
	FetchTimeout    = 20 * time.Second
)

type Config struct {
	Protocol domain.Transport
	PageSize int
	MaxPages int
}

func (c Config) Validate() error {
	if c.Protocol != domain.TransportHTTP && c.Protocol != domain.TransportSOCKS5 {
		return fmt.Errorf("protocol must be http or socks5")
	}
	if c.PageSize < 0 || c.PageSize > MaxPageSize {
		return fmt.Errorf("page_size must be within 1..%d when supplied", MaxPageSize)
	}
	if c.MaxPages < 0 || c.MaxPages > MaxPages {
		return fmt.Errorf("max_pages must be within 1..%d when supplied", MaxPages)
	}
	return nil
}

type Entry struct {
	Conn     domain.Connection
	RecordID string
	Claims   map[string]string
}

type Result struct {
	Entries               []Entry
	Fetched, Rejected     int
	PagesDone, PagesTotal int
	Complete              bool
}

// Error contains only code-owned diagnostic text. Vendor response bodies and
// account/proxy credentials must never enter API errors or CLI reports.
type Error struct {
	Code       string
	Status     int
	Detail     string
	RetryAfter time.Duration
}

func (e *Error) Error() string { return e.Code + ": " + e.Detail }

func failure(code, detail string) *Error { return &Error{Code: code, Status: 502, Detail: detail} }

type pageEnvelope struct {
	Count   *int            `json:"count"`
	Next    json.RawMessage `json:"next"`
	Results json.RawMessage `json:"results"`
}

type proxyRecord struct {
	ID          json.RawMessage `json:"id"`
	Username    *string         `json:"username"`
	Password    *string         `json:"password"`
	Address     *string         `json:"proxy_address"`
	Port        json.RawMessage `json:"port"`
	Valid       *bool           `json:"valid"`
	CountryCode string          `json:"country_code"`
	CreatedAt   string          `json:"created_at"`
	LastCheck   string          `json:"last_verification"`
}

// Fetch only publishes a complete, bounded snapshot. A failed page, changed
// count, repeated ID, malformed valid row or pagination overflow rejects the
// whole refresh so the application can retain its previous (hard-TTL) batch.
// endpoint/client are injection seams; production uses the fixed endpoint
// and sourcehttp, not source-configurable URLs or http.DefaultClient.
func Fetch(ctx context.Context, client *http.Client, endpoint, token string, cfg Config, allowPrivate bool) (Result, error) {
	if err := cfg.Validate(); err != nil {
		return Result{}, failure("SOURCE_CONFIG_ERROR", err.Error())
	}
	if token == "" || len(token) > 4096 || strings.ContainsFunc(token, func(r rune) bool { return r <= ' ' || r >= 127 }) {
		return Result{}, failure("SOURCE_AUTH_FAILED", "account API key is missing or invalid")
	}
	if endpoint == "" {
		endpoint = DefaultEndpoint
	}
	base, err := url.Parse(endpoint)
	if err != nil || base.User != nil || base.Host == "" || (base.Scheme != "https" && base.Scheme != "http") || base.RawQuery != "" || base.Fragment != "" {
		return Result{}, failure("SOURCE_CONFIG_ERROR", "invalid account endpoint")
	}
	if client == nil {
		client = sourcehttp.NewHTTPClient(sourcehttp.Options{AllowPrivate: allowPrivate, MaxBytes: MaxPageBytes, NoRedirects: true})
	}
	// Also constrain injected clients: account auth is never followed through
	// redirects, even to another URL at the same origin.
	boundedClient := *client
	boundedClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	ctx, cancel := context.WithTimeout(ctx, FetchTimeout)
	defer cancel()
	pageSize, maxPages := cfg.PageSize, cfg.MaxPages
	if pageSize == 0 {
		pageSize = MaxPageSize
	}
	if maxPages == 0 {
		maxPages = 10
	}
	result := Result{}
	seenIDs := make(map[string]bool)
	seenConnections := make(map[string]bool)
	count, rowsRead := -1, 0
	for page := 1; page <= maxPages; page++ {
		u := *base
		q := url.Values{"mode": {"direct"}, "page": {strconv.Itoa(page)}, "page_size": {strconv.Itoa(pageSize)}}
		u.RawQuery = q.Encode()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		req.Header.Set("Authorization", "Token "+token)
		req.Header.Set("Accept", "application/json")
		resp, err := boundedClient.Do(req)
		if err != nil {
			return Result{}, requestError(ctx, err)
		}
		if resp.StatusCode != http.StatusOK {
			// Inspect status/Retry-After before touching an untrusted error
			// body. A giant or slow 429 page must not erase account backoff.
			_ = resp.Body.Close()
			switch resp.StatusCode {
			case http.StatusUnauthorized, http.StatusForbidden:
				return Result{}, failure("SOURCE_AUTH_FAILED", "Webshare rejected the account API key or its permissions")
			case http.StatusTooManyRequests:
				e := failure("SOURCE_RATE_LIMITED", "Webshare account API is rate limited")
				e.RetryAfter = retryAfter(resp.Header.Get("Retry-After"), time.Now())
				return Result{}, e
			case http.StatusBadRequest:
				return Result{}, failure("SOURCE_CONFIG_ERROR", "Webshare rejected direct-list request parameters")
			default:
				return Result{}, failure("SOURCE_UNAVAILABLE", fmt.Sprintf("Webshare returned HTTP %d", resp.StatusCode))
			}
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, MaxPageBytes+1))
		_ = resp.Body.Close()
		if readErr != nil {
			return Result{}, requestError(ctx, readErr)
		}
		if len(body) > MaxPageBytes {
			return Result{}, failure("SOURCE_FORMAT_ERROR", "account list page exceeds the body limit")
		}
		var env pageEnvelope
		if json.Unmarshal(body, &env) != nil || env.Count == nil || *env.Count < 0 || len(env.Next) == 0 || len(env.Results) == 0 || strings.TrimSpace(string(env.Results)) == "null" {
			return Result{}, failure("SOURCE_FORMAT_ERROR", "Webshare response is not a direct-list envelope")
		}
		if count == -1 {
			count = *env.Count
			if count > maxPages*pageSize || count > MaxRecords {
				return Result{}, failure("SOURCE_SNAPSHOT_INCOMPLETE", "account list exceeds the configured full-snapshot limit")
			}
			result.PagesTotal = (count + pageSize - 1) / pageSize
			if result.PagesTotal == 0 {
				result.PagesTotal = 1
			}
		} else if *env.Count != count {
			return Result{}, failure("SOURCE_SNAPSHOT_INCOMPLETE", "account list changed size during pagination")
		}
		var records []proxyRecord
		if json.Unmarshal(env.Results, &records) != nil || len(records) > pageSize {
			return Result{}, failure("SOURCE_FORMAT_ERROR", "invalid Webshare results array")
		}
		rowsRead += len(records)
		if rowsRead > count {
			return Result{}, failure("SOURCE_SNAPSHOT_INCOMPLETE", "account list row count exceeds its declared count")
		}
		for _, record := range records {
			id, err := recordID(record.ID)
			if err != nil || seenIDs[id] || record.Valid == nil {
				return Result{}, failure("SOURCE_FORMAT_ERROR", "account list has an invalid or repeated record identity/validity field")
			}
			seenIDs[id] = true
			if !*record.Valid {
				result.Rejected++
				continue
			}
			conn, err := recordConnection(record, cfg.Protocol, allowPrivate)
			if err != nil {
				return Result{}, failure("SOURCE_FORMAT_ERROR", "account list has a valid record with incomplete or invalid connection fields")
			}
			result.Fetched++
			key := domain.ConnectionFingerprint("webshare-parser", conn.Transport, conn.Host, conn.Port, conn.Username, conn.Password)
			if seenConnections[key] {
				continue
			}
			seenConnections[key] = true
			claims := map[string]string{"vendor_id": "webshare", "vendor_record_id": id, "vendor_valid": "true", "vendor_protocol": string(cfg.Protocol)}
			if len(record.CountryCode) == 2 {
				claims["vendor_country"] = strings.ToUpper(record.CountryCode)
			}
			for name, raw := range map[string]string{"vendor_created_at": record.CreatedAt, "vendor_last_verification": record.LastCheck} {
				if parsed, err := time.Parse(time.RFC3339, raw); err == nil {
					claims[name] = parsed.UTC().Format(time.RFC3339)
				}
			}
			result.Entries = append(result.Entries, Entry{Conn: conn, RecordID: id, Claims: claims})
		}
		result.PagesDone++
		var next *string
		if json.Unmarshal(env.Next, &next) != nil {
			return Result{}, failure("SOURCE_FORMAT_ERROR", "invalid account pagination next field")
		}
		if next == nil {
			if rowsRead != count {
				return Result{}, failure("SOURCE_SNAPSHOT_INCOMPLETE", "account list ended before its declared count")
			}
			result.Complete = true
			return result, nil
		}
		if len(records) == 0 || page == maxPages {
			return Result{}, failure("SOURCE_SNAPSHOT_INCOMPLETE", "account list pagination is incomplete")
		}
		if !validNext(base, *next, page+1, pageSize) {
			return Result{}, failure("SOURCE_FORMAT_ERROR", "account pagination changed origin, path or request parameters")
		}
	}
	return Result{}, failure("SOURCE_SNAPSHOT_INCOMPLETE", "account list pagination exceeded its limit")
}

func requestError(ctx context.Context, err error) *Error {
	var ferr *sourcehttp.Error
	if errors.As(err, &ferr) {
		return &Error{Code: ferr.Code, Status: ferr.Status, Detail: ferr.Detail, RetryAfter: ferr.RetryAfter}
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return &Error{Code: "SOURCE_TIMEOUT", Status: 504, Detail: "account list download timed out"}
	}
	if errors.Is(err, context.Canceled) || ctx.Err() != nil {
		return &Error{Code: "REQUEST_CANCELED", Status: 499, Detail: "account list download was cancelled"}
	}
	return &Error{Code: "SOURCE_UNAVAILABLE", Status: 503, Detail: "account list request failed"}
}

func validNext(base *url.URL, raw string, page, pageSize int) bool {
	u, err := base.Parse(raw)
	if err != nil || u.User != nil || u.Fragment != "" || u.Scheme != base.Scheme || !strings.EqualFold(u.Host, base.Host) || u.EscapedPath() != base.EscapedPath() {
		return false
	}
	q := u.Query()
	return len(q) == 3 && len(q["mode"]) == 1 && q.Get("mode") == "direct" && len(q["page"]) == 1 && q.Get("page") == strconv.Itoa(page) && len(q["page_size"]) == 1 && q.Get("page_size") == strconv.Itoa(pageSize)
}

func recordID(raw json.RawMessage) (string, error) {
	var id string
	if json.Unmarshal(raw, &id) == nil && id != "" && len(id) <= 128 && !strings.ContainsFunc(id, func(r rune) bool { return r <= ' ' || r == 127 }) {
		return id, nil
	}
	var n json.Number
	if json.Unmarshal(raw, &n) == nil {
		if value, err := strconv.ParseInt(n.String(), 10, 64); err == nil && value >= 0 {
			return n.String(), nil
		}
	}
	return "", errors.New("invalid record id")
}

func recordConnection(r proxyRecord, transport domain.Transport, allowPrivate bool) (domain.Connection, error) {
	if r.Address == nil || r.Username == nil || r.Password == nil || *r.Username == "" || *r.Password == "" || len(*r.Username) > custom.MaxCredentialBytes || len(*r.Password) > custom.MaxCredentialBytes {
		return domain.Connection{}, errors.New("missing connection fields")
	}
	if err := custom.ValidateCredentials(*r.Username, *r.Password); err != nil {
		return domain.Connection{}, errors.New("invalid connection credentials")
	}
	var number json.Number
	if json.Unmarshal(r.Port, &number) != nil {
		return domain.Connection{}, errors.New("invalid port")
	}
	port, err := strconv.Atoi(number.String())
	if err != nil || port < 1 || port > 65535 {
		return domain.Connection{}, errors.New("invalid port")
	}
	host, err := custom.NormalizeHost(*r.Address, allowPrivate)
	if err != nil {
		return domain.Connection{}, errors.New("invalid address")
	}
	return domain.Connection{Transport: transport, Host: host, Port: port, Username: *r.Username, Password: *r.Password}, nil
}

func retryAfter(raw string, now time.Time) time.Duration {
	return sourcehttp.ParseRetryAfter(raw, now)
}
