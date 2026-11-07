// Package scdn adapts the proxy.scdn.io provider API documented at
// https://proxy.scdn.io/api_docs.php. The vendor returns free public proxies
// as bare "host:port" entries; the requested protocol is the only transport
// signal, so it is declared in the channel configuration and mapped here.
package scdn

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
	DefaultEndpoint = "https://proxy.scdn.io/api/get_proxy.php"
	// MaxResponseBytes bounds the vendor payload; 20 entries of host:port
	// fit far below this, so anything larger is treated as malformed.
	MaxResponseBytes = 64 << 10
	MaxCount         = 20
)

type Config struct {
	// Protocol is the vendor-side filter and the declared transport signal:
	// http/https map to the pool's http transport (https additionally claims
	// CONNECT support), socks5 maps to socks5. "all" and "socks4" are
	// rejected because the response carries no per-proxy protocol metadata.
	Protocol    string
	CountryCode string
	FetchCount  int
}

func (c Config) Validate() error {
	switch c.Protocol {
	case "http", "https", "socks5":
	case "socks4":
		return fmt.Errorf("protocol %q is not supported by this pool; use http, https, or socks5", c.Protocol)
	case "", "all":
		return fmt.Errorf("protocol %q is ambiguous: the vendor response carries no per-proxy protocol; declare http, https, or socks5", display(c.Protocol))
	default:
		return fmt.Errorf("unknown protocol %q; supported: http, https, socks5", c.Protocol)
	}
	if c.CountryCode != "" {
		if len(c.CountryCode) != 2 || strings.ToUpper(c.CountryCode) != c.CountryCode ||
			!isASCIILetters(c.CountryCode) {
			return fmt.Errorf("country_code %q must be a 2-letter ISO 3166-1 code", c.CountryCode)
		}
	}
	if c.FetchCount == 0 {
		c.FetchCount = MaxCount
	}
	if c.FetchCount < 1 || c.FetchCount > MaxCount {
		return fmt.Errorf("fetch_count %d outside 1..%d", c.FetchCount, MaxCount)
	}
	return nil
}

func (c Config) count() int {
	if c.FetchCount == 0 {
		return MaxCount
	}
	return c.FetchCount
}

func (c Config) QueryParams() url.Values {
	q := url.Values{}
	q.Set("protocol", c.Protocol)
	q.Set("count", strconv.Itoa(c.count()))
	if c.CountryCode != "" {
		q.Set("country_code", c.CountryCode)
	}
	return q
}

type Entry struct {
	Transport domain.Transport
	Host      string
	Port      int
}

type EntryError struct {
	Line  int    `json:"line"`
	Input string `json:"input"`
	Error string `json:"error"`
}

type FetchResult struct {
	Endpoint      string
	Requested     int
	Entries       []Entry
	Errors        []EntryError
	RejectedCount int
	Fetched       int
}

type ErrorKind string

const (
	KindNetwork  ErrorKind = "network"
	KindFormat   ErrorKind = "format"
	KindProvider ErrorKind = "provider"
)

type Error struct {
	Kind       ErrorKind
	Status     int
	Detail     string
	Code       string
	RetryAfter time.Duration
}

func (e *Error) Error() string { return string(e.Kind) + ": " + e.Detail }

type apiEnvelope struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		Proxies *[]string `json:"proxies"`
		Count   int       `json:"count"`
	} `json:"data"`
}

// Fetch performs one bounded GET against the vendor endpoint and parses the
// JSON envelope. Invalid rows are isolated as EntryError entries; only an
// unusable payload (transport error, non-200 status, bad JSON, vendor error
// envelope) fails the whole fetch.
func Fetch(ctx context.Context, client *http.Client, endpoint string, cfg Config, allowPrivate bool) (FetchResult, error) {
	result := FetchResult{Endpoint: endpoint, Requested: cfg.count()}
	if err := cfg.Validate(); err != nil {
		return result, &Error{Kind: KindFormat, Status: 400, Detail: "invalid provider configuration"}
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.User != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.RawQuery != "" || u.Fragment != "" {
		return result, &Error{Kind: KindFormat, Status: 400, Detail: "invalid provider endpoint"}
	}
	u.RawQuery = cfg.QueryParams().Encode()
	if client == nil {
		client = sourcehttp.NewHTTPClient(sourcehttp.Options{AllowPrivate: allowPrivate, MaxBytes: MaxResponseBytes})
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return result, &Error{Kind: KindFormat, Status: 500, Detail: "could not build provider request"}
	}
	resp, err := client.Do(req)
	if err != nil {
		return result, sourceRequestError(ctx, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return result, providerError(resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxResponseBytes+1))
	if err != nil {
		return result, sourceRequestError(ctx, err)
	}
	if len(body) > MaxResponseBytes {
		return result, &Error{Kind: KindFormat, Status: 502,
			Detail: fmt.Sprintf("vendor response exceeds %d bytes", MaxResponseBytes)}
	}
	var env apiEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return result, &Error{Kind: KindFormat, Status: 502, Detail: "vendor response is not a valid proxy-list envelope"}
	}
	if env.Code != http.StatusOK {
		return result, providerError(env.Code, resp.Header.Get("Retry-After"))
	}
	if env.Data.Proxies == nil || len(*env.Data.Proxies) > 1000 {
		return result, &Error{Kind: KindFormat, Status: 502, Detail: "vendor proxy array is missing or exceeds the record limit"}
	}
	seen := map[string]bool{}
	transport, terr := transportFor(cfg.Protocol)
	if terr != nil {
		return result, terr
	}
	for i, raw := range *env.Data.Proxies {
		entry, err := parseEntry(transport, raw, allowPrivate)
		if err != nil {
			result.RejectedCount++
			if len(result.Errors) < 100 {
				result.Errors = append(result.Errors, EntryError{Line: i + 1, Error: err.Error()})
			}
			continue
		}
		result.Fetched++
		key := string(entry.Transport) + "|" + entry.Host + "|" + strconv.Itoa(entry.Port)
		if seen[key] {
			continue
		}
		seen[key] = true
		result.Entries = append(result.Entries, entry)
		if len(result.Entries) >= MaxCount {
			break
		}
	}
	if len(*env.Data.Proxies) > 0 && len(result.Entries) == 0 {
		return result, &Error{Kind: KindFormat, Status: 502, Detail: "vendor response has no valid proxy records"}
	}
	return result, nil
}

func providerError(status int, retry string) *Error {
	e := &Error{Kind: KindProvider, Status: 502, Detail: fmt.Sprintf("vendor status/code %d", status)}
	switch status {
	case 401, 403:
		e.Code = "SOURCE_AUTH_FAILED"
	case 429:
		e.Code = "SOURCE_RATE_LIMITED"
	}
	e.RetryAfter = sourcehttp.ParseRetryAfter(retry, time.Now())
	return e
}

func sourceRequestError(ctx context.Context, err error) *Error {
	var ferr *sourcehttp.Error
	if errors.As(err, &ferr) {
		kind := KindNetwork
		if ferr.Code == "SOURCE_FORMAT_ERROR" || ferr.Code == "SOURCE_RESPONSE_TOO_LARGE" || ferr.Code == "SOURCE_INVALID_URL" {
			kind = KindFormat
		}
		return &Error{Kind: kind, Code: ferr.Code, Status: ferr.Status, RetryAfter: ferr.RetryAfter, Detail: ferr.Detail}
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return &Error{Kind: KindNetwork, Status: 504, Code: "SOURCE_TIMEOUT", Detail: "provider request timed out"}
	}
	if errors.Is(err, context.Canceled) || ctx.Err() != nil {
		return &Error{Kind: KindNetwork, Status: 499, Code: "REQUEST_CANCELED", Detail: "provider request was cancelled"}
	}
	return &Error{Kind: KindNetwork, Status: 503, Code: "SOURCE_UNAVAILABLE", Detail: "vendor request failed"}
}

func transportFor(protocol string) (domain.Transport, error) {
	switch protocol {
	case "http", "https":
		return domain.TransportHTTP, nil
	case "socks5":
		return domain.TransportSOCKS5, nil
	default:
		return "", &Error{Kind: KindFormat, Status: 500,
			Detail: fmt.Sprintf("protocol %q has no pool transport mapping", protocol)}
	}
}

func parseEntry(transport domain.Transport, raw string, allowPrivate bool) (Entry, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return Entry{}, fmt.Errorf("empty entry")
	}
	if len(raw) > 260 {
		return Entry{}, fmt.Errorf("entry too long")
	}
	idx := strings.LastIndex(raw, ":")
	if idx <= 0 || idx == len(raw)-1 {
		return Entry{}, fmt.Errorf("entry is not host:port")
	}
	host, portStr := raw[:idx], raw[idx+1:]
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return Entry{}, fmt.Errorf("invalid port")
	}
	if strings.HasPrefix(host, "[") {
		if !strings.HasSuffix(host, "]") {
			return Entry{}, fmt.Errorf("unbalanced IPv6 brackets")
		}
		host = strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
	} else if strings.Contains(host, ":") {
		// A bare IPv6 literal is indistinguishable from host:port in the
		// vendor's colon-delimited format, so it cannot be accepted safely.
		return Entry{}, fmt.Errorf("ambiguous IPv6 entry")
	}
	host, err = custom.NormalizeHost(host, allowPrivate)
	if err != nil {
		return Entry{}, err
	}
	return Entry{Transport: transport, Host: host, Port: port}, nil
}

func isASCIILetters(s string) bool {
	for _, r := range s {
		if !('A' <= r && r <= 'Z') {
			return false
		}
	}
	return len(s) > 0
}

func display(s string) string {
	if s == "" {
		return "all(default)"
	}
	return s
}
