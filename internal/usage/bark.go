package usage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"proxypool/internal/domain"
)

// parseBarkURL validates a Bark base URL. The device key rides in the path,
// so credentials in userinfo or the query are rejected and plain http is
// only allowed for local testing addresses.
func parseBarkURL(raw string) (*url.URL, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, errors.New("bark_url is empty")
	}
	u, err := url.Parse(raw)
	if err != nil {
		// Never echo the raw URL back: it carries the device key.
		return nil, errors.New("bark_url is not a valid absolute URL")
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("bark_url must not contain credentials, query, or fragment")
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return nil, errors.New("bark_url must use http(s)")
	}
	if u.Host == "" {
		return nil, errors.New("bark_url requires a host")
	}
	if u.Scheme == "http" {
		host := u.Hostname()
		ip := net.ParseIP(host)
		if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return nil, errors.New("bark_url outside local testing must use https")
		}
	}
	return u, nil
}

// barkDialContext pins the dial to pre-resolved, policy-checked addresses.
// Every resolved IP must pass the shared domain.AddrAllowed policy (it
// rejects loopback, CGNAT, link-local, ULA, metadata and other non-public
// ranges with full IPv6 unmapping), and the connection then uses that exact
// address, so DNS re-resolution cannot swap in a private endpoint between
// check and dial.
func barkDialContext(allowPrivate bool) func(context.Context, string, string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		switch network {
		case "tcp", "tcp4", "tcp6":
		default:
			return nil, errors.New("bark push blocked: unsupported network")
		}
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, errors.New("bark push blocked: invalid endpoint address")
		}
		ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		if err != nil {
			return nil, fmt.Errorf("bark push failed: dns error (%s)", errcode(err))
		}
		pinned := ""
		for _, ip := range ips {
			if !domain.AddrAllowed(ip.Unmap(), allowPrivate) {
				return nil, errors.New("bark push blocked: endpoint address is not public")
			}
			if pinned == "" {
				pinned = net.JoinHostPort(ip.Unmap().String(), port)
			}
		}
		return dialer.DialContext(ctx, network, pinned)
	}
}

// errcode reduces a resolver error to a DNS status keyword; the resolver
// error text may embed hostnames but the guard here stays categorical.
func errcode(err error) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "no such host"):
		return "NXDOMAIN"
	case strings.Contains(msg, "context deadline exceeded"):
		return "timeout"
	case strings.Contains(msg, "canceled"):
		return "cancelled"
	default:
		return "dns_error"
	}
}

// barkTransport builds a client transport that never uses environment
// proxies, never follows redirects, refuses non-public endpoints unless the
// deployment explicitly allowed them, and bounds the response header size.
func barkTransport(allowPrivate bool) *http.Transport {
	return &http.Transport{
		Proxy:                  nil,
		DialContext:            barkDialContext(allowPrivate),
		MaxIdleConns:           2,
		IdleConnTimeout:        30 * time.Second,
		TLSHandshakeTimeout:    10 * time.Second,
		ExpectContinueTimeout:  time.Second,
		MaxResponseHeaderBytes: 16 << 10,
		DisableCompression:     true,
		ForceAttemptHTTP2:      false,
	}
}

// BarkClient delivers one push per call with a bounded timeout and a limited
// retry budget. Errors are sanitized categories plus the HTTP status: the
// request URL carries the device key, so neither the URL nor a remote
// response body may ever appear in an error.
type BarkClient struct {
	base    *url.URL
	client  *http.Client
	retries int
}

// BarkSendBudget is the worst-case wall-clock budget for one logical push:
// every HTTP attempt can consume the per-attempt timeout, and retry waits are
// 500ms, 1s, 1.5s, ... before later attempts.
func BarkSendBudget(timeout time.Duration, retries int) time.Duration {
	const maxBudget = 10 * time.Minute
	if retries > 100 {
		retries = 100
	}
	if retries < 0 {
		retries = 0
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	if timeout > maxBudget {
		timeout = maxBudget
	}
	total := time.Duration(retries+1) * timeout
	if total > maxBudget {
		return maxBudget
	}
	for attempt := 1; attempt <= retries; attempt++ {
		total += time.Duration(attempt) * 500 * time.Millisecond
		if total > maxBudget {
			return maxBudget
		}
	}
	return total
}

// NewBarkClient builds the push client. The optional allowPrivate argument
// permits loopback and private dials for fixture tests; production omits it.
// Plain-http URLs are restricted to local hosts by parseBarkURL, so their
// dials need the allowance to reach a loopback endpoint at all.
func NewBarkClient(baseURL string, timeout time.Duration, retries int, allowPrivate ...bool) (*BarkClient, error) {
	base, err := parseBarkURL(baseURL)
	if err != nil {
		return nil, fmt.Errorf("usage: %v", err)
	}
	if retries < 0 {
		retries = 0
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	private := len(allowPrivate) > 0 && allowPrivate[0]
	transport := barkTransport(private)
	return &BarkClient{
		base: base,
		client: &http.Client{
			Timeout:       timeout,
			Transport:     transport,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
			Jar:           nil,
		},
		retries: retries,
	}, nil
}

// Push posts a JSON message to the configured Bark endpoint.
func (c *BarkClient) Push(ctx context.Context, title, body string) error {
	payload, err := json.Marshal(map[string]string{"title": title, "body": body})
	if err != nil {
		return err
	}
	var lastErr error
	for attempt := 0; attempt <= c.retries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return fmt.Errorf("bark push failed: cancelled before retry")
			case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
			}
		}
		err := c.attempt(ctx, payload)
		if err == nil {
			return nil
		}
		lastErr = err
	}
	return lastErr
}

func (c *BarkClient) attempt(ctx context.Context, payload []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base.String(), bytes.NewReader(payload))
	if err != nil {
		// Building the request cannot leak the URL: it just fails.
		return fmt.Errorf("bark push failed: request error")
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("Accept", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		// err (typically *url.Error) embeds the full URL with the device
		// key; collapse it to a category and the error code only.
		return fmt.Errorf("bark push failed: connection error (%s)", urlErrorKind(err))
	}
	defer resp.Body.Close()
	// Check Bark's application acknowledgement without echoing remote text,
	// which could contain the device key. Empty 2xx gateway replies remain
	// compatible, but a nonempty reply must be a successful Bark response.
	data, readErr := io.ReadAll(io.LimitReader(resp.Body, 8193))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if readErr != nil || len(data) > 8192 {
			return errors.New("bark push failed: incomplete acknowledgement")
		}
		if len(bytes.TrimSpace(data)) > 0 {
			var acknowledgement struct {
				Code *int `json:"code"`
			}
			if err := json.Unmarshal(data, &acknowledgement); err != nil || acknowledgement.Code == nil {
				return errors.New("bark push failed: invalid acknowledgement")
			}
			if *acknowledgement.Code != 200 {
				return errors.New("bark push failed: endpoint rejected notification")
			}
		}
		return nil
	}
	return fmt.Errorf("bark push failed: HTTP status %d", resp.StatusCode)
}

// urlErrorKind collapses a transport error to a category. The underlying
// *url.Error string contains the full request URL including the device key
// and must never reach logs or the database.
func urlErrorKind(err error) string {
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "timeout"
	}
	return "network"
}
