package api

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"proxypool/internal/application"
	"proxypool/internal/domain"
	"proxypool/internal/validation"
)

type acquireValidationRequest struct {
	Mode   string `json:"mode"`
	Target string `json:"target"`
}

type acquireV1Request struct {
	Count      optionalInt               `json:"count"`
	TimeoutSec *int64                    `json:"timeout"`
	CDSeconds  *int64                    `json:"cd"`
	Country    []string                  `json:"country"`
	Protocols  []string                  `json:"protocols"`
	SourceIDs  optionalStringSlice       `json:"source_ids"`
	Validation *acquireValidationRequest `json:"validation"`
	Mode       string                    `json:"mode"`
	SessionID  string                    `json:"session_id"`
}

const maxSourceFilter = 256

func (s *Server) handleAcquireV1(w http.ResponseWriter, r *http.Request) {
	var body *acquireV1Request
	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxRequestBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		writeError(w, domain.InvalidRequest("invalid JSON body: %v", err))
		return
	}
	if body == nil {
		writeError(w, domain.InvalidRequest("JSON body must be an object"))
		return
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		writeError(w, domain.InvalidRequest("unexpected trailing data after JSON object"))
		return
	}

	req, derr := s.buildAcquireV1(r, body)
	if derr != nil {
		writeError(w, derr)
		return
	}
	result, derr := s.acquire.Acquire(r.Context(), *req)
	if derr != nil {
		writeError(w, derr)
		return
	}
	s.writeAcquireResponse(w, result)
}

// wireConnection mirrors domain.Connection but controls field presence:
// the username appears only when set, and the password appears with its
// actual value (including an explicit empty string) whenever the connection
// is authenticated. No protocol alias is emitted.
type wireConnection struct {
	Transport domain.Transport `json:"transport"`
	Host      string           `json:"host"`
	Port      int              `json:"port"`
	Username  *string          `json:"username,omitempty"`
	Password  *string          `json:"password,omitempty"`
}

type wireValidation struct {
	Mode        string `json:"mode"`
	Target      string `json:"target,omitempty"`
	CompletedAt string `json:"completed_at"`
	ValidUntil  string `json:"valid_until"`
	LatencyMS   int64  `json:"latency_ms"`
	CacheHit    bool   `json:"cache_hit"`
}

type wireObservation struct {
	ExitIP      string `json:"exit_ip"`
	CountryCode string `json:"country_code,omitempty"`
	Country     string `json:"country,omitempty"`
	Region      string `json:"region,omitempty"`
	City        string `json:"city,omitempty"`
	ISP         string `json:"isp,omitempty"`
	ObservedAt  string `json:"observed_at"`
}

type wireAcquireItem struct {
	LeaseID  string         `json:"lease_id"`
	SourceID string         `json:"source_id"`
	Conn     wireConnection `json:"connection"`
	ProxyURI string         `json:"proxy_uri"`
	// ProviderExpiresAt stays a pointer so an unknown supplier deadline is
	// serialized as an explicit null, never a substituted TTL.
	ProviderExpiresAt *string          `json:"provider_expires_at"`
	Observed          *wireObservation `json:"observed,omitempty"`
	Validation        wireValidation   `json:"validation"`
	LeaseExpiresAt    string           `json:"lease_expires_at"`
}

type wireAcquireResponse struct {
	RequestID string            `json:"request_id"`
	Requested int               `json:"requested"`
	Delivered int               `json:"delivered"`
	Missing   int               `json:"missing"`
	Partial   bool              `json:"partial"`
	Reason    string            `json:"reason,omitempty"`
	Items     []wireAcquireItem `json:"items"`
	// Session fields are pointers: ordinary responses carry none of them,
	// while sticky responses always emit session_changed explicitly, even
	// when it is false.
	SessionID        *string `json:"session_id,omitempty"`
	SessionChanged   *bool   `json:"session_changed,omitempty"`
	BindingVersion   *int64  `json:"binding_version,omitempty"`
	BindingExpiresAt *string `json:"binding_expires_at,omitempty"`
	ChangeReason     *string `json:"change_reason,omitempty"`
	CooldownSeconds  int64   `json:"cooldown_seconds,omitempty"`
}

func wireTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// writeAcquireResponse serializes the acquire result to the canonical wire
// contract: one reason field, proxy_uri, three independent deadlines, and
// no-store caching for credential-bearing bodies.
func (s *Server) writeAcquireResponse(w http.ResponseWriter, result application.AcquireResult) {
	resp := wireAcquireResponse{
		RequestID: result.RequestID,
		Requested: result.Requested,
		Delivered: result.Delivered,
		Missing:   result.Missing,
		Partial:   result.Partial,
		Reason:    result.ShortfallReason,
		Items:     make([]wireAcquireItem, 0, len(result.Items)),
	}
	for _, item := range result.Items {
		wire := wireAcquireItem{
			LeaseID:  item.LeaseID,
			SourceID: item.SourceID,
			ProxyURI: item.ProxyURI,
			Validation: wireValidation{
				Mode:        item.Validation.Mode,
				Target:      item.Validation.Target,
				CompletedAt: wireTime(item.Validation.CompletedAt),
				LatencyMS:   item.Validation.LatencyMS,
				CacheHit:    item.Validation.CacheHit,
			},
			LeaseExpiresAt: wireTime(item.ExpiresAt),
		}
		conn := item.Conn
		wire.Conn = wireConnection{Transport: conn.Transport, Host: conn.Host, Port: conn.Port}
		if conn.Username != "" {
			username := conn.Username
			wire.Conn.Username = &username
		}
		if conn.Username != "" || conn.Password != "" {
			password := conn.Password
			wire.Conn.Password = &password
		}
		if item.ProviderExpiresAt != nil {
			expiry := wireTime(*item.ProviderExpiresAt)
			wire.ProviderExpiresAt = &expiry
		}
		if item.Observed != nil {
			wire.Observed = &wireObservation{
				ExitIP:      item.Observed.ExitIP,
				CountryCode: item.Observed.CountryCode,
				Country:     item.Observed.Country,
				Region:      item.Observed.Region,
				City:        item.Observed.City,
				ISP:         item.Observed.ISP,
				ObservedAt:  wireTime(item.Observed.ObservedAt),
			}
		}
		if wire.Validation.ValidUntil == "" {
			// Delivery supplies the proof deadline; an evidence record at
			// rest may not carry one, so the configured proof age applies.
			if item.Validation.ValidUntil != nil {
				wire.Validation.ValidUntil = wireTime(*item.Validation.ValidUntil)
			} else {
				wire.Validation.ValidUntil = wireTime(
					item.Validation.CompletedAt.Add(s.cfg.MaxProofAge()))
			}
		}
		resp.Items = append(resp.Items, wire)
	}
	if result.SessionID != "" {
		sessionID := result.SessionID
		changed := result.SessionChanged
		version := result.BindingVersion
		resp.SessionID = &sessionID
		resp.SessionChanged = &changed
		resp.BindingVersion = &version
		bindingExpiry := result.BindingExpiresAt
		if bindingExpiry.IsZero() && len(result.Items) == 1 {
			bindingExpiry = result.Items[0].BindingDeadline
		}
		if !bindingExpiry.IsZero() {
			expiry := wireTime(bindingExpiry)
			resp.BindingExpiresAt = &expiry
		}
		if result.SessionChanged && result.ChangeReason != "" {
			reason := result.ChangeReason
			resp.ChangeReason = &reason
		}
	}
	resp.CooldownSeconds = result.CooldownSeconds
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) buildAcquireV1(r *http.Request, body *acquireV1Request) (*application.AcquireRequest, *domain.Error) {
	if body == nil {
		return nil, domain.InvalidRequest("JSON body must be an object")
	}
	ident0 := identityFrom(r.Context())
	count := 1
	if body.Count.Present {
		if body.Count.Null {
			return nil, domain.InvalidRequest("count must not be null")
		}
		count = body.Count.Value
		if count < 1 {
			return nil, domain.InvalidRequest("count must be at least 1")
		}
		if count > s.cfg.MaxCount {
			return nil, domain.InvalidRequest("count %d exceeds the maximum of %d", count, s.cfg.MaxCount)
		}
	}

	// Default chain: request > project > global config ceiling.
	deadlineMS := ident0.DefaultTimeoutMS
	if deadlineMS > s.cfg.MaxAcquireDeadlineMS {
		deadlineMS = s.cfg.MaxAcquireDeadlineMS
	}
	if body.TimeoutSec != nil {
		if *body.TimeoutSec < 1 {
			return nil, domain.InvalidRequest("timeout must be at least 1 second")
		}
		maxSec := s.cfg.MaxAcquireDeadlineMS / 1000
		if *body.TimeoutSec > maxSec {
			return nil, domain.InvalidRequest("timeout %d seconds exceeds the maximum of %d seconds",
				*body.TimeoutSec, maxSec)
		}
		deadlineMS = *body.TimeoutSec * 1000
	}

	if body.CDSeconds != nil && *body.CDSeconds < 0 {
		return nil, domain.InvalidRequest("cd must not be negative")
	}

	countries := make([]string, 0, len(body.Country))
	for i, raw := range body.Country {
		country := strings.ToUpper(strings.TrimSpace(raw))
		if len(country) != 2 || country[0] < 'A' || country[0] > 'Z' ||
			country[1] < 'A' || country[1] > 'Z' {
			return nil, domain.InvalidRequest("country[%d] must be a two-letter ISO country code", i)
		}
		countries = append(countries, country)
	}

	transports := make([]domain.Transport, 0, len(body.Protocols))
	for _, p := range body.Protocols {
		switch domain.Transport(p) {
		case domain.TransportHTTP, domain.TransportSOCKS5:
			transports = append(transports, domain.Transport(p))
		default:
			return nil, domain.InvalidRequest("unknown protocol %q; supported: http, socks5", p)
		}
	}

	var normalizedScope *domain.ValidationScope
	if body.Validation != nil {
		switch body.Validation.Mode {
		case "":
			if body.Validation.Target != "" {
				return nil, domain.InvalidRequest(`validation.target requires validation.mode "tls"`)
			}
		case "basic":
			if body.Validation.Target != "" {
				return nil, domain.InvalidRequest("validation.target is not used by basic validation")
			}
			// An explicit basic request intentionally outranks a project
			// TLS default; the service pins profile and security hash.
			normalizedScope = &domain.ValidationScope{Mode: validation.ModeBasic.String()}
		case "tls":
			scope, derr := validation.NormalizeOptions("tls", body.Validation.Target)
			if derr != nil {
				return nil, derr
			}
			normalizedScope = &domain.ValidationScope{
				Mode:           scope.Mode,
				TargetHostPort: scope.TargetHostPort,
			}
		default:
			return nil, domain.InvalidRequest("unknown validation.mode %q; supported: basic, tls", body.Validation.Mode)
		}
	}

	sessionID := strings.TrimSpace(body.SessionID)
	switch body.Mode {
	case "", "ordinary":
		if sessionID != "" {
			return nil, domain.InvalidRequest(`session_id is only valid with mode "sticky"`)
		}
	case "sticky":
		if sessionID == "" {
			return nil, domain.InvalidRequest(`mode "sticky" requires session_id`)
		}
		if len(sessionID) > 128 {
			return nil, domain.InvalidRequest("session_id must not exceed 128 characters")
		}
		if count != 1 {
			return nil, domain.InvalidRequest("sticky mode delivers one proxy; count must be 1")
		}
	default:
		return nil, domain.InvalidRequest("unknown mode %q; supported: ordinary, sticky", body.Mode)
	}

	ident := ident0
	sourceIDs, derr := s.resolveSourceScope(ident, body.SourceIDs)
	if derr != nil {
		return nil, derr
	}
	req := &application.AcquireRequest{
		Count:      count,
		Validation: normalizedScope,
		Filters:    application.Filters{Transports: transports, SourceIDs: sourceIDs, Countries: countries},
		DeadlineMS: deadlineMS,
		CDSeconds:  body.CDSeconds,
		ConsumerID: ident.ConsumerID,
		KeyID:      ident.KeyID,
		ProjectID:  ident.ProjectID,
		Lease:      application.LeasePolicy{Mode: body.Mode},
		SessionID:  sessionID,
	}
	if body.CDSeconds == nil {
		req.CooldownMS = ident.DefaultCooldownMS
	}
	return req, nil
}

// resolveSourceScope applies the project allowlist. An omitted filter
// resolves to the allowlist for restricted projects and to "all sources"
// for unrestricted ones. An explicit array — including an empty one — is
// the exact delivery scope: an empty array means no sources, never "all".
func (s *Server) resolveSourceScope(ident authIdentity, requested optionalStringSlice) ([]string, *domain.Error) {
	if requested.Present && len(requested.Values) > maxSourceFilter {
		return nil, domain.InvalidRequest("source_ids must not exceed %d entries", maxSourceFilter)
	}
	if !ident.SourceScopeRestricted {
		if !requested.Present || requested.Null {
			return nil, nil
		}
		return requested.Values, nil
	}
	// A restricted project with an empty allowlist denies everything: an
	// empty filter would mean "all sources" downstream, so reject here.
	if len(ident.AllowedSourceIDs) == 0 {
		return nil, domain.E("SOURCE_NOT_ALLOWED", 403,
			"this project has no allowed sources")
	}
	allowed := make(map[string]bool, len(ident.AllowedSourceIDs))
	for _, id := range ident.AllowedSourceIDs {
		allowed[id] = true
	}
	if !requested.Present || requested.Null {
		return ident.AllowedSourceIDs, nil
	}
	for _, id := range requested.Values {
		if !allowed[id] {
			return nil, domain.E("SOURCE_NOT_ALLOWED", 403,
				"source %q is not allowed for this project", id)
		}
	}
	return requested.Values, nil
}
