package application

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"proxypool/internal/domain"
	"proxypool/internal/integrations/sites"
	"proxypool/internal/integrations/webshare"
)

type AccountConfig struct {
	VendorID string `json:"vendor_id"`
	Protocol string `json:"protocol"`
	Mode     string `json:"mode,omitempty"`
	PageSize int    `json:"page_size,omitempty"`
	MaxPages int    `json:"max_pages,omitempty"`
}

func (c AccountConfig) Validate() error {
	if c.VendorID != "webshare" {
		return errors.New("only the verified webshare direct-list contract is supported")
	}
	if c.Mode != "" && c.Mode != "direct" {
		return errors.New("account mode must be direct; backbone/rotating modes are not this adapter")
	}
	return (webshare.Config{Protocol: domain.Transport(c.Protocol), PageSize: c.PageSize, MaxPages: c.MaxPages}).Validate()
}

type sourceSecretBox struct{ aead cipher.AEAD }

// ConfigureSourceSecrets is a boot-time setting. It reuses the deployment
// master-key configuration with separate derivation/AAD, never IdentitySecret
// or the public admin token. Missing configuration prevents secret writes.
func (s *IngestService) ConfigureSourceSecrets(master string) error {
	var box *sourceSecretBox
	if master != "" {
		if len(master) < 32 {
			return errors.New("secret_encryption_key must be at least 32 bytes")
		}
		key := sha256.Sum256([]byte("proxy-pool/source-account-secrets/v1\x00" + master))
		block, err := aes.NewCipher(key[:])
		if err != nil {
			return err
		}
		aead, err := cipher.NewGCM(block)
		if err != nil {
			return err
		}
		box = &sourceSecretBox{aead: aead}
	}
	s.fetchMu.Lock()
	s.secretBox = box
	s.fetchMu.Unlock()
	return nil
}

func (s *IngestService) accountSecretBox() *sourceSecretBox {
	s.fetchMu.Lock()
	defer s.fetchMu.Unlock()
	return s.secretBox
}

func (b *sourceSecretBox) seal(id string, version int64, value string) (string, error) {
	nonce := make([]byte, b.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	raw := b.aead.Seal(nonce, nonce, []byte(value), []byte(fmt.Sprintf("source/%s/secret/%d", id, version)))
	return "v1:" + base64.RawStdEncoding.EncodeToString(raw), nil
}

func (b *sourceSecretBox) open(id string, version int64, ciphertext string) (string, error) {
	if !strings.HasPrefix(ciphertext, "v1:") {
		return "", errors.New("invalid ciphertext version")
	}
	raw, err := base64.RawStdEncoding.DecodeString(strings.TrimPrefix(ciphertext, "v1:"))
	if err != nil || len(raw) < b.aead.NonceSize() {
		return "", errors.New("invalid ciphertext")
	}
	value, err := b.aead.Open(nil, raw[:b.aead.NonceSize()], raw[b.aead.NonceSize():], []byte(fmt.Sprintf("source/%s/secret/%d", id, version)))
	if err != nil {
		return "", errors.New("account secret could not be decrypted with the configured master key")
	}
	return string(value), nil
}

type SourceSecretStatus struct {
	SourceID        string     `json:"source_id"`
	Configured      bool       `json:"configured"`
	SecretVersion   int64      `json:"secret_version"`
	ConfigVersion   int64      `json:"config_version"`
	EncryptionReady bool       `json:"encryption_ready"`
	UpdatedAt       *time.Time `json:"updated_at"`
}

func (s *IngestService) SourceSecretStatus(ctx context.Context, id string) (SourceSecretStatus, *domain.Error) {
	src, err := s.store.GetSource(ctx, id)
	if err != nil {
		return SourceSecretStatus{}, domain.AsError(err)
	}
	if src.AdapterType != AdapterAccount {
		return SourceSecretStatus{}, domain.InvalidRequest("only account_list sources have account secrets")
	}
	record, err := s.store.SourceAccount(ctx, id)
	if err != nil {
		return SourceSecretStatus{}, domain.StoreUnavailable(err)
	}
	return SourceSecretStatus{SourceID: id, Configured: record.Ciphertext != "", SecretVersion: record.SecretVersion, ConfigVersion: record.ConfigVersion, EncryptionReady: s.accountSecretBox() != nil, UpdatedAt: record.UpdatedAt}, nil
}

func (s *IngestService) SetSourceSecret(ctx context.Context, id string, expectedVersion int64, value string) (SourceSecretStatus, *domain.Error) {
	if expectedVersion <= 0 {
		return SourceSecretStatus{}, domain.InvalidRequest("config_version is required for secret rotation/clear")
	}
	value = strings.TrimSpace(value)
	if len(value) > 4096 || strings.ContainsFunc(value, func(r rune) bool { return r <= ' ' || r >= 127 }) {
		return SourceSecretStatus{}, domain.InvalidRequest("secret must be a bounded printable API key without whitespace")
	}
	status, derr := s.SourceSecretStatus(ctx, id)
	if derr != nil {
		return SourceSecretStatus{}, derr
	}
	if status.ConfigVersion != expectedVersion {
		return SourceSecretStatus{}, domain.E("SOURCE_VERSION_CONFLICT", 409, "source changed; reload before rotating its secret")
	}
	ciphertext := ""
	if value != "" {
		box := s.accountSecretBox()
		if box == nil {
			return SourceSecretStatus{}, domain.E("SECRET_ENCRYPTION_NOT_CONFIGURED", 409, "configure secret_encryption_key in the deployment before storing account secrets")
		}
		var err error
		ciphertext, err = box.seal(id, status.SecretVersion+1, value)
		if err != nil {
			return SourceSecretStatus{}, domain.E("SECRET_ENCRYPTION_FAILED", 500, "account secret could not be encrypted")
		}
	}
	if err := s.store.ReplaceSourceSecret(ctx, id, expectedVersion, status.SecretVersion, ciphertext, s.Now()); err != nil {
		return SourceSecretStatus{}, domain.AsError(err)
	}
	s.static.invalidate(id)
	return s.SourceSecretStatus(ctx, id)
}

func (s *IngestService) readAccountToken(ctx context.Context, sourceID string) (string, *domain.Error) {
	box := s.accountSecretBox()
	if box == nil {
		return "", domain.E("SECRET_ENCRYPTION_NOT_CONFIGURED", 409, "configure secret_encryption_key before fetching account lists")
	}
	record, err := s.store.SourceAccount(ctx, sourceID)
	if err != nil {
		return "", domain.StoreUnavailable(err)
	}
	if record.Ciphertext == "" {
		return "", domain.E("SOURCE_AUTH_FAILED", 502, "configure a Webshare account API key before fetching")
	}
	token, err := box.open(sourceID, record.SecretVersion, record.Ciphertext)
	if err != nil {
		return "", domain.E("SOURCE_SECRET_UNREADABLE", 409, "account secret cannot be decrypted; check the deployment master key or rotate the secret")
	}
	return token, nil
}

func (s *IngestService) fetchAccountList(ctx context.Context, cfg SourceConfig, sourceID string) (webshare.Result, *domain.Error) {
	if cfg.Account == nil {
		return webshare.Result{}, domain.InvalidRequest("account source has no account config")
	}
	if err := cfg.Account.Validate(); err != nil {
		return webshare.Result{}, domain.InvalidRequest("account config: %v", err)
	}
	if cfg.Access == nil || cfg.Access.ProjectIDs == nil {
		return webshare.Result{}, domain.E("SOURCE_CONFIG_ERROR", 409, "account source must have private project access")
	}
	token, derr := s.readAccountToken(ctx, sourceID)
	if derr != nil {
		return webshare.Result{}, derr
	}
	rateKey := domain.CredentialVersion(s.secret, "webshare-account-api", token)
	if err := s.store.ClaimAccountFetch(ctx, rateKey, s.Now(), 30*time.Second); err != nil {
		return webshare.Result{}, domain.AsError(err)
	}
	result, err := webshare.Fetch(ctx, s.WebshareHTTPClient, s.WebshareEndpoint, token, webshare.Config{Protocol: domain.Transport(cfg.Account.Protocol), PageSize: cfg.Account.PageSize, MaxPages: cfg.Account.MaxPages}, s.allowPrivate)
	if err != nil {
		var ferr *webshare.Error
		if errors.As(err, &ferr) {
			derr := domain.E(ferr.Code, ferr.Status, "%s", ferr.Detail)
			derr.RetryAfter = ferr.RetryAfter
			if ferr.RetryAfter > 0 {
				writeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
				_ = s.store.ExtendAccountFetchLimit(writeCtx, rateKey, s.Now().Add(ferr.RetryAfter))
				cancel()
			}
			return webshare.Result{}, derr
		}
		return webshare.Result{}, domain.E("SOURCE_UNAVAILABLE", 503, "Webshare account list fetch failed")
	}
	return result, nil
}

func (s *IngestService) fetchAccountEntries(ctx context.Context, cfg SourceConfig, sourceID string) ([]sites.Entry, *domain.Error) {
	result, derr := s.fetchAccountList(ctx, cfg, sourceID)
	if derr != nil {
		return nil, derr
	}
	entries := make([]sites.Entry, 0, len(result.Entries))
	for _, e := range result.Entries {
		entries = append(entries, sites.Entry{Conn: e.Conn, RecordID: e.RecordID, Claims: e.Claims, VendorProtocol: string(e.Conn.Transport)})
	}
	return entries, nil
}
