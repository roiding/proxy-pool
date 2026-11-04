package application

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"time"

	"proxypool/internal/domain"
	"proxypool/internal/storage/sqlite"
)

// maxUploadKeySources bounds one key's source allowlist so a single
// credential cannot fan out over an unbounded channel list.
const maxUploadKeySources = 256

// UploadKeyService manages the credentials that let external programs push
// imports into designated upload sources. Upload keys share the key model
// (HMAC-hashed tokens, plaintext shown once) but carry no acquisition,
// admin, job or source-management authority.
type UploadKeyService struct {
	store  *sqlite.Store
	secret string
	Now    func() time.Time
}

func NewUploadKeyService(store *sqlite.Store, secret string) *UploadKeyService {
	return &UploadKeyService{store: store, secret: secret, Now: time.Now}
}

// Create generates an upload token scoped to the given sources. The source
// list must be non-empty and every source must already exist as a custom
// upload channel; anything else is rejected so a key can never outlive the
// permissions it was created with.
func (s *UploadKeyService) Create(ctx context.Context, name string, sourceIDs []string) (domain.UploadKey, string, error) {
	name = strings.TrimSpace(name)
	if !apiKeyNamePattern.MatchString(name) {
		return domain.UploadKey{}, "", domain.InvalidRequest(
			"upload key name must match [a-z0-9][a-z0-9_-]{0,63}")
	}
	seen := make(map[string]bool, len(sourceIDs))
	scoped := make([]string, 0, len(sourceIDs))
	for _, raw := range sourceIDs {
		id := strings.TrimSpace(raw)
		if id == "" {
			return domain.UploadKey{}, "", domain.InvalidRequest("source_ids must not contain empty entries")
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		scoped = append(scoped, id)
	}
	if len(scoped) == 0 {
		return domain.UploadKey{}, "", domain.InvalidRequest("source_ids must contain at least one source")
	}
	if len(scoped) > maxUploadKeySources {
		return domain.UploadKey{}, "", domain.InvalidRequest(
			"source_ids must not exceed %d entries", maxUploadKeySources)
	}
	for _, id := range scoped {
		src, err := s.store.GetSource(ctx, id)
		if err != nil {
			return domain.UploadKey{}, "", domain.AsError(err)
		}
		if src.AdapterType != AdapterCustom {
			return domain.UploadKey{}, "", domain.InvalidRequest(
				"source %q is a %q channel; upload keys can only target custom upload sources",
				id, src.AdapterType)
		}
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return domain.UploadKey{}, "", err
	}
	token := "uk_" + base64.RawURLEncoding.EncodeToString(raw)
	now := s.Now()
	key := domain.UploadKey{
		ID:          domain.NewID("uk_"),
		Name:        name,
		TokenPrefix: token[:11],
		Enabled:     true,
		SourceIDs:   scoped,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if err := s.store.CreateUploadKey(ctx, key, s.hash(token)); err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return domain.UploadKey{}, "", domain.E("UPLOAD_KEY_NAME_EXISTS", 409,
				"an upload key named %q already exists", name)
		}
		return domain.UploadKey{}, "", domain.StoreUnavailable(err)
	}
	return key, token, nil
}

func (s *UploadKeyService) List(ctx context.Context) ([]domain.UploadKey, error) {
	return s.store.ListUploadKeys(ctx)
}

func (s *UploadKeyService) Update(ctx context.Context, id string, name *string, enabled *bool) error {
	if name != nil {
		v := strings.TrimSpace(*name)
		if !apiKeyNamePattern.MatchString(v) {
			return domain.InvalidRequest("upload key name must match [a-z0-9][a-z0-9_-]{0,63}")
		}
		name = &v
	}
	err := s.store.UpdateUploadKey(ctx, id, name, enabled, s.Now())
	if err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed") {
		return domain.E("UPLOAD_KEY_NAME_EXISTS", 409,
			"an upload key with that name already exists")
	}
	return err
}

func (s *UploadKeyService) Delete(ctx context.Context, id string) error {
	return s.store.DeleteUploadKey(ctx, id)
}

func (s *UploadKeyService) Authenticate(ctx context.Context, token string) (domain.UploadKey, bool) {
	if len(token) < 8 || len(token) > 256 {
		return domain.UploadKey{}, false
	}
	key, err := s.store.UploadKeyByHash(ctx, s.hash(token))
	if err != nil {
		return domain.UploadKey{}, false
	}
	return key, key.Enabled && len(key.SourceIDs) > 0
}

func (s *UploadKeyService) hash(token string) string {
	mac := hmac.New(sha256.New, []byte(s.secret))
	_, _ = mac.Write([]byte(token))
	return hex.EncodeToString(mac.Sum(nil))
}
