package application

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"regexp"
	"strings"
	"time"

	"proxypool/internal/domain"
	"proxypool/internal/storage/sqlite"
)

var apiKeyNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

type KeyService struct {
	store  *sqlite.Store
	secret string
	Now    func() time.Time
}

func NewKeyService(store *sqlite.Store, secret string) *KeyService {
	return &KeyService{store: store, secret: secret, Now: time.Now}
}

// Create generates a downstream token and stores only its HMAC hash. The
// plaintext token is returned once and cannot be recovered later.
func (s *KeyService) Create(ctx context.Context, name string, defaultCooldown time.Duration) (domain.APIKey, string, error) {
	return s.CreateForProject(ctx, sqlite.DefaultProjectID, name, defaultCooldown)
}

// CreateForProject generates a token bound to an existing project. Project
// CD, defaults, and source allowlist follow the project, not the key.
func (s *KeyService) CreateForProject(ctx context.Context, projectID, name string,
	defaultCooldown time.Duration) (domain.APIKey, string, error) {
	if projectID == "" {
		projectID = sqlite.DefaultProjectID
	}
	if _, err := s.store.GetProject(ctx, projectID); err != nil {
		return domain.APIKey{}, "", err
	}
	name = strings.TrimSpace(name)
	if !apiKeyNamePattern.MatchString(name) {
		return domain.APIKey{}, "", domain.InvalidRequest(
			"key name must match [a-z0-9][a-z0-9_-]{0,63}")
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return domain.APIKey{}, "", err
	}
	token := "pk_" + base64.RawURLEncoding.EncodeToString(raw)
	now := s.Now()
	key := domain.APIKey{
		ID:                domain.NewID("key_"),
		ProjectID:         projectID,
		Name:              name,
		TokenPrefix:       token[:11],
		Enabled:           true,
		DefaultCooldownMS: defaultCooldown.Milliseconds(),
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	if err := s.store.CreateAPIKey(ctx, key, s.hash(token)); err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return domain.APIKey{}, "", domain.E("API_KEY_NAME_EXISTS", 409,
				"an API key named %q already exists", name)
		}
		return domain.APIKey{}, "", err
	}
	return key, token, nil
}

func (s *KeyService) Authenticate(ctx context.Context, token string) (domain.APIKey, bool) {
	if len(token) < 8 || len(token) > 256 {
		return domain.APIKey{}, false
	}
	key, err := s.store.APIKeyByHash(ctx, s.hash(token))
	if err != nil {
		return domain.APIKey{}, false
	}
	return key, key.Enabled
}

func (s *KeyService) List(ctx context.Context) ([]domain.APIKey, error) {
	return s.store.ListAPIKeys(ctx)
}

func (s *KeyService) Update(ctx context.Context, id string, name *string,
	enabled *bool, defaultCooldown *time.Duration) error {
	if name != nil {
		v := strings.TrimSpace(*name)
		if !apiKeyNamePattern.MatchString(v) {
			return domain.InvalidRequest("key name must match [a-z0-9][a-z0-9_-]{0,63}")
		}
		name = &v
	}
	var cd *int64
	if defaultCooldown != nil {
		v := defaultCooldown.Milliseconds()
		cd = &v
	}
	err := s.store.UpdateAPIKey(ctx, id, name, enabled, cd, s.Now())
	if err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed") {
		return domain.E("API_KEY_NAME_EXISTS", 409, "an API key with that name already exists")
	}
	return err
}

func (s *KeyService) Delete(ctx context.Context, id string) error {
	return s.store.DeleteAPIKey(ctx, id)
}

func (s *KeyService) hash(token string) string {
	mac := hmac.New(sha256.New, []byte(s.secret))
	_, _ = mac.Write([]byte(token))
	return hex.EncodeToString(mac.Sum(nil))
}
