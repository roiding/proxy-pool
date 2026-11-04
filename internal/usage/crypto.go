package usage

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
)

// settingsKey derives the settings cipher key from the deployment master key
// with its own domain separation, mirroring the sticky-binding derivation.
func settingsKey(master string) []byte {
	sum := sha256.Sum256([]byte("proxy-pool/usage-settings/v1\x00" + master))
	return sum[:]
}

type sealedBox struct {
	gcm cipher.AEAD
}

func newSealedBox(masterKey string) (*sealedBox, error) {
	if len(masterKey) < 32 {
		return nil, errors.New("usage: secret_encryption_key must be configured (>= 32 chars) before storing secrets")
	}
	block, err := aes.NewCipher(settingsKey(masterKey))
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &sealedBox{gcm: gcm}, nil
}

// seal encrypts one field with a fresh random nonce and binds the field name
// as AAD, so ciphertexts cannot be swapped between settings.
func (b *sealedBox) seal(fieldName, plaintext string) (string, error) {
	nonce := make([]byte, b.gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", fmt.Errorf("usage: nonce: %w", err)
	}
	sealed := b.gcm.Seal(nonce, nonce, []byte(plaintext), []byte(fieldName))
	return base64.RawStdEncoding.EncodeToString(sealed), nil
}

func (b *sealedBox) open(fieldName, ciphertext string) (string, error) {
	raw, err := base64.RawStdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", fmt.Errorf("usage: decode %s: %w", fieldName, err)
	}
	if len(raw) < b.gcm.NonceSize() {
		return "", fmt.Errorf("usage: %s ciphertext too short", fieldName)
	}
	plain, err := b.gcm.Open(nil, raw[:b.gcm.NonceSize()], raw[b.gcm.NonceSize():], []byte(fieldName))
	if err != nil {
		return "", fmt.Errorf("usage: decrypt %s: ciphertext rejected (wrong master key?)", fieldName)
	}
	return string(plain), nil
}
