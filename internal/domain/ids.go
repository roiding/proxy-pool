package domain

import (
	crand "crypto/rand"
	"encoding/hex"
)

func NewID(prefix string) string {
	b := make([]byte, 12)
	if _, err := crand.Read(b); err != nil {
		panic(err)
	}
	return prefix + hex.EncodeToString(b)
}
