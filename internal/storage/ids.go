package storage

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"time"
)

// GeneratePointID produces a deterministic UUID-like identifier from the content hash and URL.
func GeneratePointID(contentHash, url string) string {
	input := contentHash + url
	if input == "" {
		input = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	sum := sha256.Sum256([]byte(input))
	b := make([]byte, 16)
	copy(b, sum[:])
	b[6] = (b[6] & 0x0f) | 0x40 // UUID version 4 variant bits
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// EncodePageID encodes a URL into a path-safe identifier.
func EncodePageID(url string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(url))
}

// DecodePageID decodes a page identifier back into its original URL.
func DecodePageID(id string) (string, error) {
	data, err := base64.RawURLEncoding.DecodeString(id)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
