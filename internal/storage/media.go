package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"mime"
	"os"
	"path/filepath"
	"strings"
)

// FileMediaStore writes image binaries to the local filesystem.
type FileMediaStore struct {
	baseDir string
}

// NewFileMediaStore constructs a filesystem-backed media store.
func NewFileMediaStore(baseDir string) (*FileMediaStore, error) {
	if strings.TrimSpace(baseDir) == "" {
		return nil, fmt.Errorf("base directory must be provided")
	}
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("create media directory: %w", err)
	}
	return &FileMediaStore{baseDir: baseDir}, nil
}

// SaveImage persists the image bytes to disk and returns the relative path.
func (s *FileMediaStore) SaveImage(ctx context.Context, image ImageDocument) (string, error) {
	if s == nil || len(image.Data) == 0 {
		return "", nil
	}
	if ctx != nil {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
		}
	}
	sum := sha256.Sum256(image.Data)
	hash := hex.EncodeToString(sum[:])
	subdir := hash[:2]
	filename := hash[2:]
	ext := pickImageExtension(image.ContentType, image.SourceURL)
	if ext != "" && !strings.HasPrefix(ext, ".") {
		ext = "." + ext
	}
	relative := filepath.Join(subdir, filename+ext)
	fullPath := filepath.Join(s.baseDir, relative)

	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		return "", fmt.Errorf("create media subdir: %w", err)
	}
	if _, err := os.Stat(fullPath); err != nil {
		if !os.IsNotExist(err) {
			return "", fmt.Errorf("stat media file: %w", err)
		}
		if err := os.WriteFile(fullPath, image.Data, 0o644); err != nil {
			return "", fmt.Errorf("write media file: %w", err)
		}
	}
	return relative, nil
}

func pickImageExtension(contentType, sourceURL string) string {
	ct := strings.ToLower(strings.TrimSpace(contentType))
	if ct != "" {
		if exts, err := mime.ExtensionsByType(ct); err == nil {
			for _, ext := range exts {
				if ext != "" {
					return strings.TrimPrefix(ext, ".")
				}
			}
		}
	}
	if idx := strings.Index(sourceURL, "?"); idx >= 0 {
		sourceURL = sourceURL[:idx]
	}
	if dot := strings.LastIndex(sourceURL, "."); dot >= 0 && dot < len(sourceURL)-1 {
		ext := sourceURL[dot+1:]
		ext = strings.ToLower(ext)
		if len(ext) <= 5 {
			return ext
		}
	}
	return ""
}
