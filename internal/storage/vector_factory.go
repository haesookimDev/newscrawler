package storage

import (
	"os"
	"strings"

	"xgen-crawler/internal/config"
)

// NewVectorStore selects a vector store implementation based on configuration.
func NewVectorStore(cfg config.VectorDBConfig) (VectorStore, error) {
	provider := strings.TrimSpace(strings.ToLower(cfg.Provider))
	if provider == "" {
		return nil, nil
	}

	switch provider {
	case "qdrant":
		embedBase := strings.TrimSpace(os.Getenv("XGEN_EMBEDDING_BASE_URL"))
		if embedBase == "" {
			embedBase = "http://embedding-service:8000"
		}
		return NewQdrantStore(cfg, embedBase)
	default:
		return NoopVectorStore{}, nil
	}
}
