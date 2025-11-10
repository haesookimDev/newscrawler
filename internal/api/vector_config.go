package api

import (
	"strings"

	"xgen-crawler/internal/config"
)

func vectorRequestToConfig(req *VectorDBRequest) config.VectorDBConfig {
	if req == nil {
		return config.VectorDBConfig{}
	}
	cfg := config.VectorDBConfig{
		Provider:        strings.TrimSpace(req.Provider),
		Endpoint:        strings.TrimSpace(req.Endpoint),
		APIKey:          strings.TrimSpace(req.APIKey),
		Index:           strings.TrimSpace(req.Index),
		Namespace:       strings.TrimSpace(req.Namespace),
		Dimension:       req.Dimension,
		EmbeddingModel:  strings.TrimSpace(req.EmbeddingModel),
		UpsertBatchSize: req.UpsertBatchSize,
	}
	return cfg
}

func mergeVectorConfig(base, override config.VectorDBConfig) config.VectorDBConfig {
	cfg := base
	if v := strings.TrimSpace(override.Provider); v != "" {
		cfg.Provider = v
	}
	if v := strings.TrimSpace(override.Endpoint); v != "" {
		cfg.Endpoint = v
	}
	if v := strings.TrimSpace(override.APIKey); v != "" {
		cfg.APIKey = v
	}
	if v := strings.TrimSpace(override.Index); v != "" {
		cfg.Index = v
	}
	if v := strings.TrimSpace(override.Namespace); v != "" {
		cfg.Namespace = v
	}
	if override.Dimension > 0 {
		cfg.Dimension = override.Dimension
	}
	if v := strings.TrimSpace(override.EmbeddingModel); v != "" {
		cfg.EmbeddingModel = v
	}
	if override.UpsertBatchSize > 0 {
		cfg.UpsertBatchSize = override.UpsertBatchSize
	}
	return cfg
}

func vectorConfigProvided(cfg config.VectorDBConfig) bool {
	return strings.TrimSpace(cfg.Provider) != "" || strings.TrimSpace(cfg.Endpoint) != ""
}

func vectorConfigComplete(cfg config.VectorDBConfig) bool {
	return strings.TrimSpace(cfg.Provider) != "" && strings.TrimSpace(cfg.Endpoint) != ""
}
