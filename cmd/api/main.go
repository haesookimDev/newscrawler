package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"xgen-crawler/internal/api"
	"xgen-crawler/internal/config"
	"xgen-crawler/internal/sessionstate"
	"xgen-crawler/internal/storage"
)

func main() {
	cfgPath := flag.String("config", "configs/config.yaml", "Path to base crawler configuration")
	addr := flag.String("addr", ":8080", "HTTP listen address")
	maxConcFlag := flag.Int("max-concurrency", 0, "Maximum concurrent crawler sessions")
	flag.Parse()

	baseCfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	maxConcurrency := resolveMaxConcurrency(*maxConcFlag)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{AddSource: true}))

	logger.Info("starting api server", "addr", *addr, "max_concurrency", maxConcurrency)

	stateStore, err := sessionstate.NewRedisStoreFromEnv()
	if err != nil {
		logger.Error("failed to initialise redis session store", "error", err)
	}
	if stateStore != nil {
		defer stateStore.Close()
	}

	manager := api.NewSessionManager(*baseCfg, maxConcurrency, ctx, logger, stateStore)

	pageStore, err := storage.NewSQLWriter(baseCfg.DB)
	if err != nil {
		logger.Error("initialise page store failed", "error", err)
		log.Fatalf("failed to initialise page store: %v", err)
	}
	defer pageStore.Close()

	server := api.NewServer(manager, pageStore, logger)

	httpServer := &http.Server{
		Addr:    *addr,
		Handler: server,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			logger.Error("http shutdown error", "error", err)
		}
		manager.Shutdown()
		_ = pageStore.Close()
	}()

	logger.Info("api server listening", "addr", *addr, "max_concurrency", maxConcurrency)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}
	log.Println("API server stopped")
}

func resolveMaxConcurrency(flagValue int) int {
	if flagValue > 0 {
		return flagValue
	}
	if raw := os.Getenv("CRAWLER_MAX_CONCURRENCY"); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			return v
		}
	}
	return 5
}
