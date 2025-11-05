package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"xgen-crawler/internal/api"
	"xgen-crawler/internal/config"
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

	manager := api.NewSessionManager(*baseCfg, maxConcurrency, ctx)
	server := api.NewServer(manager)

	httpServer := &http.Server{
		Addr:    *addr,
		Handler: server,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("http shutdown error: %v", err)
		}
		manager.Shutdown()
	}()

	log.Printf("API server listening on %s (max concurrency %d)", *addr, maxConcurrency)
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
