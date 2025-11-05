package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"xgen-crawler/internal/config"
	"xgen-crawler/internal/crawler"
)

func main() {
	cfgPath := flag.String("config", "configs/config.yaml", "Path to crawler configuration file")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	engine, err := crawler.NewEngine(*cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialise engine: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := engine.Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "crawler stopped with error: %v\n", err)
		os.Exit(1)
	}
}
