package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/getsentry/sentry-go"
	sentrygin "github.com/getsentry/sentry-go/gin"

	"TemporalObserver/client"
	"TemporalObserver/collector"
	"TemporalObserver/config"
	"TemporalObserver/db"
	"TemporalObserver/metrics"
	"TemporalObserver/routes"
)

func main() {
	cfg := config.Load()
	if err := cfg.Validate(); err != nil {
		log.Fatalf("invalid configuration: %v", err)
	}
	if cfg.MonitorAllNamespaces {
		log.Println("[config] OBSERVER_NAMESPACES=* - monitoring EVERY namespace on the cluster, including any other teams'")
	} else {
		log.Printf("[config] monitoring namespace allowlist: %v", cfg.Namespaces)
	}

	if cfg.SentryDSN != "" {
		if err := sentry.Init(sentry.ClientOptions{
			Dsn:         cfg.SentryDSN,
			Environment: "production",
			Release:     "v1.0.0",
		}); err != nil {
			log.Fatalf("sentry.Init: %s", err)
		}
	}

	if err := db.InitDB(cfg.DatabaseURL); err != nil {
		log.Fatalf("db.InitDB: %v", err)
	}
	defer db.CloseDB()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	registry, err := client.NewRegistry(ctx, cfg.TemporalHostPort, cfg.Namespaces)
	if err != nil {
		log.Fatalf("client.NewRegistry: %v", err)
	}
	defer registry.CloseAll()
	go registry.RunAutoDiscovery(ctx, cfg.NamespaceDiscoveryInterval)

	go collector.Run(ctx, cfg, registry)
	go collector.RunBaselineLoop(ctx, cfg)
	go collector.RunAlertLoop(ctx, cfg, registry)
	go metrics.RefreshLoop(ctx, cfg.AlertInterval)

	router := routes.SetupRouter(cfg, registry)
	if cfg.SentryDSN != "" {
		router.Use(sentrygin.New(sentrygin.Options{Repanic: true}))
	}

	go func() {
		if err := router.Run(cfg.ListenAddr); err != nil {
			log.Fatalf("failed to start server: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("shutting down TemporalObserver")
}
