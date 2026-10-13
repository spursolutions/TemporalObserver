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
	"TemporalObserver/routes"
)

func main() {
	cfg := config.Load()

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

	clients, err := client.NewClients(cfg.TemporalHostPort, cfg.Namespaces)
	if err != nil {
		log.Fatalf("client.NewClients: %v", err)
	}
	defer client.CloseAll(clients)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go collector.Run(ctx, cfg, clients)

	router := routes.SetupRouter()
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
