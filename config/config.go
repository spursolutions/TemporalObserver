package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all runtime configuration for TemporalObserver, loaded from environment
// variables. Nothing here reaches into any existing service's config - this is a fully
// standalone, read-only consumer of the Temporal server's public gRPC API.
type Config struct {
	TemporalHostPort   string
	Namespaces         []string
	DatabaseURL        string
	SentryDSN          string
	ListenAddr         string
	PollInterval       time.Duration
	OpenLookbackWindow time.Duration
	HistoryFetchBatch  int
	ReconcileBatch     int
}

func Load() Config {
	return Config{
		TemporalHostPort:   getEnv("TEMPORAL_HOST_PORT", "temporal:7233"),
		Namespaces:         splitCSV(getEnv("OBSERVER_NAMESPACES", "bytelink,nexsys,walle")),
		DatabaseURL:        getEnv("DATABASE_URL", "postgres://observer:observer@temporalobserver-postgresql:5432/temporalobserver"),
		SentryDSN:          os.Getenv("SENTRY_DSN"),
		ListenAddr:         ":" + getEnv("PORT", "8080"),
		PollInterval:       getEnvDuration("OBSERVER_POLL_INTERVAL_SECONDS", 30) * time.Second,
		OpenLookbackWindow: getEnvDuration("OBSERVER_OPEN_LOOKBACK_DAYS", 30) * 24 * time.Hour,
		HistoryFetchBatch:  getEnvInt("OBSERVER_HISTORY_FETCH_BATCH", 25),
		ReconcileBatch:     getEnvInt("OBSERVER_RECONCILE_BATCH", 50),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func getEnvDuration(key string, fallback int64) time.Duration {
	return time.Duration(getEnvInt(key, int(fallback)))
}

func splitCSV(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
