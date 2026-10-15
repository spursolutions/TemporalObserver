package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// monitorAllSentinel is the explicit opt-in value for OBSERVER_NAMESPACES that means "watch
// every namespace on the cluster". It has to be typed out on purpose - an empty/unset
// OBSERVER_NAMESPACES is treated as a misconfiguration (see Validate), never as this.
const monitorAllSentinel = "*"

// Config holds all runtime configuration for TemporalObserver, loaded from environment
// variables. Nothing here reaches into any existing service's config - this is a fully
// standalone, read-only consumer of the Temporal server's public gRPC API.
type Config struct {
	TemporalHostPort string
	// Namespaces is the operator-approved allowlist (OBSERVER_NAMESPACES, comma-separated) -
	// the only namespaces this service will ever discover, poll, or store data for. It is
	// empty unless MonitorAllNamespaces is also true (see Validate): the Temporal cluster this
	// talks to hosts other teams' namespaces too, so there is no silent "watch everything"
	// default anymore.
	Namespaces []string
	// MonitorAllNamespaces is true only when OBSERVER_NAMESPACES was explicitly set to "*" -
	// an intentional, auditable opt-in to monitoring the whole shared cluster, as opposed to
	// an empty/unset value, which Validate refuses to start with.
	MonitorAllNamespaces       bool
	DatabaseURL                string
	SentryDSN                  string
	ListenAddr                 string
	PollInterval               time.Duration
	OpenLookbackWindow         time.Duration
	HistoryFetchBatch          int
	ReconcileBatch             int
	BaselineInterval           time.Duration
	AlertInterval              time.Duration
	NamespaceDiscoveryInterval time.Duration
	APIKey                     string
	TemporalUIBaseURL          string
}

func Load() Config {
	rawNamespaces := strings.TrimSpace(os.Getenv("OBSERVER_NAMESPACES"))
	monitorAll := rawNamespaces == monitorAllSentinel

	var namespaces []string
	if !monitorAll {
		namespaces = splitCSV(rawNamespaces)
	}

	return Config{
		TemporalHostPort:           getEnv("TEMPORAL_HOST_PORT", "temporal:7233"),
		Namespaces:                 namespaces,
		MonitorAllNamespaces:       monitorAll,
		DatabaseURL:                getEnv("DATABASE_URL", "postgres://observer:observer@temporalobserver-postgresql:5432/temporalobserver"),
		SentryDSN:                  os.Getenv("SENTRY_DSN"),
		ListenAddr:                 ":" + getEnv("PORT", "8080"),
		PollInterval:               getEnvDuration("OBSERVER_POLL_INTERVAL_SECONDS", 30) * time.Second,
		OpenLookbackWindow:         getEnvDuration("OBSERVER_OPEN_LOOKBACK_DAYS", 30) * 24 * time.Hour,
		HistoryFetchBatch:          getEnvInt("OBSERVER_HISTORY_FETCH_BATCH", 25),
		ReconcileBatch:             getEnvInt("OBSERVER_RECONCILE_BATCH", 50),
		BaselineInterval:           getEnvDuration("OBSERVER_BASELINE_INTERVAL_SECONDS", 600) * time.Second,
		AlertInterval:              getEnvDuration("OBSERVER_ALERT_INTERVAL_SECONDS", 60) * time.Second,
		NamespaceDiscoveryInterval: getEnvDuration("OBSERVER_NAMESPACE_DISCOVERY_INTERVAL_SECONDS", 300) * time.Second,
		APIKey:                     os.Getenv("OBSERVER_API_KEY"),
		TemporalUIBaseURL:          getEnv("TEMPORAL_UI_BASE_URL", "http://localhost:8086"),
	}
}

// Validate refuses to start with a configuration that would silently monitor every namespace
// on the cluster - including other teams' namespaces - just because OBSERVER_NAMESPACES was
// left unset. Watching the whole cluster is still possible, but only via the explicit
// OBSERVER_NAMESPACES=* opt-in, never as a default a deployment can fall into by accident.
func (c Config) Validate() error {
	if len(c.Namespaces) == 0 && !c.MonitorAllNamespaces {
		return fmt.Errorf(
			"OBSERVER_NAMESPACES is not set - refusing to start. This Temporal cluster hosts " +
				"other teams' namespaces too, so an empty value no longer silently means \"monitor " +
				"everything\". Set OBSERVER_NAMESPACES to an explicit comma-separated allowlist " +
				"(e.g. bytelink,nexsys,walle), or set it to \"*\" to explicitly opt into monitoring " +
				"every namespace on the cluster")
	}
	return nil
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

// splitCSV returns nil for an empty string, otherwise the trimmed, non-empty comma-separated
// entries.
func splitCSV(v string) []string {
	if v == "" {
		return nil
	}
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
