package metrics

import (
	"context"
	"log"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"TemporalObserver/db"
)

var activeAlerts = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "observer_active_alerts",
	Help: "Currently unresolved TemporalObserver alerts.",
}, []string{"namespace", "workflow_type", "severity"})

func init() {
	prometheus.MustRegister(activeAlerts)
}

// RefreshLoop periodically re-derives the Prometheus gauge from the alerts table so it always
// reflects current state even between pushes.
func RefreshLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	refresh()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refresh()
		}
	}
}

func refresh() {
	counts, err := db.CountActiveAlertsBySeverity(context.Background())
	if err != nil {
		log.Printf("[metrics] failed to refresh alert gauge: %v", err)
		return
	}
	activeAlerts.Reset()
	for key, count := range counts {
		activeAlerts.WithLabelValues(key[0], key[1], key[2]).Set(float64(count))
	}
}

// PromHandler is the /metrics HTTP handler for an optional Grafana/Prometheus stack to scrape.
// Nothing in this service requires Prometheus to be present; this is purely additive.
var PromHandler = promhttp.Handler
