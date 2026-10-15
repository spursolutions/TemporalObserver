package collector

import (
	"context"
	"log"
	"time"

	"github.com/getsentry/sentry-go"

	"TemporalObserver/config"
	"TemporalObserver/db"
)

// RunBaselineLoop periodically recomputes p50/p95 duration baselines from everything
// collected so far. Separate from the main poll loop since it scans the whole table rather
// than an incremental window, and doesn't need to run as often.
func RunBaselineLoop(ctx context.Context, cfg config.Config) {
	ticker := time.NewTicker(cfg.BaselineInterval)
	defer ticker.Stop()

	recompute()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			recompute()
		}
	}
}

func recompute() {
	if err := db.RecomputeBaselines(context.Background()); err != nil {
		log.Printf("[baseline] recompute failed: %v", err)
		sentry.CaptureException(err)
	}
}
