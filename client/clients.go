package client

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
)

// systemNamespaces are Temporal's own internal namespaces, never user workflow namespaces -
// excluded from discovery so the collector never tries to poll/alert on them.
var systemNamespaces = map[string]bool{
	"temporal-system": true,
}

// Registry is a thread-safe, auto-discovering set of read-only Temporal clients, one per
// namespace actually registered on the cluster. Nothing about which namespaces exist is
// hardcoded anywhere in this service - Refresh queries Temporal itself (ListNamespaces) and
// adds/removes clients to match, so a namespace created after this service started is picked
// up automatically on the next refresh, with no restart and no config change. The filter (set
// by the operator via OBSERVER_NAMESPACES, not baked into any code path) narrows this down to
// an explicit allowlist; an empty filter only ever reaches here when config.Config.Validate
// has already confirmed the operator explicitly opted into monitoring every namespace on the
// cluster (OBSERVER_NAMESPACES=*) - an unset/empty OBSERVER_NAMESPACES fails startup instead.
type Registry struct {
	hostPort string
	filter   map[string]bool // nil/empty = no filter, monitor everything (requires explicit opt-in - see above)
	mu       sync.RWMutex
	clients  map[string]client.Client
}

// NewRegistry connects a bootstrap client (namespace choice is irrelevant - ListNamespaces is
// a cluster-wide call, not scoped to one namespace) and performs an initial discovery pass.
// namespaceFilter, when non-empty, restricts discovery to exactly those names.
func NewRegistry(ctx context.Context, hostPort string, namespaceFilter []string) (*Registry, error) {
	r := &Registry{hostPort: hostPort, clients: map[string]client.Client{}}
	if len(namespaceFilter) > 0 {
		r.filter = make(map[string]bool, len(namespaceFilter))
		for _, ns := range namespaceFilter {
			r.filter[ns] = true
		}
	}
	if err := r.Refresh(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

// Refresh queries Temporal for the current set of namespaces and reconciles this registry's
// clients to match: connects any newly-seen namespace, closes and drops any that no longer
// exist. Read-only throughout - this service never starts, signals, or cancels a workflow.
func (r *Registry) Refresh(ctx context.Context) error {
	bootstrap, err := client.NewLazyClient(client.Options{HostPort: r.hostPort, Namespace: "default"})
	if err != nil {
		return fmt.Errorf("failed to connect to Temporal at %s: %w", r.hostPort, err)
	}
	defer bootstrap.Close()

	resp, err := bootstrap.WorkflowService().ListNamespaces(ctx, &workflowservice.ListNamespacesRequest{})
	if err != nil {
		return fmt.Errorf("failed to list namespaces: %w", err)
	}

	seen := map[string]bool{}
	for _, nsInfo := range resp.GetNamespaces() {
		name := nsInfo.GetNamespaceInfo().GetName()
		if name == "" || systemNamespaces[name] {
			continue
		}
		if r.filter != nil && !r.filter[name] {
			continue
		}
		seen[name] = true

		r.mu.RLock()
		_, exists := r.clients[name]
		r.mu.RUnlock()
		if exists {
			continue
		}

		c, err := client.NewLazyClient(client.Options{HostPort: r.hostPort, Namespace: name})
		if err != nil {
			log.Printf("[client] failed to create client for newly-discovered namespace %q: %v", name, err)
			continue
		}
		r.mu.Lock()
		r.clients[name] = c
		r.mu.Unlock()
		log.Printf("[client] discovered namespace %q, Temporal client ready at %s", name, r.hostPort)
	}

	r.mu.Lock()
	for name, c := range r.clients {
		if !seen[name] {
			c.Close()
			delete(r.clients, name)
			log.Printf("[client] namespace %q no longer exists, closed its client", name)
		}
	}
	r.mu.Unlock()
	return nil
}

// RunAutoDiscovery periodically calls Refresh so namespaces created after startup are picked
// up without a restart.
func (r *Registry) RunAutoDiscovery(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.Refresh(ctx); err != nil {
				log.Printf("[client] namespace discovery refresh failed: %v", err)
			}
		}
	}
}

// Snapshot returns a point-in-time copy of the current namespace -> client map, safe to
// range over without holding the registry's lock.
func (r *Registry) Snapshot() map[string]client.Client {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]client.Client, len(r.clients))
	for ns, c := range r.clients {
		out[ns] = c
	}
	return out
}

// Namespaces returns the current set of discovered namespace names.
func (r *Registry) Namespaces() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.clients))
	for ns := range r.clients {
		out = append(out, ns)
	}
	return out
}

func (r *Registry) CloseAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.clients {
		c.Close()
	}
}
