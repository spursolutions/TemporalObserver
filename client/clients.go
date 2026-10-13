package client

import (
	"fmt"
	"log"

	"go.temporal.io/sdk/client"
)

// NewClients connects a read-only Temporal client per namespace. TemporalObserver never
// starts, signals, or cancels workflows - only visibility/history read calls are used
// anywhere in this service.
func NewClients(hostPort string, namespaces []string) (map[string]client.Client, error) {
	clients := make(map[string]client.Client, len(namespaces))
	for _, ns := range namespaces {
		c, err := client.NewLazyClient(client.Options{
			HostPort:  hostPort,
			Namespace: ns,
		})
		if err != nil {
			// Close any clients already opened before returning the error.
			for _, opened := range clients {
				opened.Close()
			}
			return nil, fmt.Errorf("failed to create Temporal client for namespace %q: %w", ns, err)
		}
		clients[ns] = c
		log.Printf("Temporal client ready for namespace %q at %s", ns, hostPort)
	}
	return clients, nil
}

func CloseAll(clients map[string]client.Client) {
	for _, c := range clients {
		c.Close()
	}
}
