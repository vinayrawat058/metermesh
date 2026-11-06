// Package cluster assigns each customer to one live node.
// Nodes publish a short-lived heartbeat in Redis. When a node disappears,
// its customers move to the remaining nodes.
package cluster
