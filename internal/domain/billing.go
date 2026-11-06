// Package domain holds the billing types shared across the node.
package domain

import "time"

// UsageEvent is one reported unit of customer usage, such as API tokens.
type UsageEvent struct {
	CustomerID string    `json:"customer_id"`
	Metric     string    `json:"metric"`
	Quantity   int64     `json:"quantity"`
	Timestamp  time.Time `json:"timestamp,omitempty"`
}

// AggregateKey identifies one hourly usage total.
type AggregateKey struct {
	CustomerID string
	Metric     string
	Window     time.Time
}

// Key builds the hourly bucket for an event. A zero timestamp uses now.
func (e UsageEvent) Key(now time.Time) AggregateKey {
	ts := e.Timestamp
	if ts.IsZero() {
		ts = now
	}
	return AggregateKey{
		CustomerID: e.CustomerID,
		Metric:     e.Metric,
		Window:     ts.UTC().Truncate(time.Hour),
	}
}
