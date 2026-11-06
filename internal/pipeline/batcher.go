// Package pipeline aggregates usage counts in memory.
package pipeline

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"metermesh/internal/domain"
)

// ErrQueueFull means this node is at its local event limit.
var ErrQueueFull = errors.New("usage queue is full")

// Counters track how many events this node kept and how many rows it wrote.
// events_accepted divided by rows_flushed is the compression of the flush.
type Counters struct {
	Accepted        atomic.Int64
	Forwarded       atomic.Int64
	RowsFlushed     atomic.Int64
	QuantityFlushed atomic.Int64
}

// Store writes one flush of hourly deltas. Quantities are increments.
type Store interface {
	Upsert(ctx context.Context, totals map[domain.AggregateKey]int64) error
}

// Batcher accepts events through a bounded queue and totals them by hour.
// totals is everything accepted since the process started. pending is the
// part not yet acknowledged by the store.
type Batcher struct {
	events   chan domain.UsageEvent
	log      *slog.Logger
	counters *Counters

	mu      sync.Mutex
	totals  map[domain.AggregateKey]int64
	pending map[domain.AggregateKey]int64
}

// New returns a batcher whose queue holds at most queueSize events.
func New(queueSize int, log *slog.Logger) *Batcher {
	if log == nil {
		log = slog.Default()
	}
	return &Batcher{
		events:   make(chan domain.UsageEvent, queueSize),
		log:      log,
		counters: &Counters{},
		totals:   make(map[domain.AggregateKey]int64),
		pending:  make(map[domain.AggregateKey]int64),
	}
}

// Counters returns the compression counters for this node.
func (b *Batcher) Counters() *Counters {
	return b.counters
}

// Submit enqueues an event. It returns ErrQueueFull instead of blocking
// when the node is already holding queueSize events.
func (b *Batcher) Submit(event domain.UsageEvent) error {
	select {
	case b.events <- event:
		return nil
	default:
		return ErrQueueFull
	}
}

// Run folds queued events into the in-memory totals until ctx is canceled,
// then drains anything still sitting in the queue.
func (b *Batcher) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			b.drain()
			return
		case event := <-b.events:
			b.add(event)
		}
	}
}

// Total returns the current quantity for one hourly key.
func (b *Batcher) Total(key domain.AggregateKey) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.totals[key]
}

// Flush hands unsent totals to the store. A failed write puts them back
// so a later flush can retry.
func (b *Batcher) Flush(ctx context.Context, store Store) error {
	batch := b.takePending()
	if len(batch) == 0 {
		return nil
	}
	if err := store.Upsert(ctx, batch); err != nil {
		b.restore(batch)
		b.log.Error("flush failed", "error", err, "rows", len(batch))
		return err
	}

	var quantity int64
	for _, n := range batch {
		quantity += n
	}
	b.counters.RowsFlushed.Add(int64(len(batch)))
	b.counters.QuantityFlushed.Add(quantity)
	b.log.Info("flushed usage", "rows", len(batch), "quantity", quantity)
	return nil
}

// Snapshot copies the totals accepted since this process started.
func (b *Batcher) Snapshot() map[domain.AggregateKey]int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[domain.AggregateKey]int64, len(b.totals))
	for key, qty := range b.totals {
		out[key] = qty
	}
	return out
}

func (b *Batcher) drain() {
	for {
		select {
		case event := <-b.events:
			b.add(event)
		default:
			return
		}
	}
}

func (b *Batcher) add(event domain.UsageEvent) {
	key := event.Key(time.Now())
	b.mu.Lock()
	b.totals[key] += event.Quantity
	b.pending[key] += event.Quantity
	total := b.totals[key]
	b.mu.Unlock()

	b.log.Info("usage aggregated",
		"customer_id", key.CustomerID,
		"metric", key.Metric,
		"window", key.Window.Format(time.RFC3339),
		"quantity", event.Quantity,
		"total", total,
	)
}

func (b *Batcher) takePending() map[domain.AggregateKey]int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.pending
	b.pending = make(map[domain.AggregateKey]int64)
	return out
}

func (b *Batcher) restore(batch map[domain.AggregateKey]int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for key, qty := range batch {
		b.pending[key] += qty
	}
}
