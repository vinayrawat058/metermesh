package pipeline

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"metermesh/internal/domain"
)

func TestSubmitAggregatesSameHour(t *testing.T) {
	b := New(4, discardLog())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	stamp := time.Date(2026, 11, 6, 14, 15, 0, 0, time.UTC)
	first := domain.UsageEvent{CustomerID: "acme", Metric: "tokens", Quantity: 10, Timestamp: stamp}
	second := first
	second.Quantity = 5
	second.Timestamp = stamp.Add(20 * time.Minute)

	for _, event := range []domain.UsageEvent{first, second} {
		if err := b.Submit(event); err != nil {
			t.Fatal(err)
		}
	}

	key := first.Key(time.Now())
	waitTotal(t, b, key, 15)
}

func TestSubmitKeepsHoursSeparate(t *testing.T) {
	b := New(4, discardLog())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	early := domain.UsageEvent{
		CustomerID: "acme",
		Metric:     "tokens",
		Quantity:   3,
		Timestamp:  time.Date(2026, 11, 6, 14, 0, 0, 0, time.UTC),
	}
	later := early
	later.Quantity = 4
	later.Timestamp = early.Timestamp.Add(time.Hour)

	if err := b.Submit(early); err != nil {
		t.Fatal(err)
	}
	if err := b.Submit(later); err != nil {
		t.Fatal(err)
	}

	waitTotal(t, b, early.Key(time.Now()), 3)
	waitTotal(t, b, later.Key(time.Now()), 4)
}

func TestFlushSendsPendingOnce(t *testing.T) {
	b := New(4, discardLog())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	stamp := time.Date(2026, 11, 6, 19, 10, 0, 0, time.UTC)
	event := domain.UsageEvent{CustomerID: "acme", Metric: "tokens", Quantity: 10, Timestamp: stamp}
	if err := b.Submit(event); err != nil {
		t.Fatal(err)
	}
	key := event.Key(time.Now())
	waitTotal(t, b, key, 10)

	store := &memStore{}
	if err := b.Flush(context.Background(), store); err != nil {
		t.Fatal(err)
	}
	if store.quantity(key) != 10 {
		t.Fatalf("stored = %d", store.quantity(key))
	}
	if err := b.Flush(context.Background(), store); err != nil {
		t.Fatal(err)
	}
	if store.quantity(key) != 10 {
		t.Fatalf("second flush stored = %d, want 10", store.quantity(key))
	}
	if b.Total(key) != 10 {
		t.Fatalf("accepted total = %d, want 10", b.Total(key))
	}
}

func TestFlushRestoresPendingWhenStoreFails(t *testing.T) {
	b := New(4, discardLog())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	event := domain.UsageEvent{
		CustomerID: "acme",
		Metric:     "tokens",
		Quantity:   7,
		Timestamp:  time.Date(2026, 11, 6, 19, 10, 0, 0, time.UTC),
	}
	if err := b.Submit(event); err != nil {
		t.Fatal(err)
	}
	key := event.Key(time.Now())
	waitTotal(t, b, key, 7)

	store := &memStore{err: errors.New("db down")}
	if err := b.Flush(context.Background(), store); err == nil {
		t.Fatal("expected flush error")
	}
	store.err = nil
	if err := b.Flush(context.Background(), store); err != nil {
		t.Fatal(err)
	}
	if store.quantity(key) != 7 {
		t.Fatalf("stored = %d, want 7", store.quantity(key))
	}
}

func TestSubmitRejectsWhenQueueIsFull(t *testing.T) {
	b := New(1, discardLog())
	event := domain.UsageEvent{CustomerID: "acme", Metric: "tokens", Quantity: 1}

	if err := b.Submit(event); err != nil {
		t.Fatal(err)
	}
	err := b.Submit(event)
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("error = %v, want ErrQueueFull", err)
	}
}

type memStore struct {
	mu     sync.Mutex
	totals map[domain.AggregateKey]int64
	err    error
}

func (m *memStore) Upsert(_ context.Context, totals map[domain.AggregateKey]int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	if m.totals == nil {
		m.totals = make(map[domain.AggregateKey]int64)
	}
	for key, qty := range totals {
		m.totals[key] += qty
	}
	return nil
}

func (m *memStore) quantity(key domain.AggregateKey) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.totals[key]
}

func discardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func waitTotal(t *testing.T, b *Batcher, key domain.AggregateKey, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if b.Total(key) == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("total = %d, want %d", b.Total(key), want)
}
