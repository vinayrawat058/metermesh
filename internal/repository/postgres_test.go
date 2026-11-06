package repository

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"metermesh/internal/domain"
)

func TestUpsertAddsOntoStoredQuantity(t *testing.T) {
	url := os.Getenv("POSTGRES_URL")
	if url == "" {
		t.Skip("POSTGRES_URL not set")
	}

	store := New(url)
	t.Cleanup(func() { _ = store.Close() })

	key := domain.AggregateKey{
		CustomerID: fmt.Sprintf("flush-test-%d", time.Now().UnixNano()),
		Metric:     "tokens",
		Window:     time.Date(2026, 11, 6, 19, 0, 0, 0, time.UTC),
	}
	ctx := context.Background()
	if err := store.Upsert(ctx, map[domain.AggregateKey]int64{key: 10}); err != nil {
		t.Fatal(err)
	}
	if err := store.Upsert(ctx, map[domain.AggregateKey]int64{key: 5}); err != nil {
		t.Fatal(err)
	}

	got, err := store.Quantity(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if got != 15 {
		t.Fatalf("quantity = %d, want 15", got)
	}
}
