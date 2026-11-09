package ingestion

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"metermesh/internal/cluster"
	"metermesh/internal/domain"
	"metermesh/internal/pipeline"
)

func TestUsageAcceptedThenVisibleInTotals(t *testing.T) {
	batcher := pipeline.New(4, discardLog())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go batcher.Run(ctx)

	handler := New("local", batcher, nil, discardLog(), nil)
	body := `{"customer_id":"acme","metric":"tokens","quantity":10}`
	req := httptest.NewRequest(http.MethodPost, "/v1/usage", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		totals := httptest.NewRecorder()
		handler.ServeHTTP(totals, httptest.NewRequest(http.MethodGet, "/v1/totals", nil))
		if totals.Code == http.StatusOK && strings.Contains(totals.Body.String(), `"quantity":10`) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for aggregated total")
}

func TestUsageRejectsWhenQueueIsFull(t *testing.T) {
	handler := New("local", pipeline.New(1, discardLog()), nil, discardLog(), nil)
	post := func() int {
		req := httptest.NewRequest(http.MethodPost, "/v1/usage", strings.NewReader(`{"customer_id":"acme","metric":"tokens","quantity":1}`))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec.Code
	}

	if code := post(); code != http.StatusAccepted {
		t.Fatalf("first status = %d", code)
	}
	if code := post(); code != http.StatusTooManyRequests {
		t.Fatalf("second status = %d, want 429", code)
	}
}

func TestUsageRejectsBadEvents(t *testing.T) {
	handler := New("local", pipeline.New(2, discardLog()), nil, discardLog(), nil)
	req := httptest.NewRequest(http.MethodPost, "/v1/usage", strings.NewReader(`{"customer_id":"acme","metric":"tokens","quantity":0}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestUsageForwardsToTheOwningNode(t *testing.T) {
	ownerBatcher := pipeline.New(4, discardLog())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ownerBatcher.Run(ctx)

	ownerServer := httptest.NewServer(New("node2", ownerBatcher, nil, discardLog(), nil))
	defer ownerServer.Close()

	edgeBatcher := pipeline.New(4, discardLog())
	dir := cluster.NewDirectory(cluster.Member{ID: "node1", URL: "http://node1:8080"}, nil, discardLog())
	dir.ReplaceMembers([]cluster.Member{{ID: "node2", URL: ownerServer.URL}})
	edge := New("node1", edgeBatcher, dir, discardLog(), nil)

	req := httptest.NewRequest(http.MethodPost, "/v1/usage", strings.NewReader(`{"customer_id":"acme","metric":"tokens","quantity":10}`))
	rec := httptest.NewRecorder()
	edge.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if edgeBatcher.Counters().Accepted.Load() != 0 || edgeBatcher.Counters().Forwarded.Load() != 1 {
		t.Fatalf("edge accepted %d forwarded %d", edgeBatcher.Counters().Accepted.Load(), edgeBatcher.Counters().Forwarded.Load())
	}
	if ownerBatcher.Counters().Accepted.Load() != 1 {
		t.Fatalf("owner accepted %d", ownerBatcher.Counters().Accepted.Load())
	}
}

func TestForwardedEventStaysOnThisNode(t *testing.T) {
	batcher := pipeline.New(4, discardLog())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go batcher.Run(ctx)

	dir := cluster.NewDirectory(cluster.Member{ID: "node1", URL: "http://node1:8080"}, nil, discardLog())
	dir.ReplaceMembers([]cluster.Member{{ID: "node2", URL: "http://127.0.0.1:9"}})
	handler := New("node1", batcher, dir, discardLog(), nil)

	req := httptest.NewRequest(http.MethodPost, "/v1/usage", strings.NewReader(`{"customer_id":"acme","metric":"tokens","quantity":4}`))
	req.Header.Set("X-MeterMesh-Forwarded", "1")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if batcher.Counters().Accepted.Load() != 1 || batcher.Counters().Forwarded.Load() != 0 {
		t.Fatal("forwarded event should be counted locally")
	}
}

func TestStatsShowCompression(t *testing.T) {
	batcher := pipeline.New(4, discardLog())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go batcher.Run(ctx)
	handler := New("local", batcher, nil, discardLog(), nil)

	for _, body := range []string{
		`{"customer_id":"acme","metric":"tokens","quantity":10}`,
		`{"customer_id":"acme","metric":"tokens","quantity":5}`,
	} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/usage", strings.NewReader(body)))
		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d", rec.Code)
		}
	}
	waitTotal(t, batcher, domain.UsageEvent{CustomerID: "acme", Metric: "tokens", Quantity: 1}.Key(time.Now()), 15)
	if err := batcher.Flush(context.Background(), okStore{}); err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/stats", nil))
	body := rec.Body.String()
	if !strings.Contains(body, `"events_accepted":2`) || !strings.Contains(body, `"rows_flushed":1`) {
		t.Fatalf("stats = %s", body)
	}
}

func TestAdminExitCallsShutdown(t *testing.T) {
	var called atomic.Bool
	handler := New("local", pipeline.New(1, discardLog()), nil, discardLog(), func() {
		called.Store(true)
	})
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/admin/exit", nil))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d", rec.Code)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if called.Load() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("shutdown was not called")
}

type okStore struct{}

func (okStore) Upsert(context.Context, map[domain.AggregateKey]int64) error { return nil }

func waitTotal(t *testing.T, b *pipeline.Batcher, key domain.AggregateKey, want int64) {
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

func discardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
