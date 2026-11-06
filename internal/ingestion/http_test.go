package ingestion

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"metermesh/internal/pipeline"
)

func TestUsageAcceptedThenVisibleInTotals(t *testing.T) {
	batcher := pipeline.New(4, discardLog())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go batcher.Run(ctx)

	handler := New("local", batcher, discardLog())
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
	handler := New("local", pipeline.New(1, discardLog()), discardLog())
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
	handler := New("local", pipeline.New(2, discardLog()), discardLog())
	req := httptest.NewRequest(http.MethodPost, "/v1/usage", strings.NewReader(`{"customer_id":"acme","metric":"tokens","quantity":0}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", rec.Code)
	}
}

func discardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
