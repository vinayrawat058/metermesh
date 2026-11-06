// Package ingestion accepts usage events over HTTP and applies local backpressure.
package ingestion

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"metermesh/internal/domain"
	"metermesh/internal/pipeline"
)

// Server is the HTTP front of one node.
type Server struct {
	nodeID  string
	batcher *pipeline.Batcher
	log     *slog.Logger
}

// New returns a handler for health, ingestion, and the in-memory totals.
func New(nodeID string, batcher *pipeline.Batcher, log *slog.Logger) http.Handler {
	if log == nil {
		log = slog.Default()
	}
	s := &Server{nodeID: nodeID, batcher: batcher, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("POST /v1/usage", s.handleUsage)
	mux.HandleFunc("GET /v1/totals", s.handleTotals)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "ok",
		"node_id": s.nodeID,
	})
}

func (s *Server) handleUsage(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	defer r.Body.Close()

	var event domain.UsageEvent
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&event); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid usage event"})
		return
	}

	event.CustomerID = strings.TrimSpace(event.CustomerID)
	event.Metric = strings.TrimSpace(event.Metric)
	if event.CustomerID == "" || event.Metric == "" || event.Quantity <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "customer_id, metric, and a positive quantity are required",
		})
		return
	}

	if err := s.batcher.Submit(event); err != nil {
		if errors.Is(err, pipeline.ErrQueueFull) {
			s.log.Warn("usage rejected", "node_id", s.nodeID, "customer_id", event.CustomerID, "error", err)
			writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "too many requests"})
			return
		}
		s.log.Error("usage submit failed", "node_id", s.nodeID, "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "submit failed"})
		return
	}

	s.log.Info("usage accepted", "node_id", s.nodeID, "customer_id", event.CustomerID, "metric", event.Metric, "quantity", event.Quantity)
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}

type totalResponse struct {
	CustomerID string    `json:"customer_id"`
	Metric     string    `json:"metric"`
	Window     time.Time `json:"window"`
	Quantity   int64     `json:"quantity"`
}

func (s *Server) handleTotals(w http.ResponseWriter, _ *http.Request) {
	snapshot := s.batcher.Snapshot()
	totals := make([]totalResponse, 0, len(snapshot))
	for key, qty := range snapshot {
		totals = append(totals, totalResponse{
			CustomerID: key.CustomerID,
			Metric:     key.Metric,
			Window:     key.Window,
			Quantity:   qty,
		})
	}
	sort.Slice(totals, func(i, j int) bool {
		if totals[i].CustomerID != totals[j].CustomerID {
			return totals[i].CustomerID < totals[j].CustomerID
		}
		if totals[i].Metric != totals[j].Metric {
			return totals[i].Metric < totals[j].Metric
		}
		return totals[i].Window.Before(totals[j].Window)
	})
	writeJSON(w, http.StatusOK, map[string]any{"totals": totals})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
