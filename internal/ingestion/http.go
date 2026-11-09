// Package ingestion accepts usage events over HTTP and applies local backpressure.
// A node that does not own the customer forwards the event to the owner.
package ingestion

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"metermesh/internal/cluster"
	"metermesh/internal/domain"
	"metermesh/internal/pipeline"
)

const forwardedHeader = "X-MeterMesh-Forwarded"

// Server is the HTTP front of one node.
type Server struct {
	nodeID   string
	batcher  *pipeline.Batcher
	dir      *cluster.Directory
	client   *http.Client
	log      *slog.Logger
	shutdown func()
}

// New returns a handler for health, ingestion, totals, ownership, and compression.
// A nil directory means this process owns every customer.
// shutdown, when set, is called by POST /v1/admin/exit for a graceful stop.
func New(nodeID string, batcher *pipeline.Batcher, dir *cluster.Directory, log *slog.Logger, shutdown func()) http.Handler {
	if log == nil {
		log = slog.Default()
	}
	s := &Server{
		nodeID:   nodeID,
		batcher:  batcher,
		dir:      dir,
		log:      log,
		client:   &http.Client{Timeout: 2 * time.Second},
		shutdown: shutdown,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("POST /v1/usage", s.handleUsage)
	mux.HandleFunc("GET /v1/totals", s.handleTotals)
	mux.HandleFunc("GET /v1/stats", s.handleStats)
	mux.HandleFunc("GET /v1/route", s.handleRoute)
	mux.HandleFunc("POST /v1/admin/exit", s.handleExit)
	return mux
}

func (s *Server) handleExit(w http.ResponseWriter, _ *http.Request) {
	if s.shutdown == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "shutdown not configured"})
		return
	}
	s.log.Info("admin exit requested", "node_id", s.nodeID)
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "shutting_down", "node_id": s.nodeID})
	go s.shutdown()
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "ok",
		"node_id": s.nodeID,
	})
}

func (s *Server) handleUsage(w http.ResponseWriter, r *http.Request) {
	event, ok := decodeUsage(w, r)
	if !ok {
		return
	}

	if r.Header.Get(forwardedHeader) == "" {
		owner := s.owner(event.CustomerID)
		if owner.ID != "" && owner.ID != s.nodeID {
			s.forward(w, event, owner)
			return
		}
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

	s.batcher.Counters().Accepted.Add(1)
	s.log.Info("usage accepted", "node_id", s.nodeID, "customer_id", event.CustomerID, "metric", event.Metric, "quantity", event.Quantity)
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}

func (s *Server) forward(w http.ResponseWriter, event domain.UsageEvent, owner cluster.Member) {
	if owner.URL == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "owner unavailable"})
		return
	}
	payload, err := json.Marshal(event)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "encode failed"})
		return
	}
	req, err := http.NewRequest(http.MethodPost, strings.TrimRight(owner.URL, "/")+"/v1/usage", bytes.NewReader(payload))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "forward failed"})
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(forwardedHeader, "1")

	resp, err := s.client.Do(req)
	if err != nil {
		s.log.Warn("forward failed", "node_id", s.nodeID, "owner", owner.ID, "customer_id", event.CustomerID, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "owner unavailable"})
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode == http.StatusAccepted {
		s.batcher.Counters().Forwarded.Add(1)
		s.log.Info("usage forwarded", "node_id", s.nodeID, "owner", owner.ID, "customer_id", event.CustomerID)
	}
	if resp.Header.Get("Content-Type") != "" {
		w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
	} else {
		w.Header().Set("Content-Type", "application/json")
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(body)
}

func (s *Server) owner(customerID string) cluster.Member {
	if s.dir == nil {
		return cluster.Member{ID: s.nodeID}
	}
	return s.dir.Owner(customerID)
}

func decodeUsage(w http.ResponseWriter, r *http.Request) (domain.UsageEvent, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	defer r.Body.Close()

	var event domain.UsageEvent
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&event); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid usage event"})
		return domain.UsageEvent{}, false
	}
	event.CustomerID = strings.TrimSpace(event.CustomerID)
	event.Metric = strings.TrimSpace(event.Metric)
	if event.CustomerID == "" || event.Metric == "" || event.Quantity <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "customer_id, metric, and a positive quantity are required",
		})
		return domain.UsageEvent{}, false
	}
	return event, true
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

func (s *Server) handleStats(w http.ResponseWriter, _ *http.Request) {
	counters := s.batcher.Counters()
	accepted := counters.Accepted.Load()
	rows := counters.RowsFlushed.Load()
	body := map[string]any{
		"node_id":          s.nodeID,
		"events_accepted":  accepted,
		"events_forwarded": counters.Forwarded.Load(),
		"rows_flushed":     rows,
		"quantity_flushed": counters.QuantityFlushed.Load(),
		"members":          s.memberViews(),
	}
	if rows > 0 {
		body["events_per_row"] = float64(accepted) / float64(rows)
	}
	writeJSON(w, http.StatusOK, body)
}

func (s *Server) handleRoute(w http.ResponseWriter, r *http.Request) {
	customerID := strings.TrimSpace(r.URL.Query().Get("customer_id"))
	if customerID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "customer_id is required"})
		return
	}
	owner := s.owner(customerID)
	writeJSON(w, http.StatusOK, map[string]any{
		"customer_id": customerID,
		"owner_id":    owner.ID,
		"owner_url":   owner.URL,
		"local":       owner.ID == s.nodeID || owner.ID == "",
	})
}

func (s *Server) memberViews() []map[string]string {
	if s.dir == nil {
		return []map[string]string{{"id": s.nodeID}}
	}
	members := s.dir.Members()
	views := make([]map[string]string, 0, len(members))
	for _, member := range members {
		views = append(views, map[string]string{"id": member.ID, "url": member.URL})
	}
	return views
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
