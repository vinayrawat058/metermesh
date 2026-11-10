package ingestion

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// MetricsEvent is one SSE payload for the live dashboard.
type MetricsEvent struct {
	NodeID            string              `json:"node_id"`
	Time              time.Time           `json:"time"`
	EventsAccepted    int64               `json:"events_accepted"`
	EventsForwarded   int64               `json:"events_forwarded"`
	RowsFlushed       int64               `json:"rows_flushed"`
	QuantityFlushed   int64               `json:"quantity_flushed"`
	EventsPerSec      float64             `json:"events_per_sec"`
	PendingKeys       int                 `json:"pending_keys"`
	TotalsKeys        int                 `json:"totals_keys"`
	EventsPerRow      float64             `json:"events_per_row,omitempty"`
	Members           []map[string]string `json:"members"`
	LastFlushAt       *time.Time          `json:"last_flush_at,omitempty"`
	LastFlushRows     int                 `json:"last_flush_rows,omitempty"`
	LastFlushQuantity int64               `json:"last_flush_quantity,omitempty"`
}

func (s *Server) handleMetricsStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var prevAccepted int64
	var prevTime time.Time
	first := true

	send := func() error {
		event := s.snapshotMetrics(prevAccepted, prevTime, first)
		prevAccepted = event.EventsAccepted
		prevTime = event.Time
		first = false
		payload, err := json.Marshal(event)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "event: metrics\ndata: %s\n\n", payload); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}

	if err := send(); err != nil {
		return
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			if err := send(); err != nil {
				return
			}
		}
	}
}

func (s *Server) snapshotMetrics(prevAccepted int64, prevTime time.Time, first bool) MetricsEvent {
	counters := s.batcher.Counters()
	accepted := counters.Accepted.Load()
	rows := counters.RowsFlushed.Load()
	now := time.Now().UTC()

	var rate float64
	if !first && !prevTime.IsZero() {
		elapsed := now.Sub(prevTime).Seconds()
		if elapsed > 0 {
			rate = float64(accepted-prevAccepted) / elapsed
			if rate < 0 {
				rate = 0
			}
		}
	}

	event := MetricsEvent{
		NodeID:          s.nodeID,
		Time:            now,
		EventsAccepted:  accepted,
		EventsForwarded: counters.Forwarded.Load(),
		RowsFlushed:     rows,
		QuantityFlushed: counters.QuantityFlushed.Load(),
		EventsPerSec:    rate,
		PendingKeys:     s.batcher.PendingKeys(),
		TotalsKeys:      len(s.batcher.Snapshot()),
		Members:         s.memberViews(),
	}
	if rows > 0 {
		event.EventsPerRow = float64(accepted) / float64(rows)
	}
	if at, flushRows, qty, ok := s.batcher.LastFlush(); ok {
		event.LastFlushAt = &at
		event.LastFlushRows = flushRows
		event.LastFlushQuantity = qty
	}
	return event
}
