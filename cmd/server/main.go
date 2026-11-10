// Command server is the MeterMesh node process.
// It loads this node's config, owns a share of customers, and flushes their hourly totals to Postgres.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"metermesh/internal/cluster"
	"metermesh/internal/config"
	"metermesh/internal/ingestion"
	"metermesh/internal/pipeline"
	"metermesh/internal/repository"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load()
	if err != nil {
		log.Error("config", "error", err)
		os.Exit(1)
	}

	rdb, err := cluster.OpenRedis(cfg.RedisURL)
	if err != nil {
		log.Warn("redis membership unavailable; this node owns every customer", "error", err)
		rdb = nil
	} else {
		defer rdb.Close()
	}
	dir := cluster.NewDirectory(cluster.Member{ID: cfg.NodeID, URL: cfg.AdvertiseURL}, rdb, log)
	beatCtx, cancelBeat := context.WithTimeout(context.Background(), 2*time.Second)
	dir.Beat(beatCtx)
	cancelBeat()

	batcher := pipeline.New(cfg.QueueSize, log)
	store := repository.New(cfg.PostgresURL)

	runCtx, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	flushDone := make(chan struct{})
	go func() {
		batcher.Run(runCtx)
		close(runDone)
	}()
	go dir.Run(runCtx)
	go func() {
		flushLoop(runCtx, batcher, store, cfg.FlushInterval)
		close(flushDone)
	}()

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := &http.Server{
		Addr: cfg.ListenAddr,
		Handler: ingestion.New(cfg.NodeID, batcher, dir, log, func() {
			stop()
		}),
		// WriteTimeout stays unset so the SSE metrics stream can stay open.
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-sigCtx.Done()
		log.Info("shutdown started", "node_id", cfg.NodeID)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Error("http shutdown", "error", err)
		}
		cancelRun()
	}()

	log.Info("node listening",
		"node_id", cfg.NodeID,
		"addr", cfg.ListenAddr,
		"queue_size", cfg.QueueSize,
		"flush_interval", cfg.FlushInterval.String(),
		"advertise_url", cfg.AdvertiseURL,
	)
	listenErr := srv.ListenAndServe()
	cancelRun()
	<-runDone
	<-flushDone

	// Flush the last totals while this node is still listed in Redis, then leave
	// so survivors take its customers only after those counts are saved.
	flushCtx, cancelFlush := context.WithTimeout(context.Background(), 5*time.Second)
	flushErr := batcher.Flush(flushCtx, store)
	cancelFlush()
	if flushErr != nil {
		log.Error("final flush", "error", flushErr)
	} else {
		log.Info("final flush complete", "node_id", cfg.NodeID)
	}

	leaveCtx, cancelLeave := context.WithTimeout(context.Background(), 2*time.Second)
	dir.Leave(leaveCtx)
	cancelLeave()

	closeErr := store.Close()
	if closeErr != nil {
		log.Error("close postgres", "error", closeErr)
	}
	if listenErr != nil && !errors.Is(listenErr, http.ErrServerClosed) {
		log.Error("listen", "error", listenErr)
		os.Exit(1)
	}
	if flushErr != nil {
		os.Exit(1)
	}
}

func flushLoop(ctx context.Context, batcher *pipeline.Batcher, store pipeline.Store, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = batcher.Flush(flushCtx, store)
			cancel()
		}
	}
}
