// Command ops is a localhost demo control plane for MeterMesh containers.
// It can start killed node containers through the Docker engine socket.
// Do not expose this service on a public network.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := loadConfig()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "ops"})
	})
	mux.HandleFunc("POST /v1/admin/nodes/{id}/start", func(w http.ResponseWriter, r *http.Request) {
		nodeID := r.PathValue("id")
		container, err := containerForNode(cfg.Project, nodeID)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if err := dockerStart(r.Context(), cfg.DockerHost, container); err != nil {
			log.Error("start failed", "node_id", nodeID, "container", container, "error", err)
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
			return
		}
		log.Info("started node", "node_id", nodeID, "container", container)
		writeJSON(w, http.StatusAccepted, map[string]string{
			"status":    "starting",
			"node_id":   nodeID,
			"container": container,
		})
	})

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           withCORS(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Info("ops listening",
		"addr", cfg.ListenAddr,
		"project", cfg.Project,
		"docker_host", cfg.DockerHost,
	)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("listen", "error", err)
		os.Exit(1)
	}
}

type config struct {
	ListenAddr string
	Project    string
	DockerHost string
}

func loadConfig() config {
	return config{
		ListenAddr: env("LISTEN_ADDR", ":8090"),
		Project:    env("COMPOSE_PROJECT", "metermesh"),
		DockerHost: env("DOCKER_HOST", "unix:///var/run/docker.sock"),
	}
}

func containerForNode(project, nodeID string) (string, error) {
	switch nodeID {
	case "node1", "node2", "node3":
		return fmt.Sprintf("%s-%s-1", project, nodeID), nil
	default:
		return "", fmt.Errorf("unknown node id %q", nodeID)
	}
}

func dockerStart(ctx context.Context, dockerHost, container string) error {
	transport, err := dockerTransport(dockerHost)
	if err != nil {
		return err
	}
	client := &http.Client{Transport: transport, Timeout: 15 * time.Second}
	url := "http://docker/containers/" + container + "/start"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	// 204 No Content = started, 304 Not Modified = already running.
	if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotModified {
		return nil
	}
	if len(body) == 0 {
		return fmt.Errorf("docker start returned %s", resp.Status)
	}
	return fmt.Errorf("docker start returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
}

func dockerTransport(dockerHost string) (http.RoundTripper, error) {
	switch {
	case strings.HasPrefix(dockerHost, "unix://"):
		path := strings.TrimPrefix(dockerHost, "unix://")
		return &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", path)
			},
		}, nil
	case strings.HasPrefix(dockerHost, "tcp://"):
		return &http.Transport{}, nil
	default:
		return nil, fmt.Errorf("unsupported DOCKER_HOST %q", dockerHost)
	}
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func env(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}
