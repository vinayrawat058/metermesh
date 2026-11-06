// Package repository writes aggregated usage to PostgreSQL.
package repository

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"metermesh/internal/domain"
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS customer_usage (
    customer_id  TEXT        NOT NULL,
    metric       TEXT        NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    quantity     BIGINT      NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (customer_id, metric, window_start)
)`

const upsertSQL = `
INSERT INTO customer_usage (customer_id, metric, window_start, quantity)
VALUES ($1, $2, $3, $4)
ON CONFLICT (customer_id, metric, window_start)
DO UPDATE SET quantity = customer_usage.quantity + EXCLUDED.quantity,
              updated_at = now()`

// Postgres stores hourly usage increments.
type Postgres struct {
	url string

	mu sync.Mutex
	db *sql.DB
}

// New returns a client that connects on the first write.
func New(url string) *Postgres {
	return &Postgres{url: url}
}

// Upsert adds each quantity to the stored hourly total.
func (p *Postgres) Upsert(ctx context.Context, totals map[domain.AggregateKey]int64) error {
	if len(totals) == 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.connect(ctx); err != nil {
		return err
	}
	if err := p.upsert(ctx, totals); err != nil {
		_ = p.close()
		return err
	}
	return nil
}

// Quantity reads one stored hourly total. Missing rows are zero.
func (p *Postgres) Quantity(ctx context.Context, key domain.AggregateKey) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.connect(ctx); err != nil {
		return 0, err
	}

	var quantity int64
	err := p.db.QueryRowContext(ctx, `
		SELECT quantity
		FROM customer_usage
		WHERE customer_id = $1 AND metric = $2 AND window_start = $3`,
		key.CustomerID, key.Metric, key.Window,
	).Scan(&quantity)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return quantity, nil
}

// Close closes the database connection.
func (p *Postgres) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.close()
}

func (p *Postgres) connect(ctx context.Context) error {
	if p.db != nil {
		return nil
	}
	db, err := sql.Open("pgx", p.url)
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	db.SetMaxOpenConns(4)
	db.SetConnMaxLifetime(30 * time.Minute)

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return fmt.Errorf("ping postgres: %w", err)
	}
	if _, err := db.ExecContext(pingCtx, schemaSQL); err != nil {
		_ = db.Close()
		return fmt.Errorf("ensure customer_usage: %w", err)
	}
	p.db = db
	return nil
}

func (p *Postgres) upsert(ctx context.Context, totals map[domain.AggregateKey]int64) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin flush: %w", err)
	}
	defer tx.Rollback()

	for key, quantity := range totals {
		if quantity == 0 {
			continue
		}
		if _, err := tx.ExecContext(ctx, upsertSQL, key.CustomerID, key.Metric, key.Window, quantity); err != nil {
			return fmt.Errorf("upsert usage: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit flush: %w", err)
	}
	return nil
}

func (p *Postgres) close() error {
	if p.db == nil {
		return nil
	}
	err := p.db.Close()
	p.db = nil
	return err
}
