# MeterMesh

[![CI](https://github.com/vinayrawat058/metermesh/actions/workflows/ci.yml/badge.svg)](https://github.com/vinayrawat058/metermesh/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![GHCR](https://img.shields.io/badge/GHCR-metermesh-black)](https://github.com/vinayrawat058/metermesh/pkgs/container/metermesh)

Distributed usage ingest for SaaS billing. Your app posts usage events. MeterMesh totals them in memory by customer and hour, then adds those sums into **your** Postgres database.

It is not an invoice or pricing engine. It is the mesh in front of the database so thousands of usage events do not become thousands of writes.

<p align="center">
  <img src="docs/images/dashboard-live.png" alt="MeterMesh live cluster dashboard" width="900" />
</p>

## Why it exists

Usage-based products need a counter that can:

- accept traffic on any node
- keep each customer on one owner node
- compress many events into one hourly upsert
- survive a node dying without taking the whole mesh down

## Quick start

```bash
git clone https://github.com/vinayrawat058/metermesh.git
cd metermesh
docker compose up --build
```

| Surface | URL |
|---|---|
| Dashboard | http://localhost:3000 |
| Ingest (load balancer) | http://localhost:8080 |
| Node ports | http://localhost:8081 · 8082 · 8083 |
| Ops (restart, localhost only) | http://127.0.0.1:8090 |

Post usage:

```bash
curl -X POST http://localhost:8080/v1/usage \
  -H "Content-Type: application/json" \
  -d '{"customer_id":"acme","metric":"tokens","quantity":10}'
```

Load + failover demo:

```bash
bash scripts/stress_test.sh
```

## Docker image

```bash
docker pull ghcr.io/vinayrawat058/metermesh:latest
```

Or pin a release tag:

```bash
docker pull ghcr.io/vinayrawat058/metermesh:v0.1.0
```

Run a single node (point it at your Postgres/Redis):

```bash
docker run --rm -p 8080:8080 \
  -e NODE_ID=local \
  -e POSTGRES_URL='postgres://user:pass@host:5432/metermesh?sslmode=disable' \
  -e REDIS_URL='redis://host:6379/0' \
  ghcr.io/vinayrawat058/metermesh:latest
```

For the full local cluster (Postgres, Redis, three nodes, LB, dashboard, ops), prefer `docker compose up --build`.

## Integrate with a SaaS app

Whenever your product knows a customer used something, POST once:

```http
POST /v1/usage
Content-Type: application/json

{"customer_id":"user_123","metric":"tokens","quantity":42}
```

Typical hook points: after an API call succeeds, after a model response returns token counts, or when a billable job finishes. Later, read `customer_usage` in Postgres to build invoices.

## Architecture

```text
[SaaS app]
    │
    ▼
[nginx LB :8080]
    │
    ├─► node1 ──┐
    ├─► node2 ──┼─► Redis heartbeats (who is alive)
    └─► node3 ──┘
         │
         │ owner totals hourly usage in RAM
         ▼
      PostgreSQL customer_usage
```

1. The load balancer picks any live node.
2. That node hashes the customer across live members and forwards if it is not the owner.
3. The owner adds the quantity to the current UTC hour in memory.
4. Every few seconds it upserts with `quantity = quantity + EXCLUDED.quantity`.
5. On graceful stop: stop HTTP → flush → leave Redis. Survivors take the customers.
6. Dashboard kill/restart uses node exit + a localhost ops service that talks to the Docker socket.

<p align="center">
  <img src="docs/images/dashboard-compression.png" alt="Dashboard showing event compression into few flushed rows" width="900" />
</p>

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/usage` | Accept a usage event |
| `GET` | `/v1/totals` | In-memory totals on this node |
| `GET` | `/v1/stats` | Accepted / forwarded / flushed counters |
| `GET` | `/v1/route?customer_id=` | Current owner for a customer |
| `GET` | `/v1/metrics/stream` | SSE live metrics |
| `POST` | `/v1/admin/exit` | Graceful shutdown |
| `POST` | `http://127.0.0.1:8090/v1/admin/nodes/{id}/start` | Restart a killed Compose node |

## Layout

```text
cmd/server/              node process
cmd/ops/                 localhost restart control plane
internal/config/         environment configuration
internal/domain/         billing types
internal/cluster/        membership and customer ownership
internal/ingestion/      HTTP, forwarding, SSE, backpressure
internal/pipeline/       in-memory aggregation and flush
internal/repository/     PostgreSQL upserts
frontend/                React live dashboard
deploy/nginx/            load balancer config
migrations/              SQL schema
scripts/stress_test.sh   load and failover check
docs/images/             dashboard screenshots
```

## Known limits

- A crash before a successful flush drops counts still only in memory.
- If Postgres saves a batch and the reply is lost, a retry can add that batch twice.
- Ops requires the Docker socket and is bound to localhost for demos. Do not expose it publicly.
- There is no invoice, tax, seat, or pricing logic here.

## Future plan

### Safer flush (exactly-once style acknowledgements)

Today a flush is “take pending → upsert → forget.” If the database commits and the node dies before it records success, the next retry can double-count.

Planned direction:

1. Give each flush batch an idempotency key.
2. Persist “in flight” batch metadata before the upsert (or use a transactional outbox).
3. Record success only after Postgres acknowledges the write.
4. On restart, skip or safely replay batches that already landed.

Until that lands, treat MeterMesh as **at-most-once across crashes** for unflushed memory, and **at-least-once** across lost flush replies. 

## Development

```bash
go test ./...
go run ./cmd/server

cd frontend && npm install && npm run dev
```

CI runs Go tests, builds the frontend, and builds the Docker images on every push to `main`. Pushing a tag like `v0.1.0` publishes the GHCR image and a GitHub Release with binaries.

## License

MIT. See [LICENSE](LICENSE).
