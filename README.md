# MeterMesh

Distributed usage ingestion for SaaS billing. API clients send usage events to any node. Nodes total those events in memory, and the node holding the Redis lease flushes the sums to PostgreSQL.

A crash drops counts that have not been flushed yet.

## Layout

```
cmd/server/            node process
internal/config/       environment configuration
internal/domain/       billing types
internal/cluster/      Redis leadership lease
internal/ingestion/    HTTP ingestion and backpressure
internal/pipeline/     in-memory aggregation
internal/repository/   PostgreSQL upserts
frontend/              live cluster dashboard
migrations/            SQL schema
scripts/stress_test.sh load and failover check
```

## Run one node

```
go run ./cmd/server
```

`POST /v1/usage` accepts an event and adds it to an hourly total in memory. Every few seconds the node adds those new totals into Postgres. `GET /v1/totals` shows what this process has accepted. Redis is not used yet. A crash before a successful flush drops the counts still in memory.

```
curl -X POST http://localhost:8080/v1/usage -H "Content-Type: application/json" -d "{\"customer_id\":\"acme\",\"metric\":\"tokens\",\"quantity\":10}"
```

## Local cluster

```
docker compose up --build
```

Nodes listen on ports 8081, 8082, and 8083.
