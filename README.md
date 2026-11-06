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

## Local cluster

```
docker compose up --build
```

Nodes listen on ports 8081, 8082, and 8083.
