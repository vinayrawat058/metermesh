# MeterMesh

MeterMesh is an ingest mesh for usage-based billing. Another service posts usage events. MeterMesh totals them in memory and adds those hourly sums into a Postgres database that service already has.

Each customer is owned by one node. Events that arrive anywhere else are forwarded to that owner. The owner is the only node that totals that customer and writes the row. Redis only tracks which nodes are alive. When a node disappears, its customers move to the nodes that remain.

There is no invoice, price, or tax logic here. A crash drops counts that have not been flushed yet.

## How a request moves

1. Clients post `POST /v1/usage` to the load balancer on port 8080, which picks any live node.
2. That node hashes the customer across the live nodes and forwards the event when it is not the owner.
3. The owner adds the quantity to that customer's total for the current UTC hour.
4. Every few seconds the owner adds the new totals into `customer_usage`.
5. `GET /v1/stats` shows events accepted on this node and rows written, so you can see many events become one database write.

On a graceful stop the node stops accepting traffic, flushes the totals still in memory, and only then deletes its Redis heartbeat. Survivors take its customers after that. A hard kill skips the flush, so those counts are lost and the heartbeat expires after a few seconds.

`GET /v1/route?customer_id=acme` shows which node owns that customer. `GET /v1/totals` shows what this process has accepted. `GET /v1/metrics/stream` is an SSE feed of live node metrics. `POST /v1/admin/exit` starts a graceful shutdown on that node.

## Layout

```
cmd/server/            node process
internal/config/       environment configuration
internal/domain/       billing types
internal/cluster/      live nodes and customer ownership
internal/ingestion/    HTTP ingestion, forwarding, and backpressure
internal/pipeline/     in-memory aggregation and flush
internal/repository/   PostgreSQL upserts
frontend/              live cluster dashboard
deploy/nginx/          load balancer config
migrations/            SQL schema
scripts/stress_test.sh load and failover check
```

## Run one node

```
go run ./cmd/server
```

Without Redis, this process owns every customer.

```
curl -X POST http://localhost:8080/v1/usage -H "Content-Type: application/json" -d "{\"customer_id\":\"acme\",\"metric\":\"tokens\",\"quantity\":10}"
```

## Local cluster

```
docker compose up --build
```

Postgres, Redis, three nodes, the nginx load balancer, and the dashboard come up. Post usage to [http://localhost:8080](http://localhost:8080). Direct node ports stay on 8081–8083 for the dashboard and admin exit. The dashboard is on [http://localhost:3000](http://localhost:3000). Inside the Compose network the nodes reach each other as `node1`, `node2`, and `node3`.

### Dashboard only

```
cd frontend
npm install
npm run dev
```

The Vite app streams metrics from ports 8081–8083 by default. Override with `VITE_NODES`.

## Load and failover check

```
bash scripts/stress_test.sh
```

The script sends a burst of usage events through the load balancer, prints per-node stats, asks the current owner to exit, waits for the route to change, and posts again through the balancer.
