# DBOS Go Demo: Crash-Proof Order Processing

An order pipeline built on [DBOS Transact (Go)](https://github.com/dbos-inc/dbos-transact-golang). It uses Postgres as its only infrastructure: there's no separate workflow server, message broker or cron service.

Each order runs through `reserve_inventory → charge_payment → (approval if ≥ $500) → durable sleep → ship_order`, and the demo lets you break that flow on purpose.

| DBOS feature | How you see it in the demo |
|---|---|
| **Crash recovery** | `kill -9` the worker mid-order. On restart, `Recovered pending workflows` appears and each order resumes from its last completed step. |
| **Exactly-once steps** | `📦/💳/🚚 EXECUTING …` lines print only once per order across crashes. The charge ID after recovery is the original one, read back from the checkpoint. |
| **Automatic retries** | The fake payment gateway fails the first 2 attempts. DBOS retries the step with exponential backoff. |
| **Durable timers** | The 20s warehouse-pickup sleep survives restarts and only waits for the time left. |
| **Human-in-the-loop** | Orders of $500 or more wait for `approve`/`reject`, sent from a *different process*. The wait survives restarts, and a decision sent early is held until the order reaches the approval step. |
| **Saga compensation** | A rejected order, or one with no decision within 5 min, gets `refund_payment` and `release_inventory`. |
| **Queues + concurrency** | Orders go onto a Postgres-backed queue, and each worker processes at most 2 at a time. |
| **Idempotency** | The order ID is the workflow ID. Running `submit` again never creates duplicate orders or double charges. |
| **Introspection** | `status` shows each order's live progress event and its step-by-step checkpoint history, including outputs. |

## Prerequisites

- Go 1.26+
- Docker (for Postgres)

## Run it

```bash
docker compose up -d          # Postgres 16 on localhost:5433
```

Terminal 1 (the worker):

```bash
go run . worker
```

Terminal 2 (the client):

```bash
go run . submit               # enqueue 4 demo orders (1002 and 1004 need approval)
go run . status               # watch progress and per-step checkpoints
```

### Things to try

1. **Crash it.** About 5–10s after `submit`, kill the worker hard. Use Ctrl-\ (SIGQUIT), or run `kill -9 $(pgrep -f 'dbos-example worker')`. Then start it again with `go run . worker`. Completed steps don't print `EXECUTING` again, and the orders pick up where they left off.
2. **Approve / reject.** `go run . approve order-1002` and `go run . reject order-1004`. The rejected order is refunded. You can send these before the order is even running.
3. **Resubmit.** `go run . submit` again prints `already exists, not duplicated`.
4. **Custom order.** `go run . submit order-42 650`.

Reset everything with `docker compose down -v`.

## Configuration

The default system database is `postgres://postgres:postgres@localhost:5433/dbos_demo?sslmode=disable`. Point the demo at a different Postgres with:

```bash
export DBOS_SYSTEM_DATABASE_URL='postgres://user:pass@host:5432/dbname?sslmode=disable'
```
