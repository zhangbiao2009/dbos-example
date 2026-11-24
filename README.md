# DBOS Go Demo: Crash-Proof Order Processing

An order pipeline built on [DBOS Transact (Go)](https://github.com/dbos-inc/dbos-transact-golang) that changes **real data**: it updates stock in a Postgres database and charges cards through an HTTP payment API. You can kill the worker anywhere, even right after a card is charged, and `go run . report` will show that nothing was lost or done twice. The only infrastructure is Postgres. There's no workflow server and no message broker.

## What happens to an order

```
reserve stock ──▶ charge card ──▶ [≥ $500: wait for approve/reject] ──▶ sleep 20s ──▶ record shipment
 (DB transaction)   (HTTP call, retried)     (message from another process)   (durable timer)   (DB transaction)
                          │                        │ reject / 5 min timeout
                          ▼                        ▼
                   payment failed ──────▶ refund card + put stock back   (saga compensation)
```

| Kind of work | DBOS API | Guarantee |
|---|---|---|
| Writes to **our database** (`shop.inventory`, `shop.orders`) | `dbos.RunAsTransaction` | Writes and checkpoint commit in **one transaction** → exactly once, with no gap |
| Calls to an **external API** (payment provider) | `dbos.RunAsStep` + idempotency key | Re-run only if interrupted before it finished; the provider ignores the repeat |
| Waiting for a **human decision** | `dbos.Recv` ← `dbos.Send` | The wait and the decision survive restarts |
| **Timers** | `dbos.Sleep` | Resumes with only the remaining time |
| **Status** for other processes | `dbos.SetEvent` → `dbos.GetEvent` | Latest progress, readable by any process |
| **Throughput control** | queue with `WithWorkerConcurrency(3)` | At most 3 orders in flight per worker |
| **No duplicate orders** | order ID = workflow ID | Submitting the same order again is a no-op |

The payment provider is a fake third party served from inside the worker on `127.0.0.1:8089`. Like real providers, it is **flaky**: the first 2 calls for each order return 503, so you can watch DBOS retry. It is also **idempotent**: a repeated request with the same key returns the original charge.

## Run it

Prerequisites: Go 1.26+, Docker.

```bash
docker compose up -d                      # Postgres 16 on localhost:5433
go run . worker                           # terminal 1: worker + payment provider
go run . submit                           # terminal 2: enqueue 5 demo orders
go run . status                           # each workflow's progress and saved steps
go run . report                           # business tables + exactly-once checks
go run . approve order-1002               # orders >= $500 wait for a decision
go run . reject  order-1004               # → refunded, stock released
```

The demo orders cover each path:

| Order | Product | Path |
|---|---|---|
| order-1001 | keyboard $129 | straight through to shipped |
| order-1002 | monitor $749 | waits for approval |
| order-1003 | usb-c hub $39.50 | straight through to shipped |
| order-1004 | standing desk $980 | waits for approval (reject it to see the refund) |
| order-1005 | graphics card $399 | sold out → cancelled, never charged |

## The crash test

This is the case that is hard to get right without DBOS: the process dies after the card was charged but before the program saved that fact.

```bash
docker compose down -v && docker compose up -d   # start clean
go run . worker --crash-after-charge             # terminal 1
go run . submit                                  # terminal 2
```

The worker dies right after the first charge:

```
[gateway] 💳 charged $129.00 for order-1001 -> ch_a920991a
[order-1001] 💥 CRASHING: card charged (ch_a920991a) but the step has not returned, so DBOS has no checkpoint for it
```

Restart it with `go run . worker`. DBOS recovers the in-flight orders and re-runs the unfinished charge step with the **same idempotency key**:

```
Recovered pending workflows count=3
[gateway] duplicate request for order-1001: returning existing ch_a920991a, NOT charging again
```

The finished `TX reserve_inventory` lines don't print again. Those transactions committed along with their checkpoints, so recovery returns the saved results instead of decrementing stock again. After everything finishes, `go run . report` prints:

```
CHECKS
  ✅ no order charged more than once (0 double-charged)
  ✅ KB-01 stock adds up: 9 in stock + 1 reserved/shipped = 10 initial
  ...
```

You can also `kill -9 $(pgrep -f 'dbos-example worker')` at any other moment, for example during the 20s sleep or while an order waits for approval, then restart and run `report` again.

## Code map

| File | Contents |
|---|---|
| [workflow.go](workflow.go) | The durable workflow `ProcessOrder` and its compensation `cancelOrder` |
| [shop.go](shop.go) | The application database: schema, the 3 transactions, `report` |
| [gateway.go](gateway.go) | The fake payment provider (HTTP server) and the client the steps use |
| [main.go](main.go) | Commands and DBOS setup (context, data source, queue, launch) |

## Databases

| Database | Default URL | Override with | Contents |
|---|---|---|---|
| DBOS system DB | `postgres://postgres:postgres@localhost:5433/dbos_demo` | `DBOS_SYSTEM_DATABASE_URL` | workflows, step checkpoints, queue, messages (schema `dbos`) |
| Application DB | `postgres://postgres:postgres@localhost:5433/shop` | `APP_DATABASE_URL` | `shop.inventory`, `shop.orders`, the provider's `gateway.charges`, and DBOS's `transaction_completion` table |

The worker creates the `shop` database on first start. `docker compose down -v` resets everything.
