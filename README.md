# DBOS Go Durable Workflow Demo

This is a tiny Go example to try DBOS's core feature: durable workflow recovery after process crashes.

## What this demo does

- Starts a workflow with fixed ID `durable-hello-demo`.
- The workflow executes:
  - `step1_prepare` (durable step)
  - `dbos.Sleep(10s)` (durable sleep)
  - `step2_finalize` (durable step)
- If `CRASH_ONCE=1` is set, the process exits 3 seconds after starting the workflow.
- On restart (without `CRASH_ONCE`), DBOS recovers the in-flight workflow and finishes it.

## Prerequisites

- Go installed
- Docker or local PostgreSQL

## Quick start

1. Start PostgreSQL:

```bash
docker compose up -d
```

2. Download dependencies:

```bash
go mod tidy
```

3. First run: intentionally crash while the workflow is running:

```bash
CRASH_ONCE=1 go run .
```

4. Second run: restart normally and observe recovery:

```bash
go run .
```

## Optional environment variable

If you use your own database, set:

```bash
export DBOS_SYSTEM_DATABASE_URL='postgres://postgres:postgres@localhost:5432/dbos_demo?sslmode=disable'
```
