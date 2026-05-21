# temporal-order-saga

> Durable order-processing saga on Temporal: charge payment, reserve inventory, ship, with automatic compensation on failure and signal-based cancellation. **100% all-or-nothing outcome across 200 fault-injected runs (124 completed, 76 cleanly compensated, 0 partial states)**, run on Temporal's time-skipping test server. 6 tests.

[![ci](https://github.com/Tajaddin/temporal-order-saga/actions/workflows/ci.yml/badge.svg)](https://github.com/Tajaddin/temporal-order-saga/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](pyproject.toml)

## Hero metrics

Reproducible with no external Temporal server (the test environment downloads a local one):

```bash
python -m benchmarks.saga_bench --count 200
```

| Metric | Value |
|---|---:|
| Orders run (random fault injection) | 200 |
| Completed (payment + shipment) | 124 |
| Compensated (rolled back, no shipment) | 76 |
| Partial / inconsistent states | **0** |
| **All-or-nothing rate** | **100%** |

Every order ends in a consistent terminal state. When a step fails permanently, the workflow runs its compensations in reverse, so an order is never shipped without payment, nor charged without shipping. Transient failures self-heal through Temporal's activity retries.

## What it is

A saga (distributed transaction with compensation), the canonical use case for a durable workflow engine:

```
charge_payment ──ok──► reserve_inventory ──ok──► ship_order ──► COMPLETED
     │                       │                       │
     │ permanent fail        │ permanent fail        │ permanent fail
     ▼                       ▼                       ▼
 COMPENSATED  ◄── refund ◄── release_inventory ◄── (reverse-order rollback)
```

| Concern | Implementation |
|---|---|
| Durable execution | Temporal persists workflow state; a worker crash resumes exactly where it left off (Temporal's core guarantee) |
| Retries | `RetryPolicy(maximum_attempts=5)` per activity; transient failures recover without workflow code changes |
| Compensation | Each completed step registers a compensation; on abort they run in reverse (refund payment, release inventory) |
| Cancellation | A `cancel` signal compensates whatever has run and returns `cancelled` |
| Determinism | All side effects live in activities; the workflow body is pure and replay-safe |

## Why this matters for hiring

Role categories unlocked: **Backend / Distributed Systems**, Platform Engineering, orchestration.

Durable execution (Temporal, Cadence, AWS Step Functions) is how modern systems run long-lived, fault-tolerant business processes. This repo backs the "Temporal / workflow orchestration" resume line with a real saga, compensation logic, signals, and a fault-injection benchmark that proves the all-or-nothing guarantee.

## Run it

### Tests + benchmark (no server needed)

```bash
pip install -e ".[dev]"
pytest                                  # 6 tests on the time-skipping test server
python -m benchmarks.saga_bench --count 200
```

### Against a real Temporal server

```bash
docker compose up --build      # Temporal :7233, Web UI :8233, worker, Postgres
```

```python
from temporalio.client import Client
from ordersaga.models import OrderRequest
from ordersaga.workflow import OrderWorkflow

client = await Client.connect("localhost:7233")
result = await client.execute_workflow(
    OrderWorkflow.run,
    OrderRequest(order_id="o-1", amount_cents=1999, sku="WIDGET", quantity=1),
    id="order-o-1", task_queue="order-saga",
)
print(result.status)  # "completed"
```

Watch executions, retries, and compensations live in the Temporal Web UI at http://localhost:8233.

## Testing

```bash
pytest -q     # 6 tests
```

- happy path completes with all three steps
- transient payment failure recovers via retries
- permanent inventory failure compensates the payment (charge then refund)
- permanent payment failure leaves nothing charged
- permanent shipping failure releases inventory and refunds payment
- cancel signal never yields a partial ship

All run on `WorkflowEnvironment.start_time_skipping()`, which downloads a local test server and skips retry backoff, so the suite is fast and needs no standing Temporal.

## Project layout

```
src/ordersaga/
  models.py       # OrderRequest / OrderResult / StepLog (+ fault-injection knobs)
  activities.py   # payment / inventory / shipping + compensations
  workflow.py     # OrderWorkflow saga: retries, compensation, cancel signal
  worker.py       # worker entrypoint (order-worker)
benchmarks/saga_bench.py   # all-or-nothing fault-injection hero
docker-compose.yml         # Temporal server + UI + worker + Postgres
```

## Stack

Python 3.10+, temporalio (Temporal Python SDK), pytest + pytest-asyncio, Docker, GitHub Actions. Local dev stack via `temporalio/auto-setup` + Temporal UI.

## License

MIT
