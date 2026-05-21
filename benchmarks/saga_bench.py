"""Fault-injection benchmark for the saga's all-or-nothing guarantee.

Runs N orders through the real workflow on Temporal's time-skipping test
environment, injecting transient and permanent failures across random steps,
and verifies every run ends in a consistent terminal state: either fully
completed (payment + shipment) or fully compensated (no shipment). No partial
state is ever observed.

    python -m benchmarks.saga_bench --count 200
"""
from __future__ import annotations

import argparse
import asyncio
import json
import random
import sys
import uuid
from pathlib import Path

from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from ordersaga import activities
from ordersaga.models import OrderRequest
from ordersaga.workflow import OrderWorkflow

TASK_QUEUE = "bench-order-saga"
RESULTS = Path(__file__).parent / "results" / "saga.json"


async def main_async(count: int) -> dict:
    rng = random.Random(7)
    completed = 0
    compensated = 0
    consistency_violations = 0

    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[OrderWorkflow],
            activities=[
                activities.charge_payment,
                activities.refund_payment,
                activities.reserve_inventory,
                activities.release_inventory,
                activities.ship_order,
            ],
        ):
            for i in range(count):
                step = rng.choice(["", "payment", "inventory", "shipping"])
                permanent = step != "" and rng.random() < 0.5
                transient = 0 if permanent else rng.randint(0, 3)
                req = OrderRequest(
                    order_id=f"b-{i}",
                    amount_cents=1000 + i,
                    sku="SKU",
                    quantity=1,
                    fail_step=step,
                    transient_failures=transient,
                    permanent_failure=permanent,
                )
                res = await env.client.execute_workflow(
                    OrderWorkflow.run, req, id=f"b-{uuid.uuid4()}", task_queue=TASK_QUEUE
                )
                if res.status == "completed":
                    completed += 1
                    if not (res.payment_id and res.shipment_id):
                        consistency_violations += 1
                elif res.status == "compensated":
                    compensated += 1
                    if res.shipment_id:  # shipped but rolled back == partial state
                        consistency_violations += 1

    total = completed + compensated
    return {
        "orders": count,
        "completed": completed,
        "compensated": compensated,
        "terminal_total": total,
        "consistency_violations": consistency_violations,
        "all_or_nothing_rate_pct": round(100 * (total - consistency_violations) / count, 2) if count else 0,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--count", type=int, default=200)
    args = ap.parse_args()
    summary = asyncio.run(main_async(args.count))
    RESULTS.parent.mkdir(parents=True, exist_ok=True)
    RESULTS.write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))
    return 0 if summary["consistency_violations"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
