"""Worker entrypoint: connects to a Temporal server and serves the saga.

    order-worker          # needs a Temporal server (docker compose up)

Env: TEMPORAL_ADDRESS (default localhost:7233), TEMPORAL_TASK_QUEUE (default order-saga).
"""
from __future__ import annotations

import asyncio
import os

from temporalio.client import Client
from temporalio.worker import Worker

from ordersaga import activities
from ordersaga.workflow import OrderWorkflow

TASK_QUEUE = os.getenv("TEMPORAL_TASK_QUEUE", "order-saga")


async def main() -> None:
    address = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
    client = await Client.connect(address)
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[OrderWorkflow],
        activities=[
            activities.charge_payment,
            activities.refund_payment,
            activities.reserve_inventory,
            activities.release_inventory,
            activities.ship_order,
        ],
    )
    print(f"order-saga worker polling task queue '{TASK_QUEUE}' at {address}")
    await worker.run()


def main_sync() -> None:
    asyncio.run(main())


if __name__ == "__main__":
    main_sync()
