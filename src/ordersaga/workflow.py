"""OrderWorkflow: a durable saga.

Executes charge_payment -> reserve_inventory -> ship_order. Each activity is
retried per the RetryPolicy, so transient failures self-heal. If a step still
fails after retries, the workflow runs the registered compensations in reverse
order and returns "compensated" -- never a partial state. A cancel signal
compensates whatever has run so far and returns "cancelled".

Each compensation is registered before its step runs, because a step can take
effect and still fail (a timeout after the payment provider charged).
Compensations retry until they succeed: giving up would leave the order half
rolled back.

Workflow code is deterministic: all side effects live in activities, and the
only nondeterministic inputs (failure injection) are passed in the request.
"""
from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from ordersaga import activities
    from ordersaga.models import OrderRequest, OrderResult, StepLog

_RETRY = RetryPolicy(
    initial_interval=timedelta(milliseconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(milliseconds=50),
    maximum_attempts=5,
)
# No attempt limit: a failing refund keeps retrying (and shows up in the UI)
# instead of ending the workflow with the customer still charged.
_COMPENSATION_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
)
_TIMEOUT = timedelta(seconds=10)


@workflow.defn
class OrderWorkflow:
    def __init__(self) -> None:
        self._cancelled = False

    @workflow.signal
    def cancel(self) -> None:
        self._cancelled = True

    @workflow.run
    async def run(self, req: OrderRequest) -> OrderResult:
        result = OrderResult(order_id=req.order_id, status="completed")
        # Each step and its compensation share an idempotency key, so a retried
        # step takes effect once and its compensation can find it by key.
        payment_key = activities.idempotency_key(req.order_id, "payment")
        reservation_key = activities.idempotency_key(req.order_id, "inventory")
        # Compensations to run (in reverse) if the saga aborts.
        compensations: list[tuple[str, Any, str]] = []  # (step, activity, key)

        async def compensate() -> None:
            for step, undo, key in reversed(compensations):
                undone = await workflow.execute_activity(
                    undo,
                    args=[req, key],
                    start_to_close_timeout=_TIMEOUT,
                    retry_policy=_COMPENSATION_RETRY,
                )
                # A step that failed before taking effect had nothing to undo.
                if undone:
                    result.steps.append(StepLog(step=step, outcome="compensated", detail=key))

        if self._cancelled:
            result.status = "cancelled"
            return result

        try:
            # Compensating a step that never took effect is a no-op (same key).
            compensations.append(("payment", activities.refund_payment, payment_key))
            payment_id = await workflow.execute_activity(
                activities.charge_payment,
                args=[req, payment_key],
                start_to_close_timeout=_TIMEOUT,
                retry_policy=_RETRY,
            )
            result.payment_id = payment_id
            result.steps.append(StepLog(step="payment", outcome="ok", detail=payment_id))

            if self._cancelled:
                await compensate()
                result.status = "cancelled"
                return result

            compensations.append(("inventory", activities.release_inventory, reservation_key))
            reservation_id = await workflow.execute_activity(
                activities.reserve_inventory,
                args=[req, reservation_key],
                start_to_close_timeout=_TIMEOUT,
                retry_policy=_RETRY,
            )
            result.steps.append(StepLog(step="inventory", outcome="ok", detail=reservation_id))

            if self._cancelled:
                await compensate()
                result.status = "cancelled"
                return result

            shipment_id = await workflow.execute_activity(
                activities.ship_order, req, start_to_close_timeout=_TIMEOUT, retry_policy=_RETRY
            )
            result.shipment_id = shipment_id
            result.steps.append(StepLog(step="shipping", outcome="ok", detail=shipment_id))

            return result

        except Exception:
            # A step failed after exhausting retries: roll back everything that
            # may have taken effect.
            await compensate()
            result.status = "compensated"
            return result
