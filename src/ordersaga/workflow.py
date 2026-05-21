"""OrderWorkflow: a durable saga.

Executes charge_payment -> reserve_inventory -> ship_order. Each activity is
retried per the RetryPolicy, so transient failures self-heal. If a step still
fails after retries, the workflow runs the registered compensations in reverse
order and returns "compensated" -- never a partial state. A cancel signal
compensates whatever has run so far and returns "cancelled".

Workflow code is deterministic: all side effects live in activities, and the
only nondeterministic inputs (failure injection) are passed in the request.
"""
from __future__ import annotations

from datetime import timedelta

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
        # Compensations to run (in reverse) if the saga aborts.
        compensations: list[tuple[str, str]] = []  # (kind, id)

        async def compensate() -> None:
            for kind, ident in reversed(compensations):
                if kind == "payment":
                    await workflow.execute_activity(
                        activities.refund_payment, ident, start_to_close_timeout=_TIMEOUT, retry_policy=_RETRY
                    )
                    result.steps.append(StepLog(step="payment", outcome="compensated", detail=ident))
                elif kind == "inventory":
                    await workflow.execute_activity(
                        activities.release_inventory, ident, start_to_close_timeout=_TIMEOUT, retry_policy=_RETRY
                    )
                    result.steps.append(StepLog(step="inventory", outcome="compensated", detail=ident))

        if self._cancelled:
            result.status = "cancelled"
            return result

        try:
            payment_id = await workflow.execute_activity(
                activities.charge_payment, req, start_to_close_timeout=_TIMEOUT, retry_policy=_RETRY
            )
            compensations.append(("payment", payment_id))
            result.payment_id = payment_id
            result.steps.append(StepLog(step="payment", outcome="ok", detail=payment_id))

            if self._cancelled:
                await compensate()
                result.status = "cancelled"
                return result

            reservation_id = await workflow.execute_activity(
                activities.reserve_inventory, req, start_to_close_timeout=_TIMEOUT, retry_policy=_RETRY
            )
            compensations.append(("inventory", reservation_id))
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
            # A step failed after exhausting retries: roll back everything done.
            await compensate()
            result.status = "compensated"
            return result
