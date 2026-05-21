"""Order saga activities and their compensations.

Activities are the side-effecting steps. In a real system they call payment,
inventory, and shipping services; here they are in-memory and accept
fault-injection flags so tests and the benchmark can drive every path
(transient ret--then-succeed, permanent failure --> compensation).

A process-global counter tracks transient-failure attempts per (order, step)
so a retried activity eventually succeeds after `transient_failures` attempts.
"""
from __future__ import annotations

from temporalio import activity

from ordersaga.models import OrderRequest

# Per-(order_id, step) attempt counter. Survives within a worker process so
# Temporal's activity retries can exhaust the injected transient failures.
_attempts: dict[tuple[str, str], int] = {}


class StepFailure(Exception):
    """Raised by an activity to signal a (possibly transient) failure."""


def _should_fail(req: OrderRequest, step: str) -> bool:
    if req.fail_step != step:
        return False
    if req.permanent_failure:
        return True
    key = (req.order_id, step)
    seen = _attempts.get(key, 0)
    _attempts[key] = seen + 1
    return seen < req.transient_failures


@activity.defn
async def charge_payment(req: OrderRequest) -> str:
    if _should_fail(req, "payment"):
        raise StepFailure(f"payment failed for {req.order_id}")
    return f"pay_{req.order_id}"


@activity.defn
async def refund_payment(payment_id: str) -> None:
    # Idempotent compensation; safe to retry.
    return None


@activity.defn
async def reserve_inventory(req: OrderRequest) -> str:
    if _should_fail(req, "inventory"):
        raise StepFailure(f"inventory failed for {req.order_id}")
    return f"resv_{req.order_id}"


@activity.defn
async def release_inventory(reservation_id: str) -> None:
    return None


@activity.defn
async def ship_order(req: OrderRequest) -> str:
    if _should_fail(req, "shipping"):
        raise StepFailure(f"shipping failed for {req.order_id}")
    return f"ship_{req.order_id}"


def reset_attempts() -> None:
    """Clear the transient-failure counters (used between test cases)."""
    _attempts.clear()
