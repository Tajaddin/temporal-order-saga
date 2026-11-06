"""Order saga activities and their compensations.

Activities are the side-effecting steps. In a real system they call payment,
inventory, and shipping services; here they are in-memory and accept
fault-injection flags so tests and the benchmark can drive every path
(transient ret--then-succeed, permanent failure --> compensation, a step that
takes effect but loses its response).

A process-global counter tracks transient-failure attempts per (order, step)
so a retried activity eventually succeeds after `transient_failures` attempts.
"""
from __future__ import annotations

from collections.abc import Callable

from temporalio import activity

from ordersaga.models import OrderRequest

# Per-(order_id, step) attempt counter. Survives within a worker process so
# Temporal's activity retries can exhaust the injected transient failures.
_attempts: dict[tuple[str, str], int] = {}

# In-memory payment and inventory services. Like a real payment API they key
# every call on the caller's idempotency key: a retried charge charges once,
# and refunding a key that was never charged is a no-op.
charges: dict[str, int] = {}  # idempotency key -> amount_cents, until refunded
reservations: dict[str, int] = {}  # idempotency key -> quantity, until released


class StepFailure(Exception):
    """Raised by an activity to signal a (possibly transient) failure."""


def idempotency_key(order_id: str, step: str) -> str:
    return f"{order_id}:{step}"


def _attempt_fails(order_id: str, step: str, failures: int) -> bool:
    key = (order_id, step)
    seen = _attempts.get(key, 0)
    _attempts[key] = seen + 1
    return seen < failures


def _should_fail(req: OrderRequest, step: str) -> bool:
    if req.fail_step != step:
        return False
    if req.permanent_failure:
        return True
    return _attempt_fails(req.order_id, step, req.transient_failures)


def _apply(req: OrderRequest, step: str, effect: Callable[[], object]) -> None:
    failing = _should_fail(req, step)
    if failing and not req.fail_after_effect:
        raise StepFailure(f"{step} failed for {req.order_id}")
    effect()
    if failing:
        raise StepFailure(f"{step} for {req.order_id} took effect but its response was lost")


@activity.defn
async def charge_payment(req: OrderRequest, idempotency_key: str) -> str:
    _apply(req, "payment", lambda: charges.setdefault(idempotency_key, req.amount_cents))
    return f"pay_{req.order_id}"


@activity.defn
async def refund_payment(req: OrderRequest, idempotency_key: str) -> bool:
    """Refund the charge under the key; False if there was none to refund."""
    if _attempt_fails(req.order_id, "refund", req.compensation_failures):
        raise StepFailure(f"refund failed for {req.order_id}")
    return charges.pop(idempotency_key, None) is not None


@activity.defn
async def reserve_inventory(req: OrderRequest, idempotency_key: str) -> str:
    _apply(req, "inventory", lambda: reservations.setdefault(idempotency_key, req.quantity))
    return f"resv_{req.order_id}"


@activity.defn
async def release_inventory(req: OrderRequest, idempotency_key: str) -> bool:
    """Release the reservation under the key; False if there was none."""
    if _attempt_fails(req.order_id, "release", req.compensation_failures):
        raise StepFailure(f"release failed for {req.order_id}")
    return reservations.pop(idempotency_key, None) is not None


@activity.defn
async def ship_order(req: OrderRequest) -> str:
    if _should_fail(req, "shipping"):
        raise StepFailure(f"shipping failed for {req.order_id}")
    return f"ship_{req.order_id}"


def reset_attempts() -> None:
    """Clear the fault-injection counters and the in-memory services (between test cases)."""
    _attempts.clear()
    charges.clear()
    reservations.clear()
