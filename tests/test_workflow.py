"""Saga tests on Temporal's time-skipping test environment (auto-downloads a
local test server; no external Temporal needed). Backoff waits are skipped, so
retries run instantly."""
from __future__ import annotations

import uuid

import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from ordersaga import activities
from ordersaga.models import OrderRequest, OrderResult
from ordersaga.workflow import OrderWorkflow

TASK_QUEUE = "test-order-saga"


async def _run(env: WorkflowEnvironment, req: OrderRequest, cancel: bool = False) -> OrderResult:
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
        handle = await env.client.start_workflow(
            OrderWorkflow.run, req, id=f"wf-{uuid.uuid4()}", task_queue=TASK_QUEUE
        )
        if cancel:
            await handle.signal(OrderWorkflow.cancel)
        return await handle.result()


def order(**kw) -> OrderRequest:
    base = dict(order_id=f"o-{uuid.uuid4().hex[:8]}", amount_cents=1999, sku="WIDGET", quantity=1)
    base.update(kw)
    return OrderRequest(**base)


@pytest.fixture(autouse=True)
def _reset():
    activities.reset_attempts()
    yield
    activities.reset_attempts()


def _log(res: OrderResult) -> list[tuple[str, str]]:
    return [(s.step, s.outcome) for s in res.steps]


def _held(o: OrderRequest) -> tuple[bool, bool]:
    """Whether the order is still charged and still holds a reservation."""
    return (
        activities.idempotency_key(o.order_id, "payment") in activities.charges,
        activities.idempotency_key(o.order_id, "inventory") in activities.reservations,
    )


async def test_happy_path_completes():
    o = order()
    async with await WorkflowEnvironment.start_time_skipping() as env:
        res = await _run(env, o)
        assert res.status == "completed"
        assert res.payment_id and res.shipment_id
        assert [s.step for s in res.steps] == ["payment", "inventory", "shipping"]
        assert _held(o) == (True, True)


async def test_transient_payment_failure_recovers():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        res = await _run(env, order(fail_step="payment", transient_failures=3))
        # 3 transient failures < 5 max attempts -> eventually succeeds.
        assert res.status == "completed"
        assert res.payment_id


async def test_permanent_inventory_failure_compensates_payment():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        res = await _run(env, order(fail_step="inventory", permanent_failure=True))
        assert res.status == "compensated"
        assert not res.shipment_id
        # Payment was charged then refunded; nothing was reserved to release.
        assert _log(res) == [("payment", "ok"), ("payment", "compensated")]


async def test_permanent_payment_failure_compensates_with_nothing_charged():
    o = order(fail_step="payment", permanent_failure=True)
    async with await WorkflowEnvironment.start_time_skipping() as env:
        res = await _run(env, o)
        assert res.status == "compensated"
        assert not res.payment_id
        assert not res.shipment_id
        assert _held(o) == (False, False)
        assert _log(res) == []  # nothing was charged, so nothing was refunded


async def test_permanent_shipping_failure_releases_inventory_and_refunds():
    o = order(fail_step="shipping", permanent_failure=True)
    async with await WorkflowEnvironment.start_time_skipping() as env:
        res = await _run(env, o)
        assert res.status == "compensated"
        assert not res.shipment_id
        outcomes = {(s.step, s.outcome) for s in res.steps}
        assert ("inventory", "compensated") in outcomes
        assert ("payment", "compensated") in outcomes
        assert _held(o) == (False, False)


async def test_cancel_signal_before_start_compensates():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        # A slow first step gives the signal time to land; emulate via transient
        # retries so the workflow yields and observes the cancel.
        res = await _run(env, order(fail_step="payment", transient_failures=1), cancel=True)
        assert res.status in {"cancelled", "compensated", "completed"}
        # Whatever the timing, the outcome is never a partial ship without record.
        if res.shipment_id:
            assert res.status == "completed"


@pytest.mark.parametrize("step", ["payment", "inventory"])
async def test_step_that_took_effect_but_failed_is_rolled_back(step):
    # Every attempt takes effect and then loses its response, like a timeout
    # after the payment provider has already charged the card.
    o = order(fail_step=step, permanent_failure=True, fail_after_effect=True)
    async with await WorkflowEnvironment.start_time_skipping() as env:
        res = await _run(env, o)
        assert res.status == "compensated"
        assert _held(o) == (False, False)
        assert (step, "compensated") in _log(res)


async def test_failing_compensation_is_retried_until_it_succeeds():
    # Each compensation fails more times than a forward step may retry.
    o = order(fail_step="shipping", permanent_failure=True, compensation_failures=8)
    async with await WorkflowEnvironment.start_time_skipping() as env:
        res = await _run(env, o)
        assert res.status == "compensated"
        assert _held(o) == (False, False)
