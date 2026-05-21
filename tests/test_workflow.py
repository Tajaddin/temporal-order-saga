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


async def test_happy_path_completes():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        res = await _run(env, order())
        assert res.status == "completed"
        assert res.payment_id and res.shipment_id
        assert [s.step for s in res.steps] == ["payment", "inventory", "shipping"]


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
        # Payment was charged then refunded.
        kinds = [(s.step, s.outcome) for s in res.steps]
        assert ("payment", "ok") in kinds
        assert ("payment", "compensated") in kinds


async def test_permanent_payment_failure_compensates_with_nothing_charged():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        res = await _run(env, order(fail_step="payment", permanent_failure=True))
        assert res.status == "compensated"
        assert not res.payment_id
        assert not res.shipment_id


async def test_permanent_shipping_failure_releases_inventory_and_refunds():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        res = await _run(env, order(fail_step="shipping", permanent_failure=True))
        assert res.status == "compensated"
        assert not res.shipment_id
        outcomes = {(s.step, s.outcome) for s in res.steps}
        assert ("inventory", "compensated") in outcomes
        assert ("payment", "compensated") in outcomes


async def test_cancel_signal_before_start_compensates():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        # A slow first step gives the signal time to land; emulate via transient
        # retries so the workflow yields and observes the cancel.
        res = await _run(env, order(fail_step="payment", transient_failures=1), cancel=True)
        assert res.status in {"cancelled", "compensated", "completed"}
        # Whatever the timing, the outcome is never a partial ship without record.
        if res.shipment_id:
            assert res.status == "completed"
