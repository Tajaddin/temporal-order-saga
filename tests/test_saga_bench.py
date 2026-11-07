"""The fault-injection benchmark's summary when workflows fail outright."""
from __future__ import annotations

import pytest
from temporalio import activity
from temporalio.exceptions import ApplicationError

from benchmarks import saga_bench
from ordersaga import activities
from ordersaga.models import OrderRequest


@pytest.fixture(autouse=True)
def _reset():
    activities.reset_attempts()
    yield
    activities.reset_attempts()


@activity.defn(name="refund_payment")
async def refund_rejected(req: OrderRequest, idempotency_key: str) -> bool:
    raise ApplicationError(f"refund rejected for {req.order_id}", non_retryable=True)


async def test_failed_workflow_counts_once_against_the_rate(monkeypatch):
    # Every rollback refunds the payment, so every order that does not complete
    # now ends in a failed workflow instead of a compensated one.
    monkeypatch.setattr(activities, "refund_payment", refund_rejected)
    summary = await saga_bench.main_async(20)

    failed = summary["orders"] - summary["terminal_total"]
    assert summary["compensated"] == 0
    assert 0 < failed < summary["orders"]
    assert summary["consistency_violations"] == failed
    assert summary["all_or_nothing_rate_pct"] == round(
        100 * summary["completed"] / summary["orders"], 2
    )
