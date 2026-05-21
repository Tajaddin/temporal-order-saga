from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class OrderRequest:
    order_id: str
    amount_cents: int
    sku: str
    quantity: int
    # Test/benchmark fault-injection knobs. Which step should fail, and how
    # many transient failures it should emit before (optionally) succeeding.
    fail_step: str = ""          # "payment" | "inventory" | "shipping" | ""
    transient_failures: int = 0  # transient errors before success
    permanent_failure: bool = False  # if True, the failing step never succeeds


@dataclass
class OrderResult:
    order_id: str
    status: str  # "completed" | "compensated" | "cancelled"
    steps: list[StepLog] = field(default_factory=list)
    payment_id: str = ""
    shipment_id: str = ""


@dataclass
class StepLog:
    step: str
    outcome: str  # "ok" | "compensated"
    detail: str = ""
