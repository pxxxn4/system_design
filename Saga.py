from dataclasses import dataclass, field
from typing import Any
from enum import Enum
import logging
import uuid

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger(__name__)


class StepStatus(Enum):
    PENDING = "pending"
    COMPLETED = "completed"
    COMPENSATED = "compensated"
    FAILED = "failed"


@dataclass
class SagaStep:
    name: str
    execute: callable
    compensate: callable
    status: StepStatus = StepStatus.PENDING
    result: Any = None


@dataclass
class SagaContext:
    order_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    payment_id: str = None
    reserved_items: list = field(default_factory=list)
    shipment_id: str = None


class SagaOrchestrator:
    def __init__(self, steps: list[SagaStep]):
        self.steps = steps
        self.completed: list[SagaStep] = []

    def run(self, ctx: SagaContext) -> bool:
        log.info(f"=== Starting Saga for order {ctx.order_id} ===")

        for step in self.steps:
            log.info(f"[{step.name}] Executing...")
            try:
                step.result = step.execute(ctx)
                step.status = StepStatus.COMPLETED
                self.completed.append(step)
                log.info(f"[{step.name}] ✓ Done → {step.result}")
            except Exception as e:
                step.status = StepStatus.FAILED
                log.error(f"[{step.name}] ✗ Failed: {e}")
                self._compensate(ctx)
                return False

        log.info("=== Saga completed successfully ===")
        return True

    def _compensate(self, ctx: SagaContext):
        log.warning("--- Starting compensation (reverse order) ---")
        for step in reversed(self.completed):
            log.info(f"[{step.name}] Compensating...")
            try:
                step.compensate(ctx)
                step.status = StepStatus.COMPENSATED
                log.info(f"[{step.name}] ↩ Compensated")
            except Exception as e:
                log.error(f"[{step.name}] Compensation failed: {e}")
        log.warning("--- Compensation finished ---")