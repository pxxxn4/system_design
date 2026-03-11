from Saga import SagaOrchestrator, SagaStep, SagaContext
from Steps import (
    payment_execute, payment_compensate,
    inventory_execute, inventory_compensate,
    shipping_execute, shipping_compensate,
)


def build_steps():
    return [
        SagaStep("Payment",   payment_execute,   payment_compensate),
        SagaStep("Inventory", inventory_execute, inventory_compensate),
        SagaStep("Shipping",  shipping_execute,  shipping_compensate),
    ]


def run_scenario(title: str, **flags):
    print(f"\n{'='*55}")
    print(f"  SCENARIO: {title}")
    print('='*55)
    ctx = SagaContext()
    for k, v in flags.items():
        setattr(ctx, k, v)
    result = SagaOrchestrator(build_steps()).run(ctx)
    print(f"\n  Result: {'SUCCESS ✓' if result else 'ROLLED BACK ✗'}\n")


if __name__ == "__main__":
    run_scenario("All steps succeed")
    run_scenario("Inventory fails → Payment compensated", fail_inventory=True)
    run_scenario("Shipping fails  → Payment + Inventory compensated", fail_shipping=True)