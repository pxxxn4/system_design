import uuid
from Saga import SagaContext


#payment

def payment_execute(ctx: SagaContext):
    if getattr(ctx, "fail_payment", False):
        raise Exception("Card declined")
    ctx.payment_id = f"PAY-{uuid.uuid4().hex[:6].upper()}"
    return f"charged → {ctx.payment_id}"


def payment_compensate(ctx: SagaContext):
    if ctx.payment_id:
        print(f"         refund issued for {ctx.payment_id}")
        ctx.payment_id = None


#inventory

def inventory_execute(ctx: SagaContext):
    if getattr(ctx, "fail_inventory", False):
        raise Exception("Item out of stock")
    ctx.reserved_items = ["SKU-001", "SKU-002"]
    return f"reserved → {ctx.reserved_items}"


def inventory_compensate(ctx: SagaContext):
    if ctx.reserved_items:
        print(f"         released reservation {ctx.reserved_items}")
        ctx.reserved_items = []


#shipping

def shipping_execute(ctx: SagaContext):
    if getattr(ctx, "fail_shipping", False):
        raise Exception("No delivery slots available")
    ctx.shipment_id = f"SHIP-{uuid.uuid4().hex[:6].upper()}"
    return f"scheduled → {ctx.shipment_id}"


def shipping_compensate(ctx: SagaContext):
    if ctx.shipment_id:
        print(f"         cancelled shipment {ctx.shipment_id}")
        ctx.shipment_id = None