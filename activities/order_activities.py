from temporalio import activity
from dataclasses import dataclass
from typing import List
import httpx

@dataclass
class OrderItem:
    sku: str
    qty: int

@dataclass
class Order:
    order_id: str
    items: List[OrderItem]

@dataclass
class ReservationResult:
    status: str
    reservation_id: str | None
    unavailable_skus: List[str]

@dataclass
class PaymentResult:
    payment_id: str
    status: str 

@dataclass
class ReleaseResult:
    status: str       
@dataclass
class RefundResult:
    payment_id: str
    status: str
@activity.defn
async def validate_order(order: Order) -> bool:
    if not order.items:
        raise ValueError(f"Order {order.order_id} has no items")
    for item in order.items:
        if item.qty <= 0:
            raise ValueError(f"Invalid qty for {item.sku}")
    return True
    
@activity.defn
async def reserve_inventory(order: Order) -> ReservationResult:
    async with httpx.AsyncClient() as client:
        response = await client.post(
            "http://localhost:8001/inventory/reserve",
            json={
                "order_id": order.order_id,
                "items": [{"sku": item.sku, "qty": item.qty} for item in order.items]
            }
        )
    data = response.json()
    return ReservationResult(
        status=data["status"],
        reservation_id=data["reservation_id"],
        unavailable_skus=data["unavailable_skus"]
    )

@activity.defn
async def charge_payment(order: Order, idempotency_key: str) -> PaymentResult:
    activity.logger.info(f"charge_payment called with idempotency_key={idempotency_key}")
    async with httpx.AsyncClient() as client:
        response = await client.post(
            "http://localhost:8002/payments/charge",
            json={
                "order_id": order.order_id,
                "amount": 49.99,
                "idempotency_key": idempotency_key
            }
        )
    data = response.json()
    return PaymentResult(
        payment_id=data["payment_id"],
        status=data["status"]
    )
    
@activity.defn
async def release_inventory(reservation_id: str) -> ReleaseResult:
    async with httpx.AsyncClient() as client:
        response = await client.post(
            "http://localhost:8001/inventory/release",
            json={"reservation_id": reservation_id}
        )
    data = response.json()
    return ReleaseResult(status=data["status"])

@activity.defn
async def refund_payment(payment_id: str) -> RefundResult:
    async with httpx.AsyncClient() as client:
        response = await client.post(
            "http://localhost:8002/payments/refund",
            json={"payment_id": payment_id}
        )
    response.raise_for_status()
    data = response.json()
    return RefundResult(
        payment_id=data["payment_id"],
        status=data["status"]
    )    