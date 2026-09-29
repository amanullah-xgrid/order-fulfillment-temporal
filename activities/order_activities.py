from temporalio import activity
from dataclasses import dataclass
from typing import List
import httpx
import time
import json
import logging
from functools import wraps

logger = logging.getLogger(__name__)

def logged_activity(fn):
    @wraps(fn)
    async def wrapper(*args, **kwargs):
        info = activity.info()
        start = time.monotonic()
        logger.info(json.dumps({
            "order_id": info.workflow_id,
            "activity_name": info.activity_type,
            "attempt": info.attempt,
            "event": "start"
        }))
        try:
            result = await fn(*args, **kwargs)
            duration_ms = (time.monotonic() - start) * 1000
            logger.info(json.dumps({
                "order_id": info.workflow_id,
                "activity_name": info.activity_type,
                "duration_ms": round(duration_ms, 1),
                "outcome": "success"
            }))
            return result
        except Exception:
            duration_ms = (time.monotonic() - start) * 1000
            logger.warning(json.dumps({
                "order_id": info.workflow_id,
                "activity_name": info.activity_type,
                "attempt": info.attempt,
                "duration_ms": round(duration_ms, 1),
                "outcome": "attempt_failed"
            }))
            raise
    return wrapper

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
@logged_activity
async def validate_order(order: Order) -> bool:
    if not order.items:
        raise ValueError(f"Order {order.order_id} has no items")
    for item in order.items:
        if item.qty <= 0:
            raise ValueError(f"Invalid qty for {item.sku}")
    return True
    
@activity.defn
@logged_activity
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
@logged_activity
async def charge_payment(order: Order, idempotency_key: str) -> PaymentResult:
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
@logged_activity
async def release_inventory(reservation_id: str) -> ReleaseResult:
    async with httpx.AsyncClient() as client:
        response = await client.post(
            "http://localhost:8001/inventory/release",
            json={"reservation_id": reservation_id}
        )
    data = response.json()
    return ReleaseResult(status=data["status"])

@activity.defn
@logged_activity
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