from temporalio import activity
from dataclasses import dataclass
from typing import List
import httpx
import asyncio

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
    await asyncio.sleep(15)
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