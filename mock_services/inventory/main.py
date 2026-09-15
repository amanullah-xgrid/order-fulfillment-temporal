from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
import uuid

app = FastAPI()
class OrderItem(BaseModel):
    sku: str
    qty: int

class ReserveRequest(BaseModel):
    order_id: str
    items: List[OrderItem]
inventory = {
    "WIDGET-1":  {"total": 20, "reserved": 18},  # Available: 2
    "WIDGET-2":  {"total": 5,  "reserved": 0},   # Available: 5
    "WIDGET-3":  {"total": 12, "reserved": 4},   # Available: 8
    "WIDGET-4":  {"total": 50, "reserved": 10},  # Available: 40
    "WIDGET-5":  {"total": 8,  "reserved": 7},   # Available: 1
    "WIDGET-6":  {"total": 15, "reserved": 15},  # Available: 0
    "WIDGET-7":  {"total": 30, "reserved": 5},   # Available: 25
    "WIDGET-8":  {"total": 3,  "reserved": 2},   # Available: 1
    "WIDGET-9":  {"total": 25, "reserved": 12},  # Available: 13
    "WIDGET-10": {"total": 100,"reserved": 20},  # Available: 80
    "WIDGET-11": {"total": 7,  "reserved": 7},   # Available: 0
    "WIDGET-12": {"total": 18, "reserved": 6},   # Available: 12
    "WIDGET-13": {"total": 40, "reserved": 39},  # Available: 1
    "WIDGET-14": {"total": 60, "reserved": 15},  # Available: 45
    "WIDGET-15": {"total": 10, "reserved": 3},   # Available: 7
    "WIDGET-16": {"total": 2,  "reserved": 1},   # Available: 1
    "WIDGET-17": {"total": 14, "reserved": 5},   # Available: 9
    "WIDGET-18": {"total": 9,  "reserved": 8},   # Available: 1
    "WIDGET-19": {"total": 22, "reserved": 0},   # Available: 22
    "WIDGET-20": {"total": 16, "reserved": 16},  # Available: 0
}
@app.post("/inventory/reserve")
def reserve(req: ReserveRequest):
    unavailable = []
    for item in req.items:
        stock = inventory.get(item.sku)
        available = stock["total"] - stock["reserved"] if stock else 0
        if available < item.qty:
            unavailable.append(item.sku)

    if unavailable:
        return {"reservation_id": None, "status": "OUT_OF_STOCK", "unavailable_skus": unavailable}

    for item in req.items:
        inventory[item.sku]["reserved"] += item.qty

    return {"reservation_id": str(uuid.uuid4()), "status": "RESERVED", "unavailable_skus": []}