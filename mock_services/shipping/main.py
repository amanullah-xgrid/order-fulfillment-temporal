from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
import uuid

app = FastAPI()

class ShipmentItem(BaseModel):
    sku: str
    qty: int

class CreateShipmentRequest(BaseModel):
    items: List[ShipmentItem]
    idempotency_key: str

shipments = {}

@app.post("/shipping/create")
def create_shipment(req: CreateShipmentRequest):
    if req.idempotency_key in shipments:
        return shipments[req.idempotency_key]

    package_id = str(uuid.uuid4())
    result = {"package_id": package_id}
    shipments[req.idempotency_key] = result
    return result