from fastapi import FastAPI
from pydantic import BaseModel
import uuid

app = FastAPI()

class ChargeRequest(BaseModel):
    order_id: str
    amount: float
    idempotency_key: str

payments = {}

@app.post("/payments/charge")
def charge(req: ChargeRequest):
    if req.idempotency_key in payments:
        return payments[req.idempotency_key]

    payment_id = str(uuid.uuid4())
    result = {"payment_id": payment_id, "status": "PENDING"}
    payments[req.idempotency_key] = result
    return result