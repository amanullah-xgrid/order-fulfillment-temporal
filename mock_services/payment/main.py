from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uuid

app = FastAPI()

class ChargeRequest(BaseModel):
    order_id: str
    amount: float
    idempotency_key: str

class RefundRequest(BaseModel):
    payment_id: str

payments = {}
refunds = {}

@app.post("/payments/charge")
def charge(req: ChargeRequest):
    if req.idempotency_key in payments:
        return payments[req.idempotency_key]

    payment_id = str(uuid.uuid4())
    result = {"payment_id": payment_id, "status": "PENDING"}
    payments[req.idempotency_key] = result
    return result

def payment_exists(payment_id: str) -> bool:
    return any(p["payment_id"] == payment_id for p in payments.values())


@app.post("/payments/refund")
def refund(req: RefundRequest):
    if req.payment_id in refunds:
        return refunds[req.payment_id]

    if not payment_exists(req.payment_id):
        raise HTTPException(status_code=404, detail="Payment not found")

    result = {"payment_id": req.payment_id, "status": "REFUNDED"}
    refunds[req.payment_id] = result
    return result