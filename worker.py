import asyncio
import logging
from temporalio.client import Client
from temporalio.worker import Worker

from workflows.order_fulfillment_workflow import OrderFulfillmentWorkflow
from workflows.reconciliation_workflow import ReconciliationWorkflow
from activities.order_activities import validate_order, reserve_inventory, charge_payment, release_inventory, refund_payment, scan_and_cancel_stuck_orders

logging.basicConfig(level=logging.INFO)

async def main():
    client = await Client.connect("localhost:7233")

    worker = Worker(
        client,
        task_queue="order-fulfillment-tq",
        workflows=[OrderFulfillmentWorkflow, ReconciliationWorkflow],
        activities=[validate_order, reserve_inventory, charge_payment, release_inventory, refund_payment, scan_and_cancel_stuck_orders],
    )

    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())