import asyncio
import logging
from temporalio.client import Client
from temporalio.worker import Worker

from workflows.order_fulfillment_workflow import OrderFulfillmentWorkflow
from workflows.reconciliation_workflow import ReconciliationWorkflow
from workflows.shipment_workflow import ShipmentWorkflow
from activities.order_activities import (
    validate_order, reserve_inventory, charge_payment, release_inventory,
    refund_payment, scan_and_cancel_stuck_orders, create_shipment, restock_inventory
)

logging.basicConfig(level=logging.INFO)

async def main():
    client = await Client.connect("localhost:7233")

    worker = Worker(
        client,
        task_queue="order-fulfillment-tq",
        workflows=[OrderFulfillmentWorkflow, ReconciliationWorkflow, ShipmentWorkflow],
        activities=[
            validate_order, reserve_inventory, charge_payment, release_inventory,
            refund_payment, scan_and_cancel_stuck_orders, create_shipment, restock_inventory
        ],
    )

    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())