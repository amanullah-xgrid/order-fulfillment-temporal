import asyncio
from temporalio.client import Client
from temporalio.worker import Worker

from workflows.order_fulfillment_workflow import OrderFulfillmentWorkflow
from activities.order_activities import validate_order, reserve_inventory, charge_payment, release_inventory

async def main():
    client = await Client.connect("localhost:7233")

    worker = Worker(
        client,
        task_queue="order-fulfillment-tq",
        workflows=[OrderFulfillmentWorkflow],
        activities=[validate_order, reserve_inventory, charge_payment, release_inventory],
    )

    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())