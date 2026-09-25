import asyncio
import argparse
import json
from temporalio.client import Client

from workflows.order_fulfillment_workflow import OrderFulfillmentWorkflow
from activities.order_activities import Order, OrderItem

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--order", required=True, help="Path to a JSON order file")
    args = parser.parse_args()

    with open(args.order) as f:
        data = json.load(f)

    order = Order(
        order_id=data["order_id"],
        items=[OrderItem(sku=i["sku"], qty=i["qty"]) for i in data["items"]]
    )

    client = await Client.connect("localhost:7233")

    result = await client.execute_workflow(
        OrderFulfillmentWorkflow.run,
        order,
        id=f"order-{order.order_id}",
        task_queue="order-fulfillment-tq",
    )

    print(f"Workflow result: {result}")

if __name__ == "__main__":
    asyncio.run(main())