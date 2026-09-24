import asyncio
import argparse
from temporalio.client import Client

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--order-id", required=True)
    parser.add_argument("--status", required=True, choices=["CONFIRMED", "FAILED"])
    args = parser.parse_args()

    client = await Client.connect("localhost:7233")
    handle = client.get_workflow_handle(f"order-{args.order_id}")
    await handle.signal("payment_webhook", args.status)
    print(f"Sent payment_webhook signal: {args.status} to order-{args.order_id}")

if __name__ == "__main__":
    asyncio.run(main())