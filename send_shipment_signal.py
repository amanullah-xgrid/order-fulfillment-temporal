import asyncio
import argparse
from temporalio.client import Client


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workflow-id", required=True)
    parser.add_argument(
        "--status",
        required=True,
        choices=["mark_delivered", "mark_lost"]
    )
    args = parser.parse_args()

    client = await Client.connect("localhost:7233")

    handle = client.get_workflow_handle(args.workflow_id)

    await handle.signal(args.status)

    print(f"Sent {args.status} signal to {args.workflow_id}")


if __name__ == "__main__":
    asyncio.run(main())