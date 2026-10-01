import asyncio
import argparse
from datetime import datetime
from temporalio.client import Client

from workflows.reconciliation_workflow import ReconciliationWorkflow

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--threshold-seconds", type=int, default=120)
    args = parser.parse_args()

    client = await Client.connect("localhost:7233")

    workflow_id = f"reconciliation-manual-{datetime.now().strftime('%Y%m%d%H%M%S')}"

    result = await client.execute_workflow(
        ReconciliationWorkflow.run,
        args.threshold_seconds,
        id=workflow_id,
        task_queue="order-fulfillment-tq",
    )

    print(f"Cancelled order IDs: {result}")

if __name__ == "__main__":
    asyncio.run(main())