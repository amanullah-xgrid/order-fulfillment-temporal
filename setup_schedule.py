import asyncio
from temporalio.client import (
    Client, Schedule, ScheduleActionStartWorkflow, ScheduleSpec, ScheduleCalendarSpec, ScheduleRange
)
from workflows.reconciliation_workflow import ReconciliationWorkflow

async def main():
    client = await Client.connect("localhost:7233")

    await client.create_schedule(
        "nightly-reconciliation",
        Schedule(
            action=ScheduleActionStartWorkflow(
                ReconciliationWorkflow.run,
                120,
                id="reconciliation-scheduled",
                task_queue="order-fulfillment-tq",
            ),
            spec=ScheduleSpec(
                calendars=[ScheduleCalendarSpec(hour=[ScheduleRange(2)])]
            ),
        ),
    )
    print("Schedule 'nightly-reconciliation' created.")

if __name__ == "__main__":
    asyncio.run(main())