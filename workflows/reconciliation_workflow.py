from temporalio import workflow
from datetime import timedelta

with workflow.unsafe.imports_passed_through():
    from activities.order_activities import scan_and_cancel_stuck_orders

@workflow.defn
class ReconciliationWorkflow:
    @workflow.run
    async def run(self, threshold_seconds: int = 120) -> list[str]:
        result = await workflow.execute_activity(
            scan_and_cancel_stuck_orders,
            threshold_seconds,
            start_to_close_timeout=timedelta(seconds=30)
        )
        return result.cancelled_order_ids