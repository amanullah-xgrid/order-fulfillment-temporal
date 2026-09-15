from temporalio import workflow
from datetime import timedelta

with workflow.unsafe.imports_passed_through():
    from activities.order_activities import Order, validate_order, reserve_inventory

@workflow.defn
class OrderFulfillmentWorkflow:
    @workflow.run
    async def run(self, order: Order) -> str:
        await workflow.execute_activity(validate_order, order, start_to_close_timeout=timedelta(seconds=5))
        result = await workflow.execute_activity(reserve_inventory, order, start_to_close_timeout=timedelta(seconds=30))
        if result.status == "RESERVED":
            return "RESERVED"
        else:
            return "OUT_OF_STOCK"