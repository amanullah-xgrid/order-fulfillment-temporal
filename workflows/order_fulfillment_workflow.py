from temporalio import workflow
from temporalio.common import RetryPolicy
from datetime import timedelta

with workflow.unsafe.imports_passed_through():
    from activities.order_activities import Order, validate_order, reserve_inventory, charge_payment, release_inventory

@workflow.defn
class OrderFulfillmentWorkflow:
    def __init__(self):
        self.payment_confirmed = False
        self.payment_failed = False
        self.cancel_requested = False

    @workflow.signal
    def payment_webhook(self, status: str):
        if status == "CONFIRMED":
            self.payment_confirmed = True
        elif status == "FAILED":
            self.payment_failed = True

    @workflow.signal
    def cancel_order(self):
        self.cancel_requested = True

    @workflow.query
    def get_order_status(self) -> str:
        if self.payment_confirmed:
            return "PAID"
        elif self.payment_failed:
            return "PAYMENT_FAILED"
        elif self.cancel_requested:
            return "CANCELLED"
        else:
            return "AWAITING_PAYMENT_CONFIRMATION"

    @workflow.run
    async def run(self, order: Order) -> str:
        await workflow.execute_activity(validate_order, order, start_to_close_timeout=timedelta(seconds=5))
        result = await workflow.execute_activity(reserve_inventory, order, start_to_close_timeout=timedelta(seconds=5))

        if result.status != "RESERVED":
            return "OUT_OF_STOCK"

        payment = await workflow.execute_activity(
            charge_payment,
            order,
            start_to_close_timeout=timedelta(seconds=15),
            retry_policy=RetryPolicy(
                maximum_attempts=3,
                initial_interval=timedelta(seconds=2),
                backoff_coefficient=2.0
            )
        )

        try:
            await workflow.wait_condition(
                lambda: self.payment_confirmed or self.payment_failed or self.cancel_requested,
                timeout=timedelta(seconds=60)  # shortened for dev; doc says 2h in production
            )
        except TimeoutError:
            await workflow.execute_activity(release_inventory, result.reservation_id, start_to_close_timeout=timedelta(seconds=5))
            return "PAYMENT_TIMEOUT"

        if self.cancel_requested:
            await workflow.execute_activity(release_inventory, result.reservation_id, start_to_close_timeout=timedelta(seconds=5))
            return "CANCELLED"
        elif self.payment_failed:
            await workflow.execute_activity(release_inventory, result.reservation_id, start_to_close_timeout=timedelta(seconds=5))
            return "PAYMENT_FAILED"
        else:
            return "PAID"