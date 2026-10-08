from temporalio import workflow
from temporalio.common import RetryPolicy
from datetime import timedelta
from typing import List

with workflow.unsafe.imports_passed_through():
    from activities.order_activities import create_shipment, OrderItem


@workflow.defn
class ShipmentWorkflow:
    def __init__(self):
        self.delivered = False
        self.lost = False

    @workflow.signal
    def mark_delivered(self):
        self.delivered = True

    @workflow.signal
    def mark_lost(self):
        self.lost = True

    @workflow.run
    async def run(self, package: List[OrderItem], idempotency_key: str) -> str:
        try:
            await workflow.execute_activity(
                create_shipment,
                args=[package, idempotency_key],
                start_to_close_timeout=timedelta(seconds=20),
                retry_policy=RetryPolicy(
                maximum_attempts=3,
                initial_interval=timedelta(seconds=2),
                backoff_coefficient=2.0
                )
            )
        except Exception:
            return "LOST_IN_TRANSIT"

        try:
            await workflow.wait_condition(
                lambda: self.delivered or self.lost,
                timeout=timedelta(seconds=60)
            )
        except TimeoutError:
            return "LOST_IN_TRANSIT"

        if self.delivered:
            return "DELIVERED"
        elif self.lost:
            return "LOST_IN_TRANSIT"