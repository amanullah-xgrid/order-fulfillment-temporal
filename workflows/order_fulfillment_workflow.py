import asyncio
from temporalio import workflow
from temporalio.common import RetryPolicy
from datetime import timedelta

with workflow.unsafe.imports_passed_through():
    from activities.order_activities import (
        Order, validate_order, reserve_inventory, charge_payment,
        release_inventory, refund_payment, split_into_packages,
    )
    from workflows.shipment_workflow import ShipmentWorkflow

@workflow.defn
class OrderFulfillmentWorkflow:
    def __init__(self):
        self.payment_confirmed = False
        self.payment_failed = False
        self.cancel_requested = False
        self.compensations = []
        self.payment_wait_started_at = None

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

    @workflow.query
    def get_payment_wait_started_at(self) -> str | None:
        return self.payment_wait_started_at.isoformat() if self.payment_wait_started_at else None

    async def _compensate(self):
        while self.compensations:
            fn, arg = self.compensations.pop()
            await workflow.execute_activity(fn, arg, start_to_close_timeout=timedelta(seconds=5))

    @workflow.run
    async def run(self, order: Order) -> str:
        try:
            await workflow.execute_activity(
                validate_order,
                order,
                start_to_close_timeout=timedelta(seconds=5),
                retry_policy=RetryPolicy(non_retryable_error_types=["ValueError"]),
            )
            result = await workflow.execute_activity(reserve_inventory, order, start_to_close_timeout=timedelta(seconds=5))

            if result.status != "RESERVED":
                return "OUT_OF_STOCK"

            self.compensations.append((release_inventory, result.reservation_id))

            idempotency_key = str(workflow.uuid4())

            payment = await workflow.execute_activity(
                charge_payment,
                args=[order, idempotency_key],
                start_to_close_timeout=timedelta(seconds=15),
                retry_policy=RetryPolicy(
                    maximum_attempts=3,
                    initial_interval=timedelta(seconds=2),
                    backoff_coefficient=2.0,
                ),
            )

            self.compensations.append((refund_payment, payment.payment_id))

            self.payment_wait_started_at = workflow.now()

            try:
                await workflow.wait_condition(
                    lambda: self.payment_confirmed or self.payment_failed or self.cancel_requested,
                    timeout=timedelta(seconds=60),  # shortened for dev; doc says 2h in production
                )
            except TimeoutError:
                await self._compensate()
                return "PAYMENT_TIMEOUT"

            # await asyncio.sleep(15)  # uncomment to reliably demo the CONFIRMED+cancel race

            if self.cancel_requested:
                await self._compensate()
                return "CANCELLED"
            elif self.payment_failed:
                await self._compensate()
                return "PAYMENT_FAILED"

            packages = split_into_packages(order)

            shipment_handles = []
            for pkg in packages:
                ship_key = str(workflow.uuid4())
                handle = await workflow.start_child_workflow(
                    ShipmentWorkflow.run,
                    args=[pkg, ship_key],
                    id=f"{workflow.info().workflow_id}-shipment-{len(shipment_handles)}",
                )
                shipment_handles.append(handle)

            results = await asyncio.gather(*shipment_handles, return_exceptions=True)

            lost_count = sum(1 for r in results if r == "LOST_IN_TRANSIT" or isinstance(r, Exception))

            if lost_count > 0:
                # Known limitation: refunds the full order amount on any lost package,
                # not proportional to what was actually lost. True per-package refunds
                # would need per-item pricing, which Order/OrderItem don't currently track.
                await workflow.execute_activity(
                    refund_payment,
                    payment.payment_id,
                    start_to_close_timeout=timedelta(seconds=5),
                )
                return "PARTIALLY_LOST"

            return "COMPLETED"
        except Exception:
            await self._compensate()
            raise