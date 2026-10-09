import asyncio
import pytest
from datetime import timedelta
from temporalio.exceptions import ApplicationError
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from typing import List
from temporalio.service import RPCError
from workflows.shipment_workflow import ShipmentWorkflow

from workflows.order_fulfillment_workflow import OrderFulfillmentWorkflow
from activities.order_activities import (
    Order, OrderItem, validate_order,
    ReservationResult, PaymentResult, ReleaseResult, RefundResult, ShipmentResult, RestockResult
)


@activity.defn(name="reserve_inventory")
async def fake_reserve_inventory_success(order: Order) -> ReservationResult:
    return ReservationResult(
        status="RESERVED",
        reservation_id="fake-reservation-123",
        unavailable_skus=[]
    )


@activity.defn(name="charge_payment")
async def fake_charge_payment_success(order: Order, idempotency_key: str) -> PaymentResult:
    return PaymentResult(
        payment_id="fake-payment-123",
        status="PENDING"
    )


@activity.defn(name="release_inventory")
async def fake_release_inventory(reservation_id: str) -> ReleaseResult:
    return ReleaseResult(status="RELEASED")


@activity.defn(name="refund_payment")
async def fake_refund_payment(payment_id: str) -> RefundResult:
    return RefundResult(payment_id=payment_id, status="REFUNDED")


@pytest.mark.asyncio
async def test_paid_delivered_and_completed():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="test-tq",
            workflows=[OrderFulfillmentWorkflow, ShipmentWorkflow],
            activities=[
                validate_order,
                fake_reserve_inventory_success,
                fake_charge_payment_success,
                fake_release_inventory,
                fake_refund_payment,
                fake_create_shipment,
            ],
        ):
            order = Order(order_id="TEST-001", items=[OrderItem(sku="WIDGET-1", qty=1)])

            handle = await env.client.start_workflow(
                OrderFulfillmentWorkflow.run,
                order,
                id="test-order-TEST-001",
                task_queue="test-tq",
            )

            await handle.signal("payment_webhook", "CONFIRMED")
            await signal_when_started(env.client, "test-order-TEST-001-shipment-0", "mark_delivered")

            result = await handle.result()
            assert result == "COMPLETED"

@activity.defn(name="reserve_inventory")
async def fake_reserve_inventory_out_of_stock(order: Order) -> ReservationResult:
    return ReservationResult(
        status="OUT_OF_STOCK",
        reservation_id=None,
        unavailable_skus=["WIDGET-1"]
    )

@pytest.mark.asyncio
async def test_out_of_stock_flow():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="test-tq",
            workflows=[OrderFulfillmentWorkflow],
            activities=[
                validate_order,
                fake_reserve_inventory_out_of_stock,
                fake_charge_payment_success,
                fake_release_inventory,
                fake_refund_payment,
            ],
        ):

            order = Order(order_id="TEST-002", items=[OrderItem(sku="WIDGET-1", qty=1)])

            handle = await env.client.start_workflow(
                OrderFulfillmentWorkflow.run,
                order,
                id="test-order-TEST-002",
                task_queue="test-tq",
            )

            result = await handle.result()
            assert result == "OUT_OF_STOCK"

charge_attempts = {"count": 0}
charge_endpoint_hits = []

@activity.defn(name="charge_payment")
async def fake_charge_payment_fails_twice(order: Order, idempotency_key: str) -> PaymentResult:
    charge_attempts["count"] += 1
    charge_endpoint_hits.append(idempotency_key)

    if charge_attempts["count"] < 3:
        raise RuntimeError("Simulated transient payment gateway failure")

    return PaymentResult(payment_id="fake-payment-456", status="PENDING")    

@pytest.mark.asyncio
async def test_charge_payment_retries_then_succeeds():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="test-tq",
            workflows=[OrderFulfillmentWorkflow, ShipmentWorkflow],
            activities=[
                validate_order,
                fake_reserve_inventory_success,
                fake_charge_payment_fails_twice,
                fake_release_inventory,
                fake_refund_payment,
                fake_create_shipment,
            ],
        ):
            order = Order(order_id="TEST-003", items=[OrderItem(sku="WIDGET-1", qty=1)])

            handle = await env.client.start_workflow(
                OrderFulfillmentWorkflow.run,
                order,
                id="test-order-TEST-003",
                task_queue="test-tq",
            )

            await handle.signal("payment_webhook", "CONFIRMED")
            await signal_when_started(env.client, "test-order-TEST-003-shipment-0", "mark_delivered")

            result = await handle.result()
            assert result == "COMPLETED"
            assert len(charge_endpoint_hits) == 3
            assert len(set(charge_endpoint_hits)) == 1

release_calls = []
refund_calls = []
refund_attempts = []

@activity.defn(name="release_inventory")
async def counting_fake_release_inventory(reservation_id: str) -> ReleaseResult:
    release_calls.append(reservation_id)
    return ReleaseResult(status="RELEASED")


@activity.defn(name="refund_payment")
async def counting_fake_refund_payment(payment_id: str) -> RefundResult:
    refund_calls.append(payment_id)
    return RefundResult(payment_id=payment_id, status="REFUNDED")

@activity.defn(name="refund_payment")
async def fake_refund_payment_not_found(payment_id: str) -> RefundResult:
    refund_attempts.append(payment_id)
    raise ApplicationError("Payment not found, cannot refund", non_retryable=True)

restock_calls = []

@activity.defn(name="create_shipment")
async def fake_create_shipment(package: List[OrderItem], idempotency_key: str) -> ShipmentResult:
    return ShipmentResult(package_id=f"fake-package-{idempotency_key}")


@activity.defn(name="restock_inventory")
async def counting_fake_restock_inventory(reservation_id: str) -> RestockResult:
    restock_calls.append(reservation_id)
    return RestockResult(status="RESTOCKED") 

async def signal_when_started(client, workflow_id, signal_name):
    handle = client.get_workflow_handle(workflow_id)
    for _ in range(150):
        try:
            await handle.signal(signal_name)
            return
        except RPCError:
            await asyncio.sleep(0.1)
    raise AssertionError(f"{workflow_id} never started")


async def wait_until(check):
    for _ in range(150):
        if await check():
            return
        await asyncio.sleep(0.1)
    raise AssertionError("condition never became true")

@pytest.mark.asyncio
async def test_two_packages_delivered_then_returned():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="test-tq",
            workflows=[OrderFulfillmentWorkflow, ShipmentWorkflow],
            activities=[
                validate_order,
                fake_reserve_inventory_success,
                fake_charge_payment_success,
                fake_release_inventory,
                counting_fake_refund_payment,
                fake_create_shipment,
                counting_fake_restock_inventory,
            ],
        ):
            order = Order(
                order_id="TEST-011",
                items=[OrderItem(sku="WIDGET-1", qty=2), OrderItem(sku="WIDGET-2", qty=2)],
            )
            handle = await env.client.start_workflow(
                OrderFulfillmentWorkflow.run,
                order,
                id="test-order-TEST-011",
                task_queue="test-tq",
            )

            await handle.signal("payment_webhook", "CONFIRMED")
            await signal_when_started(env.client, "test-order-TEST-011-shipment-0", "mark_delivered")
            await signal_when_started(env.client, "test-order-TEST-011-shipment-1", "mark_delivered")

            await wait_until(lambda: handle.query("is_return_window_open"))
            await handle.signal("request_return")

            result = await handle.result()
            assert result == "RETURNED"
            assert refund_calls == ["fake-payment-123"]
            assert restock_calls == ["fake-reservation-123"]   

@pytest.mark.asyncio
async def test_cancel_and_confirm_race():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="test-tq",
            workflows=[OrderFulfillmentWorkflow],
            activities=[
                validate_order,
                fake_reserve_inventory_success,
                fake_charge_payment_success,
                counting_fake_release_inventory,
                counting_fake_refund_payment,
            ],
        ):
            order = Order(order_id="TEST-004", items=[OrderItem(sku="WIDGET-1", qty=1)])

            handle = await env.client.start_workflow(
                OrderFulfillmentWorkflow.run,
                order,
                id="test-order-TEST-004",
                task_queue="test-tq",
            )

            await asyncio.gather(
                handle.signal("payment_webhook", "CONFIRMED"),
                handle.signal("cancel_order")
            )

            result = await handle.result()
            assert result == "CANCELLED"
            assert len(release_calls) == 1
            assert len(refund_calls) == 1                

@pytest.fixture(autouse=True)
def reset_call_tracking():
    charge_attempts["count"] = 0
    charge_endpoint_hits.clear()
    release_calls.clear()
    refund_calls.clear()
    recorded_keys.clear()
    refund_attempts.clear()
    restock_calls.clear()
@pytest.mark.asyncio
async def test_payment_timeout_flow():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="test-tq",
            workflows=[OrderFulfillmentWorkflow],
            activities=[
                validate_order,
                fake_reserve_inventory_success,
                fake_charge_payment_success,
                counting_fake_release_inventory,
                fake_refund_payment,
            ],
        ):
            order = Order(order_id="TEST-005", items=[OrderItem(sku="WIDGET-1", qty=1)])

            handle = await env.client.start_workflow(
                OrderFulfillmentWorkflow.run,
                order,
                id="test-order-TEST-005",
                task_queue="test-tq",
            )

            result = await handle.result()
            assert result == "PAYMENT_TIMEOUT"
            assert len(release_calls) == 1
            assert release_calls[0] == "fake-reservation-123"

@activity.defn(name="charge_payment")
async def fake_charge_payment_always_fails(order: Order, idempotency_key: str) -> PaymentResult:
    charge_attempts["count"] += 1
    charge_endpoint_hits.append(idempotency_key)
    raise RuntimeError("Simulated permanent payment gateway failure")

@pytest.mark.asyncio
async def test_charge_payment_exhausts_retries_and_releases():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="test-tq",
            workflows=[OrderFulfillmentWorkflow],
            activities=[
                validate_order,
                fake_reserve_inventory_success,
                fake_charge_payment_always_fails,
                counting_fake_release_inventory,
                fake_refund_payment,
            ],
        ):
            order = Order(order_id="TEST-006", items=[OrderItem(sku="WIDGET-1", qty=1)])

            handle = await env.client.start_workflow(
                OrderFulfillmentWorkflow.run,
                order,
                id="test-order-TEST-006",
                task_queue="test-tq",
            )

            with pytest.raises(Exception):
                await handle.result()

            assert charge_attempts["count"] == 3
            assert len(release_calls) == 1
            assert len(refund_calls) == 0                

@pytest.mark.asyncio
async def test_payment_failed_flow():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="test-tq",
            workflows=[OrderFulfillmentWorkflow],
            activities=[
                validate_order,
                fake_reserve_inventory_success,
                fake_charge_payment_success,
                counting_fake_release_inventory,
                fake_refund_payment,
            ],
        ):
            order = Order(order_id="TEST-007", items=[OrderItem(sku="WIDGET-1", qty=1)])

            handle = await env.client.start_workflow(
                OrderFulfillmentWorkflow.run,
                order,
                id="test-order-TEST-007",
                task_queue="test-tq",
            )

            await handle.signal("payment_webhook", "FAILED")

            result = await handle.result()
            assert result == "PAYMENT_FAILED"
            assert len(release_calls) == 1
            assert release_calls[0] == "fake-reservation-123"
            assert len(refund_calls) == 0 

recorded_keys = []

@activity.defn(name="charge_payment")
async def fake_charge_payment_records_key(order: Order, idempotency_key: str) -> PaymentResult:
    recorded_keys.append(idempotency_key)
    return PaymentResult(payment_id=f"fake-payment-{idempotency_key}", status="PENDING")

@pytest.mark.asyncio
async def test_idempotency_key_differs_across_orders():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="test-tq",
            workflows=[OrderFulfillmentWorkflow, ShipmentWorkflow],
            activities=[
                validate_order,
                fake_reserve_inventory_success,
                fake_charge_payment_records_key,
                counting_fake_release_inventory,
                fake_refund_payment,
                fake_create_shipment,
            ],
        ):
            order_a = Order(order_id="TEST-008", items=[OrderItem(sku="WIDGET-1", qty=1)])
            order_b = Order(order_id="TEST-009", items=[OrderItem(sku="WIDGET-1", qty=1)])

            handle_a = await env.client.start_workflow(
                OrderFulfillmentWorkflow.run,
                order_a,
                id="test-order-TEST-008",
                task_queue="test-tq",
            )
            handle_b = await env.client.start_workflow(
                OrderFulfillmentWorkflow.run,
                order_b,
                id="test-order-TEST-009",
                task_queue="test-tq",
            )

            await handle_a.signal("payment_webhook", "CONFIRMED")
            await handle_b.signal("payment_webhook", "CONFIRMED")
            await signal_when_started(env.client, "test-order-TEST-008-shipment-0", "mark_delivered")
            await signal_when_started(env.client, "test-order-TEST-009-shipment-0", "mark_delivered")

            result_a = await handle_a.result()
            result_b = await handle_b.result()

            assert result_a == "COMPLETED"
            assert result_b == "COMPLETED"
            assert len(recorded_keys) == 2
            assert len(set(recorded_keys)) == 2  

@pytest.mark.asyncio
async def test_refund_not_found_fails_permanently():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="test-tq",
            workflows=[OrderFulfillmentWorkflow],
            activities=[
                validate_order,
                fake_reserve_inventory_success,
                fake_charge_payment_success,
                fake_release_inventory,
                fake_refund_payment_not_found,
            ],
        ):
            order = Order(order_id="TEST-010", items=[OrderItem(sku="WIDGET-1", qty=1)])

            handle = await env.client.start_workflow(
                OrderFulfillmentWorkflow.run,
                order,
                id="test-order-TEST-010",
                task_queue="test-tq",
            )

            await handle.signal("cancel_order")

            with pytest.raises(Exception):
                await handle.result()

            assert len(refund_attempts) == 1              