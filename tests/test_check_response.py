import httpx
import pytest
from temporalio.exceptions import ApplicationError

from activities.order_activities import check_response


def make_response(status, **kwargs):
    # httpx refuses raise_for_status() on a response with no request attached
    return httpx.Response(status, request=httpx.Request("POST", "http://test"), **kwargs)


def test_good_reply_returns_the_data():
    data = check_response(make_response(200, json={"status": "RESERVED", "reservation_id": "r1"}), "status", "reservation_id")
    assert data["status"] == "RESERVED"


def test_null_value_still_counts_as_present():
    # the OUT_OF_STOCK shape: the key exists, its value is null
    body = {"status": "OUT_OF_STOCK", "reservation_id": None, "unavailable_skus": ["WIDGET-1"]}
    data = check_response(make_response(200, json=body), "status", "reservation_id", "unavailable_skus")
    assert data["reservation_id"] is None


@pytest.mark.parametrize("status", [400, 404, 422])
def test_client_errors_are_not_retryable(status):
    with pytest.raises(ApplicationError) as exc:
        check_response(make_response(status, json={"detail": "nope"}), "status")
    assert exc.value.non_retryable


@pytest.mark.parametrize("status", [408, 429, 500, 503])
def test_temporary_errors_stay_retryable(status):
    with pytest.raises(httpx.HTTPStatusError):
        check_response(make_response(status), "status")


def test_invalid_json_is_not_retryable():
    with pytest.raises(ApplicationError, match="not valid JSON") as exc:
        check_response(make_response(200, content=b"not json"), "status")
    assert exc.value.non_retryable


def test_json_that_is_not_an_object_is_not_retryable():
    with pytest.raises(ApplicationError, match="not a JSON object") as exc:
        check_response(make_response(200, json=[1, 2]), "status")
    assert exc.value.non_retryable


def test_missing_key_is_not_retryable_and_names_the_field():
    with pytest.raises(ApplicationError, match="payment_id") as exc:
        check_response(make_response(200, json={"status": "PENDING"}), "payment_id", "status")
    assert exc.value.non_retryable