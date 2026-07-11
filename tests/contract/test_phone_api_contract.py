# Contract test for phone_api: hits the real running service and validates
# the response against the shape app/worker.py's _fetch_phone_validation
# expects. See test_property_api_contract.py for why this matters more
# than a mocked unit test would.
import os

import httpx
from pydantic import BaseModel

PHONE_API_URL = os.getenv("PHONE_API_URL", "http://localhost:8002")
REQUEST_TIMEOUT_SECONDS = 10.0


class PhoneValidationResponse(BaseModel):
    """The exact shape app/worker.py's _fetch_phone_validation expects back."""

    phone: str
    is_mobile: bool
    carrier: str
    is_disconnected: bool
    timezone: str


def test_phone_api_health():
    response = httpx.get(f"{PHONE_API_URL}/health", timeout=REQUEST_TIMEOUT_SECONDS)
    assert response.status_code == 200


def test_phone_api_returns_documented_shape():
    test_phone = "+1 415-555-0100"
    response = httpx.get(
        f"{PHONE_API_URL}/validate",
        params={"phone": test_phone},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    assert response.status_code == 200, response.text

    parsed = PhoneValidationResponse.model_validate(response.json())

    assert parsed.phone == test_phone
    assert parsed.carrier != ""
    assert parsed.timezone.startswith("America/")
