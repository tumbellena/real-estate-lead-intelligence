# Contract test for property_api: hits the actual running service over
# HTTP - not a mock of it in the test-double sense - and validates the
# response against the exact shape app/worker.py assumes when it parses
# `_fetch_property_data`'s result. If property_api's response shape ever
# drifts (a field renamed, dropped, or retyped) without the worker being
# updated to match, this is what catches it - a unit test that mocks
# httpx's response would just re-assert whatever the code already believes.
from datetime import date

import httpx
import os
from pydantic import BaseModel

PROPERTY_API_URL = os.getenv("PROPERTY_API_URL", "http://localhost:8001")
REQUEST_TIMEOUT_SECONDS = 10.0


class PropertyResponse(BaseModel):
    """The exact shape app/worker.py's _fetch_property_data expects back."""

    address: str
    estimated_value: int
    bedrooms: int
    bathrooms: float
    square_footage: int
    year_built: int
    last_sale_date: date


def test_property_api_health():
    response = httpx.get(f"{PROPERTY_API_URL}/health", timeout=REQUEST_TIMEOUT_SECONDS)
    assert response.status_code == 200


def test_property_api_returns_documented_shape():
    response = httpx.get(
        f"{PROPERTY_API_URL}/property",
        params={"address": "1 Contract Test Way"},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    assert response.status_code == 200, response.text

    # Raises pydantic.ValidationError - and fails this test - if a field is
    # missing, renamed, or comes back as the wrong type.
    parsed = PropertyResponse.model_validate(response.json())

    assert parsed.address == "1 Contract Test Way"
    assert parsed.bedrooms > 0
    assert parsed.square_footage > 0
    assert 1800 < parsed.year_built <= date.today().year
    assert parsed.last_sale_date <= date.today()
