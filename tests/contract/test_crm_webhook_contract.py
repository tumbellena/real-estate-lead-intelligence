# Contract test for crm_webhook: sends a lead payload shaped like the one
# app/worker.py's post_to_crm actually sends (including the Idempotency-Key
# header) and validates the acknowledgement response. Confirms both
# directions of the contract - what we send is accepted, and what comes
# back is shaped the way our code expects.
import os
import uuid

import httpx
from pydantic import BaseModel

CRM_WEBHOOK_URL = os.getenv("CRM_WEBHOOK_URL", "http://localhost:8003")
REQUEST_TIMEOUT_SECONDS = 10.0


class CrmWebhookResponse(BaseModel):
    """The exact shape app/worker.py's _deliver_to_crm expects back."""

    status: str


def _sample_lead_payload() -> dict:
    return {
        "lead_id": str(uuid.uuid4()),
        "name": "Contract Test Lead",
        "phone": "+1 415-555-0100",
        "email": "contract-test@example.com",
        "property_address": "1 Contract Test Way",
        "property_data": {"estimated_value": 500000, "bedrooms": 3},
        "phone_validation": {"is_mobile": True, "carrier": "Verizon"},
        "score": "warm",
        "message": "This is a contract test message.",
    }


def test_crm_webhook_health():
    response = httpx.get(f"{CRM_WEBHOOK_URL}/health", timeout=REQUEST_TIMEOUT_SECONDS)
    assert response.status_code == 200


def test_crm_webhook_accepts_lead_and_acknowledges():
    payload = _sample_lead_payload()
    idempotency_key = str(uuid.uuid4())

    response = httpx.post(
        f"{CRM_WEBHOOK_URL}/leads",
        json=payload,
        headers={"Idempotency-Key": idempotency_key},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    assert response.status_code == 200, response.text

    parsed = CrmWebhookResponse.model_validate(response.json())
    assert parsed.status == "received"
