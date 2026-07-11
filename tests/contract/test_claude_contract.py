# Contract test for the Claude API - the one dependency in this suite
# that's a genuine external vendor we don't control (property_api,
# phone_api, and crm_webhook are our own mocks). Sends the same kind of
# scoring prompt app/worker.py's score_lead_with_claude sends in
# production, and asserts the response still parses into the shape the
# worker expects: valid JSON with a hot/warm/cold score and a message.
#
# This is the test that would have caught a Claude response-format change
# (e.g. a model that always prepends reasoning before the JSON, which is
# exactly what broke worker.py's original content[0] assumption during
# development - see the ThinkingBlock comment in app/worker.py) before it
# silently started failing every lead in production.
import json
import os

import pytest
from anthropic import Anthropic
from pydantic import BaseModel, field_validator

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-5")

_VALID_SCORES = {"hot", "warm", "cold"}


class LeadScoreResponse(BaseModel):
    """The exact shape app/worker.py's score_lead_with_claude expects back."""

    score: str
    message: str

    @field_validator("score")
    @classmethod
    def score_must_be_hot_warm_or_cold(cls, value: str) -> str:
        if value not in _VALID_SCORES:
            raise ValueError(f"score must be one of {_VALID_SCORES}, got {value!r}")
        return value


def _extract_text(response) -> str:
    # Some models put a ThinkingBlock before the text block in `content` -
    # mirrors the same handling in app/worker.py's score_lead_with_claude.
    text_block = next(block for block in response.content if block.type == "text")
    raw_text = text_block.text.strip()
    if raw_text.startswith("```"):
        raw_text = raw_text.strip("`").removeprefix("json").strip()
    return raw_text


@pytest.mark.skipif(not ANTHROPIC_API_KEY, reason="ANTHROPIC_API_KEY not set")
def test_claude_scoring_prompt_returns_valid_json_shape():
    client = Anthropic(api_key=ANTHROPIC_API_KEY)

    prompt = (
        "You are helping a real estate agent triage an inbound lead.\n\n"
        "Lead name: Contract Test Lead\n"
        "Property address: 1 Contract Test Way\n"
        'Property data: {"estimated_value": 500000, "bedrooms": 3, "year_built": 1998}\n'
        'Phone validation: {"is_mobile": true, "is_disconnected": false}\n\n'
        'Score how promising this lead is to contact right now as "hot", '
        '"warm", or "cold", and draft a 2-sentence personalized outreach '
        "message that references a specific detail from the property data.\n\n"
        "Respond with ONLY this JSON object and nothing else:\n"
        '{"score": "hot|warm|cold", "message": "<2 sentences>"}'
    )

    response = client.messages.create(
        model=ANTHROPIC_MODEL,
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )

    raw_text = _extract_text(response)

    # Fails the test (not silently falls back to something else) if Claude
    # didn't return valid JSON at all.
    parsed_json = json.loads(raw_text)

    # Fails the test if the JSON is valid but missing/misnamed/mistyped
    # fields, or if score isn't one of the three values the worker handles.
    parsed = LeadScoreResponse.model_validate(parsed_json)

    assert parsed.score in _VALID_SCORES
    assert len(parsed.message.split(".")) >= 2  # roughly "2 sentences"
