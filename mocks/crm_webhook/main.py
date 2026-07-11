# Mock CRM webhook receiver. Simulates the endpoint a real CRM (e.g.
# Salesforce, HubSpot) would give us to push new leads to - it just logs
# whatever it's sent so we can confirm the app is calling out correctly,
# without needing a real CRM account to test against.
import asyncio
import json
import logging
import os
import sys

import structlog
from fastapi import FastAPI, HTTPException, Request

MODE = os.getenv("MODE", "healthy")

# Structured JSON logging, matching the setup in app/logging_config.py -
# duplicated here (not imported) because this mock is its own standalone
# Docker image, built from mocks/crm_webhook/Dockerfile, and doesn't have
# the app/ package available inside its container. Routes uvicorn's own
# (stdlib-logging-based) access logs through the same JSON renderer too,
# rather than leaving them as plain text next to our structured lines.
_shared_processors = [
    structlog.contextvars.merge_contextvars,
    structlog.stdlib.add_log_level,
    structlog.processors.TimeStamper(fmt="iso", utc=True, key="timestamp"),
    structlog.processors.format_exc_info,
]
structlog.configure(
    processors=_shared_processors + [structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)
_formatter = structlog.stdlib.ProcessorFormatter(
    foreign_pre_chain=_shared_processors,
    processors=[
        structlog.stdlib.ProcessorFormatter.remove_processors_meta,
        structlog.processors.JSONRenderer(default=str),
    ],
)
_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(_formatter)
logging.getLogger().handlers = [_handler]
logging.getLogger().setLevel(logging.INFO)

structlog.contextvars.bind_contextvars(service="crm_webhook")
logger = structlog.get_logger()

app = FastAPI(title="Mock CRM Webhook")

# Idempotency-Key values we've already seen, in memory. A real CRM would
# use this to return the cached response from the first attempt instead of
# reprocessing - this mock only needs to prove the caller is sending a
# stable key across retries, so it just flags repeats.
_seen_idempotency_keys: set[str] = set()


async def apply_mode() -> None:
    """Simulates the failure modes a real third-party API can exhibit."""
    if MODE == "rate_limited":
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded",
            headers={"Retry-After": "5"},
        )
    if MODE == "broken":
        raise HTTPException(status_code=500, detail="Internal server error")
    if MODE == "slow":
        await asyncio.sleep(10)


@app.get("/health")
def health_check():
    return {"status": "ok", "mode": MODE}


@app.post("/leads")
async def receive_lead(request: Request):
    body = await request.body()

    # Best-effort only: we log lead_id when we can find it, but "a real
    # CRM's webhook contract isn't ours to define" (see below) applies to
    # parsing too - a body that isn't valid JSON, or has no lead_id, still
    # gets accepted and logged, just without that field.
    lead_id = None
    try:
        lead_id = json.loads(body).get("lead_id")
    except (json.JSONDecodeError, AttributeError):
        pass

    idempotency_key = request.headers.get("Idempotency-Key")
    if idempotency_key:
        if idempotency_key in _seen_idempotency_keys:
            logger.warning(
                "duplicate_idempotency_key_detected",
                lead_id=lead_id,
                idempotency_key=idempotency_key,
            )
        else:
            _seen_idempotency_keys.add(idempotency_key)

    await apply_mode()

    # Accept whatever the caller sends - a real CRM's webhook contract
    # isn't ours to define, so we just log the raw body rather than
    # validating it against a schema.
    logger.info(
        "lead_webhook_received",
        lead_id=lead_id,
        idempotency_key=idempotency_key,
        body=body.decode("utf-8", errors="replace"),
    )

    return {"status": "received"}
