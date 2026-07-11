# FastAPI is the web framework we use to build the API.
# "FastAPI" is a class - we create one instance of it, and that instance
# is the whole application. Uvicorn (our web server) runs this instance.
import hashlib
import os

import httpx
import structlog
from fastapi import Depends, FastAPI, Response, status
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.admin import router as admin_router
from app.database import get_db
from app.kill_switch import is_automation_enabled
from app.logging_config import configure_logging
from app.metrics import DailyCounter
from app.models import CircuitBreakerState, DeadLetterQueueEntry, Lead, LeadStatus
from app.schemas import LeadCreateRequest, LeadCreateResponse

configure_logging("app")
logger = structlog.get_logger()

# Create the application instance.
# `title` just shows up in the auto-generated API docs (visit /docs once running).
app = FastAPI(title="Real Estate Lead Intelligence API")
app.include_router(admin_router)

leads_received_counter = DailyCounter()

# The worker process's own internal metrics endpoint (see app/worker.py) -
# not published to the host, only reachable inside the docker network.
# GET /metrics below calls this to read state that only exists in the
# worker's memory (rate limiter tokens, its own enriched/failed counts).
WORKER_METRICS_URL = os.getenv("WORKER_METRICS_URL", "http://worker:9100/metrics")


# The @app.get(...) line is a "decorator" - it tells FastAPI:
# "whenever an HTTP GET request comes in for this path, run the function below."
@app.get("/health")
def health_check():
    """Simple health check endpoint used to confirm the API is running."""
    # FastAPI automatically converts this Python dict into a JSON response,
    # e.g. {"status": "ok"}, with a 200 OK status code.
    return {"status": "ok"}


def compute_idempotency_key(phone: str, email: str) -> str:
    """
    Derives a stable fingerprint for "this same lead" from phone + email, so
    that submitting the identical contact twice is detected as a duplicate
    rather than silently creating two rows.

    We normalize (strip whitespace, lowercase the email) before hashing so
    that trivially-different input that represents the same person - e.g.
    "Jane@Example.com" vs "jane@example.com" - produces the same key.
    """
    normalized = f"{phone.strip()}|{email.strip().lower()}"
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


@app.post("/leads", response_model=LeadCreateResponse)
async def create_lead(
    payload: LeadCreateRequest,
    response: Response,
    db: AsyncSession = Depends(get_db),
):
    """
    Creates a new lead, or - if we've already seen this exact phone+email
    combination - returns the existing lead instead of creating a duplicate.
    """
    idempotency_key = compute_idempotency_key(payload.phone, payload.email)

    # Check whether we've already stored a lead with this fingerprint.
    existing_lead = await db.scalar(
        select(Lead).where(Lead.idempotency_key == idempotency_key)
    )
    if existing_lead is not None:
        logger.info("lead_duplicate_detected", lead_id=existing_lead.id, source=payload.source)
        response.status_code = status.HTTP_200_OK
        return LeadCreateResponse(id=existing_lead.id)

    lead = Lead(
        name=payload.name,
        phone=payload.phone,
        email=payload.email,
        property_address=payload.property_address,
        source=payload.source,
        status=LeadStatus.PENDING,
        idempotency_key=idempotency_key,
    )
    db.add(lead)

    try:
        await db.commit()
    except IntegrityError:
        # Two requests with the same phone+email arrived at almost the same
        # time: both passed the "does it exist" check above before either
        # had committed, so both tried to INSERT - the database's unique
        # constraint on idempotency_key let only one succeed. Rather than
        # returning a confusing 500 error, we roll back our failed attempt
        # and hand back the row the other request just created.
        await db.rollback()
        existing_lead = await db.scalar(
            select(Lead).where(Lead.idempotency_key == idempotency_key)
        )
        response.status_code = status.HTTP_200_OK
        return LeadCreateResponse(id=existing_lead.id)

    await db.refresh(lead)
    leads_received_counter.increment()
    logger.info("lead_received", lead_id=lead.id, source=payload.source)
    response.status_code = status.HTTP_202_ACCEPTED
    return LeadCreateResponse(id=lead.id)


@app.get("/metrics")
async def get_metrics(db: AsyncSession = Depends(get_db)):
    """
    A snapshot of the system's current health, meant to be polled (by a
    human, a dashboard, or a monitoring agent - see this project's
    conversation history for how a real Prometheus/Datadog setup would
    consume this). Three different sources feed it, because the data
    genuinely lives in three different places:

    - leads_received_total is this process's own in-memory counter.
    - current_circuit_states / dlq_size / worker_status are read straight
      from Postgres - they're already the persisted source of truth
      (circuit_breaker_state, dead_letter_queue, system_config via the
      kill switch), so querying them fresh is more correct than keeping a
      second, potentially-stale in-memory copy in sync with the DB.
    - leads_enriched_total / leads_failed_total / rate_limiter_state exist
      only in the *worker* process's memory - a separate OS process this
      one can't read directly - so they're fetched with a short-timeout
      internal HTTP call to the worker's own metrics endpoint. If the
      worker is down or unreachable, those three come back as None/{}
      rather than failing the whole request - a monitoring endpoint that
      itself becomes unavailable exactly when something's wrong is the
      worst possible time for that to happen.
    """
    circuit_rows = await db.execute(select(CircuitBreakerState))
    current_circuit_states = {row.service_name: row.state.value for row in circuit_rows.scalars()}

    # Every row in dead_letter_queue represents a lead that was never
    # successfully enriched - successes delete their row (see process_lead
    # in app/worker.py) - so the table's size *is* the unresolved count,
    # whether a row is still being retried or has been given up on for good.
    dlq_size = await db.scalar(select(func.count()).select_from(DeadLetterQueueEntry))

    automation_enabled = await is_automation_enabled(db)
    worker_status = "running" if automation_enabled else "paused_by_kill_switch"

    leads_enriched_total = None
    leads_failed_total = None
    rate_limiter_state: dict = {}
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            response = await client.get(WORKER_METRICS_URL)
            response.raise_for_status()
            worker_metrics = response.json()
        leads_enriched_total = worker_metrics["leads_enriched_total"]
        leads_failed_total = worker_metrics["leads_failed_total"]
        rate_limiter_state = worker_metrics["rate_limiter_state"]
    except Exception:
        logger.warning("worker_metrics_unavailable")

    return {
        "leads_received_total": leads_received_counter.value(),
        "leads_enriched_total": leads_enriched_total,
        "leads_failed_total": leads_failed_total,
        "current_circuit_states": current_circuit_states,
        "dlq_size": dlq_size,
        "rate_limiter_state": rate_limiter_state,
        "worker_status": worker_status,
    }
