# Background enrichment worker. Runs as its own process/container (see the
# "worker" service in docker-compose.yml) instead of inside the FastAPI app,
# so a slow enrichment - a laggy mock service, a slow Claude response - never
# blocks the API from answering requests. It talks to the same Postgres
# database as the API, plus the three mock services and the Anthropic API.
#
# Pipeline per lead: property_api -> phone_api -> Claude (score + message)
# -> save to the lead row -> POST to crm_webhook. Every step writes a row to
# enrichment_events, so a lead's full history can be reconstructed later.
# Each of those four external calls retries itself (see retry_for_service
# below) before the pipeline ever sees a failure, is separately guarded by
# its own circuit breaker (see app/circuit_breaker.py) that stops calling a
# service altogether once it's been failing consistently, and is paced by
# its own token-bucket rate limiter (see app/rate_limiter.py) that holds
# the call rather than firing it off when a service's per-minute cap is hit.
# A lead that still fails after all of that is marked FAILED and written to
# dead_letter_queue; a scheduled sweep (see sweep_dead_letter_queue below)
# gives it a few more chances over time before giving up for good.
# The whole loop can be paused without a restart via the kill switch (see
# app/kill_switch.py) - checked at the top of every iteration below.
# Slack alerts (see app/alerts.py) fire when a circuit opens, when the
# error rate or DLQ size crosses a threshold (see check_alert_thresholds),
# and when a lead is given up on for good - each deduplicated so a
# condition that stays true doesn't re-page every time it's re-checked.
import asyncio
import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone

import httpx
import structlog
import uvicorn
from anthropic import APIConnectionError, APIStatusError, AsyncAnthropic
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv
from fastapi import FastAPI
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_random_exponential

from app.alerts import send_slack_alert
from app.circuit_breaker import build_sync_session_factory, call_with_breaker, make_breaker
from app.database import AsyncSessionLocal
from app.kill_switch import is_automation_enabled
from app.logging_config import configure_logging
from app.metrics import DailyCounter
from app.models import DeadLetterQueueEntry, EnrichmentEvent, Lead, LeadStatus
from app.rate_limiter import TokenBucket

load_dotenv()

configure_logging("worker")
logger = structlog.get_logger()

POLL_INTERVAL_SECONDS = 2
HTTP_TIMEOUT_SECONDS = 30.0

PROPERTY_API_URL = os.getenv("PROPERTY_API_URL", "http://property_api:8001")
PHONE_API_URL = os.getenv("PHONE_API_URL", "http://phone_api:8002")
CRM_WEBHOOK_URL = os.getenv("CRM_WEBHOOK_URL", "http://crm_webhook:8003")
# SLACK_WEBHOOK_URL itself lives in app/alerts.py now - every Slack alert in
# this process goes through send_slack_alert() there, for the deduplication.

ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-5")
_VALID_SCORES = {"hot", "warm", "cold"}

# Dead-letter-queue tuning: how often the sweep runs, how far out it
# reschedules a requeued item, and how many total failures (the original
# plus sweep-driven retries) a lead gets before we stop trying automatically.
DLQ_SWEEP_INTERVAL_MINUTES = 15
DLQ_RETRY_DELAY_MINUTES = 15
DLQ_MAX_ATTEMPTS = 3

# How often to re-check the level-based alert conditions (error rate, DLQ
# size) - these aren't triggered by a single event the way a circuit
# opening or a kill switch flip are, so something has to periodically ask
# "is this still true right now."
ALERT_CHECK_INTERVAL_MINUTES = 2
ERROR_RATE_WINDOW_MINUTES = 15
ERROR_RATE_ALERT_THRESHOLD_PERCENT = 10.0
DLQ_SIZE_ALERT_THRESHOLD = 20

# How often to log "kill switch engaged, worker paused" while paused - not
# every poll tick (that would spam the logs every 2 seconds), just often
# enough to prove the process is still alive and checking, not dead.
KILL_SWITCH_LOG_INTERVAL_SECONDS = 30

# Port for the internal-only /metrics endpoint (see _metrics_app below) -
# not published to the host in docker-compose.yml, only reachable inside
# the docker network. This is how the API process's own GET /metrics
# (app/main.py) reads worker-local in-memory state it has no other way to
# see, since it's a separate OS process.
WORKER_METRICS_PORT = int(os.getenv("WORKER_METRICS_PORT", "9100"))

leads_enriched_counter = DailyCounter()
leads_failed_counter = DailyCounter()


def _utcnow() -> datetime:
    # The dead_letter_queue timestamp columns are timezone-naive (matching
    # every other timestamp column in this project - see the naive/aware
    # note in app/circuit_breaker.py), so this returns naive UTC rather
    # than the deprecated datetime.utcnow() or an aware datetime.now(UTC)
    # that Postgres would silently truncate anyway.
    return datetime.now(timezone.utc).replace(tzinfo=None)


# One client, reused for the life of the process, rather than one per lead.
anthropic_client = AsyncAnthropic()

# Keeps a reference to in-flight per-lead tasks so asyncio doesn't garbage
# collect them mid-run (a well-known asyncio.create_task footgun).
_background_tasks: set[asyncio.Task] = set()

# One circuit breaker per external service, each persisted to its own row
# in circuit_breaker_state (see app/circuit_breaker.py). Built once at
# import time and reused for the life of the process, same as anthropic_client.
_sync_session_factory = build_sync_session_factory(os.environ["DATABASE_URL"])
property_breaker, _property_breaker_storage = make_breaker("property_api", _sync_session_factory)
phone_breaker, _phone_breaker_storage = make_breaker("phone_api", _sync_session_factory)
claude_breaker, _claude_breaker_storage = make_breaker("claude", _sync_session_factory)
crm_breaker, _crm_breaker_storage = make_breaker("crm_webhook", _sync_session_factory)

# One token-bucket rate limiter per external service (see app/rate_limiter.py).
# Caps are requests/minute; acquire() is called once per actual attempt
# inside each bare _fetch_*/_call_*/_deliver_* function below, so retries
# consume their own tokens too rather than slipping through unpaced.
property_rate_limiter = TokenBucket("property_api", rate_per_minute=30)
phone_rate_limiter = TokenBucket("phone_api", rate_per_minute=60)
claude_rate_limiter = TokenBucket("claude", rate_per_minute=50)
crm_rate_limiter = TokenBucket("crm_webhook", rate_per_minute=100)


class EnrichmentServiceError(Exception):
    """
    Raised when a call to an external enrichment service (property_api,
    phone_api, claude, crm_webhook) is still failing after every retry
    attempt. Wraps whatever the underlying library raised (httpx's or
    anthropic's own exception types) so the rest of the worker only has to
    handle one exception type regardless of which service failed.
    """

    def __init__(self, service_name: str, last_error: BaseException):
        self.service_name = service_name
        self.last_error = last_error
        super().__init__(f"{service_name} failed after retries: {last_error!r}")


def _is_retryable_error(exc: BaseException) -> bool:
    """
    True for problems that are plausibly transient and worth retrying:
    network-level failures, 429 (rate limited), and 5xx (the service's
    fault). False for everything else, including other 4xx responses like
    400 or 401 - those mean *we* sent something wrong, and retrying an
    identical request will just fail identically four times instead of one.
    """
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        return status == 429 or status >= 500
    if isinstance(exc, httpx.RequestError):
        # Connection refused, DNS failure, timeout, etc. - never even got
        # an HTTP response to inspect, so it's presumed transient.
        return True
    if isinstance(exc, APIStatusError):
        return exc.status_code == 429 or exc.status_code >= 500
    if isinstance(exc, APIConnectionError):
        return True
    return False


def retry_for_service(service_name: str):
    """
    Builds a @retry decorator for calls to `service_name`: up to 4 attempts
    total, exponential backoff with full jitter capped at 30s between
    attempts, retrying only network errors / 429s / 5xxs (see
    _is_retryable_error). If every attempt is exhausted, raises
    EnrichmentServiceError instead of tenacity's own RetryError.
    """

    def _log_retry(retry_state) -> None:
        exc = retry_state.outcome.exception()
        logger.info(
            "retry_attempt",
            service_name=service_name,
            attempt=retry_state.attempt_number,
            wait_seconds=round(retry_state.next_action.sleep, 2),
            error=repr(exc),
        )

    def _give_up(retry_state):
        raise EnrichmentServiceError(service_name, retry_state.outcome.exception())

    return retry(
        stop=stop_after_attempt(4),
        wait=wait_random_exponential(multiplier=1, max=30),
        retry=retry_if_exception(_is_retryable_error),
        before_sleep=_log_retry,
        retry_error_callback=_give_up,
    )


@retry_for_service("property_api")
async def _fetch_property_data(http_client: httpx.AsyncClient, address: str) -> dict:
    await property_rate_limiter.acquire()
    response = await http_client.get(f"{PROPERTY_API_URL}/property", params={"address": address})
    response.raise_for_status()
    return response.json()


@retry_for_service("phone_api")
async def _fetch_phone_validation(http_client: httpx.AsyncClient, phone: str) -> dict:
    await phone_rate_limiter.acquire()
    response = await http_client.get(f"{PHONE_API_URL}/validate", params={"phone": phone})
    response.raise_for_status()
    return response.json()


@retry_for_service("claude")
async def _call_claude(prompt: str):
    await claude_rate_limiter.acquire()
    return await anthropic_client.messages.create(
        model=ANTHROPIC_MODEL,
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )


@retry_for_service("crm_webhook")
async def _deliver_to_crm(http_client: httpx.AsyncClient, payload: dict, idempotency_key: str) -> None:
    await crm_rate_limiter.acquire()
    response = await http_client.post(
        f"{CRM_WEBHOOK_URL}/leads",
        json=payload,
        headers={"Idempotency-Key": idempotency_key},
    )
    response.raise_for_status()


async def log_event(
    db: AsyncSession,
    lead_id: uuid.UUID,
    event_type: str,
    service_name: str | None = None,
    message: str | None = None,
) -> None:
    """Writes one enrichment_events row and logs the same information."""
    db.add(
        EnrichmentEvent(
            lead_id=lead_id,
            event_type=event_type,
            service_name=service_name,
            message=message,
        )
    )
    await db.commit()
    # event_type ("started", "service_called", "failed", "dlq_requeued", ...)
    # is already exactly the short event name structured logging wants -
    # used directly as the structlog event rather than wrapped in one.
    logger.info(event_type, lead_id=lead_id, service_name=service_name, message=message)


async def claim_pending_leads(db: AsyncSession) -> list[Lead]:
    """
    Selects every PENDING lead and flips it to ENRICHING before anyone else
    can see it as PENDING.

    `.with_for_update(skip_locked=True)` is what actually makes this safe if
    more than one worker instance is ever running: it locks the matching
    rows at the database level as part of the SELECT, and tells Postgres to
    skip (not wait for) any row another transaction already has locked. So
    two workers polling at the same instant can never both select the same
    row - one gets it, the other's query simply skips it. Setting
    status='enriching' before doing any work, in the same transaction as the
    lock, is what turns "we're processing this" into a durable fact other
    workers can see even after we commit and release the lock.
    """
    result = await db.execute(
        select(Lead)
        .where(Lead.status == LeadStatus.PENDING)
        .with_for_update(skip_locked=True)
    )
    leads = list(result.scalars().all())

    for lead in leads:
        lead.status = LeadStatus.ENRICHING
    await db.commit()

    for lead in leads:
        await log_event(db, lead.id, "started")

    return leads


async def run_with_events(db: AsyncSession, lead_id: uuid.UUID, service_name: str, coro_factory) -> object:
    """
    Wraps a (possibly self-retrying) service call with service_called /
    service_succeeded enrichment_events. These two events bracket the whole
    call, retries included - a call that succeeds on its third attempt still
    only produces one service_called and one service_succeeded row; the
    retry attempts themselves are visible in the logs (see retry_for_service)
    rather than as extra event rows.
    """
    await log_event(db, lead_id, "service_called", service_name=service_name)
    result = await coro_factory()
    await log_event(db, lead_id, "service_succeeded", service_name=service_name)
    return result


async def call_guarded(
    db: AsyncSession,
    lead_id: uuid.UUID,
    breaker,
    breaker_storage,
    service_name: str,
    coro_factory,
) -> object:
    """
    The full guarded path to an external service: circuit breaker gate
    check -> enrichment_events (service_called/service_succeeded) -> the
    actual (self-retrying) call. If the circuit is open, this raises
    CircuitOpenError immediately - no events are logged and the service is
    never touched, because we generate no proof we "called" something we
    didn't. If the call eventually fails (after its own retries), that
    failure is recorded against the breaker, which may trip it open.
    """
    async def attempt():
        return await run_with_events(db, lead_id, service_name, coro_factory)

    return await call_with_breaker(breaker, breaker_storage, attempt)


async def score_lead_with_claude(
    db: AsyncSession, lead_id: uuid.UUID, lead: Lead, property_data: dict, phone_validation: dict
) -> tuple[str, str]:
    """
    Asks Claude to read the property/phone data we just gathered and return
    a hot/warm/cold score plus a 2-sentence outreach message, as a single
    JSON object so one call covers both.
    """
    prompt = (
        "You are helping a real estate agent triage an inbound lead.\n\n"
        f"Lead name: {lead.name}\n"
        f"Property address: {lead.property_address}\n"
        f"Property data: {json.dumps(property_data)}\n"
        f"Phone validation: {json.dumps(phone_validation)}\n\n"
        'Score how promising this lead is to contact right now as "hot", '
        '"warm", or "cold", and draft a 2-sentence personalized outreach '
        "message that references a specific detail from the property data.\n\n"
        "Respond with ONLY this JSON object and nothing else:\n"
        '{"score": "hot|warm|cold", "message": "<2 sentences>"}'
    )

    response = await call_guarded(
        db, lead_id, claude_breaker, _claude_breaker_storage, "claude",
        lambda: _call_claude(prompt),
    )

    # Some models (e.g. extended-thinking ones) put a ThinkingBlock before
    # the actual text block in `content`, so we can't assume content[0] is
    # the answer - find the text block explicitly instead.
    text_block = next(block for block in response.content if block.type == "text")
    raw_text = text_block.text.strip()

    # Claude sometimes wraps JSON in a ```json ... ``` fence even when asked
    # not to - strip that rather than failing the lead over formatting.
    if raw_text.startswith("```"):
        raw_text = raw_text.strip("`").removeprefix("json").strip()

    # Deliberately not retried: a malformed response is a prompting/parsing
    # problem, not a transient service failure, so retrying it four times
    # would just waste four API calls to fail the same way each time.
    parsed = json.loads(raw_text)
    score = str(parsed["score"]).strip().lower()
    message = str(parsed["message"]).strip()

    if score not in _VALID_SCORES:
        raise ValueError(f"Claude returned an unexpected score: {score!r}")

    return score, message


async def post_to_crm(db: AsyncSession, http_client: httpx.AsyncClient, lead: Lead) -> None:
    # Generated once per lead and persisted before the first send, so every
    # attempt - each tenacity retry, a half-open circuit breaker trial, or
    # even a whole new process_lead() run after a crash - reuses the exact
    # same key instead of minting a fresh one. A key generated fresh on
    # every attempt would defeat the point: the CRM can only recognize a
    # retry as "the same request" if the key is stable across attempts.
    if lead.crm_idempotency_key is None:
        lead.crm_idempotency_key = str(uuid.uuid4())
        await db.commit()

    payload = {
        "lead_id": str(lead.id),
        "name": lead.name,
        "phone": lead.phone,
        "email": lead.email,
        "property_address": lead.property_address,
        "property_data": lead.property_data,
        "phone_validation": lead.phone_validation,
        "score": lead.enrichment_score,
        "message": lead.personalized_message,
    }

    async def attempt():
        await log_event(db, lead.id, "service_called", service_name="crm_webhook")
        await _deliver_to_crm(http_client, payload, lead.crm_idempotency_key)
        await log_event(db, lead.id, "delivered_to_crm", service_name="crm_webhook")

    await call_with_breaker(crm_breaker, _crm_breaker_storage, attempt)


async def mark_failed(lead_id: uuid.UUID, service_name: str, error_message: str) -> None:
    """
    Marks a lead failed and ensures it has an active dead_letter_queue row
    pointing the sweep at it. If this is the lead's first failure, that
    means inserting a new row (attempts=1). If a row already exists -
    because the sweep already requeued this lead once and it just failed
    again - the sweep owns that row's attempts/next_retry_at from here on,
    so this only refreshes the failure reason rather than resetting the
    retry clock a second time.
    """
    # Uses a brand-new session rather than whatever session the failure
    # happened in - that one may be mid-transaction or otherwise unusable
    # after an exception, and we need this write to succeed regardless.
    async with AsyncSessionLocal() as db:
        lead = await db.get(Lead, lead_id)
        lead.status = LeadStatus.FAILED

        existing_entry = await db.scalar(
            select(DeadLetterQueueEntry)
            .where(DeadLetterQueueEntry.lead_id == lead_id)
            .where(DeadLetterQueueEntry.next_retry_at.is_not(None))
        )
        if existing_entry is not None:
            existing_entry.service_name = service_name
            existing_entry.error_message = error_message
        else:
            db.add(
                DeadLetterQueueEntry(
                    lead_id=lead_id,
                    service_name=service_name,
                    error_message=error_message,
                    attempts=1,
                    next_retry_at=_utcnow() + timedelta(minutes=DLQ_RETRY_DELAY_MINUTES),
                )
            )

        await log_event(db, lead_id, "failed", service_name=service_name, message=error_message)
    leads_failed_counter.increment()


async def process_lead(lead_id: uuid.UUID, http_client: httpx.AsyncClient) -> None:
    """Runs the full enrichment pipeline for one lead, start to finish."""
    # Bound once, here, rather than passed to every function in the call
    # chain below: asyncio.create_task() (see poll_once) gives this task
    # its own copy of the current context, so this is invisible to any
    # other lead's concurrently-running task, but automatically flows into
    # every log call this lead's processing makes - including ones deep
    # inside retry callbacks, circuit breaker transitions, and rate limiter
    # waits, none of which need to know about lead_id at all.
    structlog.contextvars.bind_contextvars(lead_id=lead_id)
    try:
        async with AsyncSessionLocal() as db:
            lead = await db.get(Lead, lead_id)

            property_data = await call_guarded(
                db, lead_id, property_breaker, _property_breaker_storage, "property_api",
                lambda: _fetch_property_data(http_client, lead.property_address),
            )
            phone_validation = await call_guarded(
                db, lead_id, phone_breaker, _phone_breaker_storage, "phone_api",
                lambda: _fetch_phone_validation(http_client, lead.phone),
            )

            score, message = await score_lead_with_claude(db, lead_id, lead, property_data, phone_validation)
            await log_event(db, lead_id, "ai_scored", service_name="claude", message=score)

            lead.property_data = property_data
            lead.phone_validation = phone_validation
            lead.enrichment_score = score
            lead.personalized_message = message
            lead.status = LeadStatus.ENRICHED
            await db.commit()

            await post_to_crm(db, http_client, lead)

            # A lead that reaches here succeeded, possibly after already
            # having failed once and been requeued by the DLQ sweep - clear
            # any leftover row so the sweep doesn't resurrect a lead that
            # has since been enriched.
            await db.execute(delete(DeadLetterQueueEntry).where(DeadLetterQueueEntry.lead_id == lead_id))
            await db.commit()

        leads_enriched_counter.increment()
        logger.info("enrichment_complete")
    except Exception as exc:
        logger.exception("enrichment_failed")
        # EnrichmentServiceError and CircuitOpenError both carry the name of
        # the service that caused this - fall back to "unknown" for
        # anything else (e.g. a Claude response that failed to parse).
        service_name = getattr(exc, "service_name", "unknown")
        await mark_failed(lead_id, service_name, str(exc))


async def alert_lead_permanently_failed(http_client: httpx.AsyncClient, entry: DeadLetterQueueEntry) -> None:
    """Alerts for a lead the DLQ sweep has given up on for good."""
    text = (
        f":rotating_light: Lead `{entry.lead_id}` permanently failed enrichment "
        f"after {entry.attempts} attempts.\n"
        f"Last failing service: *{entry.service_name}*\n"
        f"Error: {entry.error_message}"
    )
    # scope=lead_id: each lead only ever reaches "permanently failed" once
    # (there's no re-triggering it), so this scope mainly just keeps it out
    # of the way of every *other* alert_type's dedup bookkeeping.
    await send_slack_alert(
        "lead_permanently_failed", text, scope=str(entry.lead_id), http_client=http_client
    )


async def check_alert_thresholds(http_client: httpx.AsyncClient) -> None:
    """
    Runs every ALERT_CHECK_INTERVAL_MINUTES (see main()). Unlike a circuit
    opening or a kill switch flip, "error rate is too high" and "the DLQ is
    too big" aren't single events - they're conditions that are either true
    or not whenever you look, so something has to periodically look.
    """
    structlog.contextvars.bind_contextvars(service="alert_checker")

    async with AsyncSessionLocal() as db:
        window_start = _utcnow() - timedelta(minutes=ERROR_RATE_WINDOW_MINUTES)
        processed = await db.scalar(
            select(func.count())
            .select_from(Lead)
            .where(
                Lead.status.in_([LeadStatus.ENRICHED, LeadStatus.FAILED]),
                Lead.updated_at >= window_start,
            )
        )
        failed = await db.scalar(
            select(func.count())
            .select_from(Lead)
            .where(Lead.status == LeadStatus.FAILED, Lead.updated_at >= window_start)
        )
        dlq_size = await db.scalar(select(func.count()).select_from(DeadLetterQueueEntry))

    error_rate = (failed / processed * 100) if processed else 0.0
    logger.info(
        "alert_thresholds_checked",
        processed=processed, failed=failed, error_rate=round(error_rate, 1), dlq_size=dlq_size,
    )

    if processed > 0 and error_rate > ERROR_RATE_ALERT_THRESHOLD_PERCENT:
        await send_slack_alert(
            "error_rate_high",
            f":rotating_light: Error rate is *{error_rate:.1f}%* over the last "
            f"{ERROR_RATE_WINDOW_MINUTES} minutes ({failed}/{processed} leads failed).",
            http_client=http_client,
        )

    if dlq_size > DLQ_SIZE_ALERT_THRESHOLD:
        await send_slack_alert(
            "dlq_size_high",
            f":rotating_light: Dead letter queue has *{dlq_size}* unresolved items "
            f"(threshold: {DLQ_SIZE_ALERT_THRESHOLD}).",
            http_client=http_client,
        )


async def sweep_dead_letter_queue(http_client: httpx.AsyncClient) -> None:
    """
    Runs every DLQ_SWEEP_INTERVAL_MINUTES (see main()). For every DLQ row
    whose next_retry_at has passed: if it's already failed DLQ_MAX_ATTEMPTS
    times, give up on it for good (next_retry_at=NULL, so this query never
    selects it again) and send a Slack alert; otherwise requeue the lead
    (status='pending', picked up by the normal poll loop) and push
    next_retry_at another DLQ_RETRY_DELAY_MINUTES out.

    A plain SELECT, not SELECT ... FOR UPDATE: this worker runs one
    scheduler in one process, so there's no concurrent sweep to guard
    against - same single-process assumption as app/rate_limiter.py.
    """
    # APScheduler runs each job invocation as its own asyncio Task (via
    # eventloop.create_task), so - same as process_lead's lead_id binding -
    # this is invisible to the main poll loop and to any in-flight
    # process_lead task; it only affects log_event's calls made from
    # within this function's own call chain (including send_slack_alert).
    structlog.contextvars.bind_contextvars(service="dlq_sweeper")

    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(DeadLetterQueueEntry).where(DeadLetterQueueEntry.next_retry_at <= _utcnow())
        )
        due_entries = list(result.scalars().all())

        if not due_entries:
            return

        logger.info("dlq_sweep_starting", due_count=len(due_entries))

        for entry in due_entries:
            if entry.attempts >= DLQ_MAX_ATTEMPTS:
                entry.next_retry_at = None
                await log_event(
                    db, entry.lead_id, "dlq_permanently_failed",
                    service_name=entry.service_name, message=entry.error_message,
                )
                await alert_lead_permanently_failed(http_client, entry)
            else:
                entry.attempts += 1
                entry.next_retry_at = _utcnow() + timedelta(minutes=DLQ_RETRY_DELAY_MINUTES)
                lead = await db.get(Lead, entry.lead_id)
                lead.status = LeadStatus.PENDING
                await log_event(
                    db, entry.lead_id, "dlq_requeued",
                    service_name=entry.service_name, message=f"attempt {entry.attempts}",
                )


# Internal-only metrics endpoint: reports the state that only exists in
# this process's memory (rate limiter token counts, today's enriched/failed
# counts) so app/main.py's public GET /metrics can read it over HTTP - the
# only way for one OS process to see another's in-memory state without a
# shared store. Not published to the host (see docker-compose.yml); only
# reachable from other containers on the same docker network.
_metrics_app = FastAPI()


@_metrics_app.get("/metrics")
def internal_metrics() -> dict:
    return {
        "leads_enriched_total": leads_enriched_counter.value(),
        "leads_failed_total": leads_failed_counter.value(),
        "rate_limiter_state": {
            "property_api": property_rate_limiter.snapshot(),
            "phone_api": phone_rate_limiter.snapshot(),
            "claude": claude_rate_limiter.snapshot(),
            "crm_webhook": crm_rate_limiter.snapshot(),
        },
    }


def _track(task: asyncio.Task) -> None:
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


async def poll_once(http_client: httpx.AsyncClient) -> None:
    async with AsyncSessionLocal() as db:
        leads = await claim_pending_leads(db)

    if not leads:
        return

    logger.info("leads_claimed", count=len(leads), lead_ids=[str(lead.id) for lead in leads])
    for lead in leads:
        # Each lead runs as its own task so a slow one (a "slow" mock, or a
        # slow Claude response) doesn't hold up the others or the next poll.
        _track(asyncio.create_task(process_lead(lead.id, http_client)))


async def main() -> None:
    logger.info(
        "worker_starting",
        poll_interval_seconds=POLL_INTERVAL_SECONDS,
        http_timeout_seconds=HTTP_TIMEOUT_SECONDS,
        dlq_sweep_interval_minutes=DLQ_SWEEP_INTERVAL_MINUTES,
        dlq_max_attempts=DLQ_MAX_ATTEMPTS,
    )
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_SECONDS) as http_client:
        scheduler = AsyncIOScheduler()
        scheduler.add_job(
            sweep_dead_letter_queue,
            trigger=IntervalTrigger(minutes=DLQ_SWEEP_INTERVAL_MINUTES),
            args=[http_client],
            id="sweep_dead_letter_queue",
            max_instances=1,
        )
        scheduler.add_job(
            check_alert_thresholds,
            trigger=IntervalTrigger(minutes=ALERT_CHECK_INTERVAL_MINUTES),
            args=[http_client],
            id="check_alert_thresholds",
            max_instances=1,
        )
        scheduler.start()

        # Runs uvicorn's own server loop as a plain asyncio task inside this
        # same event loop, rather than a separate process - it's a single
        # lightweight route with no need for its own worker/reload/signal
        # handling, so there's no reason to pay for a second container.
        metrics_server = uvicorn.Server(
            uvicorn.Config(_metrics_app, host="0.0.0.0", port=WORKER_METRICS_PORT, log_config=None)
        )
        _track(asyncio.create_task(metrics_server.serve()))
        logger.info("worker_metrics_server_starting", port=WORKER_METRICS_PORT)

        last_kill_switch_log = 0.0
        while True:
            async with AsyncSessionLocal() as db:
                enabled = await is_automation_enabled(db)

            if not enabled:
                # time.monotonic(), not wall-clock time, so a system clock
                # adjustment can't make this log more or less often than intended.
                now = time.monotonic()
                if now - last_kill_switch_log >= KILL_SWITCH_LOG_INTERVAL_SECONDS:
                    # Was logged as the literal sentence "kill switch
                    # engaged, worker paused" before this structured-logging
                    # pass; kept as a "message" field alongside the new
                    # short event name so both old log searches and new
                    # aggregator queries (event:kill_switch_engaged) work.
                    logger.info("kill_switch_engaged", message="kill switch engaged, worker paused")
                    last_kill_switch_log = now
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
                continue

            try:
                await poll_once(http_client)
            except Exception:
                logger.exception("poll_tick_failed")
            await asyncio.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())
