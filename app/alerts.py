# Shared Slack alerting, used by both the API process (app/admin.py, for
# kill switch flips) and the worker (app/circuit_breaker.py, app/worker.py,
# for circuit-open/error-rate/DLQ-size/permanently-failed-lead alerts).
# Reuses the SLACK_WEBHOOK_URL pattern already established in app/worker.py
# (best-effort delivery: log and move on if unconfigured or unreachable,
# never let a notification problem interrupt the thing that triggered it).
#
# The one thing that pattern didn't have yet is deduplication - this module
# adds a 15-minute cooldown per (alert_type, scope) so a condition that
# stays true (a circuit that stays open, an error rate that stays elevated)
# doesn't re-page every time something re-checks it. See DEDUP_WINDOW_SECONDS.
import os
import time

import httpx
import structlog

logger = structlog.get_logger()

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

DEDUP_WINDOW_SECONDS = 15 * 60

# scope distinguishes independent incidents that happen to share an
# alert_type - e.g. "circuit_opened" for property_api and "circuit_opened"
# for claude are unrelated incidents and must not suppress each other, so
# each gets its own cooldown keyed by (alert_type, scope). Alert types with
# no natural scope (error rate, DLQ size, kill switch) pass scope=None and
# get one global cooldown.
_last_sent_at: dict[str, float] = {}


def _dedup_key(alert_type: str, scope: str | None) -> str:
    return f"{alert_type}:{scope}" if scope else alert_type


def _is_duplicate(alert_type: str, scope: str | None) -> bool:
    """
    Records this attempt's timestamp as a side effect, whether or not the
    send actually succeeds afterward - the cooldown is "don't check this
    condition's alert again for 15 minutes," not "retry until delivery
    succeeds," so a misconfigured webhook doesn't turn into a tight retry
    loop either.
    """
    key = _dedup_key(alert_type, scope)
    now = time.monotonic()
    last_sent = _last_sent_at.get(key)
    _last_sent_at[key] = now
    return last_sent is not None and (now - last_sent) < DEDUP_WINDOW_SECONDS


async def send_slack_alert(
    alert_type: str,
    text: str,
    *,
    scope: str | None = None,
    http_client: httpx.AsyncClient | None = None,
) -> None:
    """
    Posts `text` to Slack, deduplicated per (alert_type, scope) within
    DEDUP_WINDOW_SECONDS. Pass an existing http_client to reuse a
    connection pool (e.g. the worker's shared client); otherwise a
    short-lived one is created just for this call.
    """
    if _is_duplicate(alert_type, scope):
        logger.info("alert_suppressed_duplicate", alert_type=alert_type, scope=scope)
        return

    if not SLACK_WEBHOOK_URL:
        logger.warning(
            "slack_alert_skipped", alert_type=alert_type, scope=scope,
            reason="SLACK_WEBHOOK_URL_not_configured",
        )
        return

    owns_client = http_client is None
    client = http_client or httpx.AsyncClient(timeout=5.0)
    try:
        response = await client.post(SLACK_WEBHOOK_URL, json={"text": text})
        response.raise_for_status()
        logger.info("slack_alert_sent", alert_type=alert_type, scope=scope)
    except Exception:
        logger.exception("slack_alert_failed", alert_type=alert_type, scope=scope)
    finally:
        if owns_client:
            await client.aclose()
