# Structured JSON logging, shared by the FastAPI app (app/main.py) and the
# background worker (app/worker.py). One JSON object per line, every field
# a real key rather than something baked into a free-text message - see
# the explanation at the end of this project's conversation history for why
# that's what log aggregators (Datadog, Splunk, Grafana Loki, ...) need.
#
# Third-party libraries we depend on (httpx, apscheduler, uvicorn) log
# through Python's stdlib `logging`, not structlog - `foreign_pre_chain`
# below routes those through the exact same processors (so they get
# timestamp/level/service too) before the same JSONRenderer emits them,
# rather than ending up as a second, inconsistent log format mixed in with
# ours.
import logging
import sys

import structlog

_SHARED_PROCESSORS = [
    structlog.contextvars.merge_contextvars,
    structlog.stdlib.add_log_level,
    structlog.processors.TimeStamper(fmt="iso", utc=True, key="timestamp"),
    structlog.processors.StackInfoRenderer(),
    structlog.processors.format_exc_info,
]


def configure_logging(service: str) -> None:
    """
    Call once, at process startup, with the name of the running service
    ("app", "worker"). Binds service=<service> as a context variable, so
    every log line this process emits - ours or a third-party library's -
    carries it automatically without repeating it at every call site.

    lead_id works the same way but is bound per-lead, not per-process: see
    process_lead() in app/worker.py, which calls
    structlog.contextvars.bind_contextvars(lead_id=...) once at the top of
    each lead's processing. Because asyncio.create_task() gives each task
    its own copy of the current context, that binding is automatically
    visible to every log call made anywhere in that lead's call chain -
    including inside retry callbacks, circuit breaker transitions, and rate
    limiter waits, none of which need to accept or pass lead_id themselves
    - and is never visible to a *different* lead's concurrently-running
    task. (It also survives asyncio.to_thread(), which is how the circuit
    breaker's synchronous storage calls run.)
    """
    structlog.configure(
        processors=_SHARED_PROCESSORS + [structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=_SHARED_PROCESSORS,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            # default=str: without it, JSONRenderer (plain json.dumps under
            # the hood) raises TypeError on the very first UUID or datetime
            # value anyone logs - str() is a fine fallback for anything
            # that isn't already JSON-native.
            structlog.processors.JSONRenderer(default=str),
        ],
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers = [handler]
    root_logger.setLevel(logging.INFO)

    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(service=service)
