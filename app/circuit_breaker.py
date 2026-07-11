# Circuit breaker wrapper around pybreaker, adapted for two things this
# project needs that pybreaker doesn't give you out of the box:
#
# 1. asyncio compatibility. pybreaker is a synchronous library: calling
#    `breaker.call(some_async_fn, ...)` doesn't await it - it just invokes
#    some_async_fn(...), which for an `async def` function immediately
#    returns a coroutine *object* without running any of its code. pybreaker
#    would see that as an instant, successful return and record a success
#    before the real call (the HTTP request, the Claude call) ever happens.
#    pybreaker does have a call_async(), but it's built on Tornado's
#    gen.coroutine and requires the `tornado` package, which this project
#    doesn't otherwise need. So call_with_breaker() below drives the same
#    public transition methods pybreaker itself uses (open/half_open/close)
#    by hand, around a real `await`, so success/failure is recorded only
#    after the call actually ran.
#
# 2. Persistence. pybreaker's default storage is in-memory, so a worker
#    restart would forget a service was unhealthy. SQLAlchemyCircuitBreakerStorage
#    persists to the circuit_breaker_state table instead.
import asyncio
from datetime import datetime, timedelta, timezone

import pybreaker
import structlog
from sqlalchemy import create_engine, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from app.models import CircuitBreakerState, CircuitState

# No configure_logging() call here - app/worker.py (the only process that
# imports this module) already configured structlog globally by the time
# any of this runs. service="worker" and, for calls made during a lead's
# processing, lead_id both arrive via contextvars (see app/logging_config.py
# and process_lead in app/worker.py) rather than being passed in here.
logger = structlog.get_logger()

FAIL_MAX = 5
RESET_TIMEOUT_SECONDS = 900

# pybreaker spells the middle state "half-open"; our CircuitState enum (and
# the DB column) spells it "half_open". Translate between the two here so
# nowhere else in the codebase has to know pybreaker's spelling.
_TO_ENUM = {
    pybreaker.STATE_CLOSED: CircuitState.CLOSED,
    pybreaker.STATE_OPEN: CircuitState.OPEN,
    pybreaker.STATE_HALF_OPEN: CircuitState.HALF_OPEN,
}
_TO_PYBREAKER = {v: k for k, v in _TO_ENUM.items()}


class CircuitOpenError(Exception):
    """
    Raised when a circuit breaker is open and a call is short-circuited
    before it ever reaches the service.
    """

    def __init__(self, service_name: str, next_attempt_at: datetime | None):
        self.service_name = service_name
        self.next_attempt_at = next_attempt_at
        detail = f" until {next_attempt_at.isoformat()} UTC" if next_attempt_at else ""
        super().__init__(f"Circuit for {service_name} is open{detail} - refusing to call it")


def build_sync_session_factory(async_database_url: str) -> sessionmaker:
    """
    pybreaker's storage interface is synchronous, so it needs its own
    engine - the rest of the app talks to Postgres over asyncpg, which
    can't be driven from a plain (non-async) function.
    """
    sync_url = async_database_url.replace("postgresql+asyncpg://", "postgresql+psycopg2://")
    engine = create_engine(sync_url, echo=False)
    return sessionmaker(engine, expire_on_commit=False)


class SQLAlchemyCircuitBreakerStorage(pybreaker.CircuitBreakerStorage):
    """
    Persists one service's circuit breaker state to the circuit_breaker_state
    table, so a worker restart resumes with whatever state it left off in -
    including a still-open circuit - instead of forgetting a service was
    unhealthy and immediately hammering it again.
    """

    def __init__(self, service_name: str, session_factory: sessionmaker, reset_timeout_seconds: float):
        super().__init__(service_name)
        self._session_factory = session_factory
        self._reset_timeout_seconds = reset_timeout_seconds
        self._ensure_row_exists()

    def _ensure_row_exists(self) -> None:
        with self._session_factory() as session:
            row = session.get(CircuitBreakerState, self.name)
            if row is not None:
                return
            session.add(CircuitBreakerState(service_name=self.name, state=CircuitState.CLOSED))
            try:
                session.commit()
            except IntegrityError:
                # Another worker process inserted the same row between our
                # SELECT and INSERT - the row exists either way, which is
                # all we actually wanted.
                session.rollback()

    @property
    def state(self) -> str:
        with self._session_factory() as session:
            row = session.get(CircuitBreakerState, self.name)
            return _TO_PYBREAKER[row.state]

    @state.setter
    def state(self, state: str) -> None:
        with self._session_factory() as session:
            session.execute(
                update(CircuitBreakerState)
                .where(CircuitBreakerState.service_name == self.name)
                .values(state=_TO_ENUM[state])
            )
            session.commit()

    @property
    def counter(self) -> int:
        with self._session_factory() as session:
            row = session.get(CircuitBreakerState, self.name)
            return row.failure_count

    def increment_counter(self) -> None:
        # A single atomic UPDATE (not read-modify-write), so failures
        # recorded by concurrently-processing leads can't clobber each
        # other's count.
        with self._session_factory() as session:
            session.execute(
                update(CircuitBreakerState)
                .where(CircuitBreakerState.service_name == self.name)
                .values(failure_count=CircuitBreakerState.failure_count + 1)
            )
            session.commit()

    def reset_counter(self) -> None:
        with self._session_factory() as session:
            session.execute(
                update(CircuitBreakerState)
                .where(CircuitBreakerState.service_name == self.name)
                .values(failure_count=0)
            )
            session.commit()

    # call_with_breaker (below) decides half-open success/failure itself
    # rather than relying on pybreaker's own success-counter bookkeeping,
    # so these are unused in practice but required by the
    # CircuitBreakerStorage ABC.
    @property
    def success_counter(self) -> int:
        return 0

    def increment_success_counter(self) -> None:
        pass

    def reset_success_counter(self) -> None:
        pass

    @property
    def opened_at(self) -> datetime | None:
        with self._session_factory() as session:
            row = session.get(CircuitBreakerState, self.name)
            return row.opened_at

    @opened_at.setter
    def opened_at(self, value: datetime) -> None:
        # The circuit_breaker_state columns are plain (timezone-naive)
        # timestamps, matching every other timestamp column in this
        # project - but pybreaker itself calls this setter with an
        # aware `datetime.now(UTC)`. Normalize to naive UTC before storing,
        # so what we read back later can be compared against
        # datetime.utcnow() without a naive/aware TypeError.
        if value is not None and value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        next_attempt_at = value + timedelta(seconds=self._reset_timeout_seconds) if value else None
        with self._session_factory() as session:
            session.execute(
                update(CircuitBreakerState)
                .where(CircuitBreakerState.service_name == self.name)
                .values(opened_at=value, next_attempt_at=next_attempt_at)
            )
            session.commit()

    @property
    def next_attempt_at(self) -> datetime | None:
        with self._session_factory() as session:
            row = session.get(CircuitBreakerState, self.name)
            return row.next_attempt_at


# pybreaker spells states 'closed' / 'open' / 'half-open'; these are the
# short, past-tense event names structured logging wants instead.
_TRANSITION_EVENT_NAMES = {
    pybreaker.STATE_CLOSED: "circuit_closed",
    pybreaker.STATE_OPEN: "circuit_opened",
    pybreaker.STATE_HALF_OPEN: "circuit_half_opened",
}


class _StateChangeLogger(pybreaker.CircuitBreakerListener):
    """Logs every transition: closed<->open<->half_open, with the service name."""

    def state_change(self, cb: pybreaker.CircuitBreaker, old_state, new_state) -> None:
        logger.info(
            _TRANSITION_EVENT_NAMES[new_state.name],
            service_name=cb.name,
            from_state=old_state.name if old_state else None,
            to_state=new_state.name,
        )


def make_breaker(
    service_name: str, session_factory: sessionmaker
) -> tuple[pybreaker.CircuitBreaker, SQLAlchemyCircuitBreakerStorage]:
    storage = SQLAlchemyCircuitBreakerStorage(service_name, session_factory, RESET_TIMEOUT_SECONDS)
    breaker = pybreaker.CircuitBreaker(
        fail_max=FAIL_MAX,
        reset_timeout=RESET_TIMEOUT_SECONDS,
        state_storage=storage,
        listeners=[_StateChangeLogger()],
        name=service_name,
    )
    return breaker, storage


def _check_and_maybe_half_open(
    breaker: pybreaker.CircuitBreaker, storage: SQLAlchemyCircuitBreakerStorage
) -> None:
    """
    Synchronous gate check, run off the event loop via asyncio.to_thread.
    A closed or half-open circuit is a no-op (the call proceeds). An open
    circuit either lets exactly one trial call through - flipping to
    half-open, once reset_timeout has elapsed since it opened - or raises
    CircuitOpenError without the caller ever touching the network.
    """
    if breaker.current_state != pybreaker.STATE_OPEN:
        return
    next_attempt_at = storage.next_attempt_at
    # next_attempt_at is naive UTC (see opened_at.setter above) - compare
    # against another naive UTC value, not datetime.now(timezone.utc).
    if next_attempt_at is not None and datetime.utcnow() < next_attempt_at:
        raise CircuitOpenError(breaker.name, next_attempt_at)
    breaker.half_open()


def _record_success(breaker: pybreaker.CircuitBreaker, storage: SQLAlchemyCircuitBreakerStorage) -> None:
    storage.reset_counter()
    if breaker.current_state == pybreaker.STATE_HALF_OPEN:
        breaker.close()


def _record_failure(breaker: pybreaker.CircuitBreaker, storage: SQLAlchemyCircuitBreakerStorage) -> None:
    if breaker.current_state == pybreaker.STATE_HALF_OPEN:
        # The trial call failed - straight back to open, no need to
        # accumulate fail_max failures a second time.
        breaker.open()
        return
    storage.increment_counter()
    if storage.counter >= breaker.fail_max:
        breaker.open()


async def call_with_breaker(
    breaker: pybreaker.CircuitBreaker, storage: SQLAlchemyCircuitBreakerStorage, coro_factory
) -> object:
    """
    Runs `coro_factory()` (a zero-arg callable returning a coroutine, not a
    bare coroutine) through the circuit breaker for `breaker`. Using a
    factory rather than a coroutine directly means that if the circuit is
    open, we never even construct the coroutine - so nothing is left
    dangling unawaited.
    """
    await asyncio.to_thread(_check_and_maybe_half_open, breaker, storage)
    try:
        result = await coro_factory()
    except Exception:
        await asyncio.to_thread(_record_failure, breaker, storage)
        raise
    else:
        await asyncio.to_thread(_record_success, breaker, storage)
        return result
