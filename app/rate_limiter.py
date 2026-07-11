# In-memory async token bucket rate limiter - one per outbound service, so
# the worker paces its own request rate to each service's stated cap
# instead of relying on retries/circuit breakers to react after the fact.
#
# In-memory by design: a single bucket object, held in this process's
# memory, is enough as long as one worker process is making all the calls.
# If this ever needs to scale to multiple worker replicas, each replica
# would get its own independent bucket - the *combined* request rate across
# all of them could then exceed the intended cap, since none of them know
# about each other's token consumption. That's the point at which the
# bucket state would need to move somewhere shared (Redis, most likely),
# which is a config change to where the bucket lives, not a rewrite of the
# algorithm itself.
import asyncio
import time
from collections import deque

import structlog

# No configure_logging() call here - app/worker.py (the only process that
# imports this module) already configured structlog globally by the time
# any of this runs; service="worker" and lead_id (when relevant) arrive via
# contextvars rather than being passed in here.
logger = structlog.get_logger()


class TokenBucket:
    """
    Classic token bucket: `capacity` tokens sit in a bucket, refilled
    continuously at `rate_per_minute / 60` tokens per second (capped at
    `capacity`, so unused capacity doesn't accumulate forever). Each
    `acquire()` call takes one token, waiting first if none are available -
    callers are paced, never rejected.
    """

    def __init__(self, service_name: str, rate_per_minute: float, capacity: float | None = None):
        self.service_name = service_name
        self.rate_per_second = rate_per_minute / 60.0
        self.capacity = capacity if capacity is not None else rate_per_minute
        self._tokens = self.capacity
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()
        # Timestamps of granted tokens, for /metrics' requests_last_min -
        # not used for pacing itself, only for reporting.
        self._grant_times: deque[float] = deque()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._last_refill = now
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate_per_second)

    async def acquire(self) -> None:
        """Blocks until a token is available, then takes it."""
        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= 1:
                    self._tokens -= 1
                    self._grant_times.append(time.monotonic())
                    return
                # Not enough tokens yet - figure out how long until there
                # is one, release the lock, and sleep outside it so other
                # callers waiting on this same bucket aren't blocked from
                # even checking the clock while we sleep.
                wait_seconds = (1 - self._tokens) / self.rate_per_second

            logger.info("rate_limit_wait", service_name=self.service_name, wait_seconds=round(wait_seconds, 2))
            await asyncio.sleep(wait_seconds)

    def snapshot(self) -> dict:
        """
        A point-in-time read for GET /metrics. Deliberately not
        lock-protected: acquiring the lock here would mean a monitoring
        request could momentarily block the hot path (or vice versa) for a
        number that's only ever going to be approximate anyway - a
        microsecond-stale token count is a fine trade for never blocking on it.
        """
        self._refill()
        now = time.monotonic()
        while self._grant_times and now - self._grant_times[0] > 60:
            self._grant_times.popleft()
        return {
            "tokens_available": round(self._tokens, 2),
            "requests_last_min": len(self._grant_times),
        }
