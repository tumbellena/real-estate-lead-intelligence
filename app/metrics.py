# Tiny in-memory metrics helper, shared by the FastAPI app (app/main.py)
# and the worker (app/worker.py) - each process tracks its own counters in
# its own memory. They're separate OS processes with no shared memory, so
# GET /metrics on the API process (app/main.py) combines its own counters
# with a live read of the worker's (via an internal HTTP call to the
# worker's own metrics endpoint) plus a few values it reads straight from
# the database instead - see the comment on that route for why.
from datetime import datetime, timezone


class DailyCounter:
    """
    A counter that resets itself at UTC midnight, so "total today" doesn't
    need a separate scheduled reset job - it just checks the date on every
    read/write and zeroes itself the first time it notices the day changed.

    Resets to zero on process restart too, same as any in-memory counter -
    a real tradeoff versus computing the same number from the database on
    every request. Acceptable here: a brief undercount right after a
    deploy is a lot cheaper to live with than adding a query to a hot path
    (lead creation, lead completion) purely to make a dashboard number
    survive restarts.
    """

    def __init__(self) -> None:
        self._day = datetime.now(timezone.utc).date()
        self._count = 0

    def _roll_if_needed(self) -> None:
        today = datetime.now(timezone.utc).date()
        if today != self._day:
            self._day = today
            self._count = 0

    def increment(self) -> None:
        self._roll_if_needed()
        self._count += 1

    def value(self) -> int:
        self._roll_if_needed()
        return self._count
