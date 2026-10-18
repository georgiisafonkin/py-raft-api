import asyncio
import random
from typing import Protocol

from raft_api.config import Timeouts


class Clock(Protocol):
    def monotonic(self) -> float: ...

    async def sleep(self, seconds: float) -> None: ...

    def election_timeout(self) -> float: ...


class AsyncioClock:
    def __init__(self, timeouts: Timeouts, rng: random.Random | None = None) -> None:
        self._timeouts = timeouts
        self._rng = rng or random.Random()

    def monotonic(self) -> float:
        return asyncio.get_running_loop().time()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)

    def election_timeout(self) -> float:
        return self._rng.uniform(
            self._timeouts.election_timeout_min_seconds,
            self._timeouts.election_timeout_max_seconds,
        )


class ManualClock:
    """Virtual clock. ``sleep`` completes when ``advance`` moves time past the deadline."""

    def __init__(
        self,
        timeouts: Timeouts,
        rng: random.Random | None = None,
        scripted_timeouts: list[float] | None = None,
    ) -> None:
        self._timeouts = timeouts
        self._rng = rng or random.Random()
        self._scripted = list(scripted_timeouts or [])
        self._now = 0.0
        self._waiters: list[tuple[float, asyncio.Future[None]]] = []

    def monotonic(self) -> float:
        return self._now

    async def sleep(self, seconds: float) -> None:
        if seconds <= 0:
            return
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        deadline = self._now + seconds
        self._waiters.append((deadline, future))
        try:
            await future
        except asyncio.CancelledError:
            self._waiters = [(at, waiter) for at, waiter in self._waiters if waiter is not future]
            raise

    def election_timeout(self) -> float:
        if self._scripted:
            return self._scripted.pop(0)
        return self._rng.uniform(
            self._timeouts.election_timeout_min_seconds,
            self._timeouts.election_timeout_max_seconds,
        )

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("time cannot go backwards")
        self._now += seconds
        ready = [(at, waiter) for at, waiter in self._waiters if at <= self._now]
        self._waiters = [(at, waiter) for at, waiter in self._waiters if at > self._now]
        for _, waiter in ready:
            if not waiter.done():
                waiter.set_result(None)
