"""Shared fixtures and helpers for the asyncqueue test suite."""

import asyncio
from collections.abc import Coroutine

import pytest

from asyncqueue import AsyncQueue


class ConcurrencyTracker:
    """
    Records how many tasks run at the same time.

    Each coroutine built by :meth:`task` increments a counter while it sleeps and
    decrements it once done, so ``peak`` holds the highest number of tasks that
    were ever in flight together.
    """

    def __init__(self) -> None:
        self.current: int = 0
        self.peak: int = 0
        self.completed: list[int] = []

    def task(self, value: int, sleep_time: float = 0.01) -> Coroutine[None, None, int]:
        """Builds a coroutine that sleeps, then returns ``value``."""

        async def _tracked() -> int:
            self.current += 1
            self.peak = max(self.peak, self.current)
            await asyncio.sleep(sleep_time)
            self.current -= 1
            self.completed.append(value)
            return value

        return _tracked()

    def tasks(
        self, count: int, sleep_time: float = 0.01
    ) -> list[Coroutine[None, None, int]]:
        """Builds ``count`` tracked coroutines returning ``0 .. count - 1``."""
        return [self.task(i, sleep_time) for i in range(count)]


@pytest.fixture
def tracker() -> ConcurrencyTracker:
    """A fresh :class:`ConcurrencyTracker` for each test."""
    return ConcurrencyTracker()


async def echo(value: int, sleep_time: float = 0.0) -> int:
    """A minimal task: sleeps, then returns ``value``."""
    await asyncio.sleep(sleep_time)
    return value


async def boom(message: str = "task failed") -> int:
    """A task that always raises, to check error propagation."""
    raise ValueError(message)


async def drain(queue: AsyncQueue) -> None:
    """
    Closes every coroutine still sitting in ``queue`` without running it.

    Tests that only exercise ``puts`` leave un-awaited coroutines behind, which
    Python reports as ``RuntimeWarning`` when they are garbage collected.
    """
    while not queue.queue.empty():
        coroutine = queue.queue.get_nowait()
        coroutine.close()
        queue.queue.task_done()
