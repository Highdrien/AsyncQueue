"""Tests for :class:`asyncqueue.AsyncQueue`."""

import asyncio
import time

import pytest
from conftest import ConcurrencyTracker, boom, drain, echo

from asyncqueue import AsyncQueue


class TestInit:
    def test_default_max_concurrent(self) -> None:
        queue = AsyncQueue()
        assert queue.max_concurrent == 10

    def test_custom_max_concurrent(self) -> None:
        queue = AsyncQueue(max_concurrent=3)
        assert queue.max_concurrent == 3

    def test_new_queue_is_empty(self) -> None:
        assert len(AsyncQueue()) == 0

    def test_repr(self) -> None:
        queue = AsyncQueue(max_concurrent=4)
        assert repr(queue) == "AsyncQueue(max_concurrent=4, with queue size=0)"

    def test_zero_max_concurrent_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            AsyncQueue(max_concurrent=0)

    def test_negative_max_concurrent_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            AsyncQueue(max_concurrent=-1)


class TestPuts:
    async def test_len_matches_number_of_tasks(self) -> None:
        queue: AsyncQueue[int] = AsyncQueue()
        await queue.puts([echo(i) for i in range(5)])
        assert len(queue) == 5
        await drain(queue)

    async def test_puts_accepts_any_iterable(self) -> None:
        queue: AsyncQueue[int] = AsyncQueue()
        await queue.puts(echo(i) for i in range(3))
        assert len(queue) == 3
        await drain(queue)

    async def test_puts_appends_to_existing_tasks(self) -> None:
        queue: AsyncQueue[int] = AsyncQueue()
        await queue.puts([echo(i) for i in range(2)])
        await queue.puts([echo(i) for i in range(3)])
        assert len(queue) == 5
        await drain(queue)

    async def test_puts_nothing_keeps_queue_empty(self) -> None:
        queue: AsyncQueue[int] = AsyncQueue()
        await queue.puts([])
        assert len(queue) == 0

    async def test_repr_reports_queue_size(self) -> None:
        queue: AsyncQueue[int] = AsyncQueue(max_concurrent=2)
        await queue.puts([echo(i) for i in range(3)])
        assert repr(queue) == "AsyncQueue(max_concurrent=2, with queue size=3)"
        await drain(queue)


class TestRun:
    async def test_returns_every_result(self) -> None:
        queue: AsyncQueue[int] = AsyncQueue(max_concurrent=3)
        await queue.puts([echo(i) for i in range(10)])
        results = await queue.run()
        assert sorted(results) == list(range(10))

    async def test_drains_the_queue(self) -> None:
        queue: AsyncQueue[int] = AsyncQueue(max_concurrent=3)
        await queue.puts([echo(i) for i in range(10)])
        await queue.run()
        assert len(queue) == 0

    async def test_empty_queue_returns_empty_list(self) -> None:
        queue: AsyncQueue[int] = AsyncQueue()
        assert await queue.run() == []

    async def test_queue_is_reusable(self) -> None:
        queue: AsyncQueue[int] = AsyncQueue(max_concurrent=2)
        await queue.puts([echo(i) for i in range(4)])
        assert sorted(await queue.run()) == [0, 1, 2, 3]

        await queue.puts([echo(i) for i in range(4, 6)])
        assert sorted(await queue.run()) == [4, 5]

    async def test_more_workers_than_tasks(self) -> None:
        queue: AsyncQueue[int] = AsyncQueue(max_concurrent=50)
        await queue.puts([echo(i) for i in range(3)])
        assert sorted(await queue.run()) == [0, 1, 2]

    async def test_single_worker_runs_in_order(self) -> None:
        queue: AsyncQueue[int] = AsyncQueue(max_concurrent=1)
        await queue.puts([echo(i) for i in range(5)])
        assert await queue.run() == [0, 1, 2, 3, 4]

    async def test_task_exception_propagates(self) -> None:
        queue: AsyncQueue[int] = AsyncQueue(max_concurrent=1)
        await queue.puts([boom("kaboom")])
        with pytest.raises(ValueError, match="kaboom"):
            await queue.run()


class TestConcurrency:
    async def test_never_exceeds_max_concurrent(
        self, tracker: ConcurrencyTracker
    ) -> None:
        queue: AsyncQueue[int] = AsyncQueue(max_concurrent=4)
        await queue.puts(tracker.tasks(20))
        await queue.run()
        assert tracker.peak <= 4

    async def test_saturates_max_concurrent(self, tracker: ConcurrencyTracker) -> None:
        queue: AsyncQueue[int] = AsyncQueue(max_concurrent=4)
        await queue.puts(tracker.tasks(20))
        await queue.run()
        assert tracker.peak == 4

    async def test_single_worker_runs_one_task_at_a_time(
        self, tracker: ConcurrencyTracker
    ) -> None:
        queue: AsyncQueue[int] = AsyncQueue(max_concurrent=1)
        await queue.puts(tracker.tasks(5))
        await queue.run()
        assert tracker.peak == 1

    @pytest.mark.parametrize("max_concurrent", [1, 2, 5, 10])
    async def test_peak_is_bounded_for_several_sizes(
        self, tracker: ConcurrencyTracker, max_concurrent: int
    ) -> None:
        queue: AsyncQueue[int] = AsyncQueue(max_concurrent=max_concurrent)
        await queue.puts(tracker.tasks(20))
        await queue.run()
        assert tracker.peak == max_concurrent

    async def test_faster_than_sequential_execution(self) -> None:
        sleep_time, num_tasks, max_concurrent = 0.05, 10, 5

        queue: AsyncQueue[int] = AsyncQueue(max_concurrent=max_concurrent)
        await queue.puts([echo(i, sleep_time) for i in range(num_tasks)])

        start = time.perf_counter()
        await queue.run()
        elapsed = time.perf_counter() - start

        sequential = num_tasks * sleep_time
        assert elapsed < sequential / 2 + 0.1

    async def test_starts_a_new_task_as_soon_as_one_finishes(self) -> None:
        """Slow tasks must not block the workers that already finished."""
        queue: AsyncQueue[int] = AsyncQueue(max_concurrent=2)
        # One long task plus many short ones: the second worker should chew
        # through every short task while the first one is still sleeping.
        await queue.puts([echo(0, 0.2), *[echo(i, 0.01) for i in range(1, 10)]])

        start = time.perf_counter()
        results = await queue.run()
        elapsed = time.perf_counter() - start

        assert sorted(results) == list(range(10))
        assert elapsed < 0.4


class TestTypes:
    async def test_supports_non_integer_results(self) -> None:
        async def make(value: str) -> str:
            await asyncio.sleep(0)
            return value.upper()

        queue: AsyncQueue[str] = AsyncQueue(max_concurrent=2)
        await queue.puts([make(letter) for letter in "abc"])
        assert sorted(await queue.run()) == ["A", "B", "C"]
