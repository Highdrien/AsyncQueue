"""Tests for :func:`asyncqueue.run_tasks`."""

from collections.abc import Coroutine

import pytest
from conftest import ConcurrencyTracker, echo

from asyncqueue import AsyncQueue, AsyncQueueSorted, run_tasks


class TestRunTasks:
    async def test_returns_every_result(self) -> None:
        queue: AsyncQueue[int] = AsyncQueue(max_concurrent=3)
        results = await run_tasks(queue, [echo(i) for i in range(10)])
        assert sorted(results) == list(range(10))

    async def test_empty_task_list(self) -> None:
        queue: AsyncQueue[int] = AsyncQueue()
        assert await run_tasks(queue, []) == []

    async def test_drains_the_queue(self) -> None:
        queue: AsyncQueue[int] = AsyncQueue(max_concurrent=2)
        await run_tasks(queue, [echo(i) for i in range(5)])
        assert len(queue) == 0

    async def test_keeps_order_with_a_sorted_queue(self) -> None:
        sleep_times = [0.04, 0.03, 0.02, 0.01]
        queue: AsyncQueueSorted[int] = AsyncQueueSorted(max_concurrent=4)
        tasks: list[Coroutine[None, None, int]] = [
            echo(i, sleep_time) for i, sleep_time in enumerate(sleep_times)
        ]
        assert await run_tasks(queue, tasks) == [0, 1, 2, 3]

    @pytest.mark.parametrize("max_concurrent", [1, 3, 8])
    async def test_respects_max_concurrent(
        self, tracker: ConcurrencyTracker, max_concurrent: int
    ) -> None:
        queue: AsyncQueue[int] = AsyncQueue(max_concurrent=max_concurrent)
        await run_tasks(queue, tracker.tasks(16))
        assert tracker.peak == max_concurrent

    async def test_appends_to_a_queue_that_already_holds_tasks(self) -> None:
        queue: AsyncQueue[int] = AsyncQueue(max_concurrent=2)
        await queue.puts([echo(0), echo(1)])
        results = await run_tasks(queue, [echo(2), echo(3)])
        assert sorted(results) == [0, 1, 2, 3]
