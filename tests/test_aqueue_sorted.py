"""Tests for :class:`asyncqueue.AsyncQueueSorted`."""

import pytest
from conftest import ConcurrencyTracker, drain, echo

from asyncqueue import AsyncQueue, AsyncQueueSorted


class TestInit:
    def test_is_an_async_queue(self) -> None:
        assert isinstance(AsyncQueueSorted(), AsyncQueue)

    def test_default_max_concurrent(self) -> None:
        assert AsyncQueueSorted().max_concurrent == 10

    def test_id_counter_starts_at_zero(self) -> None:
        assert AsyncQueueSorted().id_counter == 0

    def test_new_queue_is_empty(self) -> None:
        assert len(AsyncQueueSorted()) == 0

    def test_sorted_queue_has_its_own_repr(self) -> None:
        assert repr(AsyncQueueSorted(max_concurrent=3)).startswith("AsyncQueueSorted(")

    def test_zero_max_concurrent_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            AsyncQueueSorted(max_concurrent=0)

    def test_negative_max_concurrent_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            AsyncQueueSorted(max_concurrent=-1)


class TestPuts:
    async def test_len_matches_number_of_tasks(self) -> None:
        queue: AsyncQueueSorted[int] = AsyncQueueSorted()
        await queue.puts([echo(i) for i in range(5)])
        assert len(queue) == 5
        await drain(queue)

    async def test_id_counter_tracks_submitted_tasks(self) -> None:
        queue: AsyncQueueSorted[int] = AsyncQueueSorted()
        await queue.puts([echo(i) for i in range(3)])
        assert queue.id_counter == 3
        await queue.puts([echo(i) for i in range(2)])
        assert queue.id_counter == 5
        await drain(queue)

    async def test_id_counter_is_not_reset_by_run(self) -> None:
        queue: AsyncQueueSorted[int] = AsyncQueueSorted(max_concurrent=2)
        await queue.puts([echo(i) for i in range(3)])
        await queue.run()
        assert queue.id_counter == 3


class TestRun:
    async def test_results_follow_submission_order(self) -> None:
        # Sleep times decrease, so completion order is the reverse of submission
        # order: only the id bookkeeping can restore the original order.
        sleep_times = [0.05, 0.04, 0.03, 0.02, 0.01]
        queue: AsyncQueueSorted[int] = AsyncQueueSorted(max_concurrent=5)
        await queue.puts(
            [echo(i, sleep_time) for i, sleep_time in enumerate(sleep_times)]
        )
        assert await queue.run() == [0, 1, 2, 3, 4]

    async def test_order_is_kept_across_several_puts(self) -> None:
        """Tasks queued by a second ``puts`` must come after the first batch."""
        queue: AsyncQueueSorted[int] = AsyncQueueSorted(max_concurrent=4)
        await queue.puts([echo(0, 0.04), echo(1, 0.03)])
        await queue.puts([echo(2, 0.02), echo(3, 0.01)])
        assert await queue.run() == [0, 1, 2, 3]

    async def test_order_is_kept_across_several_runs(self) -> None:
        queue: AsyncQueueSorted[int] = AsyncQueueSorted(max_concurrent=2)
        await queue.puts([echo(0, 0.02), echo(1, 0.01)])
        assert await queue.run() == [0, 1]

        await queue.puts([echo(2, 0.02), echo(3, 0.01)])
        assert await queue.run() == [2, 3]

    async def test_drains_the_queue(self) -> None:
        queue: AsyncQueueSorted[int] = AsyncQueueSorted(max_concurrent=2)
        await queue.puts([echo(i) for i in range(6)])
        await queue.run()
        assert len(queue) == 0

    async def test_empty_queue_returns_empty_list(self) -> None:
        queue: AsyncQueueSorted[int] = AsyncQueueSorted()
        assert await queue.run() == []

    async def test_single_worker_keeps_order(self) -> None:
        queue: AsyncQueueSorted[int] = AsyncQueueSorted(max_concurrent=1)
        await queue.puts([echo(i) for i in range(5)])
        assert await queue.run() == [0, 1, 2, 3, 4]

    async def test_results_are_unwrapped_from_their_id(self) -> None:
        queue: AsyncQueueSorted[str] = AsyncQueueSorted(max_concurrent=2)

        async def make(value: str) -> str:
            return value

        await queue.puts([make(letter) for letter in "abc"])
        assert await queue.run() == ["a", "b", "c"]


class TestConcurrency:
    @pytest.mark.parametrize("max_concurrent", [1, 2, 5])
    async def test_respects_max_concurrent(
        self, tracker: ConcurrencyTracker, max_concurrent: int
    ) -> None:
        queue: AsyncQueueSorted[int] = AsyncQueueSorted(max_concurrent=max_concurrent)
        await queue.puts(tracker.tasks(15))
        results = await queue.run()

        assert tracker.peak == max_concurrent
        assert results == list(range(15))
