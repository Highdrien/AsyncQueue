"""
Tests for behaviours the package does *not* implement yet.

Every test here is marked ``xfail(strict=True)``: it documents a gap found while
writing the suite and fails on purpose. When one of them starts passing, pytest
reports an ``XPASS`` failure, which is the signal that the feature landed and
that the marker can be dropped.
"""

import asyncio

import pytest
from conftest import boom, echo

from asyncqueue import AsyncQueue


def future(reason: str) -> pytest.MarkDecorator:
    """Marks a test as covering a potential future feature."""
    return pytest.mark.xfail(reason=f"Potential future feature: {reason}", strict=True)


class TestErrorHandling:
    @future("a failing task should not discard the results already computed")
    async def test_failing_task_keeps_the_other_results(self) -> None:
        queue: AsyncQueue[int] = AsyncQueue(max_concurrent=2)
        await queue.puts([echo(0), boom(), echo(2)])

        # Today `asyncio.gather` re-raises and every result is lost.
        results = await queue.run()
        assert sorted(result for result in results if isinstance(result, int)) == [0, 2]

    @future("an opt-in `return_exceptions` flag, like `asyncio.gather`")
    async def test_exceptions_can_be_returned_instead_of_raised(self) -> None:
        queue: AsyncQueue[int] = AsyncQueue(max_concurrent=2, return_exceptions=True)  # ty: ignore[unknown-argument]
        await queue.puts([echo(0), boom("nope")])

        results = await queue.run()
        assert any(isinstance(result, ValueError) for result in results)

    @future("failed tasks should be retried before giving up")
    async def test_tasks_are_retried_on_failure(self) -> None:
        attempts = 0

        async def flaky() -> str:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise RuntimeError("transient")
            return "ok"

        queue: AsyncQueue[str] = AsyncQueue(max_concurrent=1, max_retries=3)  # ty: ignore[unknown-argument]
        await queue.puts([flaky()])

        assert await queue.run() == ["ok"]

    @future("a failed run should leave the queue usable instead of half drained")
    async def test_queue_is_empty_after_a_failed_run(self) -> None:
        queue: AsyncQueue[int] = AsyncQueue(max_concurrent=1)
        await queue.puts([boom(), echo(1), echo(2)])

        with pytest.raises(ValueError):
            await queue.run()

        assert len(queue) == 0

    @future("`puts` should reject values that are not coroutines")
    async def test_puts_rejects_non_coroutines(self) -> None:
        queue: AsyncQueue[int] = AsyncQueue()

        # Today the error only surfaces later, inside `run`, as a bare TypeError.
        with pytest.raises(TypeError):
            await queue.puts([42])  # ty: ignore[invalid-argument-type]

    @future("a per-task timeout, so a hanging task cannot block a worker forever")
    async def test_slow_tasks_time_out(self) -> None:
        queue: AsyncQueue[int] = AsyncQueue(max_concurrent=1, timeout=0.01)  # ty: ignore[unknown-argument]
        await queue.puts([echo(0, 10)])

        with pytest.raises(asyncio.TimeoutError):
            await queue.run()


class TestErgonomics:
    @future("a `put` method for a single task, next to `puts`")
    async def test_put_a_single_task(self) -> None:
        queue: AsyncQueue[int] = AsyncQueue()
        await queue.put(echo(1))  # ty: ignore[unresolved-attribute]
        assert len(queue) == 1

    @future("using the queue as an async context manager")
    async def test_async_context_manager(self) -> None:
        async with AsyncQueue(max_concurrent=2) as queue:  # ty: ignore[invalid-context-manager]
            await queue.puts([echo(i) for i in range(3)])
            results = await queue.run()

        assert sorted(results) == [0, 1, 2]

    @future("a progress callback fired after each completed task")
    async def test_progress_callback(self) -> None:
        seen: list[int] = []
        queue: AsyncQueue[int] = AsyncQueue(max_concurrent=2, on_done=seen.append)  # ty: ignore[unknown-argument]
        await queue.puts([echo(i) for i in range(4)])
        await queue.run()

        assert sorted(seen) == [0, 1, 2, 3]
