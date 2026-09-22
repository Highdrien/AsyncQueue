from collections.abc import Coroutine
from typing import TypeVar

from asyncqueue import AsyncQueue

T = TypeVar("T")


async def run_tasks[T](
    queue: AsyncQueue, tasks: list[Coroutine[None, None, T]]
) -> list[T]:
    """
    Runs the tasks asynchronously.
    """
    await queue.puts(tasks)
    return await queue.run()
