import asyncio
import random
import time
from math import ceil
from typing import Awaitable, Optional

from src.async_queue import AsyncQueue

random.seed(42)


async def add(a: int, b: int, task_id: Optional[int] = None) -> int:
    """
    Simulates a task that takes a random amount of time to complete.
    """
    await asyncio.sleep(random.random())
    if task_id:
        print(f"Task {task_id} adding {a} and {b}: {a + b}")
    return a + b


async def simple_batch_size(tasks: list[Awaitable[int]], batch_size: int = 10):
    """
    Processes tasks in batches.
    """
    start = time.time()
    num_batches = ceil(len(tasks) / batch_size)
    for i in range(0, len(tasks), batch_size):
        print(f"---Processing batch {i // batch_size + 1}/{num_batches}---")
        batch = tasks[i : i + batch_size]
        await asyncio.gather(*batch)
    end = time.time()
    print(f"Time taken: {end - start:.2f} seconds")


async def batch_with_queue(tasks: list[Awaitable[int]], max_concurrent: int = 10):
    """
    Processes tasks asynchronously, ensuring that new tasks are started as soon as
    one finishes.
    """
    start = time.time()
    queue = asyncio.Queue()

    for task in tasks:
        await queue.put(task)

    async def worker():
        while not queue.empty():
            task = await queue.get()
            await task
            queue.task_done()

    workers = [asyncio.create_task(worker()) for _ in range(max_concurrent)]
    await asyncio.gather(*workers)

    end = time.time()
    print(f"Time taken: {end - start:.2f} seconds")


async def batch_with_queue_class(tasks: list[Awaitable[int]], max_concurrent: int = 10):
    """
    Processes tasks asynchronously, ensuring that new tasks are started as soon as
    one finishes.
    """
    start = time.time()
    queue = AsyncQueue(max_concurrent=max_concurrent)
    await queue.puts(tasks)
    print(f"Queue length: {len(queue)}")
    await queue.run()
    end = time.time()
    print(f"Time taken: {end - start:.2f} seconds")


def create_tasks(num_tasks: int = 50) -> list[Awaitable[int]]:
    return [
        add(random.randint(0, 100), random.randint(0, 100), task_id=None)
        for i in range(num_tasks)
    ]


if __name__ == "__main__":
    # asyncio.run(simple_batch_size(tasks=create_tasks(), batch_size=10))
    # asyncio.run(batch_with_queue(tasks=create_tasks(), max_concurrent=10))
    asyncio.run(batch_with_queue_class(tasks=create_tasks(), max_concurrent=10))
