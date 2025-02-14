import asyncio
import time
from math import ceil
from typing import Awaitable

from src.async_queue import AsyncQueue


async def simple_batch_size(
    tasks: list[Awaitable[int]], batch_size: int = 10
) -> list[int]:
    """
    Processes tasks in batches.
    """
    start = time.time()
    num_batches = ceil(len(tasks) / batch_size)
    responses = []
    for i in range(0, len(tasks), batch_size):
        print(f"---Processing batch {i // batch_size + 1}/{num_batches}---")
        batch = tasks[i : i + batch_size]
        responses.extend(await asyncio.gather(*batch))
    end = time.time()
    print(f"Responses: {responses}, len(responses): {len(responses)}")
    print(f"Time taken: {end - start:.2f} seconds")
    return responses


async def batch_with_queue(
    tasks: list[Awaitable[int]], max_concurrent: int = 10
) -> list[int]:
    """
    Processes tasks asynchronously, ensuring that new tasks are started as soon as
    one finishes.
    """
    start = time.time()
    queue: asyncio.Queue = asyncio.Queue()

    for task in tasks:
        await queue.put(task)

    async def worker():
        while not queue.empty():
            task = await queue.get()
            await task
            queue.task_done()

    workers = [asyncio.create_task(worker()) for _ in range(max_concurrent)]
    responses = await asyncio.gather(*workers)
    print(f"Responses: {responses}, len(responses): {len(responses)}")

    end = time.time()
    print(f"Time taken: {end - start:.2f} seconds")
    return responses


async def batch_with_queue_class(
    tasks: list[Awaitable[int]], max_concurrent: int = 10
) -> list[int]:
    """
    Processes tasks asynchronously, ensuring that new tasks are started as soon as
    one finishes.
    """
    start = time.time()
    queue = AsyncQueue(max_concurrent=max_concurrent)
    await queue.puts(tasks)
    print(f"Queue length: {len(queue)}")
    responses = await queue.run()
    print(f"Responses: {responses}, len(responses): {len(responses)}")
    end = time.time()
    print(f"Time taken: {end - start:.2f} seconds")
    return responses
