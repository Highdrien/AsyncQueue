import asyncio
import logging
import random
import time
from collections.abc import Coroutine

from asyncqueue import AsyncQueue, AsyncQueueSorted, run_tasks

random.seed(42)

logger = logging.getLogger("try asyncqueue")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


NUM_TASKS = 30
MAX_CONCURRENT_TASKS = 10
RANDOM_SLEEP_TIMES = [random.random() for _ in range(NUM_TASKS)]
NUMBER_TO_ADD = [
    (random.random() * 100, random.random() * 100) for _ in range(NUM_TASKS)
]


def create_tasks() -> list[Coroutine[None, None, float]]:
    """
    Creates a list of tasks that take a random amount of time to complete.
    """

    async def _random_sleep_then_return_sum(
        sleep_time: float, a: float, b: float
    ) -> float:
        """
        Simulates a task that takes a random amount of time to complete.
        """
        await asyncio.sleep(sleep_time)
        logger.debug(f"Sleeping for {sleep_time:.2f} seconds")
        return a + b

    return [
        _random_sleep_then_return_sum(sleep_time, a, b)
        for sleep_time, (a, b) in zip(RANDOM_SLEEP_TIMES, NUMBER_TO_ADD)
    ]


def sync_run_tasks() -> None:
    """
    Runs the tasks synchronously.
    """
    start_time = time.perf_counter()
    results = [asyncio.run(task) for task in create_tasks()]
    end_time = time.perf_counter()
    logger.info(f"Run Normal in {end_time - start_time:.2f} seconds")
    logger.info(f"3 first results: {results[:3]}")


def run_tasks_with_normal_asyncqueue() -> None:
    """
    Runs the tasks asynchronously.
    """
    queue = AsyncQueue(max_concurrent=MAX_CONCURRENT_TASKS)
    tasks = create_tasks()
    start_time = time.perf_counter()
    results = asyncio.run(run_tasks(queue, tasks))
    end_time = time.perf_counter()
    logger.info(f"Run Normal AsyncQueue in {end_time - start_time:.2f} seconds")
    logger.info(f"3 first results: {results[:3]}")
    logger.warning("You can see the 3 first results is not in order")


def run_tasks_with_sorted_asyncqueue() -> None:
    """
    Runs the tasks asynchronously.
    """
    queue = AsyncQueueSorted(max_concurrent=MAX_CONCURRENT_TASKS)
    tasks = create_tasks()
    start_time = time.perf_counter()
    results = asyncio.run(run_tasks(queue, tasks))
    end_time = time.perf_counter()
    logger.info(f"Run Sorted AsyncQueue in {end_time - start_time:.2f} seconds")
    logger.info(f"3 first results: {results[:3]}")


if __name__ == "__main__":
    logger.info("Starting tests...")
    logger.info("Running synchronously...")
    sync_run_tasks()
    logger.info("Running with normal AsyncQueue...")
    run_tasks_with_normal_asyncqueue()
    logger.info("Running with sorted AsyncQueue...")
    run_tasks_with_sorted_asyncqueue()
    logger.info("Tests completed.")
