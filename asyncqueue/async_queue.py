import asyncio
from typing import Any, Awaitable, Iterable


class AsyncQueue:
    """
    A queue that processes tasks asynchronously, ensuring that new tasks are started as
    soon as one finishes.

    Attributes:
        max_concurrent (int): The maximum number of tasks that can run concurrently.
            default: 10
        keep_order (bool): Whether to keep the order of the tasks (but slower).
            default: False
        queue (asyncio.Queue): The queue that holds the tasks.

    Methods:
        puts: Puts a list of tasks into the queue.
        run: Runs the queue and returns a list of results.
        __len__: Returns the number of tasks in the queue.

    Example:
    ```python
    tasks = [asyncio.sleep(10 * random.random()) for _ in range(100)]
    queue = AsyncQueue(max_concurrent=10)
    await queue.puts(tasks)
    print(f"Queue length: {len(queue)}")
    await queue.run()
    print(f"Queue length: {len(queue)}")
    ```
    """

    def __init__(self, max_concurrent: int = 10, keep_order: bool = False) -> None:
        """
        Init AsyncQueue.

        Args:
            max_concurrent: The maximum number of tasks that can run concurrently.
            keep_order: Whether to keep the order of the tasks.
        """
        self.queue: asyncio.Queue = asyncio.Queue()
        self.max_concurrent: int = max_concurrent
        self.keep_order: bool = keep_order

    async def puts(self, tasks: Iterable[Awaitable[Any]]) -> None:
        """
        Puts a list of tasks into the queue.

        Args:
            tasks (Iterable[Awaitable[Any]]): A list of tasks to put into the queue.
        """
        if not self.keep_order:
            for task in tasks:
                await self.queue.put(task)
        else:
            for i, task in enumerate(tasks, start=self.queue.qsize()):
                await self.queue.put(self._task_with_id(task, _id=i))

    async def run(self) -> list[Any]:
        """
        Runs the queue and returns a list of results.

        Returns:
            (list[Any]) A list of results from the tasks.
        """
        workers = [
            asyncio.create_task(self._worker()) for _ in range(self.max_concurrent)
        ]
        worker_results = await asyncio.gather(*workers)

        # Flatten the list of lists into a single list of results
        results = [
            result for worker_result in worker_results for result in worker_result
        ]

        # if keep_order is True, results is a list of tuples (id, result)
        # we need to sort the results by id and return a list of results
        if self.keep_order:
            results.sort(key=lambda x: x[0])
            results = [result[1] for result in results]

        return results

    def __repr__(self) -> str:
        return (
            f"AsyncQueue(max_concurrent={self.max_concurrent}, "
            + f"with queue size={len(self)})"
        )

    def __len__(self) -> int:
        """
        Returns the number of tasks in the queue.
        """
        return self.queue.qsize()

    async def _worker(self) -> list[Any]:
        """
        A worker that processes tasks from the queue.
        """
        results: list[Any] = []
        while not self.queue.empty():
            task = await self.queue.get()
            results.append(await task)
            self.queue.task_done()
        return results

    async def _task_with_id(self, task: Awaitable[Any], _id: int) -> tuple[int, Any]:
        """
        A wrapper that adds an id to a task.
        """
        return _id, await task
