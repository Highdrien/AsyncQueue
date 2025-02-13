import asyncio
from typing import Any, Awaitable, Iterable


class AsyncQueue:
    """
    A queue that processes tasks asynchronously, ensuring that new tasks are started as
    soon as one finishes.

    Attributes:
        max_concurrent (int): The maximum number of tasks that can run concurrently.
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

    def __init__(self, max_concurrent: int = 10) -> None:
        """
        Init AsyncQueue.

        Args:
            max_concurrent: The maximum number of tasks that can run concurrently.
        """
        super().__init__()
        self.max_concurrent = max_concurrent
        self.queue = asyncio.Queue()

    async def puts(self, tasks: Iterable[Awaitable[Any]]) -> None:
        """
        Puts a list of tasks into the queue.

        Args:
            tasks (Iterable[Awaitable[Any]]): A list of tasks to put into the queue.
        """
        for task in tasks:
            await self.queue.put(task)

    async def run(self) -> list[Any]:
        """
        Runs the queue and returns a list of results.

        Returns:
            (list[Any]) A list of results from the tasks.
        """
        workers = [
            asyncio.create_task(self._worker()) for _ in range(self.max_concurrent)
        ]
        return await asyncio.gather(*workers)

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

    async def _worker(self):
        """
        A worker that processes tasks from the queue.
        """
        while not self.queue.empty():
            task = await self.queue.get()
            await task
            self.queue.task_done()
