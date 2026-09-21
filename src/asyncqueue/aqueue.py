import asyncio
from collections.abc import Coroutine, Iterable
from typing import TypeVar

T = TypeVar("T")


class AsyncQueue[T]:
    """
    A queue that processes tasks asynchronously, ensuring that new tasks are started as
    soon as one finishes.

    Attributes:
        max_concurrent (int): The maximum number of tasks that can run concurrently.
            default: 10
        queue (asyncio.Queue): The queue that holds the tasks.

    Methods:
        puts: Puts a list of tasks into the queue.
        run: Runs the queue and returns a list of results.
        __len__: Returns the number of tasks in the queue.

    Example:
    ```python
    task = lambda _: asyncio.sleep(10 * random.random())
    tasks = [task for _ in range(100)]
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
            keep_order: Whether to keep the order of the tasks.
        """
        self.queue: asyncio.Queue = asyncio.Queue()
        self.max_concurrent: int = max_concurrent

    async def puts(self, tasks: Iterable[Coroutine[None, None, T]]) -> None:
        """
        Puts a list of tasks into the queue.

        Args:
            tasks (Iterable[Coroutine[None, None, T]]): A list of tasks to put into the queue.
        """
        for task in tasks:
            await self.queue.put(task)

    async def run(self) -> list[T]:
        """
        Runs the queue and returns a list of results.

        Returns:
            (list[Any]) A list of results from the tasks.
        """
        workers = [
            asyncio.create_task(self.__aworker()) for _ in range(self.max_concurrent)
        ]
        worker_results = await asyncio.gather(*workers)

        # Flatten the list of lists into a single list of results
        results = [
            result for worker_result in worker_results for result in worker_result
        ]

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

    async def __aworker(self) -> list[T]:
        """
        Runs a worker that processes tasks from the queue.
        """
        results: list[T] = []
        while not self.queue.empty():
            task = await self.queue.get()
            results.append(await task)
            self.queue.task_done()
        return results


class AsyncQueueSorted[T](AsyncQueue[T]):
    def __init__(self, max_concurrent: int = 10) -> None:
        super().__init__(max_concurrent)

    async def puts(self, tasks: Iterable[Coroutine[None, None, T]]) -> None:
        """
        Puts a list of tasks into the queue.

        Args:
            tasks (Iterable[Coroutine[None, None, T]]): A list of tasks to put into the queue.
        """
        for i, task in enumerate(tasks, start=self.queue.qsize()):
            await self.queue.put(self.__atask_with_id(task, _id=i))

    async def run(self) -> list[T]:
        """
        Runs the queue and returns a list of results.

        Returns:
            (list[Any]) A list of results from the tasks.
        """
        # Run the queue and get the results
        results = await super().run()

        # Get the results sorted by id
        results.sort(key=lambda x: x[0])
        results = [result[1] for result in results]

        return results

    async def __atask_with_id(
        self, task: Coroutine[None, None, T], _id: int
    ) -> tuple[int, T]:
        """
        A wrapper that adds an id to a task.
        """
        return _id, await task
