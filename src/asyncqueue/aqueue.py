import asyncio
from collections.abc import Coroutine, Iterable
from typing import TypeVar, cast

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
        __repr__: Returns a string representation of the queue.

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

        if max_concurrent <= 0:
            raise ValueError("max_concurrent must be greater than 0")

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
    """
    A queue that processes tasks asynchronously, ensuring that new tasks are started as
    soon as one finishes.

    Attributes:
        max_concurrent (int): The maximum number of tasks that can run concurrently.
            default: 10
        queue (asyncio.Queue): The queue that holds the tasks.
    """

    def __init__(self, max_concurrent: int = 10) -> None:
        super().__init__(max_concurrent)
        self.id_counter: int = 0

    def __repr__(self) -> str:
        return (
            f"AsyncQueueSorted(max_concurrent={self.max_concurrent}, "
            + f"with queue size={len(self)})"
        )

    async def puts(self, tasks: Iterable[Coroutine[None, None, T]]) -> None:
        """
        Puts a list of tasks into the queue.

        Args:
            tasks (Iterable[Coroutine[None, None, T]]): A list of tasks to put into the queue.
        """
        for task in tasks:
            await self.queue.put(self.__atask_with_id(task, _id=self.id_counter))
            self.id_counter += 1

    async def run(self) -> list[T]:
        """
        Runs the queue and returns a list of results.

        Returns:
            (list[Any]) A list of results from the tasks.
        """
        # Run the queue and get the results. The worker returns whatever the
        # tasks queued by `puts` produce, i.e. `(id, result)` pairs here.
        indexed_results = cast(list[tuple[int, T]], await super().run())

        # Get the results sorted by id
        indexed_results.sort(key=lambda indexed_result: indexed_result[0])

        return [result for _, result in indexed_results]

    async def __atask_with_id(
        self, task: Coroutine[None, None, T], _id: int
    ) -> tuple[int, T]:
        """
        A wrapper that adds an id to a task.
        """
        return _id, await task
