import asyncio
import random
from typing import Any, Coroutine

from asyncqueue import AsyncQueue, batcher
from asyncqueue.tasks import add as task_add


def create_tasks(
    num_tasks: int = 50,
) -> tuple[list[Coroutine[Any, Any, int]], list[int]]:
    num_to_add = [
        (random.randint(0, 100), random.randint(0, 100)) for _ in range(num_tasks)
    ]
    expected = [a + b for a, b in num_to_add]
    tasks = [task_add(a, b) for a, b in num_to_add]
    return tasks, expected


class TestSimpleBatchSize:
    @classmethod
    def setup_class(cls):
        """
        Setup the class for the tests. This is run once before any tests are run.
        """
        cls.tasks, cls.expected = create_tasks(num_tasks=20)
        cls.responses = asyncio.run(batcher.simple_batch_size(cls.tasks))

    def test_set_of_responses(self):
        assert set(self.responses) == set(self.expected)

    def test_list_of_responses(self):
        assert self.responses == self.expected


class TestBatchWithQueueClass:
    @classmethod
    def setup_class(cls):
        """
        Setup the class for the tests. This is run once before any tests are run.
        """
        cls.tasks, cls.expected = create_tasks(num_tasks=20)
        cls.responses = asyncio.run(
            batcher.batch_with_queue_class(cls.tasks, keep_order=True)
        )

    def test_set_of_responses(self):
        assert set(self.responses) == set(self.expected)

    def test_list_of_responses(self):
        assert self.responses == self.expected

    def test_set_of_response_without_order(self):
        tasks, expected = create_tasks(num_tasks=20)
        responses = asyncio.run(batcher.batch_with_queue_class(tasks, keep_order=False))
        assert set(responses) == set(expected)


class TestAsyncQueue:
    def test_disorder(self):
        tasks, expected = create_tasks(num_tasks=20)
        queue = AsyncQueue(max_concurrent=10, keep_order=False)
        asyncio.run(queue.puts(tasks))
        responses = asyncio.run(queue.run())
        assert set(responses) == set(expected)

    def test_easy_order(self):
        tasks, expected = create_tasks(num_tasks=20)
        queue = AsyncQueue(max_concurrent=10, keep_order=True)
        asyncio.run(queue.puts(tasks))
        responses = asyncio.run(queue.run())
        assert responses == expected

    def test_by_adding_multiple_tasks(self):
        # Add 20 tasks
        tasks, expected = create_tasks(num_tasks=20)
        queue = AsyncQueue(max_concurrent=10, keep_order=True)
        asyncio.run(queue.puts(tasks))
        # Add 10 new tasks
        new_tasks, new_expected = create_tasks(num_tasks=10)
        asyncio.run(queue.puts(new_tasks))
        # Run the queue
        responses = asyncio.run(queue.run())
        assert set(responses) == set(expected + new_expected)
        assert responses == expected + new_expected
