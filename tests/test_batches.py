import asyncio
import random
from typing import Coroutine, Any

from src import batcher
from src.tasks import add as task_add


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
        cls.responses = asyncio.run(batcher.batch_with_queue_class(cls.tasks))

    def test_set_of_responses(self):
        assert set(self.responses) == set(self.expected)

    def test_list_of_responses(self):
        assert self.responses == self.expected
