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


def test_simple_batch_size():
    tasks, expected = create_tasks(num_tasks=20)
    assert asyncio.run(batcher.simple_batch_size(tasks)) == expected


def test_batch_with_queue():
    tasks, expected = create_tasks(num_tasks=20)
    assert asyncio.run(batcher.batch_with_queue(tasks)) == expected


def test_batch_with_queue_class():
    tasks, expected = create_tasks(num_tasks=20)
    assert asyncio.run(batcher.batch_with_queue_class(tasks)) == expected
