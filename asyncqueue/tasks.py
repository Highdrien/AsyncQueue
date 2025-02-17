import asyncio
import random
from typing import Awaitable, Optional


async def add(a: int, b: int, task_id: Optional[int] = None) -> int:
    """
    Simulates a task that takes a random amount of time to complete.
    """
    await asyncio.sleep(random.random())
    if task_id:
        print(f"Task {task_id} adding {a} and {b}: {a + b}")
    return a + b


def create_tasks(num_tasks: int = 50) -> list[Awaitable[int]]:
    return [
        add(random.randint(0, 100), random.randint(0, 100), task_id=i)
        for i in range(num_tasks)
    ]
