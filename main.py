import asyncio
import random

from src import batcher, tasks

random.seed(42)


def main(num_tasks: int = 50, batch_size: int = 10) -> None:
    # Simple batch size
    asyncio.run(
        batcher.simple_batch_size(
            tasks=tasks.create_tasks(num_tasks), batch_size=batch_size
        )
    )
    # Batch with queue class
    asyncio.run(
        batcher.batch_with_queue_class(
            tasks=tasks.create_tasks(num_tasks), max_concurrent=batch_size
        )
    )


if __name__ == "__main__":
    main()
