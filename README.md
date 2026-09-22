# AsyncQueue — Async Task Processing with Python

[![PyPI](https://img.shields.io/pypi/v/asyncqueue-py)](https://pypi.org/project/asyncqueue-py/)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![PythonVersion](https://img.shields.io/badge/Python-3.12%20%7C%203.13-informational)](https://www.python.org/downloads/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v1.json)](https://github.com/astral-sh/ruff)
[![ty](https://img.shields.io/badge/types-ty-261230)](https://github.com/astral-sh/ty)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)

![Static Badge](https://img.shields.io/badge/lint-passing-green)
![Static Badge](https://img.shields.io/badge/pytest-56%20passed%20%7C%209%20xfailed-green)
![Static Badge](https://img.shields.io/badge/coverage-100%25-green)

`asyncqueue` is a small, dependency-free package that runs a batch of coroutines
with a bounded level of concurrency. A new task is started as soon as one
finishes, so the workers never idle while there is work left in the queue.

## Overview

Running `asyncio.gather` on a thousand coroutines starts a thousand coroutines at
once. Chunking them into fixed-size batches fixes that, but the whole batch then
waits for its slowest member before the next one starts.

`AsyncQueue` sits in between: it keeps exactly `max_concurrent` tasks in flight
and refills a slot the moment a task completes.

## Features

- **Bounded concurrency** — never more than `max_concurrent` tasks in flight.
- **No idle workers** — a finished task is replaced immediately, not at the end
  of a batch.
- **Ordered results on demand** — `AsyncQueue` returns results unordered;
  `AsyncQueueSorted` returns them in submission order.
- **Generic and typed** — `AsyncQueue[T]` returns `list[T]`; checked with `ty`.
- **No runtime dependencies** — standard library `asyncio` only.

## Installation

```bash
pip install asyncqueue-py
```

```bash
uv add asyncqueue-py
```

The distribution is named `asyncqueue-py` because `asyncqueue` is already
registered on PyPI; the import name is unchanged:

```python
from asyncqueue import AsyncQueue, AsyncQueueSorted, run_tasks
```

To work on the package itself, see [Development](#development).

## Usage

`AsyncQueue` returns the results in no particular order — they are collected
per worker, so a slow task does not hold the others back:

```python
import asyncio

from asyncqueue import AsyncQueue


async def work(i: int) -> int:
    await asyncio.sleep(1 / (i + 1))
    return i


async def main() -> None:
    queue: AsyncQueue[int] = AsyncQueue(max_concurrent=10)
    await queue.puts([work(i) for i in range(100)])
    print(f"Queue length: {len(queue)}")  # Queue length: 100

    results = await queue.run()
    print(f"Queue length: {len(queue)}")  # Queue length: 0
    print(results[:3])  # unordered


asyncio.run(main())
```

`AsyncQueueSorted` has the same API but returns the results in *submission*
order, whatever the completion order:

```python
from asyncqueue import AsyncQueueSorted, run_tasks

queue: AsyncQueueSorted[int] = AsyncQueueSorted(max_concurrent=10)
results = await run_tasks(queue, [work(i) for i in range(100)])
assert results == list(range(100))
```

`run_tasks(queue, tasks)` is a shortcut for `puts` followed by `run`.

A runnable comparison between sequential execution, `AsyncQueue` and
`AsyncQueueSorted` lives in [`script/try_it.py`](script/try_it.py):

```bash
uv run script/try_it.py
```

## API

### `AsyncQueue[T](max_concurrent: int = 10)`

| Member                   | Description                                                                                                                                  |
| ------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `await puts(tasks)`      | Adds an iterable of coroutines to the queue. Can be called several times, and again after `run`.                                             |
| `await run() -> list[T]` | Spawns `max_concurrent` workers, drains the queue and returns every result. The order is unspecified — use `AsyncQueueSorted` if it matters. |
| `len(queue)`             | Number of tasks still waiting in the queue.                                                                                                  |
| `repr(queue)`            | `AsyncQueue(max_concurrent=10, with queue size=0)`                                                                                           |

`max_concurrent` must be greater than `0`; the constructor raises `ValueError`
otherwise.

### `AsyncQueueSorted[T](max_concurrent: int = 10)`

Subclass of `AsyncQueue`, with its own `repr`. Tags each task with an
incrementing id at `puts` time and sorts on it in `run`, so the results come
back in submission order — across
several `puts` and several `run` calls.

### `await run_tasks(queue, tasks) -> list[T]`

Convenience wrapper: `await queue.puts(tasks)` then `await queue.run()`.

## Code Structure

```
src/asyncqueue/
├── __init__.py   # public API: AsyncQueue, AsyncQueueSorted, run_tasks
├── aqueue.py     # AsyncQueue and AsyncQueueSorted
├── py.typed      # PEP 561 marker: ships the annotations to type checkers
└── utils.py      # run_tasks helper
script/
└── try_it.py     # benchmark of the three execution strategies
tests/
├── conftest.py              # ConcurrencyTracker fixture and task helpers
├── test_aqueue.py           # AsyncQueue: puts, run, concurrency bounds
├── test_aqueue_sorted.py    # AsyncQueueSorted: ordering guarantees
├── test_utils.py            # run_tasks
└── test_future_features.py  # xfail tests for unimplemented behaviour
```

## Development

Install the dev dependencies and the pre-commit hooks:

```bash
uv sync
uv run pre-commit install
```

Run the test suite:

```bash
uv run pytest
```

With coverage:

```bash
uv run coverage run -m pytest && uv run coverage report
```

Lint and format (same checks as [`.pre-commit-config.yml`](.pre-commit-config.yml)):

```bash
uv run ruff format . && uv run ruff check --fix . && uv run ruff check --select I --fix .
```

Type check:

```bash
uv run ty check
```

### About the test suite

Tests are asynchronous and run under `pytest-asyncio` in `auto` mode, so no
`@pytest.mark.asyncio` decorator is needed. Concurrency is asserted rather than
timed wherever possible: the `ConcurrencyTracker` fixture counts how many tasks
are in flight at the same time and exposes the observed `peak`, which the tests
compare against `max_concurrent`.

## Continuous Integration

[`.github/workflows/ci-workflow.yml`](.github/workflows/ci-workflow.yml) runs on
every push to `main`, every tag and every pull request, inside the
`ghcr.io/astral-sh/uv:python3.12-bookworm-slim` image:

| Job         | What it does                                                          |
| ----------- | --------------------------------------------------------------------- |
| `lint`      | `ruff format --check`, `ruff check`, `ruff check --select I`          |
| `typecheck` | `uv run ty check`                                                     |
| `test`      | `pytest` with coverage on **Python 3.12 and 3.13**, failing under 90% |
| `publish`   | Tags only: builds and uploads to PyPI, once the three jobs above pass |

Releases go out through [PyPI Trusted Publishing](https://docs.pypi.org/trusted-publishers/),
so no API token is stored in the repository. `publish` refuses to run if the tag
does not match the `version` declared in `pyproject.toml`, so cutting a release
means bumping that version, committing, then tagging `vX.Y.Z`.

## Roadmap

`tests/test_future_features.py` keeps an executable list of what the package does
*not* do yet. Each test is marked `xfail(strict=True)`, so the day a feature
lands, the suite reports an `XPASS` failure and the marker can be removed:

- error handling: keep the results of the tasks that succeeded, an opt-in
  `return_exceptions` flag, retries, per-task timeouts, and a queue left usable
  after a failed run;
- input validation: reject values that are not coroutines in `puts`;
- ergonomics: a single-task `put`, async context manager support, and a progress
  callback.

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request
for any improvements or bug fixes. Make sure `uv run pytest`, `uv run ruff check`
and `uv run ty check` all pass before opening it.
