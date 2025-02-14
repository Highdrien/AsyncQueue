# Async Task Processing with Python

[![PythonVersion](https://img.shields.io/badge/Python-3.12-informational)](https://www.python.org/downloads/release/python-3126/)
[![Ruff_logo](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v1.json)](https://github.com/astral-sh/ruff)
[![Black_logo](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Mypy_logo](https://img.shields.io/badge/mypy-checked-blue)](https://mypy.readthedocs.io/en/stable/)
[![Isort_logo](https://img.shields.io/badge/isort-checked-yellow)](https://pycqa.github.io/isort/)

![Static Badge](https://img.shields.io/badge/lint-passing-green)
![Static Badge](https://img.shields.io/badge/pytest-8%2F8-green)
![Static Badge](https://img.shields.io/badge/coverage-98%25-green)

This project demonstrates asynchronous task processing in Python using `asyncio`. It includes different methods for processing tasks in batches and with a custom asynchronous queue class.

## Table of Contents

- [Async Task Processing with Python](#async-task-processing-with-python)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [Features](#features)
  - [Installation with uv](#installation-with-uv)
  - [Usage](#usage)
  - [Code Structure](#code-structure)
  - [Contributing](#contributing)

## Overview

The project provides a framework for processing tasks asynchronously. It includes:

- A simple batch processing function.
- A function that processes tasks using an `asyncio.Queue`.
- A custom `AsyncQueue` class that manages task execution with a specified concurrency level.

## Features

- **Asynchronous Task Execution**: Efficiently manage and execute tasks using Python's `asyncio` library.
- **Batch Processing**: Execute tasks in defined batch sizes.
- **Custom Async Queue**: Use a custom queue class to handle task execution with controlled concurrency.

## Installation with uv

```bash
pip install uv
uv venv --python 3.12
uv sync
```

## Usage

To run the task processing examples, execute the `main.py` script. You can choose between different task processing methods by uncommenting the desired function call in the `if __name__ == "__main__":` block.

```bash
uv run main.py
```

## Code Structure

- **src/async_queue.py**: Defines the `AsyncQueue` class, which manages task execution with a specified concurrency level.
- **src/batcher.py**: Contains the batch processing functions:
  - Simple batch size: Processes tasks in fixed-size batches.
  - Batch with queue: Uses an `asyncio.Queue` to manage task execution.
  - Batch with queue class: Utilizes the custom `AsyncQueue` class for task management.
- **src/tasks.py**: Contains the task functions that simulate a task that takes a random amount of time to complete.

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request for any improvements or bug fixes.
