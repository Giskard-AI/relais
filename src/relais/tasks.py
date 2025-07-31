import sys
import asyncio

# TaskGroup is available in Python 3.11+, use fallback for older versions
if sys.version_info >= (3, 11):
    from asyncio import TaskGroup  # novermin: Fallback implemented

    CompatTaskGroup = TaskGroup
    CompatExceptionGroup = ExceptionGroup  # noqa: F821
else:
    # Fallback TaskGroup implementation for older Python versions
    class CompatTaskGroup:
        def __init__(self):
            self._tasks = []
            self._results = None

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            if self._tasks:
                # Gather all results, including exceptions
                self._results = await asyncio.gather(
                    *self._tasks, return_exceptions=True
                )

                # Check if any tasks raised exceptions
                exceptions = [
                    result for result in self._results if isinstance(result, Exception)
                ]

                if exceptions:
                    # Create an ExceptionGroup-like exception with all exceptions
                    raise CompatExceptionGroup(
                        "Multiple exceptions occurred in TaskGroup", exceptions
                    )

        def create_task(self, coro):
            task = asyncio.create_task(coro)
            self._tasks.append(task)
            return task

    # ExceptionGroup is available in Python 3.11+, use fallback for older versions
    class CompatExceptionGroup(Exception):
        def __init__(self, message, exceptions):
            self.message = message
            self.exceptions = exceptions
            super().__init__(message)


class TaskGroupTasks:
    """Helper to manage a group of asyncio tasks with atomic cancellation."""

    def __init__(self):
        self.tasks = []
        self._cancelled = asyncio.Event()

    def append(self, task: asyncio.Task):
        if self._cancelled.is_set():
            task.cancel()
            return
        self.tasks.append(task)

    def cancel(self):
        if self._cancelled.is_set():
            return

        self._cancelled.set()
        for task in self.tasks:
            task.cancel()
