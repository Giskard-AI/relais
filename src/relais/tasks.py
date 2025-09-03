import asyncio
from asyncio import TaskGroup
from typing import Any, Coroutine


class CancellationError(Exception):
    """Custom exception for cancellation."""

    pass


class CancellationScope:
    """Scope for a cancellation triggered by any asyncio.Event in a list."""

    def __init__(self, cancelled: list[asyncio.Event]):
        self.cancelled = cancelled
        self.cancellation_task: asyncio.Task[None] | None = None

    async def cancellation_watcher(self):
        # Wait for any event to be set - create tasks explicitly for Python 3.13+ compatibility
        wait_tasks = [asyncio.create_task(event.wait()) for event in self.cancelled]
        _, pending = await asyncio.wait(wait_tasks, return_when=asyncio.FIRST_COMPLETED)

        # Cancel all the remaining event.wait() tasks
        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        raise CancellationError()

    async def __aenter__(self):
        # Check if any cancellation is already set
        if any(event.is_set() for event in self.cancelled):
            raise CancellationError()

        self.cancellation_task = asyncio.create_task(self.cancellation_watcher())
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.cancellation_task:
            self.cancellation_task.cancel()
            try:
                await self.cancellation_task
            except asyncio.CancelledError:
                pass


class BlockingTaskLimiter:
    """A task group that limits the number of concurrently running tasks.
    Tasks are only scheduled when a slot is available.
    """

    def __init__(self, max_tasks: int):
        self.max_tasks = max_tasks
        self._task_group = TaskGroup()
        self._semaphore = asyncio.Semaphore(max_tasks)

    async def __aenter__(self):
        await self._task_group.__aenter__()

        return self

    async def put(self, coro: Coroutine[Any, Any, Any]):
        # Acquire semaphore and be safe about cancellation
        await self._semaphore.acquire()
        try:

            async def wrapped():
                try:
                    await coro
                finally:
                    self._semaphore.release()

            self._task_group.create_task(wrapped())
        except Exception:
            # If creating the task fails, release the slot
            self._semaphore.release()
            raise

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._task_group.__aexit__(exc_type, exc_val, exc_tb)
