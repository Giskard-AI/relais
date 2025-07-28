import asyncio
from typing import Iterable, List

from ..base import Step, ItemT


class Take(Step[Iterable[ItemT], List[ItemT]]):
    """Take only the first N items from an iterable."""

    def __init__(self, n: int, *, ordered: bool = True):
        super().__init__(ordered=ordered)
        self.n = n

    async def process(
        self, data: Iterable[asyncio.Task[ItemT]], tg: asyncio.TaskGroup
    ) -> Iterable[asyncio.Task[ItemT]]:
        """Process tasks and return the first N tasks' results."""

        if self.ordered:
            return self._process_ordered(data)
        else:
            return self._process_as_completed(data)

    def _process_ordered(
        self, data: Iterable[asyncio.Task[ItemT]]
    ) -> Iterable[asyncio.Task[ItemT]]:
        """Take first N tasks in their original order."""
        taken_tasks = []
        remaining_tasks = []
        
        # Iterate through and take first N
        for i, task in enumerate(data):
            if i < self.n:
                taken_tasks.append(task)
            else:
                remaining_tasks.append(task)
        
        # Cancel remaining tasks
        for task in remaining_tasks:
            if not task.done():
                task.cancel()
                
        return taken_tasks

    def _process_as_completed(
        self, data: Iterable[asyncio.Task[ItemT]]
    ) -> List[asyncio.Task[ItemT]]:
        """Take first N tasks as they complete."""
        # Note: as_completed() requires a collection, so we still need to convert
        # But we do it more efficiently - only when needed for unordered processing
        input_tasks = list(data)
        
        as_completed_iter = asyncio.as_completed(input_tasks)
        taken_tasks = []
        
        # Take the first N from the as_completed iterator
        for _ in range(self.n):
            try:
                next_completed = next(as_completed_iter)
                taken_tasks.append(next_completed)
            except StopIteration:
                break
        
        # Cancel any remaining tasks
        for task in input_tasks:
            if task not in taken_tasks and not task.done():
                task.cancel()
                
        return taken_tasks

    async def _return_value(self, value: ItemT) -> ItemT:
        """Helper to return a value as a coroutine."""
        return value 