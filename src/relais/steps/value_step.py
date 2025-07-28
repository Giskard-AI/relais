import asyncio
from typing import Iterable

from ..base import Step, T, identity


class Value(Step[None, T]):
    """Source step for handling single values."""

    def __init__(self, value: T, *, ordered: bool = True):
        super().__init__(ordered=ordered)
        self.value = value

    async def process(self, data: None, tg: asyncio.TaskGroup) -> Iterable[asyncio.Task[T]]:
        """Convert single value into a task."""
        
        # Create a single task for the value
        task = tg.create_task(identity(self.value))
        return [task]