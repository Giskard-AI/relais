import asyncio
from typing import AsyncIterable, Iterable, Iterable as IterableType

from ..base import Step, ItemT, identity


class Iterable(Step[None, Iterable[asyncio.Task[ItemT]]]):
    """Source step for handling iterable data."""

    def __init__(
        self,
        data: IterableType[ItemT] | AsyncIterable[ItemT],
    ):
        self.data = data

    async def process(
        self, data: None, tg: asyncio.TaskGroup
    ) -> Iterable[asyncio.Task[ItemT]]:
        """Convert iterable data into asyncio.Task."""

        tasks = []
        if isinstance(self.data, AsyncIterable):
            async for item in self.data:
                if asyncio.iscoroutine(item):
                    tasks.append(tg.create_task(item))
                else:
                    tasks.append(tg.create_task(identity(item)))
        else:
            for item in self.data:
                if asyncio.iscoroutine(item):
                    tasks.append(tg.create_task(item))
                else:
                    tasks.append(tg.create_task(identity(item)))

        return tasks
