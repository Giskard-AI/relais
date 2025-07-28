import asyncio
from typing import Awaitable, Callable, Iterable, Union

from ..base import Step, ItemT, U


class Map(Step[Iterable[Awaitable[ItemT] | ItemT], Iterable[asyncio.Task[U]]]):
    """Map operation to transform each item in an iterable."""

    def __init__(
        self,
        func: Callable[[ItemT], Awaitable[U] | U],
        *,
        ordered: bool = True,
    ):
        super().__init__(ordered=ordered)
        self.func = func

    async def process(
        self, data: Iterable[Awaitable[ItemT] | ItemT], tg: asyncio.TaskGroup
    ) -> Iterable[asyncio.Task[U]]:
        """Process the input data and return mapped tasks."""

        async def _apply(item: Union[Awaitable[ItemT], ItemT]) -> U:
            value = await item if isinstance(item, Awaitable) else item

            result = self.func(value)

            if asyncio.iscoroutine(result):
                result = await result

            return result

        return (tg.create_task(_apply(item)) for item in data)
