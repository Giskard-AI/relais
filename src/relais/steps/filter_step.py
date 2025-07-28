import asyncio
from typing import Awaitable, Callable, Iterable, Union, List

from ..base import Step, ItemT


class Filter(Step[Iterable[Awaitable[ItemT] | ItemT], List[asyncio.Task[ItemT]]]):
    """Filter operation to select items that match a predicate."""

    def __init__(
        self,
        predicate: Callable[[ItemT], Awaitable[bool] | bool],
        *,
        ordered: bool = True,
    ):
        super().__init__(ordered=ordered)
        self.predicate = predicate

    async def process(
        self, data: Iterable[Awaitable[ItemT] | ItemT], tg: asyncio.TaskGroup
    ) -> Iterable[ItemT]:
        """Process the input data and return tasks for items that pass the filter."""

        async def _filter(item: Union[Awaitable[ItemT], ItemT]) -> ItemT | None:
            value = await item if isinstance(item, Awaitable) else item

            result = self.predicate(value)

            if asyncio.iscoroutine(result):
                result = await result

            return result, item

        tasks = [tg.create_task(_filter(item)) for item in data]

        if self.ordered:
            results_iterable = tasks
        else:
            results_iterable = asyncio.as_completed(tasks)

        for task in results_iterable:
            result, item = await task
            if result:
                yield item
