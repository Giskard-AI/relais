import asyncio
from typing import Any, Callable, Iterable, List, Optional

from ..base import Step, ItemT


class Sort(Step[Iterable[asyncio.Task[ItemT]], Iterable[asyncio.Task[ItemT]]]):
    """Sort operation to sort items in an iterable."""

    def __init__(
        self,
        *,
        key: Optional[Callable[[ItemT], Any]] = None,
        reverse: bool = False,
        ordered: bool = True,
    ):
        super().__init__(ordered=ordered)
        self.key = key
        self.reverse = reverse

    async def process(
        self, data: Iterable[asyncio.Task[ItemT]], tg: asyncio.TaskGroup
    ) -> Iterable[asyncio.Task[ItemT]]:
        """Process tasks, sort their results, and return new sorted tasks."""

        input_tasks = list(data)
        if not input_tasks:
            return []

        async def _get_sorted_results() -> List[ItemT]:
            """Wait for all inputs and return sorted results."""
            # Wait for all input tasks to complete
            if self.ordered:
                values = await asyncio.gather(*input_tasks)
            else:
                values = []
                for completed_task in asyncio.as_completed(input_tasks):
                    value = await completed_task
                    values.append(value)

            # Sort the values
            sorted_values = sorted(values, key=self.key, reverse=self.reverse)
            return sorted_values

        async def _create_task_for_value(value: ItemT) -> ItemT:
            """Helper to return a value as a coroutine."""
            return value

        # Get sorted results
        sorted_values = await _get_sorted_results()

        # Create new tasks for each sorted value
        return [
            tg.create_task(_create_task_for_value(value)) for value in sorted_values
        ] 