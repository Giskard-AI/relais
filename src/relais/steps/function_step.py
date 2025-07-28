import asyncio
from typing import Callable, Iterable

from ..base import Step, T, U


class Function(Step[T, U]):
    """Step that wraps a function to be applied to the pipeline result."""

    def __init__(self, func: Callable[[T], U], *, ordered: bool = True):
        super().__init__(ordered=ordered)
        self.func = func

    async def process(
        self, data: Iterable[asyncio.Task[T]], tg: asyncio.TaskGroup
    ) -> Iterable[asyncio.Task[U]]:
        """Apply the function to all task results."""

        input_tasks = list(data)
        if not input_tasks:
            return []

        async def _apply_to_all() -> U:
            """Await all tasks and apply function to the collected results."""
            values = await asyncio.gather(*input_tasks)
            result = self.func(values)

            # Handle async functions
            if asyncio.iscoroutine(result):
                return await result
            return result

        # Create a single task that applies the function to all results
        result_task = tg.create_task(_apply_to_all())
        return [result_task] 