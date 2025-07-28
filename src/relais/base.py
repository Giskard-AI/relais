import asyncio
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Generic,
    Iterable,
    List,
    Optional,
    TypeVar,
    Union,
)

# Type variables for generic type transformations
T = TypeVar("T")  # Input type
U = TypeVar("U")  # Output type
V = TypeVar("V")  # Additional type for chaining

# Constrained type variables
IterableT = TypeVar("IterableT", bound=Iterable)  # Must be iterable
ItemT = TypeVar("ItemT")  # Individual item type within iterables


def is_iterable(data: Any) -> bool:
    return (hasattr(data, "__iter__") or hasattr(data, "__aiter__")) and not isinstance(data, (str, bytes))


async def identity(item: ItemT) -> ItemT:
    return item


class Pipeline(Generic[T, U]):
    def __init__(
        self,
        steps: Optional[List["Step[Any, Any]"]] = None,
    ):
        self.steps = steps or []

    def __or__(self, other: "Step[U, V]") -> "Pipeline[T, V]":
        if hasattr(other, "bind"):  # It's a Step
            return other.bind(self)
        elif callable(other):  # It's a function like list, tuple, etc.
            from .steps.function_step import Function

            return Function(other).bind(self)  # type: ignore
        else:
            return NotImplemented

    async def run(self, data: Optional[T] = None) -> U:
        async with asyncio.TaskGroup() as tg:
            return await self._run(tg, data)

    async def collect(self, data: Optional[T] = None) -> List[U]:
        """Collect all results (alias for run)."""
        async with asyncio.TaskGroup() as tg:
            result = await self._run(tg, data)
            return await asyncio.gather(*result)

    async def stream(self, data: Optional[T] = None):
        """Stream results as they become available."""

        async with asyncio.TaskGroup() as tg:
            result = await self._run(tg, data)
            for completed_task in asyncio.as_completed(result):
                yield await completed_task

    async def _run(
        self, tg: asyncio.TaskGroup, data: Optional[T] = None
    ) -> Iterable[asyncio.Task[U]]:
        if not self.steps:
            # No steps - if data provided, return it; otherwise error
            if data is not None:
                return data  # type: ignore
            else:
                raise ValueError("Pipeline has no steps")

        steps_to_run = self.steps

        # If data is provided, prepend a data step
        if data is not None:
            if is_iterable(data):
                from .steps.iterable_step import Iterable as IterableStep

                data_step = IterableStep(data)
            else:
                from .steps.value_step import Value

                data_step = Value(data)
            steps_to_run = [data_step] + self.steps

        # Run all steps in sequence
        data = None
        for step in steps_to_run:
            data = await step.process(data, tg)

        return data


class Step(Generic[T, U]):
    """Base class for all pipeline steps."""

    def __init__(self, *, ordered: bool = True):
        self.ordered = ordered

    def bind(self, pipeline: Pipeline[Any, T]) -> Pipeline[Any, U]:
        """Bind this step to a pipeline."""
        return Pipeline(pipeline.steps + [self])

    def __or__(self, other: "Step[U, V]") -> "Pipeline[T, V]":
        return Pipeline([self, other])

    def __ror__(self, other: T) -> Pipeline[T, U]:
        if is_iterable(other):
            from .steps.iterable_step import Iterable as IterableStep

            data_step = IterableStep(other)
        else:
            from .steps.value_step import Value

            data_step = Value(other)

        return Pipeline([data_step, self])  # type: ignore

    async def process(self, data: Any, tg: asyncio.TaskGroup) -> Any:
        """Process tasks and return new tasks. Should be implemented by subclasses."""
        raise NotImplementedError

    async def run(self, data: Any) -> Any:
        """Convenience method to run this step as a single-step pipeline."""
        pipeline = Pipeline([self])
        return await pipeline.run(data)
