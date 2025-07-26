import asyncio
from typing import (
    Any,
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


class Pipeline(Generic[T, U]):
    def __init__(
        self,
        source: Optional[Union[T, "Pipeline[T, U]"]],
        steps: Optional[List["Step[Any, Any]"]] = None,
    ):
        self.source = source
        self.steps = steps or []

    def __or__(self, other: "Step[U, V]") -> "Pipeline[T, V]":
        if hasattr(other, "bind"):  # It's a Step
            return other.bind(self)
        elif callable(other):  # It's a function like list, tuple, etc.
            return FunctionStep(other).bind(self)  # type: ignore
        else:
            return NotImplemented

    async def _get_source_data(self, data: Optional[T]) -> T:
        """Get source data from various sources."""
        if data is not None:
            return data
        elif self.source is None:
            raise ValueError(
                "Pipeline has no source data. Pass data to collect()/stream() or create pipeline with source."
            )
        elif hasattr(self.source, "collect"):
            # Source is another pipeline
            return await self.source.collect()
        else:
            return self.source  # type: ignore

    async def _process_through_pipeline(self, source_data: T) -> U:
        """Process data through all pipeline steps."""
        current_data: Any = source_data

        # Pass data through each step
        for step in self.steps:
            current_data = await step.process(current_data)

        return current_data

    async def run(self, data: Optional[T] = None) -> U:
        """Run the pipeline and return the final result."""
        source_data = await self._get_source_data(data)

        if not self.steps:
            return source_data

        return await self._process_through_pipeline(source_data)

    async def collect(self, data: Optional[T] = None) -> U:
        """Collect all results (alias for run)."""
        return await self.run(data)

    async def stream(self, data: Optional[T] = None):
        """Stream results. Note: Some steps (like Sort) need all input before producing output."""
        source_data = await self._get_source_data(data)

        if not self.steps:
            # If source data is iterable, yield items; otherwise yield the data itself
            if hasattr(source_data, '__iter__') and not isinstance(source_data, (str, bytes)):
                for item in source_data:  # type: ignore
                    yield item
            else:
                yield source_data
            return

        # Process through pipeline
        result = await self._process_through_pipeline(source_data)
        
        # If result is iterable, yield items; otherwise yield the result itself
        if hasattr(result, '__iter__') and not isinstance(result, (str, bytes)):
            for item in result:  # type: ignore
                yield item
        else:
            yield result


class Step(Generic[T, U]):
    """Base class for all pipeline steps that transform tasks of type T to tasks of type U."""

    def __init__(self, *, ordered: bool = True):
        self.ordered = ordered

    def __ror__(self, other: T) -> Pipeline[T, U]:
        # Support data | step syntax  
        return Pipeline(other) | self  # type: ignore

    def __or__(self, other: "Step[U, V]") -> Pipeline[T, V]:
        # Support step | step syntax (no source data yet)
        if isinstance(other, Step):
            return Pipeline(None, [self]) | other  # type: ignore
        else:
            return NotImplemented

    def bind(self, pipeline: Pipeline[T, Any]) -> Pipeline[T, U]:
        """Add this step to the pipeline."""
        new_steps = pipeline.steps + [self]
        return Pipeline(pipeline.source, new_steps)  # type: ignore

    async def process(self, data: T) -> U:
        """Process data of type T and return data of type U. Should be implemented by subclasses."""
        raise NotImplementedError


class Map(Step[T, U]):
    def __init__(
        self, func: Callable[[T], Union[U, Awaitable[U]]], *, ordered: bool = True
    ):
        super().__init__(ordered=ordered)
        self.func = func

    async def process(self, data: T) -> U:
        """Process data through the map function."""
        # If data is iterable (like a list), apply function to each item
        if hasattr(data, '__iter__') and not isinstance(data, (str, bytes)):
            results = []
            for item in data:  # type: ignore
                result = self.func(item)
                # Handle async functions
                if asyncio.iscoroutine(result):
                    result = await result
                results.append(result)
            return results  # type: ignore
        else:
            # Apply function to single item
            result = self.func(data)
            # Handle async functions
            if asyncio.iscoroutine(result):
                result = await result
            return result


class Sort(Step):
    """Sort step that needs all items at once."""

    def __init__(
        self,
        key: Optional[Callable[[Any], Any]] = None,
        reverse: bool = False,
        *,
        ordered: bool = True,
    ):
        super().__init__(ordered=ordered)
        self.key = key
        self.reverse = reverse

    async def process(self, data: Any) -> Any:
        """Sort data if it's iterable."""
        # If data is iterable (like a list), sort it
        if hasattr(data, '__iter__') and not isinstance(data, (str, bytes)):
            values = list(data)
            # Sort the values
            if self.key:
                sorted_values = sorted(values, key=self.key, reverse=self.reverse)
            else:
                sorted_values = sorted(values, reverse=self.reverse)
            return sorted_values
        else:
            # Can't sort a single item, return as-is
            return data


class Take(Step):
    """Take only the first N items, cancelling remaining tasks."""

    def __init__(self, n: int, *, ordered: bool = True):
        super().__init__(ordered=ordered)
        self.n = n

    async def process(self, data: Any) -> Any:
        """Take first N items from data if it's iterable."""
        # If data is iterable (like a list), take first n items
        if hasattr(data, '__iter__') and not isinstance(data, (str, bytes)):
            values = list(data)
            return values[: self.n]
        else:
            # For single item, return it if n > 0, otherwise return empty list
            return data if self.n > 0 else []


class FunctionStep(Step):
    """Step that wraps a function to be applied to the pipeline result."""

    def __init__(self, func: Callable, *, ordered: bool = True):
        super().__init__(ordered=ordered)
        self.func = func

    async def process(self, data: Any) -> Any:
        """Apply the function to the data."""
        # Apply the function directly to the data
        result = self.func(data)
        
        # Handle async functions
        if asyncio.iscoroutine(result):
            result = await result
            
        return result
