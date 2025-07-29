import asyncio
from typing import Awaitable, Callable, List, TypeVar, Any

from ..base import Step, Stream, StatefulStreamProcessor, T

U = TypeVar('U')

class _NotProvided:
    """Sentinel to indicate no initial value was provided."""
    def __repr__(self):
        return "NOT_PROVIDED"

NOT_PROVIDED = _NotProvided()

class _ReduceProcessor(StatefulStreamProcessor[T, U]):
    """Reduce processor."""

    def __init__(self, input_stream: Stream[T], output_stream: Stream[U], reducer: Callable[[U, T], Awaitable[U] | U], initial: U | _NotProvided):
        super().__init__(input_stream, output_stream)
        self.reducer = reducer
        self.initial = initial

    async def _process_items(self, items: List[T]) -> List[U]:
        """Reduce all items to a single value."""
        items_with_initial = [] if self.initial is NOT_PROVIDED else [self.initial]
        items_with_initial.extend(items)

        if not items_with_initial:
            raise ValueError("Cannot reduce empty sequence without initial value")
        
        accumulator = items_with_initial[0]
        items_to_process = items_with_initial[1:]
        
        for item in items_to_process:
            result = self.reducer(accumulator, item)
            if asyncio.iscoroutine(result):
                result = await result
            accumulator = result
        
        return [accumulator]  # Return as single-item list

class Reduce(Step[T, U]):
    """Reduce operation to accumulate items into a single value."""

    def __init__(self, reducer: Callable[[U, T], Awaitable[U] | U], initial: U | _NotProvided = NOT_PROVIDED):
        self.reducer = reducer
        self.initial = initial

    def _build_processor(self, input_stream: Stream[T], output_stream: Stream[U]) -> _ReduceProcessor[T, U]:
        return _ReduceProcessor(input_stream, output_stream, self.reducer, self.initial)

def reduce(reducer: Callable[[U, T], Awaitable[U] | U], initial: U | _NotProvided = NOT_PROVIDED) -> Reduce[T, U]:
    """Reduce operation to accumulate items into a single value."""
    return Reduce(reducer, initial)