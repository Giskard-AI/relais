import asyncio
from typing import Awaitable, Callable

from ..base import Step, Stream, StatelessStreamProcessor, Indexed, T


class _FilterProcessor(StatelessStreamProcessor[T, T]):
    """Filter processor."""

    def __init__(self, input_stream: Stream[T], output_stream: Stream[T], predicate: Callable[[T], Awaitable[bool] | bool]):
        super().__init__(input_stream, output_stream)
        self.predicate = predicate
    
    async def _process_item(self, item: Indexed[T]):
        result = self.predicate(item.item)

        if asyncio.iscoroutine(result):
            result = await result

        if result:
            await self.output_stream.put(item)

class Filter(Step[T, T]):
    """Filter step."""

    def __init__(self, predicate: Callable[[T], Awaitable[bool] | bool]):
        self.predicate = predicate

    def _build_processor(self, input_stream: Stream[T], output_stream: Stream[T]) -> _FilterProcessor[T]:
        return _FilterProcessor(input_stream, output_stream, self.predicate)

def filter(predicate: Callable[[T], Awaitable[bool] | bool]) -> Filter[T]:
    """Filter step."""
    return Filter(predicate)