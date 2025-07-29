import asyncio
from typing import Awaitable, Callable, Iterable, Union

from ..base import Step, Stream, StatelessStreamProcessor, Indexed, T, U

class _MapProcessor(StatelessStreamProcessor[T, U]):
    """Map processor."""

    def __init__(self, input_stream: Stream[T], output_stream: Stream[U], func: Callable[[T], Awaitable[U] | U]):
        super().__init__(input_stream, output_stream)
        self.func = func

    async def _process_item(self, item: Indexed[T]):
        result = self.func(item.item)

        if asyncio.iscoroutine(result):
            result = await result

        await self.output_stream.put(Indexed(index=item.index, item=result))

class Map(Step[T, U]):
    """Map operation to transform each item in an iterable."""

    def __init__(
        self,
        func: Callable[[T], Awaitable[U] | U]
    ):
        super().__init__()
        self.func = func

    def _build_processor(self, input_stream: Stream[T], output_stream: Stream[U]) -> _MapProcessor[T, U]:
        return _MapProcessor(input_stream, output_stream, self.func)

def map(func: Callable[[T], Awaitable[U] | U]) -> Map[T, U]:
    """Map operation to transform each item in an iterable."""
    return Map(func)

