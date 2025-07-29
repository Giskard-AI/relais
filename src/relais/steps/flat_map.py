import asyncio
from typing import Awaitable, Callable, Iterable

from ..base import Step, Stream, StatelessStreamProcessor, Indexed, T, U, Index


class _FlatMapProcessor(StatelessStreamProcessor[T, U]):
    """FlatMap processor."""

    def __init__(self, input_stream: Stream[T], output_stream: Stream[U], func: Callable[[T], Awaitable[Iterable[U]] | Iterable[U]]):
        super().__init__(input_stream, output_stream)
        self.func = func

    async def _process_item(self, item: Indexed[T]):
        results = self.func(item.item)

        if asyncio.iscoroutine(results):
            results = await results

        for i, result in enumerate(results):
          sub_index = Index(i, None)
          new_index = Index(item.index.index, sub_index)
          await self.output_stream.put(Indexed(new_index, result))

class FlatMap(Step[T, U]):
    """FlatMap step."""

    def __init__(self, func: Callable[[T], Awaitable[Iterable[U]] | Iterable[U]]):
        self.func = func

    def _build_processor(self, input_stream: Stream[T], output_stream: Stream[U]) -> _FlatMapProcessor[T, U]:
        return _FlatMapProcessor(input_stream, output_stream, self.func)
    
def flat_map(func: Callable[[T], Awaitable[Iterable[U]] | Iterable[U]]) -> FlatMap[T, U]:
    """FlatMap step."""
    return FlatMap(func)
        