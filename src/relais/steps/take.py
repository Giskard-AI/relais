from typing import List

from ..base import Step, Stream, StatefulStreamProcessor, StatelessStreamProcessor, Indexed, T

class _OrderedTakeProcessor(StatefulStreamProcessor[T, T]):
    """Take processor."""

    def __init__(self, input_stream: Stream[T], output_stream: Stream[T], n: int):
        super().__init__(input_stream, output_stream)
        self.n = n

    async def _process_items(self, items: List[T]) -> List[T]:
        return items[:self.n]

class _UnorderedTakeProcessor(StatelessStreamProcessor[T, T]):
    """Take processor."""

    def __init__(self, input_stream: Stream[T], output_stream: Stream[T], n: int):
        super().__init__(input_stream, output_stream)
        self.n = n

    async def _process_item(self, item: Indexed[T]):
        if self.n > 0:
            await self.output_stream.put(item)
            self.n -= 1

        # TODO: close upstream streams?


class Take(Step[T, T]):
    """Take step."""

    def __init__(self, n: int, *, ordered: bool = True):
        if n < 0:
            raise ValueError("n must be greater than 0")
        
        self.n = n
        self.ordered = ordered

    def _build_processor(self, input_stream: Stream[T], output_stream: Stream[T]) -> _OrderedTakeProcessor[T] | _UnorderedTakeProcessor[T]:
        if self.ordered:
            return _OrderedTakeProcessor(input_stream, output_stream, self.n)
        else:
            return _UnorderedTakeProcessor(input_stream, output_stream, self.n)
    
def take(n: int, *, ordered: bool = True) -> Take[T]:
    """Take step."""
    return Take(n, ordered=ordered)