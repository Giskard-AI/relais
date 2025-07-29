from typing import List

from ..base import Step, Stream, StatefulStreamProcessor, StatelessStreamProcessor, Indexed, T

class _OrderedSkipProcessor(StatefulStreamProcessor[T, T]):
    """Skip processor."""

    def __init__(self, input_stream: Stream[T], output_stream: Stream[T], n: int):
        super().__init__(input_stream, output_stream)
        self.n = n

    async def _process_items(self, items: List[T]) -> List[T]:
        return items[self.n:]

class _UnorderedSkipProcessor(StatelessStreamProcessor[T, T]):
    """Skip processor."""

    def __init__(self, input_stream: Stream[T], output_stream: Stream[T], n: int):
        super().__init__(input_stream, output_stream)
        self.n = n

    async def _process_item(self, item: Indexed[T]):
        if self.n <= 0:
            await self.output_stream.put(item)
        else:
            self.n -= 1


class Skip(Step[T, T]):
    """Skip step."""

    def __init__(self, n: int, *, ordered: bool = True):
        if n < 0:
            raise ValueError("n must be greater than 0")
        
        self.n = n
        self.ordered = ordered

    def _build_processor(self, input_stream: Stream[T], output_stream: Stream[T]) -> _OrderedSkipProcessor[T] | _UnorderedSkipProcessor[T]:
        if self.ordered:
            return _OrderedSkipProcessor(input_stream, output_stream, self.n)
        else:
            return _UnorderedSkipProcessor(input_stream, output_stream, self.n)
    
def skip(n: int, *, ordered: bool = True) -> Skip[T]:
    """Skip step."""
    return Skip(n, ordered=ordered)