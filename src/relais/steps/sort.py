import asyncio
from typing import Any, Callable, List, Optional

from ..base import Step, Stream, StatefulStreamProcessor, T


class _SortProcessor(StatefulStreamProcessor[T, T]):
    """Sort processor."""

    def __init__(self, input_stream: Stream[T], output_stream: Stream[T], key: Callable[[T], Any], reverse: bool):
        super().__init__(input_stream, output_stream)
        self.key = key
        self.reverse = reverse

    async def _process_items(self, items: List[T]) -> List[T]:
        return sorted(items, key=self.key, reverse=self.reverse)
        

class Sort(Step[T, T]):
    """Sort operation to sort items in an iterable."""

    def __init__(
        self,
        *,
        key: Optional[Callable[[T], Any]] = None,
        reverse: bool = False,
        ordered: bool = True,
    ):
        super().__init__()
        self.key = key
        self.reverse = reverse
        self.ordered = ordered

    def _build_processor(self, input_stream: Stream[T], output_stream: Stream[T]) -> _SortProcessor[T]:
        return _SortProcessor(input_stream, output_stream, self.key, self.reverse)
    
def sort(key: Optional[Callable[[T], Any]] = None, reverse: bool = False, ordered: bool = True) -> Sort[T]:
    """Sort operation to sort items in an iterable."""
    return Sort(key=key, reverse=reverse, ordered=ordered)