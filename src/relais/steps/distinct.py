from typing import Callable, Any, Set, List, warnings

from ..base import Step, Stream, StatelessStreamProcessor, Indexed, T

class _DistinctProcessor(StatelessStreamProcessor[T, T]):
    """Distinct processor."""

    seen: Set[Any]
    seen_unhashable: List[Any]

    def __init__(self, input_stream: Stream[T], output_stream: Stream[T], key: Callable[[T], Any] | None = None):
        super().__init__(input_stream, output_stream)
        self.key = key
        self.seen = set()
        self.seen_unhashable = []

    async def _process_item(self, item: Indexed[T]):
        key = self.key(item.item) if self.key else item.item
        
        try:
            # Try to use set for hashable items (faster)
            if key not in self.seen:
                self.seen.add(key)
                await self.output_stream.put(item)
        except TypeError:
            # Handle unhashable items (like dicts) with list lookup
            if len(self.seen_unhashable) == 0:
                warnings.warn("Distinct is using a list to track unhashable items. This is slow.")

            if key not in self.seen_unhashable:
                self.seen_unhashable.append(key)
                await self.output_stream.put(item)  

class Distinct(Step[T, T]):
    """Distinct step."""

    def __init__(self, key: Callable[[T], Any] | None = None):
        self.key = key

    def _build_processor(self, input_stream: Stream[T], output_stream: Stream[T]) -> _DistinctProcessor[T]:
        return _DistinctProcessor(input_stream, output_stream, self.key)
    
def distinct(key: Callable[[T], Any] | None = None) -> Distinct[T]:
    """Distinct step."""
    return Distinct(key)