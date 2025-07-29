import asyncio
from typing import Any, Callable, Dict, List
from collections import defaultdict

from ..base import Step, Stream, StatefulStreamProcessor, T

class _GroupByProcessor(StatefulStreamProcessor[T, Dict[Any, List[T]]]):
    """GroupBy processor."""

    def __init__(self, input_stream: Stream[T], output_stream: Stream[Dict[Any, List[T]]], key_func: Callable[[T], Any]):
        super().__init__(input_stream, output_stream)
        self.key_func = key_func

    async def _process_items(self, items: List[T]) -> List[Dict[Any, List[T]]]:
        """Group items by key function."""
        groups: Dict[Any, List[T]] = defaultdict(list)
        
        for item in items:
            key = self.key_func(item)
            groups[key].append(item)
        
        # Return the groups dictionary as a single-item list
        return [dict(groups)]

class GroupBy(Step[T, Dict[Any, List[T]]]):
    """GroupBy operation to group items by a key function."""

    def __init__(self, key_func: Callable[[T], Any]):
        self.key_func = key_func

    def _build_processor(self, input_stream: Stream[T], output_stream: Stream[Dict[Any, List[T]]]) -> _GroupByProcessor[T]:
        return _GroupByProcessor(input_stream, output_stream, self.key_func)

def group_by(key_func: Callable[[T], Any]) -> GroupBy[T]:
    """GroupBy operation to group items by a key function."""
    return GroupBy(key_func)