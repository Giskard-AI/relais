import warnings
from typing import Any, Callable, List, Set

from relais.base import Step
from relais.processors import StatelessStreamProcessor
from relais.stream import StreamItemEvent, StreamReader, StreamWriter, T


class _DistinctProcessor(StatelessStreamProcessor[T, T]):
    """Processor that filters out duplicate items based on equality or key function.

    This processor maintains state to track items that have already been seen,
    using efficient set-based lookup for hashable items and falling back to
    list-based lookup for unhashable items (like dictionaries).

    Memory is automatically released when processing completes.
    """

    seen: Set[Any]
    seen_unhashable: List[Any]

    def __init__(
        self,
        input_stream: StreamReader[T],
        output_stream: StreamWriter[T],
        key: Callable[[T], Any] | None = None,
        max_unhashable_items: int = 10000,
    ):
        """Initialize the distinct processor.

        Args:
            input_stream: Stream to read items from
            output_stream: Stream to write unique items to
            key: Optional function to extract comparison key from each item
            max_unhashable_items: Maximum number of unhashable items to track
        """
        super().__init__(input_stream, output_stream)
        self.key = key
        self.seen = set()
        self.seen_unhashable = []
        self.max_unhashable_items = max_unhashable_items

    async def _process_item(self, item: StreamItemEvent[T]):
        """Process an item, passing it through only if not seen before.

        Args:
            item: The indexed item to check for uniqueness
        """
        key = self.key(item.item) if self.key else item.item

        try:
            # Try to use set for hashable items (faster)
            if key not in self.seen:
                self.seen.add(key)
                await self.output_stream.write(item)
        except TypeError:
            # Handle unhashable items (like dicts) with list lookup
            if len(self.seen_unhashable) == self.max_unhashable_items:
                warnings.warn(
                    "Distinct processor reached max unhashable items limit. Consider using a different key function to avoid performance degradation."
                )

            if key not in self.seen_unhashable:
                self.seen_unhashable.append(key)
                await self.output_stream.write(item)

    async def _cleanup(self):
        """Release memory by clearing tracking structures."""
        # Release memory once the stream is done
        self.seen.clear()
        self.seen_unhashable.clear()


class Distinct(Step[T, T]):
    """Pipeline step that removes duplicate items from the stream.

    The Distinct step filters out items that have been seen before, keeping only
    the first occurrence of each unique item. Uniqueness is determined by equality
    or by applying an optional key function.

    The step uses efficient set-based tracking for hashable items and falls back
    to list-based tracking for unhashable items like dictionaries.

    Example:
        >>> # Remove duplicate numbers
        >>> pipeline = [1, 2, 2, 3, 1, 4] | distinct()
        >>> await pipeline.collect()  # [1, 2, 3, 4]

        >>> # Remove duplicates based on length
        >>> words = ["hi", "hello", "world", "bye"]
        >>> pipeline = words | distinct(key=len)
        >>> await pipeline.collect()  # ["hi", "hello"] (first of each length)

        >>> # Remove duplicate objects by key
        >>> users = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}, {"id": 1, "name": "Alice"}]
        >>> pipeline = users | distinct(key=lambda u: u["id"])
        >>> # Only first user with each ID

    Performance:
        - O(1) average case for hashable items (using set)
        - O(n) worst case for unhashable items (using list)
        - Memory usage grows with number of unique items
    """

    def __init__(
        self, key: Callable[[T], Any] | None = None, max_unhashable_items: int = 10000
    ):
        """Initialize the Distinct step.

        Args:
            key: Optional function to extract comparison key from each item
            max_unhashable_items: Maximum number of unhashable items to track
        """
        self.key = key
        self.max_unhashable_items = max_unhashable_items

    def _build_processor(
        self, input_stream: StreamReader[T], output_stream: StreamWriter[T]
    ) -> _DistinctProcessor[T]:
        """Build the processor for this distinct step.

        Args:
            input_stream: Stream to read from
            output_stream: Stream to write to

        Returns:
            A configured distinct processor
        """
        return _DistinctProcessor(
            input_stream, output_stream, self.key, self.max_unhashable_items
        )
