from typing import Callable, Any, Set, List
import warnings

from relais.base import Step
from relais.stream import T, Indexed, Stream
from relais.processors import StatelessStreamProcessor


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
        input_stream: Stream[T],
        output_stream: Stream[T],
        key: Callable[[T], Any] | None = None,
        max_unhashable_items=10000,
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

    async def _process_item(self, item: Indexed[T]):
        """Process an item, passing it through only if not seen before.

        Args:
            item: The indexed item to check for uniqueness
        """
        key = self.key(item.item) if self.key else item.item

        try:
            # Try to use set for hashable items (faster)
            if key not in self.seen:
                self.seen.add(key)
                await self.output_stream.put(item)
        except TypeError:
            # Handle unhashable items (like dicts) with list lookup
            if len(self.seen_unhashable) == self.max_unhashable_items:
                warnings.warn(
                    "Distinct processor reached max unhashable items limit. Consider using a different key function to avoid performance degradation."
                )

            if key not in self.seen_unhashable:
                self.seen_unhashable.append(key)
                await self.output_stream.put(item)

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
        self, key: Callable[[T], Any] | None = None, max_unhashable_items=10000
    ):
        """Initialize the Distinct step.

        Args:
            key: Optional function to extract comparison key from each item
            max_unhashable_items: Maximum number of unhashable items to track
        """
        self.key = key
        self.max_unhashable_items = max_unhashable_items

    def _build_processor(
        self, input_stream: Stream[T], output_stream: Stream[T]
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


def distinct(
    key: Callable[[T], Any] | None = None, max_unhashable_items=10000
) -> Distinct[T]:
    """Create a distinct step that removes duplicate items from the stream.

    This function creates a deduplication operation that keeps only the first
    occurrence of each unique item. Items are compared for equality either
    directly or using an optional key function.

    Args:
        key: Optional function to extract comparison key from each item.
             If None, items are compared directly for equality.
        max_unhashable_items: Maximum number of unhashable items to track
                             before issuing a performance warning.

    Returns:
        A Distinct step that can be used in pipelines

    Examples:
        >>> # Basic deduplication
        >>> result = await ([1, 2, 2, 3, 1, 4] | distinct()).collect()
        >>> # [1, 2, 3, 4]

        >>> # Deduplicate by length
        >>> words = ["cat", "dog", "bird", "ant", "elephant"]
        >>> by_length = await (words | distinct(key=len)).collect()
        >>> # ["cat", "bird", "elephant"] (first of each length: 3, 4, 8)

        >>> # Deduplicate objects by field
        >>> people = [
        ...     {"name": "Alice", "age": 25},
        ...     {"name": "Bob", "age": 30},
        ...     {"name": "Alice", "age": 26}  # Different age, same name
        ... ]
        >>> unique_names = await (people | distinct(key=lambda p: p["name"])).collect()
        >>> # Only first person with each name

        >>> # Case-insensitive string deduplication
        >>> words = ["Hello", "world", "HELLO", "World"]
        >>> case_insensitive = await (words | distinct(key=str.lower)).collect()
        >>> # ["Hello", "world"] (first of each case-insensitive string)

    Performance Notes:
        - Uses efficient set-based lookup (O(1)) for hashable keys
        - Falls back to list-based lookup (O(n)) for unhashable keys
        - Memory usage grows with number of unique items seen
        - Consider using a key function to make items hashable for better performance

    Warning:
        If tracking many unhashable items, performance will degrade and
        a warning will be issued at the configured limit.
    """
    return Distinct(key, max_unhashable_items)
