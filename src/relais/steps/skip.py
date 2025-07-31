from typing import List

from relais.base import Step, T
from relais.stream import Indexed, Stream
from relais.processors import StatefulStreamProcessor, StatelessStreamProcessor


class _OrderedSkipProcessor(StatefulStreamProcessor[T, T]):
    """Processor that skips the first N items while preserving order.

    This processor collects all items to ensure ordering is maintained,
    then returns all items except the first N.
    """

    def __init__(self, input_stream: Stream[T], output_stream: Stream[T], n: int):
        """Initialize the ordered skip processor.

        Args:
            input_stream: Stream to read items from
            output_stream: Stream to write remaining items to
            n: Number of items to skip
        """
        super().__init__(input_stream, output_stream)
        self.n = n

    async def _process_items(self, items: List[T]) -> List[T]:
        """Return all items except the first N.

        Args:
            items: All items from the input stream

        Returns:
            All items starting from index N
        """
        return items[self.n :]


class _UnorderedSkipProcessor(StatelessStreamProcessor[T, T]):
    """Processor that skips the first N items as they arrive.

    This processor counts items as they arrive and starts passing them
    through once N items have been skipped, providing better performance
    when exact ordering isn't required.
    """

    def __init__(self, input_stream: Stream[T], output_stream: Stream[T], n: int):
        """Initialize the unordered skip processor.

        Args:
            input_stream: Stream to read items from
            output_stream: Stream to write remaining items to
            n: Number of items to skip
        """
        super().__init__(input_stream, output_stream)
        self.n = n

    async def _process_item(self, item: Indexed[T]):
        """Process an item, skipping if we haven't skipped N items yet.

        Args:
            item: The indexed item to potentially skip or pass through
        """
        if self.n <= 0:
            await self.output_stream.put(item)
        else:
            self.n -= 1


class Skip(Step[T, T]):
    """Pipeline step that skips the first N items from the stream.

    The Skip step ignores the first N items from the input stream and passes
    through all remaining items. It supports two modes:

    - Ordered (default): Collects all items to preserve ordering, then skips first N
    - Unordered: Skips items as they arrive for better performance

    Example:
        >>> # Skip first 3 numbers
        >>> pipeline = range(10) | skip(3)
        >>> await pipeline.collect()  # [3, 4, 5, 6, 7, 8, 9]

        >>> # Skip and take combination (pagination)
        >>> page_2 = await (
        ...     range(100)
        ...     | skip(10)   # Skip first page
        ...     | take(10)   # Take second page
        ... ).collect()
        >>> # [10, 11, 12, 13, 14, 15, 16, 17, 18, 19]

    Performance:
        - Ordered mode: O(n) memory for all items, preserves exact order
        - Unordered mode: O(1) memory, processes items as they arrive
    """

    def __init__(self, n: int, *, ordered: bool = True):
        """Initialize the Skip step.

        Args:
            n: Number of items to skip
            ordered: If True, preserve exact ordering (slower). If False, skip
                    items as they arrive for better performance.

        Raises:
            ValueError: If n is negative
        """
        if n < 0:
            raise ValueError("n must be greater than 0")

        self.n = n
        self.ordered = ordered

    def _build_processor(
        self, input_stream: Stream[T], output_stream: Stream[T]
    ) -> _OrderedSkipProcessor[T] | _UnorderedSkipProcessor[T]:
        """Build the appropriate processor based on ordering requirements.

        Args:
            input_stream: Stream to read from
            output_stream: Stream to write to

        Returns:
            Either an ordered or unordered skip processor
        """
        if self.ordered:
            return _OrderedSkipProcessor(input_stream, output_stream, self.n)
        else:
            return _UnorderedSkipProcessor(input_stream, output_stream, self.n)


def skip(n: int, *, ordered: bool = True) -> Skip[T]:
    """Create a skip step that ignores the first N items from the stream.

    This function creates a skipping operation that discards the first N items
    and passes through all remaining items. Supports both ordered and unordered
    modes for different performance characteristics.

    Args:
        n: Number of items to skip from the beginning. Must be non-negative.
        ordered: If True (default), preserve exact ordering by collecting all items.
                If False, skip items as they arrive for better performance.

    Returns:
        A Skip step that can be used in pipelines

    Raises:
        ValueError: If n is negative

    Examples:
        >>> # Basic usage: skip first 2 items
        >>> result = await (range(5) | skip(2)).collect()
        >>> # [2, 3, 4]

        >>> # Pagination: skip first page, take second page
        >>> page_size = 10
        >>> page_2 = await (
        ...     range(100)
        ...     | skip(page_size)      # Skip first page
        ...     | take(page_size)      # Take second page
        ... ).collect()
        >>> # [10, 11, 12, 13, 14, 15, 16, 17, 18, 19]

        >>> # Skip header in data processing
        >>> lines = ["# Header", "data1", "data2", "data3"]
        >>> data_only = await (lines | skip(1)).collect()
        >>> # ["data1", "data2", "data3"]

        >>> # Skip warm-up iterations in performance testing
        >>> measurements = await (
        ...     test_iterations
        ...     | skip(5)              # Skip warm-up
        ...     | map(measure_performance)
        ...     | take(100)            # Take actual measurements
        ... ).collect()

        >>> # Performance optimization for large streams
        >>> # Skip efficiently without loading everything into memory
        >>> large_stream = range(1000000)
        >>> efficient_skip = await (
        ...     large_stream
        ...     | skip(500000, ordered=False)  # Skip half efficiently
        ...     | take(10)
        ... ).collect()

        >>> # Chain with filtering
        >>> filtered_tail = await (
        ...     range(20)
        ...     | filter(lambda x: x % 2 == 0)  # Get evens: [0,2,4,6,8,10,12,14,16,18]
        ...     | skip(3)                        # Skip first 3: [6,8,10,12,14,16,18]
        ... ).collect()

    Performance Considerations:
        - ordered=True: Processes entire upstream, preserves exact order
        - ordered=False: Processes items as they arrive, better for large streams
        - Use ordered=False when you don't need exact ordering and have large inputs
        - Use ordered=True when exact order matters or upstream is small

    Use Cases:
        - Pagination (skip to specific page)
        - Data processing (skip headers, warm-up periods)
        - Windowing operations (sliding windows)
        - Performance testing (skip initialization phase)
        - Log processing (skip to specific time range)

    Common Patterns:
        >>> # Pagination
        >>> page_n = lambda page, size: skip(page * size) | take(size)
        >>>
        >>> # Skip and process tail
        >>> process_tail = lambda n: skip(n) | map(expensive_operation)
        >>>
        >>> # Sliding window
        >>> sliding_window = lambda step: skip(step) | take(window_size)
    """
    return Skip(n, ordered=ordered)
