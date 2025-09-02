from typing import List

from relais.base import Step, T
from relais.processors import StatefulStreamProcessor, StatelessStreamProcessor
from relais.stream import StreamItemEvent, StreamReader, StreamWriter


class _OrderedSkipProcessor(StatefulStreamProcessor[T, T]):
    """Processor that skips the first N items while preserving order.

    This processor collects all items to ensure ordering is maintained,
    then returns all items except the first N.
    """

    def __init__(
        self, input_stream: StreamReader[T], output_stream: StreamWriter[T], n: int
    ):
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

    def __init__(
        self, input_stream: StreamReader[T], output_stream: StreamWriter[T], n: int
    ):
        """Initialize the unordered skip processor.

        Args:
            input_stream: Stream to read items from
            output_stream: Stream to write remaining items to
            n: Number of items to skip
        """
        super().__init__(input_stream, output_stream)
        self.n = n

    async def _process_item(self, item: StreamItemEvent[T]):
        """Process an item, skipping if we haven't skipped N items yet.

        Args:
            item: The indexed item to potentially skip or pass through
        """
        if self.n <= 0:
            await self.output_stream.write(item)
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

    def __init__(self, n: int, *, ordered: bool = False):
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
        self, input_stream: StreamReader[T], output_stream: StreamWriter[T]
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
