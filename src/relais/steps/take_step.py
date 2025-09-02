from typing import List

from relais.base import Step, T
from relais.processors import StatefulStreamProcessor, StatelessStreamProcessor
from relais.stream import StreamItemEvent, StreamReader, StreamWriter


class _OrderedTakeProcessor(StatefulStreamProcessor[T, T]):
    """Processor that takes the first N items while preserving order.

    This processor collects all items to ensure ordering is maintained,
    then returns only the first N items.
    """

    def __init__(
        self, input_stream: StreamReader[T], output_stream: StreamWriter[T], n: int
    ):
        """Initialize the ordered take processor.

        Args:
            input_stream: Stream to read items from
            output_stream: Stream to write the first N items to
            n: Number of items to take
        """
        super().__init__(input_stream, output_stream)
        self.n = n

    async def _process_items(self, items: List[T]) -> List[T]:
        """Return the first N items from the collected items.

        Args:
            items: All items from the input stream

        Returns:
            The first N items (or all items if fewer than N)
        """
        return items[: self.n]


class _UnorderedTakeProcessor(StatelessStreamProcessor[T, T]):
    """Processor that takes the first N items and stops upstream early.

    This processor processes items as they arrive and stops the upstream
    producer once N items have been taken, providing better performance
    for large streams when ordering isn't required.
    """

    def __init__(
        self, input_stream: StreamReader[T], output_stream: StreamWriter[T], n: int
    ):
        """Initialize the unordered take processor.

        Args:
            input_stream: Stream to read items from
            output_stream: Stream to write the first N items to
            n: Number of items to take
        """
        super().__init__(input_stream, output_stream)
        self.n = n
        self.taken = 0

    async def process_stream(self):
        """Process items from the input stream, taking only the first N."""
        # Special case: if n=0, complete immediately without processing anything
        if self.n == 0:
            await self.output_stream.complete()
            return

        # Otherwise, use the default stateless processing
        await super().process_stream()

    async def _process_item(self, item: StreamItemEvent[T]):
        """Process an item if we haven't taken N items yet.

        Args:
            item: The indexed item to potentially take
        """
        if self.taken < self.n:
            await self.output_stream.write(item)
            self.taken += 1

        # If we've taken enough items, signal upstream to stop producing
        if self.taken >= self.n:
            await self.output_stream.complete()


class Take(Step[T, T]):
    """Pipeline step that takes only the first N items from the stream.

    The Take step limits the output to the first N items from the input stream.
    It supports two modes:

    - Ordered (default): Collects all items to preserve ordering, then takes first N
    - Unordered: Takes items as they arrive and stops upstream early for performance

    Example:
        >>> # Take first 3 numbers
        >>> pipeline = range(10) | take(3)
        >>> await pipeline.collect()  # [0, 1, 2]

        >>> # Take first 5 from a large stream (ordered)
        >>> pipeline = range(1000000) | take(5, ordered=True)
        >>> # Will process all items to maintain order

        >>> # Take first 5 from a large stream (unordered, faster)
        >>> pipeline = range(1000000) | take(5, ordered=False)
        >>> # Will stop upstream after taking 5 items

    Performance:
        - Ordered mode: O(n) memory for all items, preserves exact order
        - Unordered mode: O(1) memory, stops upstream early, may not preserve order
    """

    def __init__(self, n: int, *, ordered: bool = False):
        """Initialize the Take step.

        Args:
            n: Number of items to take
            ordered: If True, preserve exact ordering (slower). If False, allow
                    early termination for better performance.

        Raises:
            ValueError: If n is negative
        """
        if n <= 0:
            raise ValueError("n must be greater than 0")

        self.n = n
        self.ordered = ordered

    def _build_processor(
        self, input_stream: StreamReader[T], output_stream: StreamWriter[T]
    ) -> _OrderedTakeProcessor[T] | _UnorderedTakeProcessor[T]:
        """Build the appropriate processor based on ordering requirements.

        Args:
            input_stream: Stream to read from
            output_stream: Stream to write to

        Returns:
            Either an ordered or unordered take processor
        """
        if self.ordered:
            return _OrderedTakeProcessor(input_stream, output_stream, self.n)
        else:
            return _UnorderedTakeProcessor(input_stream, output_stream, self.n)
