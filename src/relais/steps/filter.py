import asyncio
from typing import Awaitable, Callable

from ..base import Step, Stream, StatelessStreamProcessor, Indexed, T


class _FilterProcessor(StatelessStreamProcessor[T, T]):
    """Processor that filters items based on a predicate function.

    Only items that satisfy the predicate (return True) are passed through
    to the output stream. Items that don't match are dropped.
    """

    def __init__(
        self,
        input_stream: Stream[T],
        output_stream: Stream[T],
        predicate: Callable[[T], Awaitable[bool] | bool],
    ):
        """Initialize the filter processor.

        Args:
            input_stream: Stream to read items from
            output_stream: Stream to write filtered items to
            predicate: Function that determines if an item should be kept
        """
        super().__init__(input_stream, output_stream)
        self.predicate = predicate

    async def _process_item(self, item: Indexed[T]):
        """Apply the predicate to an item and pass it through if it matches.

        Args:
            item: The indexed item to test
        """
        result = self.predicate(item.item)

        if asyncio.iscoroutine(result):
            result = await result

        if result:
            await self.output_stream.put(item)


class Filter(Step[T, T]):
    """Pipeline step that filters items based on a predicate function.

    The Filter step only allows items that satisfy the predicate condition
    to pass through to the next step. Items that don't match are dropped
    from the pipeline.

    The predicate function can be synchronous or asynchronous and should
    return True for items to keep, False for items to drop.

    Example:
        >>> # Keep only even numbers
        >>> pipeline = range(10) | filter(lambda x: x % 2 == 0)
        >>> await pipeline.collect()  # [0, 2, 4, 6, 8]

        >>> # Filter strings by length
        >>> words = ["hi", "hello", "world", "a", "python"]
        >>> pipeline = words | filter(lambda s: len(s) > 2)
        >>> await pipeline.collect()  # ["hello", "world", "python"]
    """

    def __init__(self, predicate: Callable[[T], Awaitable[bool] | bool]):
        """Initialize the Filter step.

        Args:
            predicate: Function that returns True for items to keep
        """
        self.predicate = predicate

    def _build_processor(
        self, input_stream: Stream[T], output_stream: Stream[T]
    ) -> _FilterProcessor[T]:
        """Build the processor for this filter step.

        Args:
            input_stream: Stream to read from
            output_stream: Stream to write to

        Returns:
            A configured filter processor
        """
        return _FilterProcessor(input_stream, output_stream, self.predicate)


def filter(predicate: Callable[[T], Awaitable[bool] | bool]) -> Filter[T]:
    """Create a filter step that keeps items matching a predicate.

    This function creates a filter operation that only allows items satisfying
    the predicate condition to pass through the pipeline. The predicate can be
    either synchronous or asynchronous.

    Args:
        predicate: Function that returns True for items to keep, False to drop.
                  Can be async or sync.

    Returns:
        A Filter step that can be used in pipelines

    Examples:
        >>> # Filter numbers
        >>> evens = filter(lambda x: x % 2 == 0)
        >>> result = await (range(6) | evens).collect()
        >>> # [0, 2, 4]

        >>> # Filter with complex condition
        >>> valid_users = filter(lambda user: user.get('active', False) and user.get('age', 0) >= 18)
        >>>
        >>> # Async predicate
        >>> async def is_valid_email(email):
        ...     # Simulate email validation API call
        ...     await asyncio.sleep(0.01)
        ...     return '@' in email and '.' in email
        >>>
        >>> emails = ["test@example.com", "invalid", "user@domain.org"]
        >>> valid_emails = await (emails | filter(is_valid_email)).collect()
        >>> # ["test@example.com", "user@domain.org"]
    """
    return Filter(predicate)
