import asyncio
from typing import Awaitable, Callable, List, TypeVar

from ..base import Step, Stream, StatefulStreamProcessor, T

U = TypeVar("U")


class _NotProvided:
    """Sentinel to indicate no initial value was provided.

    This is used to distinguish between an explicit None initial value
    and no initial value being provided at all.
    """

    def __repr__(self):
        return "NOT_PROVIDED"


NOT_PROVIDED = _NotProvided()


class _ReduceProcessor(StatefulStreamProcessor[T, U]):
    """Processor that reduces all items to a single accumulated value.

    This processor implements the classic reduce/fold operation, applying
    a binary function cumulatively to items in the sequence from left to right
    to reduce the sequence to a single value.

    The processor is stateful because it needs access to all items before
    it can begin the reduction process.
    """

    def __init__(
        self,
        input_stream: Stream[T],
        output_stream: Stream[U],
        reducer: Callable[[U, T], Awaitable[U] | U],
        initial: U | _NotProvided,
    ):
        """Initialize the reduce processor.

        Args:
            input_stream: Stream to read items from
            output_stream: Stream to write the reduced result to
            reducer: Binary function that takes (accumulator, item) and returns new accumulator
            initial: Initial value for the accumulator, or NOT_PROVIDED
        """
        super().__init__(input_stream, output_stream)
        self.reducer = reducer
        self.initial = initial

    async def _process_items(self, items: List[T]) -> List[U]:
        """Reduce all items to a single accumulated value.

        Args:
            items: All items from the input stream

        Returns:
            List containing the single reduced value

        Raises:
            ValueError: If sequence is empty and no initial value provided
        """
        items_with_initial = [] if self.initial is NOT_PROVIDED else [self.initial]
        items_with_initial.extend(items)

        if not items_with_initial:
            raise ValueError("Cannot reduce empty sequence without initial value")

        accumulator = items_with_initial[0]
        items_to_process = items_with_initial[1:]

        for item in items_to_process:
            result = self.reducer(accumulator, item)
            if asyncio.iscoroutine(result):
                result = await result
            accumulator = result

        return [accumulator]  # Return as single-item list


class Reduce(Step[T, U]):
    """Pipeline step that reduces all items to a single accumulated value.

    The Reduce step applies a binary function cumulatively to items in the
    sequence, from left to right, to reduce the sequence to a single value.
    This is equivalent to Python's built-in reduce() function.

    The operation is stateful and requires all items to be collected before
    processing can begin.

    Example:
        >>> # Sum all numbers
        >>> total = await (range(5) | reduce(lambda acc, x: acc + x, 0)).collect()
        >>> # [10]  (Note: reduce returns a list with one item)

        >>> # Find maximum
        >>> maximum = await ([3, 1, 4, 1, 5] | reduce(max)).collect()
        >>> # [5]

        >>> # Build a string
        >>> text = await (["a", "b", "c"] | reduce(lambda acc, x: acc + x, "")).collect()
        >>> # ["abc"]

    Warning:
        This operation loads all items into memory and returns a list with
        a single item (the reduced result).
    """

    def __init__(
        self,
        reducer: Callable[[U, T], Awaitable[U] | U],
        initial: U | _NotProvided = NOT_PROVIDED,
    ):
        """Initialize the Reduce step.

        Args:
            reducer: Binary function that takes (accumulator, item) and returns new accumulator
            initial: Initial value for the accumulator. If not provided, the first item is used.
        """
        self.reducer = reducer
        self.initial = initial

    def _build_processor(
        self, input_stream: Stream[T], output_stream: Stream[U]
    ) -> _ReduceProcessor[T, U]:
        """Build the processor for this reduce step.

        Args:
            input_stream: Stream to read from
            output_stream: Stream to write to

        Returns:
            A configured reduce processor
        """
        return _ReduceProcessor(input_stream, output_stream, self.reducer, self.initial)


def reduce(
    reducer: Callable[[U, T], Awaitable[U] | U],
    initial: U | _NotProvided = NOT_PROVIDED,
) -> Reduce[T, U]:
    """Create a reduce step that accumulates items into a single value.

    This function creates a reduction operation that applies a binary function
    cumulatively to items in the sequence from left to right. The reducer
    function can be synchronous or asynchronous.

    Args:
        reducer: Binary function that takes (accumulator, item) and returns
                a new accumulator value. Can be async.
        initial: Initial value for the accumulator. If not provided, the
                first item in the sequence is used as the initial value.

    Returns:
        A Reduce step that can be used in pipelines

    Raises:
        ValueError: If the sequence is empty and no initial value is provided

    Examples:
        >>> # Sum numbers
        >>> total = await (range(1, 6) | reduce(lambda acc, x: acc + x)).collect()
        >>> # [15]  (1 + 2 + 3 + 4 + 5)

        >>> # Sum with initial value
        >>> total = await (range(1, 6) | reduce(lambda acc, x: acc + x, 100)).collect()
        >>> # [115]  (100 + 1 + 2 + 3 + 4 + 5)

        >>> # Find maximum
        >>> maximum = await ([3, 7, 2, 9, 1] | reduce(max)).collect()
        >>> # [9]

        >>> # Build concatenated string
        >>> text = await (["Hello", " ", "World"] | reduce(lambda a, b: a + b)).collect()
        >>> # ["Hello World"]

        >>> # Complex accumulation
        >>> async def async_accumulate(acc, item):
        ...     await asyncio.sleep(0.01)  # Simulate async work
        ...     return acc + item ** 2
        >>>
        >>> sum_squares = await (range(5) | reduce(async_accumulate, 0)).collect()
        >>> # [30]  (0 + 1 + 4 + 9 + 16)

    Note:
        - The result is always a list with one item (the final accumulated value)
        - This is a stateful operation that loads all items into memory
        - The reducer function is called (n-1) times for n items (or n times with initial)
        - Execution order is left-to-right: reduce(f, [a, b, c]) = f(f(a, b), c)
    """
    return Reduce(reducer, initial)
