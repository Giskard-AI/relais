from typing import Any, Callable, List, Optional

from relais.base import Step, T
from relais.stream import Stream
from relais.processors import StatefulStreamProcessor


class _SortProcessor(StatefulStreamProcessor[T, T]):
    """Processor that sorts all items using Python's built-in sorted function.

    This is a stateful processor that must collect all items before sorting,
    as sorting requires access to the complete dataset.
    """

    def __init__(
        self,
        input_stream: Stream[T],
        output_stream: Stream[T],
        key: Callable[[T], Any],
        reverse: bool,
    ):
        """Initialize the sort processor.

        Args:
            input_stream: Stream to read items from
            output_stream: Stream to write sorted items to
            key: Function to extract comparison key from each item
            reverse: Whether to sort in descending order
        """
        super().__init__(input_stream, output_stream)
        self.key = key
        self.reverse = reverse

    async def _process_items(self, items: List[T]) -> List[T]:
        """Sort the collected items.

        Args:
            items: All items from the input stream

        Returns:
            Sorted list of items
        """
        return sorted(items, key=self.key, reverse=self.reverse)


class Sort(Step[T, T]):
    """Pipeline step that sorts all items in the stream.

    The Sort step collects all items from the input stream and sorts them
    using Python's built-in sorting algorithm. This is a stateful operation
    that requires all items to be in memory before processing.

    Supports custom key functions and reverse sorting, just like Python's
    sorted() function.

    Example:
        >>> # Basic sorting
        >>> pipeline = [3, 1, 4, 1, 5] | sort()
        >>> await pipeline.collect()  # [1, 1, 3, 4, 5]

        >>> # Sort with custom key
        >>> words = ["apple", "pie", "a", "longer"]
        >>> pipeline = words | sort(key=len)
        >>> await pipeline.collect()  # ["a", "pie", "apple", "longer"]

        >>> # Reverse sorting
        >>> pipeline = range(5) | sort(reverse=True)
        >>> await pipeline.collect()  # [4, 3, 2, 1, 0]

    Warning:
        This operation loads all items into memory, which may not be suitable
        for very large datasets.
    """

    def __init__(
        self,
        *,
        key: Optional[Callable[[T], Any]] = None,
        reverse: bool = False,
        ordered: bool = True,
    ):
        """Initialize the Sort step.

        Args:
            key: Function to extract comparison key from each item
            reverse: If True, sort in descending order
            ordered: Legacy parameter, kept for compatibility
        """
        super().__init__()
        self.key = key
        self.reverse = reverse
        self.ordered = ordered

    def _build_processor(
        self, input_stream: Stream[T], output_stream: Stream[T]
    ) -> _SortProcessor[T]:
        """Build the processor for this sort step.

        Args:
            input_stream: Stream to read from
            output_stream: Stream to write to

        Returns:
            A configured sort processor
        """
        return _SortProcessor(input_stream, output_stream, self.key, self.reverse)


def sort(
    key: Optional[Callable[[T], Any]] = None,
    reverse: bool = False,
    ordered: bool = True,
) -> Sort[T]:
    """Create a sort step that sorts all items in the stream.

    This function creates a sorting operation that collects all items and
    sorts them using Python's built-in sorting algorithm. The sort is stable,
    meaning equal elements maintain their relative order.

    Args:
        key: Optional function to extract comparison key from each item.
             If None, items are compared directly.
        reverse: If True, sort in descending order. Defaults to False.
        ordered: Legacy parameter for compatibility. Always True.

    Returns:
        A Sort step that can be used in pipelines

    Examples:
        >>> # Sort numbers
        >>> numbers = [3, 1, 4, 1, 5, 9, 2, 6]
        >>> result = await (numbers | sort()).collect()
        >>> # [1, 1, 2, 3, 4, 5, 6, 9]

        >>> # Sort strings by length
        >>> words = ["python", "is", "awesome", "and", "fun"]
        >>> by_length = await (words | sort(key=len)).collect()
        >>> # ["is", "and", "fun", "python", "awesome"]

        >>> # Sort objects by attribute
        >>> users = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        >>> by_age = await (users | sort(key=lambda u: u["age"])).collect()
        >>> # [{"name": "Bob", ...}, {"name": "Alice", ...}]

        >>> # Reverse sort
        >>> descending = await (range(5) | sort(reverse=True)).collect()
        >>> # [4, 3, 2, 1, 0]

    Note:
        This is a stateful operation that loads all items into memory.
        For very large datasets, consider using external sorting or
        streaming approaches.
    """
    return Sort(key=key, reverse=reverse, ordered=ordered)
