from collections import defaultdict
from typing import Any, Callable, Dict, List

from relais.base import Step
from relais.processors import StatefulStreamProcessor
from relais.stream import StreamReader, StreamWriter, T


class _GroupByProcessor(StatefulStreamProcessor[T, Dict[Any, List[T]]]):
    """Processor that groups items by a key function into a dictionary.

    This processor collects all items and groups them by the result of applying
    a key function to each item. The output is a single dictionary mapping
    keys to lists of items.
    """

    def __init__(
        self,
        input_stream: StreamReader[T],
        output_stream: StreamWriter[Dict[Any, List[T]]],
        key_func: Callable[[T], Any],
    ):
        """Initialize the group_by processor.

        Args:
            input_stream: Stream to read items from
            output_stream: Stream to write the grouped dictionary to
            key_func: Function to extract grouping key from each item
        """
        super().__init__(input_stream, output_stream)
        self.key_func = key_func

    async def _process_items(self, items: List[T]) -> List[Dict[Any, List[T]]]:
        """Group items by the result of the key function.

        Args:
            items: All items from the input stream

        Returns:
            List containing a single dictionary mapping keys to item lists
        """
        groups: Dict[Any, List[T]] = defaultdict(list)

        for item in items:
            key = self.key_func(item)
            groups[key].append(item)

        # Return the groups dictionary as a single-item list
        return [dict(groups)]


class GroupBy(Step[T, Dict[Any, List[T]]]):
    """Pipeline step that groups items by a key function into a dictionary.

    The GroupBy step collects all items and organizes them into groups based on
    the result of applying a key function to each item. The output is a single
    dictionary where keys are the grouping values and values are lists of items
    that share that key.

    This is a stateful operation that requires all items to be collected before
    grouping can occur.

    Example:
        >>> # Group numbers by remainder when divided by 3
        >>> numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        >>> pipeline = numbers | group_by(lambda x: x % 3)
        >>> await pipeline.collect()
        >>> # [{0: [3, 6, 9], 1: [1, 4, 7], 2: [2, 5, 8]}]

        >>> # Group words by length
        >>> words = ["cat", "dog", "bird", "elephant", "ant"]
        >>> pipeline = words | group_by(len)
        >>> await pipeline.collect()
        >>> # [{3: ["cat", "dog", "ant"], 4: ["bird"], 8: ["elephant"]}]

        >>> # Group objects by attribute
        >>> users = [
        ...     {"name": "Alice", "dept": "eng"},
        ...     {"name": "Bob", "dept": "sales"},
        ...     {"name": "Charlie", "dept": "eng"}
        ... ]
        >>> pipeline = users | group_by(lambda u: u["dept"])
        >>> # {"eng": [{"name": "Alice", ...}, {"name": "Charlie", ...}], "sales": [...]}

    Note:
        The result is always a list with a single dictionary. This allows
        consistent chaining with other pipeline operations.
    """

    def __init__(self, key_func: Callable[[T], Any]):
        """Initialize the GroupBy step.

        Args:
            key_func: Function to extract grouping key from each item
        """
        self.key_func = key_func

    def _build_processor(
        self,
        input_stream: StreamReader[T],
        output_stream: StreamWriter[Dict[Any, List[T]]],
    ) -> _GroupByProcessor[T]:
        """Build the processor for this group_by step.

        Args:
            input_stream: Stream to read from
            output_stream: Stream to write to

        Returns:
            A configured group_by processor
        """
        return _GroupByProcessor(input_stream, output_stream, self.key_func)
