import asyncio
from typing import Awaitable, Callable, Iterable

from relais.base import Step, T, U
from relais.stream import StreamReader, StreamWriter, StreamItemEvent
from relais.processors import StatelessStreamProcessor


class _FlatMapProcessor(StatelessStreamProcessor[T, U]):
    """Processor that applies a function that returns iterables and flattens the results.

    This processor applies a transformation function to each item, where the function
    returns an iterable. The results are then flattened into individual items in the
    output stream, with proper indexing to maintain ordering.
    """

    def __init__(
        self,
        input_stream: StreamReader[T],
        output_stream: StreamWriter[U],
        func: Callable[[T], Awaitable[Iterable[U]] | Iterable[U]],
    ):
        """Initialize the flat_map processor.

        Args:
            input_stream: Stream to read items from
            output_stream: Stream to write flattened results to
            func: Function that takes an item and returns an iterable of results
        """
        super().__init__(input_stream, output_stream)
        self.func = func

    async def _process_item(self, item: StreamItemEvent[T]):
        """Apply the function and flatten the results.

        Args:
            item: The indexed item to transform
        """
        results = self.func(item.item)

        if asyncio.iscoroutine(results):
            results = await results

        for i, result in enumerate(results):
            await self.output_stream.write(
                StreamItemEvent(item=result, index=item.index.with_sub_index(i))
            )


class FlatMap(Step[T, U]):
    """Pipeline step that applies a function returning iterables and flattens results.

    The FlatMap step is used for operations where each input item can produce
    multiple output items. It applies a function to each item that returns an
    iterable, then flattens all the results into a single stream.

    This is particularly useful for:
    - Expanding items into multiple results
    - Breaking down complex objects into components
    - One-to-many transformations
    - Parsing operations that yield multiple tokens

    Example:
        >>> # Split strings into words
        >>> sentences = ["hello world", "python rocks"]
        >>> pipeline = sentences | flat_map(lambda s: s.split())
        >>> await pipeline.collect()  # ["hello", "world", "python", "rocks"]

        >>> # Generate number ranges
        >>> limits = [3, 2, 4]
        >>> pipeline = limits | flat_map(range)
        >>> await pipeline.collect()  # [0, 1, 2, 0, 1, 0, 1, 2, 3]

        >>> # Extract nested list items
        >>> nested = [[1, 2], [3, 4, 5], [6]]
        >>> pipeline = nested | flat_map(lambda x: x)
        >>> await pipeline.collect()  # [1, 2, 3, 4, 5, 6]

    Note:
        The function can return any iterable (list, tuple, generator, etc.)
        and can be synchronous or asynchronous.
    """

    def __init__(self, func: Callable[[T], Awaitable[Iterable[U]] | Iterable[U]]):
        """Initialize the FlatMap step.

        Args:
            func: Function that takes an item and returns an iterable of results
        """
        self.func = func

    def _build_processor(
        self, input_stream: StreamReader[T], output_stream: StreamWriter[U]
    ) -> _FlatMapProcessor[T, U]:
        """Build the processor for this flat_map step.

        Args:
            input_stream: Stream to read from
            output_stream: Stream to write to

        Returns:
            A configured flat_map processor
        """
        return _FlatMapProcessor(input_stream, output_stream, self.func)


def flat_map(
    func: Callable[[T], Awaitable[Iterable[U]] | Iterable[U]],
) -> FlatMap[T, U]:
    """Create a flat_map step that applies a function and flattens the results.

    This function creates a flat mapping operation that applies a transformation
    function to each item, where the function returns an iterable of results.
    All results are flattened into a single output stream.

    Args:
        func: Function that takes an item and returns an iterable of results.
              Can be synchronous or asynchronous.

    Returns:
        A FlatMap step that can be used in pipelines

    Examples:
        >>> # Text processing: split sentences into words
        >>> sentences = ["Hello world", "Python is great", "Async pipelines rock"]
        >>> words = await (sentences | flat_map(str.split)).collect()
        >>> # ["Hello", "world", "Python", "is", "great", "Async", "pipelines", "rock"]

        >>> # Number expansion: create ranges
        >>> sizes = [3, 2, 4]
        >>> numbers = await (sizes | flat_map(range)).collect()
        >>> # [0, 1, 2, 0, 1, 0, 1, 2, 3]

        >>> # Data extraction: flatten nested structures
        >>> nested_data = [[1, 2, 3], [4, 5], [6, 7, 8, 9]]
        >>> flat_data = await (nested_data | flat_map(lambda x: x)).collect()
        >>> # [1, 2, 3, 4, 5, 6, 7, 8, 9]

        >>> # File processing: read lines from multiple files
        >>> async def read_file_lines(filename):
        ...     with open(filename) as f:
        ...         return f.readlines()
        >>>
        >>> filenames = ["file1.txt", "file2.txt"]
        >>> all_lines = await (filenames | flat_map(read_file_lines)).collect()

        >>> # Advanced: generate multiple items per input
        >>> def generate_variants(word):
        ...     return [word.upper(), word.lower(), word.title()]
        >>>
        >>> words = ["hello", "world"]
        >>> variants = await (words | flat_map(generate_variants)).collect()
        >>> # ["HELLO", "hello", "Hello", "WORLD", "world", "World"]

    Use Cases:
        - Text processing (tokenization, word splitting)
        - Data flattening (nested lists, hierarchical structures)
        - One-to-many transformations
        - Parsing operations that yield multiple results
        - Expanding compressed or encoded data

    Note:
        The function should return any iterable type. Empty iterables are valid
        and will contribute no items to the output stream.
    """
    return FlatMap(func)
