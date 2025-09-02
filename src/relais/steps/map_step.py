import asyncio
from typing import Awaitable, Callable, cast

from relais.base import Step, T, U
from relais.processors import StatelessStreamProcessor
from relais.stream import StreamItemEvent, StreamReader, StreamWriter


class _MapProcessor(StatelessStreamProcessor[T, U]):
    """Processor that applies a transformation function to each item.

    This processor handles both sync and async transformation functions,
    automatically detecting coroutines and awaiting them as needed.
    """

    def __init__(
        self,
        input_stream: StreamReader[T],
        output_stream: StreamWriter[U],
        func: Callable[[T], Awaitable[U] | U],
    ):
        """Initialize the map processor.

        Args:
            input_stream: Stream to read items from
            output_stream: Stream to write transformed items to
            func: Transformation function (sync or async)
        """
        super().__init__(input_stream, output_stream)
        self.func = func

    async def _process_item(self, item: StreamItemEvent[T]):
        """Apply the transformation function to an item.

        Args:
            item: The indexed item to transform
        """
        result = self.func(item.item)

        if asyncio.iscoroutine(result):
            result = await result

        await self.output_stream.write(
            StreamItemEvent(item=cast(U, result), index=item.index)
        )


class Map(Step[T, U]):
    """Pipeline step that transforms each item using a function.

    The Map step applies a transformation function to each item in the stream.
    This is one of the most fundamental pipeline operations and supports both
    synchronous and asynchronous transformation functions.

    The transformation happens concurrently for maximum performance while
    maintaining the original ordering of items.

    Example:
        >>> # Synchronous transformation
        >>> pipeline = range(5) | map(lambda x: x * 2)
        >>> await pipeline.collect()  # [0, 2, 4, 6, 8]

        >>> # Asynchronous transformation
        >>> async def fetch_data(id):
        ...     await asyncio.sleep(0.1)
        ...     return f"data_{id}"
        >>> pipeline = range(3) | map(fetch_data)
        >>> await pipeline.collect()  # ["data_0", "data_1", "data_2"]
    """

    def __init__(self, func: Callable[[T], Awaitable[U] | U]):
        """Initialize the Map step.

        Args:
            func: Function to apply to each item. Can be sync or async.
        """
        super().__init__()
        self.func = func

    def _build_processor(
        self, input_stream: StreamReader[T], output_stream: StreamWriter[U]
    ) -> _MapProcessor[T, U]:
        """Build the processor for this map step.

        Args:
            input_stream: Stream to read from
            output_stream: Stream to write to

        Returns:
            A configured map processor
        """
        return _MapProcessor(input_stream, output_stream, self.func)
