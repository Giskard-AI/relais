import asyncio
from typing import List

from relais.base import Step
from relais.processors import StatelessStreamProcessor
from relais.stream import Index, StreamItemEvent, StreamReader, StreamWriter, T


class _BatchProcessor(StatelessStreamProcessor[T, List[T]]):
    """Processor that groups items into batches of a specified size.

    This processor collects items as they arrive and groups them into
    lists of the specified size. When a batch is complete, it's emitted
    to the output stream. Any remaining items form a partial batch at the end.

    The processor is thread-safe and handles concurrent item processing.
    """

    def __init__(
        self,
        input_stream: StreamReader[T],
        output_stream: StreamWriter[List[T]],
        size: int,
    ):
        """Initialize the batch processor.

        Args:
            input_stream: Stream to read items from
            output_stream: Stream to write batches to
            worker_group: Worker group for concurrent processing
            size: Maximum size of each batch
        """
        super().__init__(input_stream, output_stream)
        self.size = size
        self.current_batch: List[T] = []
        self.batch_index = 0
        self._lock = asyncio.Lock()

    async def _create_batch(self, batch: List[T]):
        """Create and emit a batch of items.

        Args:
            batch: List of items to emit as a batch
        """
        await self.output_stream.write(
            StreamItemEvent(item=batch, index=Index(self.batch_index))
        )

    async def _process_item(self, item: StreamItemEvent[T]):
        """Process an item by adding it to the current batch.

        When the batch reaches the specified size, it's emitted and
        a new batch is started.

        Args:
            item: The indexed item to add to the current batch
        """
        async with self._lock:
            self.current_batch.append(item.item)

            if len(self.current_batch) >= self.size:
                # Emit completed batch
                await self._create_batch(self.current_batch)
                self.current_batch = []
                self.batch_index += 1

    async def _cleanup(self):
        """Emit any remaining items in the final partial batch.

        This ensures that items that don't fill a complete batch
        are still emitted at the end of processing.
        """
        async with self._lock:
            if self.current_batch:
                await self._create_batch(self.current_batch)


class Batch(Step[T, List[T]]):
    """Pipeline step that groups items into batches of a specified size.

    The Batch step collects items as they flow through and groups them into
    lists of the specified maximum size. This is useful for:

    - Processing items in bulk (e.g., database batch inserts)
    - Rate limiting API calls
    - Memory management with large datasets
    - Grouping for parallel processing

    The last batch may contain fewer items if the total number of items
    is not evenly divisible by the batch size.

    Example:
        >>> # Batch numbers into groups of 3
        >>> pipeline = range(10) | batch(3)
        >>> await pipeline.collect()
        >>> # [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]

        >>> # Batch API requests
        >>> user_ids = range(100)
        >>> batched_requests = user_ids | batch(10) | map(fetch_users_batch)
    """

    def __init__(self, size: int):
        """Initialize the Batch step.

        Args:
            size: Maximum number of items per batch

        Raises:
            ValueError: If size is not positive
        """
        if size <= 0:
            raise ValueError("Batch size must be greater than 0")
        self.size = size

    def _build_processor(
        self, input_stream: StreamReader[T], output_stream: StreamWriter[List[T]]
    ) -> _BatchProcessor[T]:
        """Build the processor for this batch step.

        Args:
            input_stream: Stream to read from
            output_stream: Stream to write to

        Returns:
            A configured batch processor
        """
        return _BatchProcessor(input_stream, output_stream, self.size)
