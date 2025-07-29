from typing import List
import asyncio

from ..base import Step, Stream, StatelessStreamProcessor, Indexed, T

class _BatchProcessor(StatelessStreamProcessor[T, List[T]]):
    """Batch processor."""

    def __init__(self, input_stream: Stream[T], output_stream: Stream[List[T]], size: int):
        super().__init__(input_stream, output_stream)
        self.size = size
        self.current_batch: List[T] = []
        self.batch_index = 0
        self._lock = asyncio.Lock()

    async def _create_batch(self, batch: List[T]):
        """Create a batch of items."""
        await self.output_stream.put(Indexed(self.batch_index, batch))

    async def _process_item(self, item: Indexed[T]):
        """Process an item and add to current batch."""
        async with self._lock:
            self.current_batch.append(item.item)
        
            if len(self.current_batch) >= self.size:
                # Emit completed batch
                await self._create_batch(self.current_batch)
                self.current_batch = []
                self.batch_index += 1

    async def _cleanup(self):
        """Emit any remaining items in the final batch."""
        async with self._lock:
            if self.current_batch:
                await self._create_batch(self.current_batch)

class Batch(Step[T, List[T]]):
    """Batch operation to group items into chunks of specified size."""

    def __init__(self, size: int):
        if size <= 0:
            raise ValueError("Batch size must be greater than 0")
        self.size = size

    def _build_processor(self, input_stream: Stream[T], output_stream: Stream[List[T]]) -> _BatchProcessor[T]:
        return _BatchProcessor(input_stream, output_stream, self.size)

def batch(size: int) -> Batch[T]:
    """Batch operation to group items into chunks of specified size."""
    return Batch(size)