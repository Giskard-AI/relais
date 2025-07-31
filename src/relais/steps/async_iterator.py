from typing import AsyncIterator
from relais.base import Step
from relais.stream import T, Indexed, Stream
from relais.processors import StatelessStreamProcessor


class _AsyncIteratorProcessor(StatelessStreamProcessor[None, T]):
    """Processor that consumes from an async iterator and feeds the output stream."""

    def __init__(
        self,
        input_stream: Stream[None],
        output_stream: Stream[T],
        async_iter: AsyncIterator[T],
    ):
        super().__init__(input_stream, output_stream)
        self.async_iter = async_iter

    async def process_stream(self):
        """Consume from async iterator and feed output stream."""
        try:
            index = 0
            async for item in self.async_iter:
                # Check if downstream wants us to stop producing
                if self.output_stream.is_producer_cancelled():
                    break

                indexed_item = Indexed(index=index, item=item)
                await self.output_stream.put(indexed_item)
                index += 1

        except Exception as e:
            await self.output_stream.handle_error(
                e, Indexed(-1, None).index, self.__class__.__name__
            )
        finally:
            await self._cleanup()
            if not self.output_stream.is_consumer_cancelled():
                await self.output_stream.end()

    async def _process_item(self, item):
        # This method is not used since we override process_stream completely
        pass


class AsyncIteratorStep(Step[None, T]):
    """Step that converts an async iterator into a stream."""

    def __init__(self, async_iter: AsyncIterator[T]):
        self.async_iter = async_iter

    def _build_processor(
        self, input_stream: Stream[None], output_stream: Stream[T]
    ) -> _AsyncIteratorProcessor[T]:
        return _AsyncIteratorProcessor(input_stream, output_stream, self.async_iter)


def from_async_iterator(async_iter: AsyncIterator[T]) -> AsyncIteratorStep[T]:
    """Create a pipeline step from an async iterator."""
    return AsyncIteratorStep(async_iter)
