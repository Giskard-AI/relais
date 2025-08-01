from typing import AsyncIterator
from relais.base import Step
from relais.stream import (
    T,
    StreamReader,
    StreamWriter,
    StreamItemEvent,
    Index,
    StreamErrorEvent,
)
from relais.processors import StatelessStreamProcessor
from relais.errors import PipelineError


class _AsyncIteratorProcessor(StatelessStreamProcessor[None, T]):
    """Processor that consumes from an async iterator and feeds the output stream."""

    def __init__(
        self,
        input_stream: StreamReader[None],
        output_stream: StreamWriter[T],
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
                if self.output_stream.is_cancelled():
                    break

                await self.output_stream.write(
                    StreamItemEvent(item=item, index=Index(index))
                )
                index += 1

        except Exception as e:
            await self.output_stream.handle_error(
                StreamErrorEvent(
                    PipelineError(str(e), e, self.__class__.__name__, Index(index)),
                    Index(index),
                )
            )

        finally:
            await self.output_stream.complete()

            if self.output_stream.error:
                raise self.output_stream.error

    async def _process_item(self, item):
        # This method is not used since we override process_stream completely
        pass


class AsyncIteratorStep(Step[None, T]):
    """Step that converts an async iterator into a stream."""

    def __init__(self, async_iter: AsyncIterator[T]):
        self.async_iter = async_iter

    def _build_processor(
        self, input_stream: StreamReader[None], output_stream: StreamWriter[T]
    ) -> _AsyncIteratorProcessor[T]:
        return _AsyncIteratorProcessor(input_stream, output_stream, self.async_iter)


def from_async_iterator(async_iter: AsyncIterator[T]) -> AsyncIteratorStep[T]:
    """Create a pipeline step from an async iterator."""
    return AsyncIteratorStep(async_iter)
