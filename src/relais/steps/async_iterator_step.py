from typing import AsyncIterable

from relais.base import Step
from relais.errors import PipelineError
from relais.processors import StatelessStreamProcessor
from relais.stream import (
    Index,
    StreamErrorEvent,
    StreamItemEvent,
    StreamReader,
    StreamWriter,
    T,
)


class _AsyncIteratorProcessor(StatelessStreamProcessor[None, T]):
    """Processor that consumes from an async iterator and feeds the output stream."""

    def __init__(
        self,
        input_stream: StreamReader[None],
        output_stream: StreamWriter[T],
        async_iterable: AsyncIterable[T],
    ):
        super().__init__(input_stream, output_stream)
        self.async_iterable = async_iterable

    async def process_stream(self):
        """Consume from async iterator and feed output stream."""
        index = 0
        try:
            async for item in self.async_iterable:
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

    def __init__(self, async_iterable: AsyncIterable[T]):
        self.async_iterable = async_iterable

    def _build_processor(
        self, input_stream: StreamReader[None], output_stream: StreamWriter[T]
    ) -> _AsyncIteratorProcessor[T]:
        return _AsyncIteratorProcessor(input_stream, output_stream, self.async_iterable)
