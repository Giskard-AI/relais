from abc import ABC
from typing import Generic, TypeVar

from relais.base import PipelineError
from relais.index import Index
from relais.stream import StreamErrorEvent, StreamItemEvent, StreamReader, StreamWriter
from relais.tasks import BlockingTaskLimiter, CancellationError, CompatExceptionGroup

T = TypeVar("T")
U = TypeVar("U")


class StreamProcessor(ABC, Generic[T, U]):
    """Base class for all stream processors.

    StreamProcessor defines the interface for processing items from an input
    stream and producing results to an output stream. Subclasses implement
    specific processing logic while the base class handles error propagation
    and stream lifecycle management.

    The processor operates by:
    1. Reading items from input_stream
    2. Processing items according to subclass logic
    3. Writing results to output_stream
    4. Handling errors according to error policy
    5. Propagating cancellation signals

    Attributes:
        input_stream: Stream to read items from
        output_stream: Stream to write results to
    """

    input_stream: StreamReader[T]
    output_stream: StreamWriter[U]

    def __init__(self, input_stream: StreamReader[T], output_stream: StreamWriter[U]):
        """Initialize the processor with input and output streams.

        Args:
            input_stream: Stream to read items from
            output_stream: Stream to write results to
        """
        self.input_stream = input_stream
        self.output_stream = output_stream

    async def process_stream(self):
        """Process items from input stream to output stream.

        This method must be implemented by subclasses to define the specific
        processing logic. It should handle reading from input_stream,
        processing items, and writing to output_stream.

        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError

    async def _cleanup(self):
        """Perform any necessary cleanup after processing.

        Subclasses can override this method to implement custom cleanup
        logic such as closing resources or finalizing state.
        """
        pass


class StatelessStreamProcessor(StreamProcessor[T, U]):
    """Stream processor for operations that don't require state between items.

    StatelessStreamProcessor is designed for operations like map, filter, and
    other transformations that can process each item independently. Items are
    processed concurrently using TaskGroup for maximum parallelism.

    The processor:
    - Processes items as they arrive (no buffering)
    - Creates concurrent tasks for each item
    - Maintains ordering through the Index system
    - Handles errors per item according to error policy

    Example operations: map, filter, async transformations
    """

    def __init__(
        self,
        input_stream: StreamReader[T],
        output_stream: StreamWriter[U],
        max_concurrent_tasks: int = 100,
    ):
        super().__init__(input_stream, output_stream)
        self.max_concurrent_tasks = max_concurrent_tasks

    async def process_stream(self):
        """Process items concurrently as they arrive from the input stream.

        Items are processed immediately upon arrival using concurrent tasks.
        This enables maximum parallelism for stateless operations.

        Raises:
            PipelineError: If processing fails and error policy is FAIL_FAST
        """
        try:
            async with BlockingTaskLimiter(self.max_concurrent_tasks) as tasks:
                async for item in self.input_stream:
                    # Check if we should stop processing
                    if (
                        self.output_stream.is_cancelled()
                        or self.output_stream.is_completed()
                    ):
                        break

                    await tasks.put(self._safe_process_item(item))

        finally:
            await self._cleanup()

            await self.output_stream.complete()

            if self.output_stream.error:
                raise self.output_stream.error

    async def _safe_process_item(self, item: StreamItemEvent[T] | StreamErrorEvent):
        """Process a single item with error handling.

        This wrapper method handles errors according to the stream's error policy,
        allowing subclasses to focus on the core processing logic.
        """
        try:
            async with self.output_stream.cancellation_scope():
                try:
                    if isinstance(item, StreamErrorEvent):
                        await self._process_error(item)
                    else:
                        await self._process_item(item)
                except CancellationError:
                    pass  # This means the item was cancelled
                except PipelineError as e:
                    # This means the error was already handled
                    await self.output_stream.handle_error(
                        StreamErrorEvent(e, item.index)
                    )
                except CompatExceptionGroup as e:
                    for error in e.exceptions:
                        await self.output_stream.handle_error(
                            StreamErrorEvent(
                                PipelineError(
                                    str(error),
                                    error,
                                    self.__class__.__name__,
                                    Index(-1),
                                ),
                                Index(-1),
                            )
                        )
                except Exception as e:
                    await self.output_stream.handle_error(
                        StreamErrorEvent(
                            PipelineError(
                                f"Processing failed in {self.__class__.__name__}: {str(e)}",
                                e,
                                self.__class__.__name__,
                                item.index,
                            ),
                            item.index,
                        )
                    )
        except CancellationError:
            pass  # This means the item was cancelled

    async def _process_item(self, item: StreamItemEvent[T]):
        """Process a single item and write results to output stream.

        Subclasses must implement this method to define their specific
        processing logic. The method should:
        1. Transform the input item
        2. Create output items with appropriate indices
        3. Put results into the output stream

        Args:
            item: The indexed item to process

        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError

    async def _process_error(self, error: StreamErrorEvent):
        """Process an error.

        This method is used to process an error.
        """
        await self.output_stream.handle_error(error)


class StatefulStreamProcessor(StreamProcessor[T, U]):
    """Stream processor for operations that require access to all items.

    StatefulStreamProcessor is designed for operations that need to see all
    input items before producing output, such as sorting, grouping, or
    aggregation operations. It buffers all items in memory before processing.

    The processor:
    - Waits for the entire input stream to complete
    - Loads all items into memory via to_sorted_list()
    - Processes the complete dataset
    - Outputs results in batch

    Example operations: sort, group_by, reduce, distinct

    Warning:
        This processor loads all items into memory, which may not be suitable
        for very large datasets.
    """

    async def process_stream(self):
        """Wait for input stream completion, then process all items.

        This method first collects all items from the input stream into a
        sorted list, then processes them as a batch. This is necessary for
        operations that need access to the complete dataset.

        Raises:
            PipelineError: If processing fails and error policy is FAIL_FAST
        """
        try:
            async with self.output_stream.cancellation_scope():
                try:
                    input_data, errors = await self.input_stream.collect_with_errors()

                    output_data = await self._process_items(input_data)

                    for index, item in enumerate(output_data):
                        await self.output_stream.write(
                            StreamItemEvent(item, Index(index))
                        )

                    for index, error in enumerate(errors):
                        await self.output_stream.handle_error(
                            StreamErrorEvent(error, Index(index + len(output_data)))
                        )
                except CancellationError:
                    pass  # This means the stream was cancelled
                except CompatExceptionGroup as e:
                    for error in e.exceptions:
                        await self.output_stream.handle_error(
                            StreamErrorEvent(
                                PipelineError(
                                    str(error),
                                    error,
                                    self.__class__.__name__,
                                    Index(-1),
                                ),
                                Index(-1),
                            )
                        )
                except Exception as e:
                    # In case of an unexpected error, we can't pinpoint which item caused the error
                    await self.output_stream.handle_error(
                        StreamErrorEvent(
                            PipelineError(
                                f"Processing failed in {self.__class__.__name__}: {str(e)}",
                                e,
                                self.__class__.__name__,
                                Index(-1),
                            ),
                            Index(-1),
                        )
                    )
                finally:
                    await self._cleanup()

                    await self.output_stream.complete()

                    if self.output_stream.error:
                        raise self.output_stream.error
        except CancellationError:
            pass  # This means the stream was cancelled

    async def _process_items(self, items: list[T]) -> list[U]:
        """Process the complete list of items.

        Subclasses must implement this method to define their batch processing
        logic. The method receives all items from the input stream and should
        return the processed results.

        Args:
            items: Complete list of items from the input stream

        Returns:
            List of processed items to output

        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError
