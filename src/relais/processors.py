from typing import Generic, TypeVar, List
from abc import ABC
from relais.stream import Stream, Indexed, Index
from relais.base import ErrorPolicy, PipelineError, ErrorEvent
from relais.tasks import CompatTaskGroup, CompatExceptionGroup, TaskGroupTasks

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

    input_stream: Stream[T]
    output_stream: Stream[U]

    def __init__(self, input_stream: Stream[T], output_stream: Stream[U]):
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

    async def process_stream(self):
        """Process items concurrently as they arrive from the input stream.

        Items are processed immediately upon arrival using concurrent tasks.
        This enables maximum parallelism for stateless operations.

        Raises:
            PipelineError: If processing fails and error policy is FAIL_FAST
        """
        original_error = None
        try:
            async with CompatTaskGroup() as tg:
                tasks = TaskGroupTasks()
                async for item in self.input_stream:
                    # Check if we should stop processing
                    if self.input_stream.is_consumer_cancelled():
                        tasks.cancel()
                        break

                    tasks.append(tg.create_task(self._safe_process_item(item, tasks)))

        except Exception as e:
            # Extract the original error from TaskGroup exceptions
            if isinstance(e, CompatExceptionGroup):
                original_error = e.exceptions[0]
            else:
                original_error = e

            # For fail-fast: re-raise with preserved original error
            if self.output_stream.error_policy == ErrorPolicy.FAIL_FAST:
                raise PipelineError(
                    f"Processing failed in {self.__class__.__name__}",
                    original_error,
                    self.__class__.__name__,
                )
            # For other policies, errors are already handled in _safe_process_item
        finally:
            # Propagate cancellation
            if self.input_stream.is_consumer_cancelled():
                self.output_stream.stop_consumer(self.input_stream.error)
            elif self.output_stream.is_producer_cancelled():
                self.input_stream.stop_producer()

            await self._cleanup()

            # Only end output stream if we haven't been cancelled
            if not self.output_stream.is_consumer_cancelled():
                await self.output_stream.end()

    async def _safe_process_item(self, item: Indexed[T], tasks: TaskGroupTasks = None):
        """Process a single item with error handling.

        This wrapper method handles errors according to the stream's error policy,
        allowing subclasses to focus on the core processing logic.

        Args:
            item: The indexed item to process
            task_group: The TaskGroup to cancel if FAIL_FAST error occurs
        """
        try:
            await self._process_item(item)
        except Exception as e:
            # For FAIL_FAST, cancel all other tasks immediately
            if self.output_stream.error_policy == ErrorPolicy.FAIL_FAST and tasks:
                tasks.cancel()

            await self.output_stream.handle_error(
                e, item.index, self.__class__.__name__
            )

    async def _process_item(self, item: Indexed[T]):
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
            input_data = await self.input_stream.to_sorted_list()
            if self.input_stream.is_consumer_cancelled():
                return

            output_data = await self._process_items(input_data)

            if not self.output_stream.is_consumer_cancelled():
                await self.output_stream.put_all(output_data)

        except Exception as e:
            if self.output_stream.error_policy == ErrorPolicy.FAIL_FAST:
                self.output_stream.stop(e)
                raise PipelineError(
                    f"Processing failed in {self.__class__.__name__}",
                    e,
                    self.__class__.__name__,
                )
            elif self.output_stream.error_policy == ErrorPolicy.COLLECT:
                # For stateful processors, we can't pinpoint which item caused the error
                error_event = ErrorEvent(e, Index(-1), self.__class__.__name__)
                self.output_stream.errors.append(error_event)
                if not self.output_stream.is_consumer_cancelled():
                    await self.output_stream.put_all(
                        []
                    )  # Output empty results when ignoring stateful processing errors
            # For IGNORE policy, we also output empty results and continue
            elif self.output_stream.error_policy == ErrorPolicy.IGNORE:
                if not self.output_stream.is_consumer_cancelled():
                    await self.output_stream.put_all(
                        []
                    )  # Output empty results when ignoring stateful processing errors
        finally:
            # Propagate cancellation
            if self.input_stream.is_consumer_cancelled():
                self.output_stream.stop_consumer(self.input_stream.error)
            elif self.output_stream.is_producer_cancelled():
                self.input_stream.stop_producer()

            await self._cleanup()

    async def _process_items(self, items: List[T]) -> List[U]:
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
