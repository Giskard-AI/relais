import asyncio
import sys
from typing import Any, AsyncIterator, Generic, List, TypeVar, Union, Iterable, Callable, Optional
from abc import ABC
from enum import Enum

# TaskGroup is available in Python 3.11+, use fallback for older versions
if sys.version_info >= (3, 11):
    from asyncio import TaskGroup
else:
    # Fallback TaskGroup implementation for older Python versions
    class TaskGroup:
        def __init__(self):
            self._tasks = []
        
        async def __aenter__(self):
            return self
        
        async def __aexit__(self, exc_type, exc_val, exc_tb):
            if self._tasks:
                await asyncio.gather(*self._tasks, return_exceptions=True)
        
        def create_task(self, coro):
            task = asyncio.create_task(coro)
            self._tasks.append(task)
            return task

# Type variables
T = TypeVar('T')
U = TypeVar('U') 
V = TypeVar('V')

class ErrorPolicy(Enum):
    """Error handling policies for pipeline execution."""
    FAIL_FAST = "fail_fast"      # Stop entire pipeline on first error
    IGNORE = "ignore"            # Skip failed items, continue processing
    COLLECT = "collect"          # Collect errors, return at end

class PipelineError(Exception):
    """Exception raised when pipeline execution fails."""
    def __init__(self, message: str, original_error: Exception, step_name: Optional[str] = None):
        self.original_error = original_error
        self.step_name = step_name
        super().__init__(f"{message}: {original_error}")

class ErrorEvent:
    """Error event for collecting processing errors."""
    def __init__(self, error: Exception, item_index: 'Index', step_name: str):
        self.error = error
        self.item_index = item_index
        self.step_name = step_name


class Index():
    """Index of an item in a stream for maintaining order during parallel processing.
    
    The Index class is used to track the position of items as they flow through
    the pipeline, enabling proper ordering of results even when items are processed
    concurrently across multiple steps.
    
    Attributes:
        index: The primary index position of the item
        sub_index: Optional nested index for operations that expand items (like flat_map)
        
    Example:
        >>> idx1 = Index(0)  # First item
        >>> idx2 = Index(1, Index(2))  # Second item with sub-index 2
        >>> idx1 < idx2  # True - maintains ordering
    """
    index: int
    sub_index: Optional['Index'] = None

    def __init__(self, index: int, sub_index: Optional['Index'] = None):
        """Initialize an Index.
        
        Args:
            index: The primary index position
            sub_index: Optional nested index for expanded items
        """
        self.index = index
        self.sub_index = sub_index

    def __lt__(self, other: 'Index') -> bool:
        # Convert to tuples where None becomes a sentinel value that sorts first
      self_tuple = (self.index, self.sub_index or Index(-1, None) if self.sub_index else None)
      other_tuple = (other.index, other.sub_index or Index(-1, None) if other.sub_index else
  None)
      return self_tuple < other_tuple
    
class Indexed(Generic[T]):
    """Wrapper for items flowing through streams with ordering information.
    
    This class pairs each item with its index to maintain ordering during
    parallel processing. Items can be processed concurrently while preserving
    their original sequence when results are collected.
    
    Attributes:
        index: The Index object tracking this item's position
        item: The actual data item
        
    Example:
        >>> indexed = Indexed(0, "hello")
        >>> indexed.index.index  # 0
        >>> indexed.item  # "hello"
    """
    index: Index
    item: T

    def __init__(self, index: Index | int, item: T):
        """Initialize an Indexed item.
        
        Args:
            index: Either an Index object or int (will be converted to Index)
            item: The data item to wrap
        """
        if isinstance(index, int):
            self.index = Index(index=index, sub_index=None)
        else:
            self.index = index

        self.item = item

class EndEvent:
    """Sentinel object to signal the end of a stream.
    
    This is used internally by the Stream class to indicate that no more
    items will be produced. When a consumer receives an EndEvent, it knows
    the stream has been completed.
    """
    pass

class Stream(Generic[T]):
    """Async queue-based stream for pipeline communication.
    
    Stream provides the core communication mechanism between pipeline steps,
    using async queues to enable concurrent processing while maintaining ordering.
    Each stream supports error handling policies and graceful cancellation.
    
    The stream operates with a producer-consumer model:
    - Producers put Indexed[T] items into the stream
    - Consumers iterate over items asynchronously
    - EndEvent signals completion
    
    Stream States:
    - fed: Producer has started writing items
    - ended: Producer has signaled completion (EndEvent sent)
    - red: Consumer has started reading items  
    - consumed: Consumer has received EndEvent and finished
    
    Error Handling:
    - FAIL_FAST: Stop entire pipeline on first error
    - IGNORE: Skip failed items, continue processing
    - COLLECT: Collect errors, return at end
    
    Example:
        >>> stream = Stream[int]()
        >>> await stream.put(Indexed(0, 42))
        >>> await stream.end()
        >>> async for item in stream:
        ...     print(item.item)  # 42
    """
    
    def __init__(self, error_policy: ErrorPolicy = ErrorPolicy.FAIL_FAST):
        """Initialize a new Stream.
        
        Args:
            error_policy: How to handle errors during processing
        """
        self.queue = asyncio.Queue()
        self.ended = False # True if the stream has been ended by the producer
        self.fed = False # True if the producer has started feeding the stream
        self.red = False # True if the consumer has started reading the stream
        self.consumed = False # True if the stream has been consumed by the consumer
        self._read_lock = asyncio.Lock() # Lock to prevent concurrent reads
        self._write_lock = asyncio.Lock() # Lock to prevent concurrent writes
        
        # Stream-local cancellation
        self._producer_cancelled = asyncio.Event()  # Stop producing to this stream
        self._consumer_cancelled = asyncio.Event()  # Stop consuming from this stream
        self._error: Optional[Exception] = None
        
        self.error_policy = error_policy
        self.errors: List[ErrorEvent] = []

    def stop_producer(self):
        """Signal upstream to stop producing to this stream."""
        self._producer_cancelled.set()
    
    def stop_consumer(self, error: Optional[Exception] = None):
        """Signal this stream's consumer has stopped."""
        if error:
            self._error = error
        self._consumer_cancelled.set()

    def stop(self, error: Optional[Exception] = None):
        """Signal this stream's producer and consumer have stopped."""
        self.stop_producer()
        self.stop_consumer(error)
    
    def is_producer_cancelled(self) -> bool:
        """Check if upstream should stop producing."""
        return self._producer_cancelled.is_set()
    
    def is_consumer_cancelled(self) -> bool:
        """Check if consumer has stopped."""
        return self._consumer_cancelled.is_set()
    
    @property
    def error(self) -> Optional[Exception]:
        return self._error

    @classmethod
    async def from_list(cls, data: List[T]) -> 'Stream[T]':
        """Create a stream from a list of items.
        
        Args:
            data: List of items to put into the stream
            
        Returns:
            A new Stream containing all items from the list
        """
        return await cls.from_iterable(data)

    @classmethod
    async def from_iterable(cls, data: Iterable[T], error_policy: ErrorPolicy = ErrorPolicy.FAIL_FAST) -> 'Stream[T]':
        """Create a stream from any iterable.
        
        Args:
            data: Iterable of items to put into the stream
            error_policy: Error handling policy for the stream
            
        Returns:
            A new Stream containing all items from the iterable
        """
        stream = cls(error_policy=error_policy)
        await stream.put_all(data)
        return stream
    
    async def put_all(self, data: Iterable[T]):
        """Put all items into the stream and end it.
        
        This is a convenience method for bulk loading a stream with data.
        After all items are added, the stream is automatically ended.
        
        Args:
            data: Iterable of items to add to the stream
            
        Raises:
            ValueError: If the stream has already been fed
        """
        async with self._write_lock:
            if self.fed:
                raise ValueError("Stream already fed")

            for index, item in enumerate(data):
                await self._put(Indexed(index=index, item=item))
            await self._end()

    async def put(self, item: Indexed[T]):
        """Put a single indexed item into the stream.
        
        Args:
            item: The indexed item to add to the stream
            
        Raises:
            ValueError: If the stream has already ended
        """
        async with self._write_lock:
            await self._put(item)

    async def _put(self, item: Indexed[T]):
        if self.ended:
            raise ValueError("Stream already ended")
        
        # Check if producer should stop
        if self.is_producer_cancelled():
            return  # Silently ignore new items if producer is cancelled

        self.fed = True
        await self.queue.put(item)

    async def end(self):
        """Signal that no more items will be added to the stream.
        
        This sends an EndEvent to signal consumers that the stream is complete.
        
        Raises:
            ValueError: If the stream has already ended
        """
        async with self._write_lock:
            await self._end()

    async def _end(self):
        if self.ended:
            raise ValueError("Stream already ended")
        
        self.fed = True
        self.ended = True
        await self.queue.put(EndEvent())


    async def to_sorted_list(self) -> List[T]:
        """Convert the entire stream to a sorted list.
        
        This method consumes the entire stream, sorts items by their index,
        and returns the unwrapped items as a list. This is useful for stateful
        operations that need to process all items at once.
        
        Returns:
            List of items sorted by their original index
            
        Raises:
            ValueError: If the stream has already been read
            
        Note:
            This method loads all items into memory, so it may not be suitable
            for very large streams.
        """
        async with self._read_lock:
            if self.red:
                raise ValueError("Stream has already been read")

            items = []
            # Do not use async for loop here because it will create a deadlock
            while True:
                try:
                    item = await self._next()
                    items.append(item)
                except StopAsyncIteration:
                    break

            sorted_items = sorted(items, key=lambda x: x.index)
            return [item.item for item in sorted_items]

    def __aiter__(self) -> AsyncIterator[Indexed[T]]:
        return self

    async def __anext__(self) -> Indexed[T]:
        async with self._read_lock:
            return await self._next()

    async def _next(self) -> Indexed[T]:
        if self.consumed or self.is_consumer_cancelled():
            raise StopAsyncIteration

        self.red = True

        # Check for cancellation with timeout to allow responsive cancellation
        while True:
            try:
                item = await asyncio.wait_for(self.queue.get(), timeout=0.1)
                break
            except asyncio.TimeoutError:
                if self.is_consumer_cancelled():
                    raise StopAsyncIteration
                continue
            
        if isinstance(item, EndEvent):
            self.consumed = True
            raise StopAsyncIteration

        return item
    
    async def handle_error(self, error: Exception, item_index: Index, step_name: str):
        """Handle an error according to the stream's error policy.
        
        Args:
            error: The exception that occurred
            item_index: Index of the item being processed when error occurred
            step_name: Name of the processing step where error occurred
            
        Raises:
            Exception: Re-raises the original error if policy is FAIL_FAST
        """
        error_event = ErrorEvent(error, item_index, step_name)
        
        if self.error_policy == ErrorPolicy.FAIL_FAST:
            self.stop(error) # Stop the stream and raise the error
            raise error
        elif self.error_policy == ErrorPolicy.COLLECT:
            self.errors.append(error_event)
        # IGNORE policy: do nothing, just drop the error
    
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
        try:
            async with TaskGroup() as tg:
                async for item in self.input_stream:
                    # Check if we should stop processing
                    if self.input_stream.is_consumer_cancelled():
                        await tg.cancel_all()
                        break

                    tg.create_task(self._safe_process_item(item))
                    
        except Exception as e:
            # For fail-fast: cancellation and re-raise happens in _safe_process_item
            if self.output_stream.error_policy == ErrorPolicy.FAIL_FAST:
                raise PipelineError(f"Processing failed in {self.__class__.__name__}", e, self.__class__.__name__)
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
    
    async def _safe_process_item(self, item: Indexed[T]):
        """Process a single item with error handling.
        
        This wrapper method handles errors according to the stream's error policy,
        allowing subclasses to focus on the core processing logic.
        
        Args:
            item: The indexed item to process
        """
        try:
            await self._process_item(item)
        except Exception as e:
            await self.output_stream.handle_error(e, item.index, self.__class__.__name__)
    
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
                raise PipelineError(f"Processing failed in {self.__class__.__name__}", e, self.__class__.__name__)
            elif self.output_stream.error_policy == ErrorPolicy.COLLECT:
                # For stateful processors, we can't pinpoint which item caused the error
                error_event = ErrorEvent(e, Index(-1), self.__class__.__name__)
                self.output_stream.errors.append(error_event)
                if not self.output_stream.is_consumer_cancelled():
                    await self.output_stream.put_all([])  # Output empty results when ignoring stateful processing errors
            # For IGNORE policy, we also output empty results and continue
            elif self.output_stream.error_policy == ErrorPolicy.IGNORE:
                if not self.output_stream.is_consumer_cancelled():
                    await self.output_stream.put_all([])  # Output empty results when ignoring stateful processing errors
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

class WithPipeline(ABC, Generic[T, U]):
    """Abstract base for objects that can be chained in pipelines.
    
    WithPipeline defines the interface for objects that support the pipe
    operator (|) for chaining operations together. This includes both
    individual steps and complete pipelines.
    
    The class supports two chaining patterns:
    1. step | step  -> Pipeline (forward chaining)
    2. data | step  -> Pipeline (data binding)
    """

    def __or__(self, other: 'WithPipeline[U, V]') -> 'Pipeline[T, V]':
        """Chain this object with another using | operator.
        
        Args:
            other: The object to chain after this one
            
        Returns:
            A new Pipeline containing both objects
        """
        return self.then(other)
    
    def then(self, other: 'WithPipeline[U, V]') -> 'Pipeline[T, V]':
        """Chain this object with another sequentially.
        
        Args:
            other: The object to chain after this one
            
        Returns:
            A new Pipeline containing both objects
            
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError
    
    def __ror__(self, other: Union[T, List[T], Iterable[T]]) -> 'Pipeline[T, U]':
        """Support data | step syntax (reverse pipe operator).
        
        Args:
            other: The data to pipe into this object
            
        Returns:
            A new Pipeline with the data as input
        """
        return self.with_input(other)
    
    def with_input(self, data: Union[T, List[T], Iterable[T]]) -> 'Pipeline[T, U]':
        """Create a pipeline with this object and the given input data.
        
        Args:
            data: The input data for the pipeline
            
        Returns:
            A new Pipeline with the specified input
            
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError

class Step(WithPipeline[T, U]):
    """Base class for individual pipeline steps.
    
    Step represents a single processing operation that can be chained with other
    steps to form pipelines. Each step defines how to transform input items of
    type T into output items of type U.
    
    Steps are the building blocks of pipelines and can be:
    - Chained together: step1 | step2 | step3
    - Applied to data: data | step
    - Configured with error policies
    
    Subclasses must implement _build_processor to define their processing logic.
    """
    
    def pipe(self, stream_processor: StreamProcessor[Any, T]) -> StreamProcessor[T, U]:
        """Connect this step to an existing processor's output.
        
        Args:
            stream_processor: The processor whose output becomes this step's input
            
        Returns:
            A new processor for this step
        """
        return self.from_stream(stream_processor.output_stream)
    
    def from_stream(self, input_stream: Stream[T]) -> StreamProcessor[T, U]:
        """Create a processor for this step from an input stream.
        
        Args:
            input_stream: The stream to process
            
        Returns:
            A processor that will execute this step's logic
        """
        output_stream = Stream[U](input_stream.error_policy)
        return self._build_processor(input_stream, output_stream)

    def _build_processor(self, input_stream: Stream[T], output_stream: Stream[U]) -> StreamProcessor[T, U]:
        """Build the processor that implements this step's logic.
        
        Args:
            input_stream: Stream to read input from
            output_stream: Stream to write output to
            
        Returns:
            A processor that implements this step
            
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError
    
    def then(self, other: WithPipeline[T, U]) -> 'Pipeline[T, V]':
        """Chain this step with another step or pipeline.
        
        Args:
            other: The step or pipeline to execute after this one
            
        Returns:
            A new Pipeline containing both steps/all steps
        """
        if isinstance(other, Pipeline):
            # If other is a pipeline, create a new pipeline with this step + all other steps
            return Pipeline([self] + other.steps, error_policy=other.error_policy)
        else:
            # If other is a single step, create a pipeline with both steps
            return Pipeline([self, other])
    
    def with_input(self, data: Union[T, List[T], Iterable[T]]) -> 'Pipeline[T, U]':
        """Create a pipeline with this step and input data.
        
        Args:
            data: The input data for the pipeline
            
        Returns:
            A new Pipeline with this step and the specified input
        """
        return Pipeline([self], input_data=data)
    
    def with_error_policy(self, error_policy: ErrorPolicy) -> 'Pipeline[T, U]':
        """Create a pipeline with this step and a specific error policy.
        
        Args:
            error_policy: How to handle errors during processing
            
        Returns:
            A new Pipeline with this step and the specified error policy
        """
        return Pipeline([self], error_policy=error_policy)
    
class Pipeline(Step[T, U]):
    """A pipeline composed of multiple processing steps.
    
    Pipeline represents a sequence of processing steps that can be executed
    together to transform input data. Pipelines support:
    
    - Sequential processing: Each step processes the output of the previous step
    - Parallel execution: Steps run concurrently using async streams
    - Error handling: Configurable policies for handling failures
    - Composition: Pipelines can be combined with other steps or pipelines
    
    Usage patterns:
    1. Build from steps: Pipeline([step1, step2, step3])
    2. Chain with |: step1 | step2 | step3
    3. Apply to data: data | pipeline
    
    Example:
        >>> pipeline = range(10) | map(lambda x: x * 2) | filter(lambda x: x > 5)
        >>> result = await pipeline.collect()
        [6, 8, 10, 12, 14, 16, 18]
    
    Attributes:
        steps: List of processing steps to execute
        input_data: Optional input data for the pipeline
        error_policy: How to handle processing errors
    """

    steps: List[Step[Any, Any]]

    def __init__(self, steps: List[Step[Any, Any]], input_data: Iterable[T] | None = None, error_policy: ErrorPolicy = ErrorPolicy.FAIL_FAST):
        """Initialize a new Pipeline.
        
        Args:
            steps: List of steps to execute in sequence
            input_data: Optional input data for the pipeline
            error_policy: How to handle errors during processing
        """
        self.steps = steps
        self.input_data = input_data
        self.error_policy = error_policy

    async def _build_processors(self, input_stream: Stream[T]) -> List[StreamProcessor[Any, Any]]:
        """Build processors for each step in the pipeline.
        
        This method creates a chain of processors where each processor's
        output becomes the next processor's input, enabling data to flow
        through the pipeline steps.
        
        Args:
            input_stream: The initial input stream for the pipeline
            
        Returns:
            List of processors, one for each step
        """
        processors = []
        for step in self.steps:
            if len(processors) == 0:
                processor = step.from_stream(input_stream)
            else:
                processor = step.pipe(processors[-1])
            processors.append(processor)
            
        return processors
    
    async def _get_input_stream(self, input_data: Union[Stream[T], Iterable[T]] | None) -> Stream[T]:
        """Convert input data to a Stream for pipeline processing.
        
        This method handles different types of input data and converts them
        to the Stream format required by the pipeline. It supports:
        - Existing Stream objects (reused with updated error policy)
        - Async iterators (wrapped with AsyncIteratorStep)
        - Regular iterables (converted to Stream)
        
        Args:
            input_data: Input data to convert to a stream
            
        Returns:
            A Stream ready for pipeline processing
            
        Raises:
            ValueError: If no input is provided or input is provided twice
        """ 
        data_to_process = input_data if input_data is not None else self.input_data
        
        if data_to_process is None:
            raise ValueError("No input provided")
        
        if input_data is not None and self.input_data is not None:
            raise ValueError("Input provided twice")

        # Check if it's actually a Stream object
        if isinstance(data_to_process, Stream):
            # Update the input stream's error policy to match pipeline
            data_to_process.error_policy = self.error_policy
            return data_to_process
        
        # Check if it's an async iterator
        elif hasattr(data_to_process, '__aiter__'):
            # TODO: make this cleaner
            # Use AsyncIteratorStep to handle async iteration lazily
            from .steps.async_iterator import AsyncIteratorStep
            async_step = AsyncIteratorStep(data_to_process)
            
            # Create a dummy input stream (empty) 
            dummy_input = Stream[None](self.error_policy)
            await dummy_input._end()  # End it immediately since async iterator step doesn't need input
            
            # Create the processor and get its output stream
            processor = async_step.from_stream(dummy_input)
            
            # We need to start the processor to begin feeding the stream
            asyncio.create_task(processor.process_stream())
            
            return processor.output_stream
        else:
            # It's a regular sync iterable
            return await Stream.from_iterable(data_to_process, self.error_policy)

    async def run(self, input_data: Union[Stream[T], Iterable[T]] | None = None) -> Stream[U]:
        """Execute the pipeline and return the output stream.
        
        This method starts all processors concurrently and returns the final
        output stream. The stream can then be consumed using iteration or
        collected into a list.
        
        Args:
            input_data: Optional input data (overrides constructor input_data)
            
        Returns:
            The output stream from the final pipeline step
            
        Raises:
            PipelineError: If execution fails and error policy is FAIL_FAST
            ValueError: If no input data is provided
        """
        input_stream = await self._get_input_stream(input_data)

        processors = await self._build_processors(input_stream)
        
        if len(processors) == 0:
            return input_stream

        try:
            async with TaskGroup() as tg:
                for processor in processors:
                    tg.create_task(processor.process_stream())

        except Exception as e:
            if self.error_policy == ErrorPolicy.FAIL_FAST:
                raise PipelineError(f"Pipeline execution failed", e)
            # For other policies, errors are handled within processors
            
        return processors[-1].output_stream
    
    async def collect(self, input_data: Union[Stream[T], Iterable[T]] | None = None) -> List[U]:
        """Execute the pipeline and collect all results into a list.
        
        This is a convenience method that runs the pipeline and collects
        all output items into a sorted list. Items are sorted by their
        original index to maintain input ordering.
        
        Args:
            input_data: Optional input data (overrides constructor input_data)
            
        Returns:
            List containing all pipeline results in original order
            
        Raises:
            PipelineError: If execution fails and error policy is FAIL_FAST
            ValueError: If no input data is provided
        """
        output_stream = await self.run(input_data)
        return await output_stream.to_sorted_list()
    
    async def stream(self, input_data: Union[Stream[T], Iterable[T]] | None = None) -> AsyncIterator[U]:
        """Execute the pipeline and stream results as they become available.
        
        This method starts the pipeline and yields results as soon as they're
        produced, without waiting for the entire pipeline to complete. This
        is useful for processing large datasets or real-time data.
        
        Args:
            input_data: Optional input data (overrides constructor input_data)
            
        Yields:
            Pipeline results as they become available
            
        Raises:
            PipelineError: If execution fails and error policy is FAIL_FAST
            ValueError: If no input data is provided
        """
        input_stream = await self._get_input_stream(input_data)

        processors = await self._build_processors(input_stream)

        async with TaskGroup() as tg:
            for processor in processors:
                tg.create_task(processor.process_stream())

            async for item in processors[-1].output_stream:
                yield item.item
    
    def then(self, other: WithPipeline[T, U]) -> 'Pipeline[T, V]':
        """Chain this pipeline with another step or pipeline."""
        if isinstance(other, Pipeline):
            # If other is a pipeline, merge all steps together
            merged_steps = self.steps + other.steps
            # Use the error policy from the first pipeline unless the second has a different one
            merged_error_policy = other.error_policy if other.error_policy != ErrorPolicy.FAIL_FAST else self.error_policy
            return Pipeline(merged_steps, input_data=self.input_data, error_policy=merged_error_policy)
        else:
            # If other is a single step, add it to our steps
            return Pipeline(self.steps + [other], input_data=self.input_data, error_policy=self.error_policy)
    
    def with_input(self, data: Union[T, List[T], Iterable[T]]) -> 'Pipeline[T, U]':
        """Support data | step syntax."""
        if self.input_data is not None:
            raise ValueError("Input provided twice")
        
        return Pipeline(self.steps, input_data=data, error_policy=self.error_policy)
    
    def with_error_policy(self, error_policy: ErrorPolicy) -> 'Pipeline[T, U]':
        """Set error policy for this pipeline."""
        return Pipeline(self.steps, input_data=self.input_data, error_policy=error_policy)
