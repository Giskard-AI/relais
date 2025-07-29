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


# Helper class to track index of items in a stream
class Index():
    """Index of an item in a stream."""
    index: int
    sub_index: Optional['Index'] = None

    def __init__(self, index: int, sub_index: Optional['Index'] = None):
          self.index = index
          self.sub_index = sub_index

    def __lt__(self, other: 'Index') -> bool:
        # Convert to tuples where None becomes a sentinel value that sorts first
      self_tuple = (self.index, self.sub_index or Index(-1, None) if self.sub_index else None)
      other_tuple = (other.index, other.sub_index or Index(-1, None) if other.sub_index else
  None)
      return self_tuple < other_tuple
    
class Indexed(Generic[T]):
    """Indexed item in a stream."""
    index: Index
    item: T

    def __init__(self, index: Index | int, item: T):
        if isinstance(index, int):
            self.index = Index(index=index, sub_index=None)
        else:
            self.index = index

        self.item = item

class EndEvent:
    """Sentinel to mark end of stream."""
    pass

class Stream(Generic[T]):
    """Async queue-based stream for pipeline communication."""
    
    def __init__(self, error_policy: ErrorPolicy = ErrorPolicy.FAIL_FAST):
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
        return await cls.from_iterable(data)

    @classmethod
    async def from_iterable(cls, data: Iterable[T], error_policy: ErrorPolicy = ErrorPolicy.FAIL_FAST) -> 'Stream[T]':
        stream = cls(error_policy=error_policy)
        await stream.put_all(data)
        return stream
    
    async def put_all(self, data: Iterable[T]):
        """Put all items into the stream. End the stream after all items are put."""
        async with self._write_lock:
            if self.fed:
                raise ValueError("Stream already fed")

            for index, item in enumerate(data):
                await self._put(Indexed(index=index, item=item))
            await self._end()

    async def put(self, item: Indexed[T]):
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
        async with self._write_lock:
            await self._end()

    async def _end(self):
        if self.ended:
            raise ValueError("Stream already ended")
        
        self.fed = True
        self.ended = True
        await self.queue.put(EndEvent())


    async def to_sorted_list(self) -> List[T]:
        """Convert the stream to a sorted list."""
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
        """Handle an error based on the error policy."""
        error_event = ErrorEvent(error, item_index, step_name)
        
        if self.error_policy == ErrorPolicy.FAIL_FAST:
            self.stop(error) # Stop the stream and raise the error
            raise error
        elif self.error_policy == ErrorPolicy.COLLECT:
            self.errors.append(error_event)
        # IGNORE policy: do nothing, just drop the error
    
class StreamProcessor(ABC, Generic[T, U]):
    """Base class for all stream processors."""
    
    input_stream: Stream[T]
    output_stream: Stream[U]
    
    def __init__(self, input_stream: Stream[T], output_stream: Stream[U]):
        self.input_stream = input_stream
        self.output_stream = output_stream

    async def process_stream(self):
        """Process the stream data and put the results into the output stream."""
        raise NotImplementedError
    
    async def _cleanup(self):
        """Cleanup the processor."""
        pass
    
class StatelessStreamProcessor(StreamProcessor[T, U]):
    """Processor that does not maintain state."""
    
    async def process_stream(self):
        """Process the stream data and put the results into the output stream asynchronously."""
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
        """Process item with error handling based on policy."""
        try:
            await self._process_item(item)
        except Exception as e:
            await self.output_stream.handle_error(e, item.index, self.__class__.__name__)
    
    async def _process_item(self, item: Indexed[T]):
        """Process an item and put the result into the output stream."""
        raise NotImplementedError
    
class StatefulStreamProcessor(StreamProcessor[T, U]):
    """Processor that maintains state."""
    
    async def process_stream(self):
        """Await the input stream to be consumed and process the items."""
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
        finally:
            # Propagate cancellation
            if self.input_stream.is_consumer_cancelled():
                self.output_stream.stop_consumer(self.input_stream.error)
            elif self.output_stream.is_producer_cancelled():
                self.input_stream.stop_producer()

            await self._cleanup()

    async def _process_items(self, items: List[T]):
        """Process a list of items and put the results into the output stream."""
        raise NotImplementedError

class WithPipeline(ABC, Generic[T, U]):
    """Step that can be piped into a pipeline."""

    def __or__(self, other: 'WithPipeline[U, V]') -> 'Pipeline[T, V]':
        """Chain steps using | operator."""
        return self.then(other)
    
    def then(self, other: 'WithPipeline[U, V]') -> 'Pipeline[T, V]':
        """Chain steps using | operator."""
        raise NotImplementedError
    
    def __ror__(self, other: Stream[T]) -> 'Pipeline[T, U]':
        """Support data | step syntax."""
        return self.with_input(other)
    
    def with_input(self, data: Union[T, List[T], Iterable[T]]) -> 'Pipeline[T, U]':
        """Support data | step syntax."""
        raise NotImplementedError

class Step(WithPipeline[T, U]):
    
    def pipe(self, stream_processor: StreamProcessor[Any, T]) -> StreamProcessor[T, U]:
        """Pipe the stream through this step."""
        return self.from_stream(stream_processor.output_stream)
    
    def from_stream(self, input_stream: Stream[T]) -> StreamProcessor[T, U]:
        """Build a processor from a stream."""
        output_stream = Stream[U](input_stream.error_policy)
        return self._build_processor(input_stream, output_stream)

    def _build_processor(self, input_stream: Stream[T], output_stream: Stream[U]) -> StreamProcessor[T, U]:
        """Build the processor for this step."""
        raise NotImplementedError
    
    def then(self, other: 'Step[U, V]') -> 'Pipeline[T, V]':
        """Chain steps using | operator."""
        return Pipeline([self, other])
    
    def with_input(self, data: Union[T, List[T], Iterable[T]]) -> 'Pipeline[T, U]':
        """Support data | step syntax."""
        return Pipeline([self], input_data=data)
    
    def with_error_policy(self, error_policy: ErrorPolicy) -> 'Pipeline[T, U]':
        """Set error policy for this step as a pipeline."""
        return Pipeline([self], error_policy=error_policy)
    
class Pipeline(Step[T, U]):
    """Pipeline of steps."""

    steps: List[Step[Any, Any]]

    def __init__(self, steps: List[Step[Any, Any]], input_data: Iterable[T] | None = None, error_policy: ErrorPolicy = ErrorPolicy.FAIL_FAST):
        self.steps = steps
        self.input_data = input_data
        self.error_policy = error_policy

    async def _build_processors(self, input_stream: Stream[T]) -> List[StreamProcessor[Any, Any]]:
        """Build the processors for the pipeline."""
        processors = []
        for step in self.steps:
            if len(processors) == 0:
                processor = step.from_stream(input_stream)
            else:
                processor = step.pipe(processors[-1])
            processors.append(processor)
            
        return processors
    
    async def _get_input_stream(self, input_data: Union[Stream[T], Iterable[T]] | None) -> Stream[T]:
        """Get the input stream for the pipeline.""" 
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
        """Run the pipeline."""
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
        """Collect the results of the pipeline."""
        output_stream = await self.run(input_data)
        return await output_stream.to_sorted_list()
    
    async def stream(self, input_data: Union[Stream[T], Iterable[T]] | None = None) -> AsyncIterator[U]:
        """Stream the results as they become available."""
        input_stream = await self._get_input_stream(input_data)

        processors = await self._build_processors(input_stream)

        async with TaskGroup() as tg:
            for processor in processors:
                tg.create_task(processor.process_stream())

            async for item in processors[-1].output_stream:
                yield item
    
    def then(self, other: 'Step[U, V]') -> 'Pipeline[T, V]':
        """Chain steps using | operator."""
        return Pipeline(self.steps + [other], input_data=self.input_data, error_policy=self.error_policy)
    
    def with_input(self, data: Union[T, List[T], Iterable[T]]) -> 'Pipeline[T, U]':
        """Support data | step syntax."""
        if self.input_data is not None:
            raise ValueError("Input provided twice")
        
        return Pipeline(self.steps, input_data=data, error_policy=self.error_policy)
    
    def with_error_policy(self, error_policy: ErrorPolicy) -> 'Pipeline[T, U]':
        """Set error policy for this pipeline."""
        return Pipeline(self.steps, input_data=self.input_data, error_policy=error_policy)
