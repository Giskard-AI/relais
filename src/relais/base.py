import asyncio
import sys
from typing import Any, AsyncIterator, Generic, List, TypeVar, Union, Iterable, Callable, Optional
from abc import ABC

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
    
    def __init__(self):
        self.queue = asyncio.Queue()
        self.ended = False # True if the stream has been ended by the producer
        self.fed = False # True if the producer has started feeding the stream
        self.red = False # True if the consumer has started reading the stream
        self.consumed = False # True if the stream has been consumed by the consumer
        self._read_lock = asyncio.Lock() # Lock to prevent concurrent reads
        self._write_lock = asyncio.Lock() # Lock to prevent concurrent writes

    @classmethod
    async def from_list(cls, data: List[T]) -> 'Stream[T]':
        return await cls.from_iterable(data)

    @classmethod
    async def from_iterable(cls, data: Iterable[T]) -> 'Stream[T]':
        stream = cls()
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
        if self.consumed:
            raise StopAsyncIteration

        self.red = True

        item = await self.queue.get()
        if isinstance(item, EndEvent):
            self.consumed = True
            raise StopAsyncIteration

        return item
    
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
        async with TaskGroup() as tg:
            async for item in self.input_stream:
                tg.create_task(self._process_item(item))
            
        await self._cleanup()
        await self.output_stream.end()
    
    async def _process_item(self, item: Indexed[T]):
        """Process an item and put the result into the output stream."""
        raise NotImplementedError
    
class StatefulStreamProcessor(StreamProcessor[T, U]):
    """Processor that maintains state."""
    
    async def process_stream(self):
        """Await the input stream to be consumed and process the items."""
        input_data = await self.input_stream.to_sorted_list()
        output_data = await self._process_items(input_data)

        await self.output_stream.put_all(output_data)
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
        output_stream = Stream[U]()
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
    
class Pipeline(Step[T, U]):
    """Pipeline of steps."""

    steps: List[Step[Any, Any]]

    def __init__(self, steps: List[Step[Any, Any]], input_data: Iterable[T] | None = None):
        self.steps = steps
        self.input_data = input_data

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
    
    async def _get_input_stream(self, input_stream: Stream[T] | None) -> Stream[T]:
        """Get the input stream for the pipeline."""
        if input_stream is None and self.input_data is None:
            raise ValueError("No input provided")
        
        if input_stream is not None and self.input_data is not None:
            raise ValueError("Input provided twice")

        return input_stream or await Stream.from_iterable(self.input_data)

    async def run(self, input_stream: Stream[T] | None = None) -> Stream[U]:
        """Run the pipeline."""
        input_stream = await self._get_input_stream(input_stream)

        processors = await self._build_processors(input_stream)
        
        if len(processors) == 0:
            return input_stream

        async with TaskGroup() as tg:
            for processor in processors:
                tg.create_task(processor.process_stream())

        return processors[-1].output_stream
    
    async def collect(self, input_stream: Stream[T] | None = None) -> List[U]:
        """Collect the results of the pipeline."""
        output_stream = await self.run(input_stream)
        return await output_stream.to_sorted_list()
    
    async def stream(self, input_stream: Stream[T] | None = None) -> AsyncIterator[U]:
        """Stream the results as they become available."""
        input_stream = await self._get_input_stream(input_stream)

        processors = await self._build_processors(input_stream)

        async with TaskGroup() as tg:
            for processor in processors:
                tg.create_task(processor.process_stream())

            async for item in processors[-1].output_stream:
                yield item
    
    def then(self, other: 'Step[U, V]') -> 'Pipeline[T, V]':
        """Chain steps using | operator."""
        return Pipeline(self.steps + [other], input_data=self.input_data)
    
    def with_input(self, data: Union[T, List[T], Iterable[T]]) -> 'Pipeline[T, U]':
        """Support data | step syntax."""
        if self.input_data is not None:
            raise ValueError("Input provided twice")
        
        return Pipeline(self.steps, input_data=data)
