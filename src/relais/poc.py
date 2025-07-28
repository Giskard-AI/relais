import asyncio
import sys
from typing import Any, AsyncGenerator, AsyncIterator, Generic, List, TypeVar, Union, Optional

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

DataType = TypeVar('DataType', bound=Any)

class EndEvent:
    pass

class Stream(Generic[DataType]):
    
    def __init__(self):
        self.queue = asyncio.Queue()
        self.ended = False # True if the stream has been ended by the producer
        self.consumed = False # True if the stream has been consumed by the consumer

    @classmethod
    def from_list(cls, data: List[DataType]) -> 'Stream[DataType]':
        stream = cls()

        for item in data:
            stream.put(item)
        stream.end()
        
        return stream

    async def put(self, item: DataType):
        if self.ended:
            raise ValueError("Stream already ended")
        
        await self.queue.put(item)

    async def end(self):
        self.ended = True
        await self.queue.put(EndEvent())

    def __aiter__(self) -> AsyncIterator[DataType]:
        return self

    async def __anext__(self) -> DataType:
        if self.consumed:
            raise StopAsyncIteration

        item = await self.queue.get()

        if isinstance(item, EndEvent):
            self.consumed = True
            raise StopAsyncIteration
        
        return item
    

InputType = TypeVar('InputType', bound=Any)
OutputType = TypeVar('OutputType', bound=Any)
NextType = TypeVar('NextType', bound=Any)

class PipelineStep(Generic[InputType, NextType, OutputType]):
    _input_data: Optional[Union[List[InputType], Stream[InputType]]] = None
    _previous: Optional['PipelineStep[InputType, NextType, OutputType]'] = None
    next: Optional['PipelineStep[NextType, Any, OutputType]'] = None

    async def process(self, stream: Stream[InputType]) -> Stream[OutputType]:
        raise NotImplementedError
    
    def __or__(self, other: 'PipelineStep[NextType, Any, OutputType]') -> 'PipelineStep[InputType, Any, OutputType]':
        return self.then(other)
    
    def __ror__(self, data: Union[List[InputType], Stream[InputType]]) -> 'PipelineStep[InputType, NextType, OutputType]':
        """Support data | step syntax"""
        # Store the input data on the first step of the chain
        first_step = self._first_step()
        first_step._input_data = data
        return self
    
    def then(self, other: 'PipelineStep[NextType, Any, OutputType]') -> 'PipelineStep[InputType, Any, OutputType]':
        """
        Chain this pipeline step with another one.
        """

        # TODO: Make pipeline steps immutable and return a copy?

        last_step = self._last_step()
        last_step.next = other
        other._previous = last_step
        
        return self
    
    def _first_step(self) -> 'PipelineStep[InputType, Any, OutputType]':
        if self._previous:
            return self._previous._first_step()
        else:
            return self
    
    def _last_step(self) -> 'PipelineStep[InputType, Any, OutputType]':
        if self.next:
            return self.next._last_step()
        else:
            return self
    
    async def run(self, data: Optional[Union[List[InputType], Stream[InputType]]] = None) -> List[OutputType]:
        """Execute the pipeline and collect all results."""
        # Use provided data or stored data from | operator
        results = []
        async for item in self.stream(data):
            results.append(item)
        
        return results
    
    async def collect(self, data: Optional[Union[List[InputType], Stream[InputType]]] = None) -> List[OutputType]:
        """Alias for run() - execute and collect all results."""
        return await self.run(data)
    
    async def stream(self, data: Optional[Union[List[InputType], Stream[InputType]]] = None) -> AsyncGenerator[OutputType, None]:
        """Execute the pipeline and stream results as they become available."""
        # Use provided data or stored data from | operator
        first_step = self._first_step()
        if self is not first_step:
            raise ValueError("Cannot run an intermediate pipeline step directly, please run the first step of the pipeline instead.")
        
        input_data = data if data is not None else self._input_data
        
        if input_data is None:
            raise ValueError("No input data provided. Use data | step or step.run(data)")
        
        # Convert list to Stream if needed
        if isinstance(input_data, list):
            input_stream = Stream.from_list(input_data)
        else:
            if input_data.consumed:
                raise ValueError("Input stream has already been consumed")
            
            input_stream = input_data
        
        # Execute the pipeline starting from the first step
        return await self.process(input_stream)

class StatelessPipelineStep(PipelineStep[InputType, NextType, OutputType]):

    async def process(self, input_stream: Stream[InputType]) -> AsyncGenerator[OutputType, None]:
        next_stream = Stream[NextType]()

        async def read_input_stream():
            async def process_item(item: InputType):
                await next_stream.put(await self._process(item))

            async with TaskGroup() as tg:
                async for item in input_stream:
                    tg.create_task(process_item(item))
            
            await next_stream.end()
        
        asyncio.create_task(read_input_stream())

        output_iterator = self.next.process(next_stream) if self.next else next_stream
        async for item in output_iterator:
            yield item


    async def _process(self, item: InputType) -> NextType:
        raise NotImplementedError
    

class StatefulPipelineStep(PipelineStep[InputType, NextType, OutputType]):

    async def process(self, input_stream: Stream[InputType]) -> AsyncGenerator[OutputType, None]:
        stream_data = []
        async for item in input_stream:
            stream_data.append(item)

        output_data = await self._process(stream_data)

        for item in output_data:
            yield item

    async def _process(self, stream_data: List[InputType]) -> List[NextType]:
        raise NotImplementedError
        