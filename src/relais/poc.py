import asyncio
from typing import Any, AsyncGenerator, AsyncIterator, Generic, List, TypeVar, Union, Optional

DataType = TypeVar('DataType', bound=Any)

class EndEvent:
    pass

class Stream(Generic[DataType]):
    
    def __init__(self):
        self.queue = asyncio.Queue()
        self.ended = False # True if the stream has been ended by the producer
        self.consumed = False # True if the stream has been consumed by the consumer

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
    next: Optional['PipelineStep[NextType, Any, OutputType]'] = None

    async def process(self, stream: Stream[InputType]) -> Stream[OutputType]:
        raise NotImplementedError

class StatelessPipelineStep(PipelineStep[InputType, NextType, OutputType]):

    async def process(self, input_stream: Stream[InputType]) -> AsyncGenerator[OutputType, None]:
        next_stream = Stream[NextType]()

        async def read_input_stream():
            async def process_item(item: InputType):
                await next_stream.put(await self._process(item))

            tasks = []
            async for item in input_stream:
                tasks.append(asyncio.create_task(process_item(item)))

            await asyncio.gather(*tasks)
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
        