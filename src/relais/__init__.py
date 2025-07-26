import asyncio
from typing import Any, Awaitable, Callable, Iterable, List, Optional, Union


class Pipeline:
    def __init__(self, source: Optional[Union[Iterable[Any], 'Pipeline']], steps: Optional[List['Step']] = None):
        self.source = source
        self.steps = steps or []
    
    def __or__(self, other: 'Step') -> 'Pipeline':
        return other.bind(self)
    
    async def _get_source_data(self, data: Optional[Iterable[Any]]) -> List[Any]:
        """Get source data from various sources."""
        if data is not None:
            return list(data)
        elif self.source is None:
            raise ValueError("Pipeline has no source data. Pass data to collect()/stream() or create pipeline with source.")
        elif hasattr(self.source, 'collect'):
            # Source is another pipeline
            return await self.source.collect()
        else:
            return list(self.source)
    
    async def _create_initial_tasks(self, source_data: List[Any]) -> List[asyncio.Task[Any]]:
        """Create initial tasks for source data."""
        async def identity(item: Any) -> Any:
            return item
        
        return [asyncio.create_task(identity(item)) for item in source_data]
    
    async def _process_through_pipeline(self, source_data: List[Any]) -> List[Any]:
        """Process data through all pipeline steps."""
        # Start with tasks for each source item
        current_tasks = await self._create_initial_tasks(source_data)
        
        # Pass tasks through each step
        for step in self.steps:
            current_tasks = await step.process(current_tasks)
        
        # Await final results
        return await asyncio.gather(*current_tasks)
    
    async def collect(self, data: Optional[Iterable[Any]] = None) -> List[Any]:
        """Collect all results."""
        source_data = await self._get_source_data(data)
        
        if not self.steps:
            return source_data
        
        return await self._process_through_pipeline(source_data)
    
    async def stream(self, data: Optional[Iterable[Any]] = None):
        """Stream results. Note: Some steps (like Sort) need all input before producing output."""
        source_data = await self._get_source_data(data)
        
        if not self.steps:
            for item in source_data:
                yield item
            return
        
        # For now, process through pipeline and yield results
        # Future enhancement: detect if pipeline can stream vs needs batching
        results = await self._process_through_pipeline(source_data)
        for result in results:
            yield result


class Step:
    """Base class for all pipeline steps."""
    
    def __ror__(self, other: Iterable[Any]) -> Pipeline:
        # Support iterable | step syntax
        return Pipeline(other) | self
    
    def __or__(self, other: 'Step') -> Pipeline:
        # Support step | step syntax (no source data yet)
        if isinstance(other, Step):
            return Pipeline(None, [self]) | other
        else:
            return NotImplemented
    
    def bind(self, pipeline: Pipeline) -> Pipeline:
        """Add this step to the pipeline."""
        new_steps = pipeline.steps + [self]
        return Pipeline(pipeline.source, new_steps)
    
    async def process(self, tasks: List[asyncio.Task[Any]]) -> List[asyncio.Task[Any]]:
        """Process a list of tasks. Should be implemented by subclasses."""
        raise NotImplementedError


class Map(Step):
    def __init__(self, func: Callable[[Any], Union[Any, Awaitable[Any]]], ordered: bool = True):
        self.func = func
        self.ordered = ordered
    
    async def process(self, input_tasks: List[asyncio.Task[Any]]) -> List[asyncio.Task[Any]]:
        """Process tasks through the map function."""
        async def process_task_result(task: asyncio.Task[Any]) -> Any:
            # Await the input task to get the value
            value = await task
            # Apply the function
            result = self.func(value)
            if asyncio.iscoroutine(result):
                result = await result
            return result
        
        # Create new tasks that map over the input tasks
        output_tasks = [asyncio.create_task(process_task_result(task)) for task in input_tasks]
        
        if not self.ordered:
            # If unordered, we could await as_completed and create new tasks
            # For simplicity, we'll return the tasks and let the caller decide
            pass
        
        return output_tasks


class Sort(Step):
    """Sort step that needs all items at once."""
    
    def __init__(self, key_func: Optional[Callable[[Any], Any]] = None, reverse: bool = False):
        self.key_func = key_func
        self.reverse = reverse
    
    async def process(self, input_tasks: List[asyncio.Task[Any]]) -> List[asyncio.Task[Any]]:
        """Sort all items at once."""
        # Await all input tasks to get values
        values = await asyncio.gather(*input_tasks)
        
        # Sort the values
        if self.key_func:
            sorted_values = sorted(values, key=self.key_func, reverse=self.reverse)
        else:
            sorted_values = sorted(values, reverse=self.reverse)
        
        # Create new tasks for the sorted values
        async def identity(value: Any) -> Any:
            return value
        
        return [asyncio.create_task(identity(value)) for value in sorted_values]


class Take(Step):
    """Take only the first N items, cancelling remaining tasks."""
    
    def __init__(self, n: int):
        self.n = n
    
    async def process(self, input_tasks: List[asyncio.Task[Any]]) -> List[asyncio.Task[Any]]:
        """Take first N tasks and cancel the rest."""
        if len(input_tasks) <= self.n:
            return input_tasks
        
        # Take first n tasks
        taken_tasks = input_tasks[:self.n]
        remaining_tasks = input_tasks[self.n:]
        
        # Cancel remaining tasks to save resources
        for task in remaining_tasks:
            if not task.done():
                task.cancel()
        
        return taken_tasks



