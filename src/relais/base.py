import asyncio
from typing import Any, AsyncIterator, Generic, List, TypeVar, Union, Iterable
from abc import ABC
from relais.errors import ErrorPolicy, PipelineError
from relais.stream import Stream, ErrorEvent
from relais.processors import StreamProcessor
from relais.tasks import CompatTaskGroup, CompatExceptionGroup

# Type variables
T = TypeVar("T")
U = TypeVar("U")
V = TypeVar("V")


class PipelineResult(Generic[T]):
    """Result of pipeline execution with collected errors.

    This class holds both the successfully processed items and any errors
    that occurred during processing when using ErrorPolicy.COLLECT.

    Attributes:
        data: The successfully processed items
        errors: List of errors that occurred during processing
    """

    def __init__(self, data: List[T], errors: List[ErrorEvent]):
        """Initialize a PipelineResult.

        Args:
            data: The successfully processed items
            errors: List of errors that occurred during processing
        """
        self.data = data
        self.errors = errors

    @property
    def has_errors(self) -> bool:
        """Check if any errors occurred during processing."""
        return len(self.errors) > 0

    def raise_if_errors(self):
        """Raise a PipelineError if any errors were collected.

        Raises:
            PipelineError: If any errors occurred during processing
        """
        if self.has_errors:
            error_messages = [f"{err.step_name}: {err.error}" for err in self.errors]
            raise PipelineError(
                f"Pipeline completed with {len(self.errors)} errors: {'; '.join(error_messages)}",
                self.errors[0].error,
                "Pipeline",
            )


class WithPipeline(ABC, Generic[T, U]):
    """Abstract base for objects that can be chained in pipelines.

    WithPipeline defines the interface for objects that support the pipe
    operator (|) for chaining operations together. This includes both
    individual steps and complete pipelines.

    The class supports two chaining patterns:
    1. step | step  -> Pipeline (forward chaining)
    2. data | step  -> Pipeline (data binding)
    """

    def __or__(self, other: "WithPipeline[U, V]") -> "Pipeline[T, V]":
        """Chain this object with another using | operator.

        Args:
            other: The object to chain after this one

        Returns:
            A new Pipeline containing both objects
        """
        return self.then(other)

    def then(self, other: "WithPipeline[U, V]") -> "Pipeline[T, V]":
        """Chain this object with another sequentially.

        Args:
            other: The object to chain after this one

        Returns:
            A new Pipeline containing both objects

        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError

    def __ror__(self, other: Union[T, List[T], Iterable[T]]) -> "Pipeline[T, U]":
        """Support data | step syntax (reverse pipe operator).

        Args:
            other: The data to pipe into this object

        Returns:
            A new Pipeline with the data as input
        """
        return self.with_input(other)

    def with_input(self, data: Union[T, List[T], Iterable[T]]) -> "Pipeline[T, U]":
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

    def _build_processor(
        self, input_stream: Stream[T], output_stream: Stream[U]
    ) -> StreamProcessor[T, U]:
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

    def then(self, other: WithPipeline[T, U]) -> "Pipeline[T, V]":
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

    def with_input(self, data: Union[T, List[T], Iterable[T]]) -> "Pipeline[T, U]":
        """Create a pipeline with this step and input data.

        Args:
            data: The input data for the pipeline

        Returns:
            A new Pipeline with this step and the specified input
        """
        return Pipeline([self], input_data=data)

    def with_error_policy(self, error_policy: ErrorPolicy) -> "Pipeline[T, U]":
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

    def __init__(
        self,
        steps: List[Step[Any, Any]],
        input_data: Iterable[T] | None = None,
        error_policy: ErrorPolicy = ErrorPolicy.FAIL_FAST,
    ):
        """Initialize a new Pipeline.

        Args:
            steps: List of steps to execute in sequence
            input_data: Optional input data for the pipeline
            error_policy: How to handle errors during processing
        """
        if not isinstance(steps, list):
            raise TypeError(f"steps must be a list, got {type(steps).__name__}")
        if not isinstance(error_policy, ErrorPolicy):
            raise TypeError(
                f"error_policy must be an ErrorPolicy, got {type(error_policy).__name__}"
            )

        self.steps = steps
        self.input_data = input_data
        self.error_policy = error_policy

    async def _build_processors(
        self, input_stream: Stream[T]
    ) -> List[StreamProcessor[Any, Any]]:
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

    async def _get_input_stream(
        self, input_data: Union[Stream[T], Iterable[T]] | None
    ) -> Stream[T]:
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
        elif hasattr(data_to_process, "__aiter__"):
            # TODO: make this cleaner
            # Use AsyncIteratorStep to handle async iteration lazily
            from .steps.async_iterator import AsyncIteratorStep

            async_step = AsyncIteratorStep(data_to_process)

            # Create a dummy input stream (empty)
            dummy_input = Stream[None](self.error_policy)
            await (
                dummy_input._end()
            )  # End it immediately since async iterator step doesn't need input

            # Create the processor and get its output stream
            processor = async_step.from_stream(dummy_input)

            # We need to start the processor to begin feeding the stream
            asyncio.create_task(processor.process_stream())

            return processor.output_stream
        else:
            # It's a regular sync iterable
            return await Stream.from_iterable(data_to_process, self.error_policy)

    async def run(
        self, input_data: Union[Stream[T], Iterable[T]] | None = None
    ) -> Stream[U]:
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
            async with CompatTaskGroup() as tg:
                for processor in processors:
                    tg.create_task(processor.process_stream())

        except Exception as e:
            if isinstance(e, CompatExceptionGroup):
                raise e.exceptions[0]
            else:
                raise e

        return processors[-1].output_stream

    def _collect_all_errors(
        self, processors: List["StreamProcessor"]
    ) -> List[ErrorEvent]:
        """Collect errors from all processors in the pipeline.

        Args:
            processors: List of processors to collect errors from

        Returns:
            List of all ErrorEvent objects from all processors
        """
        all_errors = []
        for processor in processors:
            all_errors.extend(processor.output_stream.errors)
        return all_errors

    async def collect_with_errors(
        self, input_data: Union[Stream[T], Iterable[T]] | None = None
    ) -> PipelineResult[U]:
        """Execute pipeline and return results with any collected errors.

        This method is specifically for use with ErrorPolicy.COLLECT to surface
        both successful results and any errors that occurred during processing.

        Args:
            input_data: Optional input data (overrides constructor input_data)

        Returns:
            PipelineResult containing both data and errors

        Raises:
            ValueError: If not using ErrorPolicy.COLLECT
            PipelineError: If execution fails and error policy is FAIL_FAST
        """
        if self.error_policy != ErrorPolicy.COLLECT:
            raise ValueError("collect_with_errors() requires ErrorPolicy.COLLECT")

        input_stream = await self._get_input_stream(input_data)
        processors = await self._build_processors(input_stream)

        if len(processors) == 0:
            data = await input_stream.to_sorted_list()
            return PipelineResult(data=data, errors=input_stream.errors)

        try:
            async with CompatTaskGroup() as tg:
                for processor in processors:
                    tg.create_task(processor.process_stream())

        except Exception as e:
            if isinstance(e, CompatExceptionGroup):
                raise e.exceptions[0]
            else:
                raise e

        output_stream = processors[-1].output_stream
        data = await output_stream.to_sorted_list()
        all_errors = self._collect_all_errors(processors)

        return PipelineResult(data=data, errors=all_errors)

    async def collect(
        self, input_data: Union[Stream[T], Iterable[T]] | None = None
    ) -> List[U]:
        """Execute the pipeline and collect all results into a list.

        This is a convenience method that runs the pipeline and collects
        all output items into a sorted list. Items are sorted by their
        original index to maintain input ordering.

        For ErrorPolicy.COLLECT, this method will complete successfully but
        errors will be silently dropped. Use collect_with_errors() to access them.

        Args:
            input_data: Optional input data (overrides constructor input_data)

        Returns:
            List containing all pipeline results in original order

        Raises:
            PipelineError: If execution fails and error policy is FAIL_FAST
            ValueError: If no input data is provided
        """
        if self.error_policy == ErrorPolicy.COLLECT:
            result = await self.collect_with_errors(input_data)
            return result.data
        else:
            output_stream = await self.run(input_data)
            return await output_stream.to_sorted_list()

    async def stream(
        self, input_data: Union[Stream[T], Iterable[T]] | None = None
    ) -> AsyncIterator[U]:
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

        try:
            async with CompatTaskGroup() as tg:
                for processor in processors:
                    tg.create_task(processor.process_stream())

                async for item in processors[-1].output_stream:
                    yield item.item
        except Exception as e:
            if isinstance(e, CompatExceptionGroup):
                raise e.exceptions[0]
            else:
                raise e

    def then(self, other: WithPipeline[T, U]) -> "Pipeline[T, V]":
        """Chain this pipeline with another step or pipeline."""
        if isinstance(other, Pipeline):
            # If other is a pipeline, merge all steps together
            merged_steps = self.steps + other.steps
            # Use the error policy from the first pipeline unless the second has a different one
            merged_error_policy = (
                other.error_policy
                if other.error_policy != ErrorPolicy.FAIL_FAST
                else self.error_policy
            )
            return Pipeline(
                merged_steps,
                input_data=self.input_data,
                error_policy=merged_error_policy,
            )
        else:
            # If other is a single step, add it to our steps
            return Pipeline(
                self.steps + [other],
                input_data=self.input_data,
                error_policy=self.error_policy,
            )

    def with_input(self, data: Union[T, List[T], Iterable[T]]) -> "Pipeline[T, U]":
        """Support data | step syntax."""
        if self.input_data is not None:
            raise ValueError("Input provided twice")

        return Pipeline(self.steps, input_data=data, error_policy=self.error_policy)

    def with_error_policy(self, error_policy: ErrorPolicy) -> "Pipeline[T, U]":
        """Set error policy for this pipeline."""
        return Pipeline(
            self.steps, input_data=self.input_data, error_policy=error_policy
        )
