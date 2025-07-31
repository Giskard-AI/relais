from typing import Optional, Generic, Iterable, AsyncIterator, List, TypeVar
import asyncio
from relais.errors import ErrorPolicy

T = TypeVar("T")


class Index:
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
    sub_index: Optional["Index"] = None

    def __init__(self, index: int, sub_index: Optional["Index"] = None):
        """Initialize an Index.

        Args:
            index: The primary index position
            sub_index: Optional nested index for expanded items
        """
        if not isinstance(index, int):
            raise TypeError(f"Index must be an integer, got {type(index).__name__}")
        if index < -1:  # Allow -1 for sentinel values
            raise ValueError(f"Index must be >= -1, got {index}")

        self.index = index
        self.sub_index = sub_index

    def __lt__(self, other: "Index") -> bool:
        # First compare primary indices
        if self.index != other.index:
            return self.index < other.index

        # If primary indices are equal, compare sub_indices
        # None sub_index sorts before any actual sub_index
        if self.sub_index is None and other.sub_index is None:
            return False
        elif self.sub_index is None:
            return True  # None sorts first
        elif other.sub_index is None:
            return False
        else:
            return self.sub_index < other.sub_index  # Recursive comparison


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


class ErrorEvent:
    """Error event for collecting processing errors."""

    def __init__(self, error: Exception, item_index: Index, step_name: str):
        self.error = error
        self.item_index = item_index
        self.step_name = step_name


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

    IMPORTANT: This implementation is designed for small to medium-sized pipelines.
    For large datasets, consider that:
    - The internal asyncio.Queue has no size limit and can consume significant memory
    - Index tracking adds overhead for each item
    - Suitable for LLM evaluation pipelines with hundreds of items, not millions

    Typical use case: LLM evaluation pipeline
    1. Generate user inputs (10-1000 items)
    2. Run generation on target model
    3. Evaluate answers

    For large-scale data processing, consider implementing queue size limits
    and streaming-first approaches.

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
        if not isinstance(error_policy, ErrorPolicy):
            raise TypeError(
                f"error_policy must be an ErrorPolicy, got {type(error_policy).__name__}"
            )

        # NOTE: Using unbounded queue for simplicity in small pipelines
        # For large datasets, consider adding maxsize parameter to prevent memory issues
        self.queue = asyncio.Queue()
        self.ended = False  # True if the stream has been ended by the producer
        self.fed = False  # True if the producer has started feeding the stream
        self.red = False  # True if the consumer has started reading the stream
        self.consumed = False  # True if the stream has been consumed by the consumer
        self._read_lock = asyncio.Lock()  # Lock to prevent concurrent reads
        self._write_lock = asyncio.Lock()  # Lock to prevent concurrent writes

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
    async def from_list(cls, data: List[T]) -> "Stream[T]":
        """Create a stream from a list of items.

        Args:
            data: List of items to put into the stream

        Returns:
            A new Stream containing all items from the list
        """
        return await cls.from_iterable(data)

    @classmethod
    async def from_iterable(
        cls, data: Iterable[T], error_policy: ErrorPolicy = ErrorPolicy.FAIL_FAST
    ) -> "Stream[T]":
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
            self.stop(error)  # Stop the stream and raise the error
            raise error
        elif self.error_policy == ErrorPolicy.COLLECT:
            self.errors.append(error_event)
        # IGNORE policy: do nothing, just drop the error
