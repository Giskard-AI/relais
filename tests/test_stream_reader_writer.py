"""Tests for StreamReader/StreamWriter separation in the new architecture."""

import asyncio

import pytest

from relais.errors import ErrorPolicy, PipelineError
from relais.index import Index
from relais.stream import (
    Stream,
    StreamAlreadyHasReader,
    StreamAlreadyHasWriter,
    StreamErrorEvent,
    StreamItemEvent,
)


class TestStreamReaderWriterBasics:
    """Test basic StreamReader and StreamWriter functionality."""

    @pytest.mark.asyncio
    async def test_stream_reader_creation(self):
        """Test creating a StreamReader from a Stream."""
        stream = Stream[int]()
        reader = await stream.reader()

        assert reader is not None
        assert not reader.consumed

    @pytest.mark.asyncio
    async def test_stream_writer_creation(self):
        """Test creating a StreamWriter from a Stream."""
        stream = Stream[int]()
        writer = await stream.writer()

        assert writer is not None
        assert not writer.is_completed()

    @pytest.mark.asyncio
    async def test_single_reader_per_stream(self):
        """Test that only one reader can be created per stream."""
        stream = Stream[int]()

        # First reader should succeed
        reader1 = await stream.reader()
        assert reader1 is not None

        # Second reader should fail
        with pytest.raises(StreamAlreadyHasReader):
            _ = await stream.reader()

    @pytest.mark.asyncio
    async def test_single_writer_per_stream(self):
        """Test that only one writer can be created per stream."""
        stream = Stream[int]()

        # First writer should succeed
        writer1 = await stream.writer()
        assert writer1 is not None

        # Second writer should fail
        with pytest.raises(StreamAlreadyHasWriter):
            _ = await stream.writer()

    @pytest.mark.asyncio
    async def test_reader_writer_on_same_stream(self):
        """Test that a stream can have both a reader and writer."""
        stream = Stream[int]()

        # Should be able to create both
        writer = await stream.writer()
        reader = await stream.reader()

        assert writer is not None
        assert reader is not None


class TestBasicReadWrite:
    """Test basic read/write operations."""

    @pytest.mark.asyncio
    async def test_write_and_read_single_item(self):
        """Test writing and reading a single item."""
        stream = Stream[int]()
        writer = await stream.writer()
        reader = await stream.reader()

        # Write an item
        await writer.write(StreamItemEvent(item=42, index=Index(0)))
        await writer.complete()

        # Read the item
        results = await reader.collect()
        assert results == [42]

    @pytest.mark.asyncio
    async def test_write_and_read_multiple_items(self):
        """Test writing and reading multiple items."""
        stream = Stream[int]()
        writer = await stream.writer()
        reader = await stream.reader()

        # Write multiple items
        items = [10, 20, 30, 40, 50]
        for i, item in enumerate(items):
            await writer.write(StreamItemEvent(item=item, index=Index(i)))
        await writer.complete()

        # Read all items
        results = await reader.collect()
        assert results == items

    @pytest.mark.asyncio
    async def test_streaming_read_write(self):
        """Test streaming read while writing."""
        stream = Stream[int](max_size=10)
        writer = await stream.writer()
        reader = await stream.reader()

        async def producer():
            for i in range(5):
                await writer.write(StreamItemEvent(item=i * 10, index=Index(i)))
                await asyncio.sleep(0.001)  # Small delay
            await writer.complete()

        async def consumer():
            results = []
            async for event in reader:
                if isinstance(event, StreamItemEvent):
                    results.append(event.item)
            return results

        # Run producer and consumer concurrently
        producer_task = asyncio.create_task(producer())
        consumer_task = asyncio.create_task(consumer())

        await producer_task
        results = await consumer_task

        assert results == [0, 10, 20, 30, 40]


class TestStreamEvents:
    """Test handling of different stream events."""

    @pytest.mark.asyncio
    async def test_stream_item_events(self):
        """Test handling of StreamItemEvent objects."""
        stream = Stream[str]()
        writer = await stream.writer()
        reader = await stream.reader()

        # Write various items
        items = ["hello", "world", "test"]
        for i, item in enumerate(items):
            await writer.write(StreamItemEvent(item=item, index=Index(i)))
        await writer.complete()

        # Read and verify events
        events = await reader.to_list()
        assert len(events) == 3

        for i, event in enumerate(events):
            assert isinstance(event, StreamItemEvent)
            assert event.item == items[i]
            assert event.index.index == i

    @pytest.mark.asyncio
    async def test_stream_error_events(self):
        """Test handling of StreamErrorEvent objects."""
        stream = Stream[int](error_policy=ErrorPolicy.COLLECT)
        writer = await stream.writer()
        reader = await stream.reader()

        # Write some items and an error
        await writer.write(StreamItemEvent(item=10, index=Index(0)))

        error = PipelineError("Test error", ValueError("test"), "TestStep", Index(1))
        await writer.handle_error(StreamErrorEvent(error=error, index=Index(1)))

        await writer.write(StreamItemEvent(item=20, index=Index(2)))
        await writer.complete()

        # Collect items and errors separately
        items, errors = await reader.collect_with_errors()

        assert items == [10, 20]
        assert len(errors) == 1
        assert "Test error" in str(errors[0])

    @pytest.mark.asyncio
    async def test_stream_end_event(self):
        """Test that StreamEndEvent properly terminates iteration."""
        stream = Stream[int]()
        writer = await stream.writer()
        reader = await stream.reader()

        # Write some items and complete
        await writer.write(StreamItemEvent(item=1, index=Index(0)))
        await writer.write(StreamItemEvent(item=2, index=Index(1)))
        await writer.complete()  # This should add StreamEndEvent

        # Iterate manually to see all events
        events = []
        async for event in reader:
            events.append(event)

        # Should have 2 item events, iteration should stop at end event
        assert len(events) == 2
        assert all(isinstance(event, StreamItemEvent) for event in events)


class TestErrorHandlingInReaderWriter:
    """Test error handling in StreamReader/Writer operations."""

    @pytest.mark.asyncio
    async def test_fail_fast_error_propagation(self):
        """Test that FAIL_FAST errors propagate correctly through reader/writer."""
        stream = Stream[int](error_policy=ErrorPolicy.FAIL_FAST)
        writer = await stream.writer()
        _ = await stream.reader()

        # Write an item, then an error
        await writer.write(StreamItemEvent(item=10, index=Index(0)))

        error = PipelineError(
            "Critical error", RuntimeError("critical"), "TestStep", Index(1)
        )

        # This should cause the stream to enter error state
        with pytest.raises(PipelineError):
            await writer.handle_error(StreamErrorEvent(error=error, index=Index(1)))

    @pytest.mark.asyncio
    async def test_collect_error_policy(self):
        """Test COLLECT error policy in reader operations."""
        stream = Stream[int](error_policy=ErrorPolicy.COLLECT)
        writer = await stream.writer()
        reader = await stream.reader()

        # Write items with errors interspersed
        await writer.write(StreamItemEvent(item=1, index=Index(0)))

        error1 = PipelineError("Error 1", ValueError("error1"), "Step1", Index(1))
        await writer.handle_error(StreamErrorEvent(error=error1, index=Index(1)))

        await writer.write(StreamItemEvent(item=2, index=Index(2)))

        error2 = PipelineError("Error 2", RuntimeError("error2"), "Step2", Index(3))
        await writer.handle_error(StreamErrorEvent(error=error2, index=Index(3)))

        await writer.write(StreamItemEvent(item=3, index=Index(4)))
        await writer.complete()

        # Collect with errors
        items, errors = await reader.collect_with_errors()

        assert items == [1, 2, 3]
        assert len(errors) == 2
        assert "Error 1" in str(errors[0])
        assert "Error 2" in str(errors[1])


class TestStreamCancellation:
    """Test stream cancellation behavior with reader/writer."""

    @pytest.mark.asyncio
    async def test_stream_cancellation(self):
        """Test that stream cancellation affects both reader and writer."""
        stream = Stream[int]()
        writer = await stream.writer()
        reader = await stream.reader()

        # Start writing
        await writer.write(StreamItemEvent(item=1, index=Index(0)))

        # Cancel the stream
        await stream.cancel()

        # Both reader and writer should report cancelled state
        assert reader.is_cancelled()
        assert writer.is_cancelled()

    @pytest.mark.asyncio
    async def test_reader_cancellation_cleanup(self):
        """Test that reader properly handles cancellation during iteration."""
        stream = Stream[int]()
        writer = await stream.writer()
        reader = await stream.reader()

        async def slow_producer():
            for i in range(100):
                await writer.write(StreamItemEvent(item=i, index=Index(i)))
                await asyncio.sleep(0.001)
            await writer.complete()

        async def cancelling_consumer():
            results = []
            count = 0
            async for event in reader:
                if isinstance(event, StreamItemEvent):
                    results.append(event.item)
                    count += 1
                    if count >= 3:  # Cancel after 3 items
                        await reader.cancel()
                        break
            return results

        # Start both tasks
        producer_task = asyncio.create_task(slow_producer())
        consumer_task = asyncio.create_task(cancelling_consumer())

        results = await consumer_task
        producer_task.cancel()  # Clean up

        # Should have exactly 3 items
        assert len(results) == 3
        assert results == [0, 1, 2]


class TestStreamPiping:
    """Test stream piping functionality with reader/writer."""

    @pytest.mark.asyncio
    async def test_stream_piping_basic(self):
        """Test basic stream piping functionality."""
        parent_stream = Stream[int]()
        child_stream = parent_stream.pipe()

        # Should be able to create readers/writers for both
        parent_writer = await parent_stream.writer()
        child_reader = await child_stream.reader()

        assert parent_writer is not None
        assert child_reader is not None

    @pytest.mark.asyncio
    async def test_stream_reader_piping(self):
        """Test that StreamReader can create piped streams."""
        stream = Stream[int]()
        reader = await stream.reader()

        # Reader should be able to create a piped stream
        piped_stream = reader.pipe()
        assert piped_stream is not None

        # Should be able to get reader/writer from piped stream
        piped_writer = await piped_stream.writer()
        piped_reader = await piped_stream.reader()

        assert piped_writer is not None
        assert piped_reader is not None


class TestStreamReaderWriterEdgeCases:
    """Test edge cases in StreamReader/Writer functionality."""

    @pytest.mark.asyncio
    async def test_empty_stream_handling(self):
        """Test handling of empty streams."""
        stream = Stream[int]()
        writer = await stream.writer()
        reader = await stream.reader()

        # Complete immediately without writing anything
        await writer.complete()

        # Reader should handle empty stream gracefully
        results = await reader.collect()
        assert results == []

    @pytest.mark.asyncio
    async def test_large_stream_handling(self):
        """Test handling of large streams."""
        stream = Stream[int](max_size=1000)
        writer = await stream.writer()
        reader = await stream.reader()

        # Write many items
        item_count = 500
        for i in range(item_count):
            await writer.write(StreamItemEvent(item=i, index=Index(i)))
        await writer.complete()

        # Read all items
        results = await reader.collect()
        assert len(results) == item_count
        assert results == list(range(item_count))

    @pytest.mark.asyncio
    async def test_concurrent_operations_on_reader_writer(self):
        """Test concurrent operations on the same reader/writer."""
        stream = Stream[int](max_size=50)
        writer = await stream.writer()
        reader = await stream.reader()

        async def write_items(start: int, count: int):
            for i in range(count):
                await writer.write(
                    StreamItemEvent(item=start + i, index=Index(start + i))
                )
                await asyncio.sleep(0.001)

        async def read_items():
            results = []
            collected_count = 0
            async for event in reader:
                if isinstance(event, StreamItemEvent):
                    results.append(event.item)
                    collected_count += 1
                    if collected_count >= 10:  # Stop after collecting 10 items
                        break
            return results

        # Start writing and reading concurrently
        write_task = asyncio.create_task(write_items(0, 20))
        read_task = asyncio.create_task(read_items())

        results = await read_task
        write_task.cancel()  # Clean up

        # Should have collected exactly 10 items
        assert len(results) == 10
        # Items should be in order (0 through 9)
        assert results == list(range(10))


if __name__ == "__main__":
    # Run basic tests for quick validation
    async def quick_test():
        # Test basic reader/writer creation
        stream = Stream[int]()
        writer = await stream.writer()
        reader = await stream.reader()

        # Test write and read
        await writer.write(StreamItemEvent(item=42, index=Index(0)))
        await writer.complete()

        results = await reader.collect()
        assert results == [42]

        print("✅ Quick StreamReader/Writer tests passed!")

    asyncio.run(quick_test())
