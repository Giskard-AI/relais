#!/usr/bin/env python3
"""Tests for concurrent access patterns and race conditions in the new streaming architecture."""

import asyncio
import random
from typing import List

import pytest

import relais as r
from relais.errors import ErrorPolicy
from relais.index import Index
from relais.stream import Stream, StreamItemEvent
from relais.tasks import CompatTaskGroup


class TestStreamConcurrency:
    """Test concurrent access to Stream objects with new architecture."""

    @pytest.mark.asyncio
    async def test_concurrent_stream_reading(self):
        """Test that streams properly handle concurrent access via StreamReader pattern."""
        stream = await Stream.from_iterable([1, 2, 3, 4, 5])

        # The new architecture should prevent multiple readers
        reader1 = await stream.reader()

        # Second reader should fail
        with pytest.raises(Exception):  # StreamAlreadyHasReader
            _ = await stream.reader()

        # First reader should work correctly
        results = await reader1.collect()
        assert sorted(results) == [1, 2, 3, 4, 5]

    @pytest.mark.asyncio
    async def test_concurrent_stream_writing(self):
        """Test that streams prevent concurrent writers."""
        stream = Stream[int](max_size=100)

        # First writer should succeed
        writer1 = await stream.writer()

        # Second writer should fail
        with pytest.raises(Exception):  # StreamAlreadyHasWriter
            _ = await stream.writer()

        # Write some data and verify
        await writer1.write(StreamItemEvent(item=42, index=Index(0)))
        await writer1.complete()

        reader = await stream.reader()
        results = await reader.collect()
        assert results == [42]

    @pytest.mark.asyncio
    async def test_concurrent_pipeline_executions(self):
        """Test running same pipeline type concurrently with different data."""

        async def slow_multiply(x: int) -> int:
            await asyncio.sleep(0.001)  # Reduced delay for faster tests
            return x * 2

        # Create separate pipeline instances for concurrent execution
        async def run_pipeline(data: List[int]) -> List[int]:
            pipeline = r.Map[int, int](slow_multiply) | r.Filter[int](lambda x: x > 5)
            return await (data | pipeline).collect()

        # Run pipelines concurrently with different datasets
        tasks = [
            asyncio.create_task(run_pipeline([1, 2, 3, 4, 5])),
            asyncio.create_task(run_pipeline([6, 7, 8, 9, 10])),
            asyncio.create_task(run_pipeline([0, 1, 2])),
        ]

        results = await asyncio.gather(*tasks)

        expected_results = [
            [6, 8, 10],  # [1,2,3,4,5] -> [2,4,6,8,10] -> [6,8,10]
            [12, 14, 16, 18, 20],  # [6,7,8,9,10] -> [12,14,16,18,20] -> all pass filter
            [],  # [0,1,2] -> [0,2,4] -> none pass filter
        ]

        assert results == expected_results

    @pytest.mark.asyncio
    async def test_high_concurrency_processing(self):
        """Test pipeline with high concurrency levels."""

        async def variable_delay_multiply(x: int) -> int:
            # Shorter random delay to create realistic async conditions
            await asyncio.sleep(random.uniform(0.001, 0.005))
            return x * 3

        # Smaller dataset for faster tests but still meaningful
        data = list(range(20))
        pipeline = r.Map[int, int](variable_delay_multiply) | r.Filter[int](
            lambda x: x % 2 == 0
        )

        result = await (data | pipeline).collect()

        # Verify correctness despite high concurrency
        expected = [x * 3 for x in data if (x * 3) % 2 == 0]
        assert sorted(result) == sorted(expected)

    @pytest.mark.asyncio
    async def test_concurrent_error_handling(self):
        """Test error handling under concurrent conditions."""
        error_occurred = []

        async def failing_operation(x: int) -> int:
            await asyncio.sleep(0.001)
            if x == 5:
                error_occurred.append(x)
                raise ValueError(f"Intentional error at {x}")
            return x * 2

        # Test with IGNORE policy to continue despite errors
        pipeline_ignore = r.Pipeline(
            [r.Map[int, int](failing_operation)], error_policy=ErrorPolicy.IGNORE
        )

        # Multiple concurrent runs
        async def safe_run(data: List[int], policy: ErrorPolicy):
            try:
                if policy == ErrorPolicy.IGNORE:
                    return await pipeline_ignore.collect(data)
                else:
                    pipeline_fail = r.Pipeline(
                        [r.Map[int, int](failing_operation)],
                        error_policy=ErrorPolicy.FAIL_FAST,
                    )
                    return await pipeline_fail.collect(data)
            except Exception:
                return "ERROR"

        tasks = [
            asyncio.create_task(
                safe_run([1, 2, 3], ErrorPolicy.IGNORE)
            ),  # Should succeed
            asyncio.create_task(
                safe_run([4, 5, 6], ErrorPolicy.FAIL_FAST)
            ),  # Should fail at 5
            asyncio.create_task(
                safe_run([7, 8, 9], ErrorPolicy.IGNORE)
            ),  # Should succeed
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Verify mix of success and failure
        success_count = sum(1 for r in results if isinstance(r, list))
        error_count = sum(
            1 for r in results if r == "ERROR" or isinstance(r, Exception)
        )

        assert success_count >= 1
        assert error_count >= 1

    @pytest.mark.asyncio
    async def test_concurrent_stateful_operations(self):
        """Test concurrent access to stateful operations like sort."""

        async def async_identity(x: int) -> int:
            await asyncio.sleep(0.001)
            return x

        # Multiple concurrent pipelines using sort (stateful operation)
        async def run_sort_pipeline(data: List[int]) -> List[int]:
            pipeline = r.Map[int, int](async_identity) | r.Sort(reverse=True)
            return await (data | pipeline).collect()

        tasks = [
            asyncio.create_task(run_sort_pipeline([3, 1, 4, 1, 5])),
            asyncio.create_task(run_sort_pipeline([9, 2, 6, 5, 3])),
            asyncio.create_task(run_sort_pipeline([8, 7, 6])),
        ]

        results = await asyncio.gather(*tasks)

        expected_results = [[5, 4, 3, 1, 1], [9, 6, 5, 3, 2], [8, 7, 6]]
        assert results == expected_results


class TestStreamReaderWriterConcurrency:
    """Test the new StreamReader/Writer architecture under concurrent conditions."""

    @pytest.mark.asyncio
    async def test_stream_reader_writer_separation(self):
        """Test that StreamReader and StreamWriter work correctly in concurrent scenarios."""
        stream = Stream[int](max_size=50)

        async def producer():
            writer = await stream.writer()
            for i in range(10):
                await writer.write(StreamItemEvent(item=i * 10, index=Index(i)))
                await asyncio.sleep(0.001)  # Small delay to interleave
            await writer.complete()

        async def consumer():
            reader = await stream.reader()
            results = []
            async for event in reader:
                if isinstance(event, StreamItemEvent):
                    results.append(event.item)
                await asyncio.sleep(0.0005)  # Smaller delay for consumer
            return results

        # Start producer and consumer concurrently
        producer_task = asyncio.create_task(producer())
        consumer_task = asyncio.create_task(consumer())

        await producer_task
        results = await consumer_task

        # Should have consumed all items in order
        expected = [i * 10 for i in range(10)]
        assert results == expected

    @pytest.mark.asyncio
    async def test_stream_cancellation_propagation(self):
        """Test that stream cancellation propagates correctly."""
        stream = Stream[int](max_size=100)

        async def slow_producer():
            writer = await stream.writer()
            try:
                for i in range(100):  # More items than we'll consume
                    await writer.write(StreamItemEvent(item=i, index=Index(i)))
                    await asyncio.sleep(0.01)  # Slow production
                await writer.complete()
            except asyncio.CancelledError:
                pass  # Expected when cancelled

        async def limited_consumer():
            reader = await stream.reader()
            results = []
            count = 0
            async for event in reader:
                if isinstance(event, StreamItemEvent):
                    results.append(event.item)
                    count += 1
                    if count >= 5:  # Only consume 5 items
                        await stream.cancel()
                        break
            return results

        # Start both tasks
        producer_task = asyncio.create_task(slow_producer())
        consumer_task = asyncio.create_task(limited_consumer())

        results = await consumer_task
        producer_task.cancel()  # Clean up

        # Should have exactly 5 items
        assert len(results) == 5
        assert results == [0, 1, 2, 3, 4]


class TestIndexOrderingConcurrency:
    """Test Index ordering under concurrent conditions in new architecture."""

    @pytest.mark.asyncio
    async def test_index_ordering_under_concurrency(self):
        """Test that indexes remain correctly ordered under concurrent processing."""

        async def variable_delay_identity(x: int) -> int:
            # Random delays to ensure items complete out of order
            delay = random.uniform(0.001, 0.005)
            await asyncio.sleep(delay)
            return x

        # Process items that will complete in random order
        data = list(range(15))  # Smaller dataset for faster tests
        pipeline = r.Map[int, int](variable_delay_identity)

        result = await (data | pipeline).collect()

        # Despite random completion order, result should be in original order
        assert result == data

    @pytest.mark.asyncio
    async def test_hierarchical_index_preservation(self):
        """Test hierarchical index preservation in concurrent flat_map."""

        async def async_duplicate(x: int) -> List[int]:
            await asyncio.sleep(random.uniform(0.001, 0.003))
            return [x, x + 100]

        pipeline = r.FlatMap[int, int](async_duplicate) | r.Sort()
        result = await ([3, 1, 4] | pipeline).collect()

        # Should be sorted: [1, 3, 4, 101, 103, 104]
        expected = [1, 3, 4, 101, 103, 104]
        assert result == expected


class TestTaskGroupFallback:
    """Test TaskGroup fallback implementation for the new architecture."""

    @pytest.mark.asyncio
    async def test_taskgroup_creates_tasks(self):
        """Test that TaskGroup can create and manage tasks in new architecture."""
        results = []

        async def test_task(value: int):
            await asyncio.sleep(0.001)
            results.append(value * 2)

        async with CompatTaskGroup() as tg:
            tg.create_task(test_task(1))
            tg.create_task(test_task(2))
            tg.create_task(test_task(3))

        # All tasks should complete
        assert sorted(results) == [2, 4, 6]

    @pytest.mark.asyncio
    async def test_pipeline_with_taskgroup_fallback(self):
        """Test pipeline functionality with TaskGroup (tests fallback on older Python)."""

        async def async_multiply(x: int) -> int:
            await asyncio.sleep(0.001)
            return x * 3

        # This tests that pipelines work regardless of TaskGroup implementation
        pipeline = r.Map[int, int](async_multiply) | r.Filter[int](lambda x: x > 10)
        result = await ([1, 2, 3, 4, 5, 6] | pipeline).collect()

        expected = [12, 15, 18]  # [3,6,9,12,15,18] -> [12,15,18]
        assert result == expected


class TestStreamEventConcurrency:
    """Test the new StreamEvent system under concurrent conditions."""

    @pytest.mark.asyncio
    async def test_concurrent_stream_events(self):
        """Test that StreamItemEvent and StreamErrorEvent work correctly under concurrency."""

        async def mixed_processor(x: int) -> int:
            await asyncio.sleep(0.001)
            if x == 7:  # Specific value to create controlled error
                raise ValueError(f"Error processing {x}")
            return x * 2

        # Use COLLECT policy to capture both items and errors
        pipeline = r.Pipeline(
            [r.Map[int, int](mixed_processor)], error_policy=ErrorPolicy.COLLECT
        )

        data = [1, 2, 3, 7, 8, 9]  # Include error-inducing value
        result, errors = await pipeline.collect_with_errors(data)

        # Should have processed all non-error items
        expected_items = [2, 4, 6, 16, 18]  # 7 causes error, so 14 is missing
        assert sorted(result) == sorted(expected_items)

        # Should have exactly one error
        assert len(errors) == 1
        assert "Error processing 7" in str(errors[0])

    @pytest.mark.asyncio
    async def test_stream_context_manager(self):
        """Test the new StreamPipelineResult context manager under concurrent access."""

        async def slow_transform(x: int) -> int:
            await asyncio.sleep(0.001)
            return x + 10

        pipeline = r.Pipeline([r.Map[int, int](slow_transform)])
        data = [1, 2, 3, 4, 5]

        # Test that context manager works correctly
        async with await pipeline.run(data) as stream_result:
            results = []
            async for event in stream_result:
                if isinstance(event, StreamItemEvent):
                    results.append(event.item)

        expected = [11, 12, 13, 14, 15]
        assert sorted(results) == sorted(expected)


if __name__ == "__main__":
    # Run a basic test for quick validation
    async def quick_test():
        pipeline = r.Map[int, int](lambda x: x * 2)
        result = await ([1, 2, 3] | pipeline).collect()
        assert result == [2, 4, 6]
        print("✅ Quick concurrency test passed!")

    asyncio.run(quick_test())
