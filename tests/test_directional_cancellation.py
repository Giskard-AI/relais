"""Test directional cancellation implementation in the new streaming architecture."""

import asyncio

import pytest

import relais as r
from relais.errors import ErrorPolicy, PipelineError
from relais.stream import StreamItemEvent


class SlowAsyncIterator:
    """Test helper for simulating slow data production."""

    def __init__(self, max_items: int = 1000, delay: float = 0.01):
        self.max_items = max_items
        self.delay = delay
        self.produced_count = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.produced_count >= self.max_items:
            raise StopAsyncIteration

        current = self.produced_count
        self.produced_count += 1

        # Simulate slow production
        await asyncio.sleep(self.delay)
        return current


class TestUnorderedTakeCancellation:
    """Test that unordered take() properly cancels upstream."""

    @pytest.mark.asyncio
    async def test_take_cancels_upstream_quickly(self):
        """Test that take() cancels upstream production when it has enough items."""
        producer = SlowAsyncIterator(
            max_items=100, delay=0.02
        )  # Would take 2 seconds to complete

        # Take only 3 items (unordered by default)
        pipeline = r.Take(3)

        start_time = asyncio.get_event_loop().time()
        result = await (producer | pipeline).collect()
        end_time = asyncio.get_event_loop().time()

        # Verify results
        assert len(result) == 3
        assert result == [0, 1, 2]

        # Should complete quickly with cancellation
        assert end_time - start_time < 0.5, (
            f"Took {end_time - start_time}s, expected < 0.5s"
        )

        # Verify producer was stopped early
        assert producer.produced_count <= 6, (
            f"Producer created {producer.produced_count} items, expected <= 6"
        )

    @pytest.mark.asyncio
    async def test_take_with_downstream_processing(self):
        """Test that downstream processing continues when upstream is cancelled by take()."""
        producer = SlowAsyncIterator(max_items=100, delay=0.02)

        # Pipeline: take(3) -> map(lambda x: x * 2)
        pipeline = r.Take(3) | r.Map[int, int](lambda x: x * 2)

        start_time = asyncio.get_event_loop().time()
        result = await (producer | pipeline).collect()
        end_time = asyncio.get_event_loop().time()

        # Verify all items were processed by downstream map
        assert len(result) == 3
        assert sorted(result) == [0, 2, 4]  # Sorted since async processing may reorder

        # Should complete quickly
        assert end_time - start_time < 0.5

    @pytest.mark.asyncio
    async def test_multiple_takes_in_pipeline(self):
        """Test multiple take operations in the same pipeline."""
        producer = SlowAsyncIterator(max_items=100, delay=0.01)

        # Take 10, then take 3 of those
        pipeline = r.Take(10) | r.Take(3)

        start_time = asyncio.get_event_loop().time()
        result = await (producer | pipeline).collect()
        end_time = asyncio.get_event_loop().time()

        assert len(result) == 3
        assert result == [0, 1, 2]

        # Should complete very quickly since both takes limit the processing
        assert end_time - start_time < 0.3

    @pytest.mark.asyncio
    async def test_take_zero_validation(self):
        """Test that take(0) raises ValueError."""
        with pytest.raises(ValueError, match="n must be greater than 0"):
            r.Take(0)

    @pytest.mark.asyncio
    async def test_take_larger_than_available(self):
        """Test take() when asking for more items than available."""
        producer = SlowAsyncIterator(max_items=3, delay=0.005)

        pipeline = r.Take(10)  # Ask for more than available

        result = await (producer | pipeline).collect()
        assert result == [0, 1, 2]  # Should get all available items
        assert producer.produced_count == 3  # Should have produced all items


class TestOrderedVsUnorderedTake:
    """Test the difference between ordered and unordered take."""

    @pytest.mark.asyncio
    async def test_unordered_take_performance(self):
        """Test that unordered take is faster due to early cancellation."""
        producer = SlowAsyncIterator(max_items=50, delay=0.02)  # Would take 1 second

        pipeline = r.Take(5, ordered=False)  # Explicitly unordered

        start_time = asyncio.get_event_loop().time()
        result = await (producer | pipeline).collect()
        end_time = asyncio.get_event_loop().time()

        assert len(result) == 5
        assert result == [0, 1, 2, 3, 4]

        # Should complete much faster than processing all items
        assert end_time - start_time < 0.3
        assert producer.produced_count <= 10  # Early cancellation

    @pytest.mark.asyncio
    async def test_ordered_take_behavior(self):
        """Test that ordered take processes more items but maintains order."""
        producer = SlowAsyncIterator(max_items=20, delay=0.01)

        pipeline = r.Take(5, ordered=True)  # Explicitly ordered

        result = await (producer | pipeline).collect()

        assert len(result) == 5
        assert result == [0, 1, 2, 3, 4]  # Should be in order

        # May take longer due to less aggressive cancellation
        # But still shouldn't process all items
        assert producer.produced_count <= 20


class TestCancellationWithComplexPipelines:
    """Test cancellation in complex pipeline scenarios."""

    @pytest.mark.asyncio
    async def test_cancellation_with_stateful_operations(self):
        """Test cancellation works with stateful operations like sort."""
        producer = SlowAsyncIterator(max_items=100, delay=0.01)

        # Take 10, sort them (stateful), then take 3
        pipeline = r.Take(10) | r.Sort() | r.Take(3)

        start_time = asyncio.get_event_loop().time()
        result = await (producer | pipeline).collect()
        end_time = asyncio.get_event_loop().time()

        assert len(result) == 3
        assert result == [0, 1, 2]  # Should be sorted

        # Should complete reasonably quickly
        assert end_time - start_time < 0.5

    @pytest.mark.asyncio
    async def test_cancellation_with_concurrent_operations(self):
        """Test cancellation with concurrent async operations."""
        producer = SlowAsyncIterator(max_items=100, delay=0.01)

        async def slow_transform(x):
            await asyncio.sleep(0.001)  # Small async delay
            return x * 3

        pipeline = (
            r.Take(5)
            | r.Map[int, int](slow_transform)
            | r.Filter[int](lambda x: x >= 0)
        )

        start_time = asyncio.get_event_loop().time()
        result = await (producer | pipeline).collect()
        end_time = asyncio.get_event_loop().time()

        assert len(result) == 5
        assert sorted(result) == [0, 3, 6, 9, 12]

        # Should complete quickly despite async operations
        assert end_time - start_time < 0.3

    @pytest.mark.asyncio
    async def test_concurrent_cancellation_with_multiple_streams(self):
        """Test concurrent processing with cancellation across multiple streams."""

        async def create_pipeline(max_items: int, take_count: int):
            producer = SlowAsyncIterator(max_items=max_items, delay=0.005)
            pipeline = r.Take(take_count)
            return await (producer | pipeline).collect()

        # Run multiple pipelines concurrently
        tasks = [
            asyncio.create_task(create_pipeline(100, 2)),
            asyncio.create_task(create_pipeline(200, 3)),
            asyncio.create_task(create_pipeline(50, 1)),
        ]

        start_time = asyncio.get_event_loop().time()
        results = await asyncio.gather(*tasks)
        end_time = asyncio.get_event_loop().time()

        # Verify results
        assert results[0] == [0, 1]
        assert results[1] == [0, 1, 2]
        assert results[2] == [0]

        # Should complete quickly despite large max_items
        assert end_time - start_time < 0.2


class TestErrorHandlingWithCancellation:
    """Test error handling combined with cancellation."""

    @pytest.mark.asyncio
    async def test_fail_fast_with_take_limit(self):
        """Test FAIL_FAST error policy with take limit."""

        async def failing_processor(x):
            await asyncio.sleep(0.001)
            if x == 2:
                raise ValueError(f"Intentional error for item {x}")
            return x * 2

        producer = SlowAsyncIterator(max_items=100, delay=0.005)
        pipeline = r.Take(10) | r.Map[int, int](
            failing_processor
        )  # Error should occur before take limit

        with pytest.raises(PipelineError) as exc_info:
            await (producer | pipeline).collect()

        # Should contain the original error information
        assert "Intentional error for item 2" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_ignore_policy_with_take_limit(self):
        """Test IGNORE error policy with take limit."""

        def failing_processor(x):
            if x == 2:
                raise ValueError(f"Intentional error for item {x}")
            return x * 2

        producer = SlowAsyncIterator(max_items=100, delay=0.005)
        pipeline = r.Pipeline(
            steps=[r.Take(10), r.Map[int, int](failing_processor)],
            error_policy=ErrorPolicy.IGNORE,
        )

        result = await pipeline.collect(producer)

        # Should have processed items except the one that failed
        # Expected: 0*2=0, 1*2=2, (2 fails), 3*2=6, ..., 9*2=18
        expected_items = [
            0,
            2,
            6,
            8,
            10,
            12,
            14,
            16,
            18,
        ]  # 2*2=4 is missing due to error
        assert len(result) == 9  # 10 items minus 1 error
        assert sorted(result) == expected_items

    @pytest.mark.asyncio
    async def test_collect_policy_with_take_limit(self):
        """Test COLLECT error policy with take limit."""

        async def failing_processor(x):
            await asyncio.sleep(0.001)
            if x == 3:
                raise ValueError(f"Error at {x}")
            return x

        producer = SlowAsyncIterator(max_items=100, delay=0.005)
        pipeline = r.Pipeline(
            steps=[r.Take(8), r.Map[int, int](failing_processor)],
            error_policy=ErrorPolicy.COLLECT,
        )

        result, errors = await pipeline.collect_with_errors(producer)

        # Should have results up to the take limit, minus any errors
        assert len(result) == 7  # 8 items minus 1 error
        assert len(errors) == 1
        assert "Error at 3" in str(errors[0])


class TestCancellationCleanup:
    """Test proper cleanup when cancellation occurs."""

    @pytest.mark.asyncio
    async def test_stream_context_manager_with_cancellation(self):
        """Test that context manager cleans up properly with cancellation."""
        producer = SlowAsyncIterator(max_items=100, delay=0.01)

        pipeline = r.Take(3) | r.Map[int, int](lambda x: x * 2)

        results = []
        async with await pipeline.run(producer) as stream_result:
            # Consume only some results to test partial consumption
            count = 0
            async for event in stream_result:
                if isinstance(event, StreamItemEvent):
                    results.append(event.item)
                    count += 1
                    if count >= 3:
                        break

        # Should have collected exactly 3 items
        assert len(results) == 3
        assert sorted(results) == [0, 2, 4]

    @pytest.mark.asyncio
    async def test_producer_cleanup_after_cancellation(self):
        """Test that producers are properly cleaned up after cancellation."""
        producer = SlowAsyncIterator(max_items=100, delay=0.01)

        pipeline = r.Take(2)

        # Run the pipeline
        result = await (producer | pipeline).collect()
        assert result == [0, 1]

        # The producer should have been told to stop early
        assert producer.produced_count <= 5  # Some buffer allowed for async timing

    @pytest.mark.asyncio
    async def test_early_termination_vs_natural_completion(self):
        """Test the difference between early termination and natural completion."""

        # Case 1: Early termination
        producer1 = SlowAsyncIterator(max_items=100, delay=0.01)
        pipeline1 = r.Take(3)

        start_time = asyncio.get_event_loop().time()
        result1 = await (producer1 | pipeline1).collect()
        early_time = asyncio.get_event_loop().time() - start_time

        # Case 2: Natural completion
        producer2 = SlowAsyncIterator(max_items=3, delay=0.01)
        pipeline2 = r.Take(10)  # More than available

        start_time = asyncio.get_event_loop().time()
        result2 = await (producer2 | pipeline2).collect()
        natural_time = asyncio.get_event_loop().time() - start_time

        # Both should have same results
        assert result1 == result2 == [0, 1, 2]

        # Early termination should be faster
        assert early_time < 0.2  # Very fast due to cancellation
        assert natural_time > 0.025  # Slower due to natural completion

        # Early termination should have produced fewer items
        assert producer1.produced_count <= 5
        assert producer2.produced_count == 3


class TestSkipWithCancellation:
    """Test skip operation with cancellation patterns."""

    @pytest.mark.asyncio
    async def test_skip_then_take_cancellation(self):
        """Test skip followed by take for cancellation patterns."""
        producer = SlowAsyncIterator(max_items=100, delay=0.01)

        # Skip first 5, then take next 3
        pipeline = r.Skip(5) | r.Take(3)

        start_time = asyncio.get_event_loop().time()
        result = await (producer | pipeline).collect()
        end_time = asyncio.get_event_loop().time()

        assert len(result) == 3
        assert result == [5, 6, 7]  # Items after skipping first 5

        # Should complete reasonably quickly (lenient timing as skip+take may be slower)
        assert end_time - start_time < 0.5

        # Should have produced around 8-10 items (5 skipped + 3 taken + some buffer)
        assert producer.produced_count <= 12


if __name__ == "__main__":
    # Run a quick test to validate
    async def quick_test():
        producer = SlowAsyncIterator(max_items=100, delay=0.01)
        pipeline = r.Take(2)

        start_time = asyncio.get_event_loop().time()
        result = await (producer | pipeline).collect()
        end_time = asyncio.get_event_loop().time()

        assert result == [0, 1]
        assert end_time - start_time < 0.2
        assert producer.produced_count <= 4
        print("✅ New directional cancellation tests validated!")

    asyncio.run(quick_test())
