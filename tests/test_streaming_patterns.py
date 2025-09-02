"""Test streaming patterns and advanced pipeline usage in the new architecture."""

import asyncio
from typing import Any, cast

import pytest

import relais as r
from relais.errors import ErrorPolicy
from relais.stream import StreamItemEvent


class AsyncDataSource:
    """Helper class to simulate various data source patterns."""

    def __init__(self, items, delay=0.001):
        self.items = items
        self.delay = delay
        self.index = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.index >= len(self.items):
            raise StopAsyncIteration

        item = self.items[self.index]
        self.index += 1

        if self.delay > 0:
            await asyncio.sleep(self.delay)

        return item


class TestStreamingConsumption:
    """Test streaming consumption patterns."""

    @pytest.mark.asyncio
    async def test_basic_streaming(self):
        """Test basic streaming consumption."""
        data = list(range(10))
        pipeline = r.Map[int, int](lambda x: x * 2)

        results = []
        async for item in (data | pipeline).stream():
            results.append(item)

        assert len(results) == 10
        assert sorted(results) == [i * 2 for i in range(10)]

    @pytest.mark.asyncio
    async def test_streaming_with_async_source(self):
        """Test streaming with async data source."""
        source = AsyncDataSource([1, 2, 3, 4, 5], delay=0.001)
        pipeline = r.Map[int, int](lambda x: x**2)

        results = []
        async for item in (source | pipeline).stream():
            results.append(item)

        assert results == [1, 4, 9, 16, 25]

    @pytest.mark.asyncio
    async def test_streaming_with_early_break(self):
        """Test streaming consumption with early break."""
        source = AsyncDataSource(list(range(100)), delay=0.001)
        pipeline = r.Map[int, int](lambda x: x * 3)

        results = []
        async for item in (source | pipeline).stream():
            results.append(item)
            if len(results) >= 5:
                break

        assert len(results) == 5
        assert results == [0, 3, 6, 9, 12]

    @pytest.mark.asyncio
    async def test_streaming_with_filters(self):
        """Test streaming with filtering operations."""
        data = list(range(20))
        pipeline = (
            r.Map[int, int](lambda x: x * 2)
            | r.Filter[int](lambda x: x > 10)
            | r.Take(5)
        )

        results = []
        async for item in (data | pipeline).stream():
            results.append(item)

        assert len(results) == 5
        assert results == [12, 14, 16, 18, 20]  # First 5 items > 10

    @pytest.mark.asyncio
    async def test_streaming_with_errors_ignore_policy(self):
        """Test streaming with error handling (IGNORE policy)."""

        def failing_transform(x):
            if x == 3:
                raise ValueError(f"Error at {x}")
            return x * 2

        data = list(range(8))
        pipeline = r.Pipeline(
            [r.Map(failing_transform)], error_policy=ErrorPolicy.IGNORE
        )

        results = []
        async for item in pipeline.stream(data):
            results.append(item)

        # Should have all items except the failed one
        expected = [0, 2, 4, 8, 10, 12, 14]  # Missing 6 (3*2)
        assert sorted(results) == expected

    @pytest.mark.asyncio
    async def test_streaming_with_errors_collect_policy(self):
        """Test streaming with error collection."""

        def failing_transform(x):
            if x in [2, 5]:
                raise ValueError(f"Error at {x}")
            return x * 2

        data = list(range(8))
        pipeline = r.Pipeline(
            [r.Map(failing_transform)], error_policy=ErrorPolicy.COLLECT
        )

        results = []
        errors = []

        async for item in pipeline.stream_with_errors(data):
            if isinstance(item, Exception):  # Error item
                errors.append(item)
            else:  # Successful item
                results.append(item)

        # Should have successful items and errors
        expected_results = [0, 2, 6, 8, 12, 14]  # Missing 4 and 10
        assert sorted(results) == expected_results
        assert len(errors) == 2


class TestContextManagerPatterns:
    """Test context manager usage patterns."""

    @pytest.mark.asyncio
    async def test_pipeline_context_manager(self):
        """Test pipeline as context manager."""
        data = list(range(10))
        pipeline = r.Map[int, int](lambda x: x**2) | r.Filter[int](lambda x: x > 20)

        results = []
        async with await pipeline.run(data) as stream:
            async for event in stream:
                if isinstance(event, StreamItemEvent):
                    results.append(event.item)

        assert results == [25, 36, 49, 64, 81]  # 5^2, 6^2, 7^2, 8^2, 9^2

    @pytest.mark.asyncio
    async def test_context_manager_with_async_source(self):
        """Test context manager with async data source."""
        source = AsyncDataSource([1, 2, 3, 4, 5, 6], delay=0.001)
        pipeline = r.Map[int, int](lambda x: x * 3) | r.Take(4)

        results = []
        async with await pipeline.run(source) as stream:
            async for event in stream:
                if isinstance(event, StreamItemEvent):
                    results.append(event.item)

        assert len(results) == 4
        assert results == [3, 6, 9, 12]

    @pytest.mark.asyncio
    async def test_context_manager_early_exit(self):
        """Test context manager with early exit."""
        source = AsyncDataSource(list(range(100)), delay=0.001)
        pipeline = r.Pipeline([r.Map(lambda x: x * 2)])

        results = []
        async with await pipeline.run(source) as stream:
            async for event in stream:
                if isinstance(event, StreamItemEvent):
                    results.append(event.item)
                    if len(results) >= 3:
                        break  # Early exit

        # Context manager should clean up properly
        assert len(results) == 3
        assert results == [0, 2, 4]

    @pytest.mark.asyncio
    async def test_context_manager_exception_handling(self):
        """Test context manager with exceptions."""

        def failing_transform(x):
            if x == 2:
                raise RuntimeError("Intentional error")
            return x * 2

        data = [0, 1, 2, 3, 4]
        pipeline = r.Pipeline(
            [r.Map(failing_transform)], error_policy=ErrorPolicy.FAIL_FAST
        )

        with pytest.raises(Exception):  # Should propagate the error
            async with await pipeline.run(data) as stream:
                async for event in stream:
                    pass  # Just consume


class TestAdvancedPipelinePatterns:
    """Test advanced pipeline usage patterns."""

    @pytest.mark.asyncio
    async def test_fan_out_pattern(self):
        """Test fan-out pattern using multiple pipelines on same data."""
        data = list(range(10))

        # Create multiple pipelines
        pipeline1 = r.Map[int, int](lambda x: x * 2) | r.Take(5)
        pipeline2 = r.Map[int, int](lambda x: x**2) | r.Filter[int](lambda x: x > 20)
        pipeline3 = r.Filter[int](lambda x: x % 2 == 0) | r.Map[int, int](
            lambda x: x + 100
        )

        # Run them concurrently
        results = await asyncio.gather(
            (data | pipeline1).collect(),
            (data | pipeline2).collect(),
            (data | pipeline3).collect(),
        )

        assert results[0] == [0, 2, 4, 6, 8]  # First 5 doubled
        assert results[1] == [25, 36, 49, 64, 81]  # Squares > 20
        assert results[2] == [100, 102, 104, 106, 108]  # Even numbers + 100

    @pytest.mark.asyncio
    async def test_conditional_processing(self):
        """Test conditional processing patterns."""

        async def conditional_transform(x):
            await asyncio.sleep(0.001)  # Simulate async work
            if x < 5:
                return x * 2
            else:
                return x * 3

        data = list(range(10))
        pipeline = r.Map(conditional_transform) | r.Sort()

        result = await (data | pipeline).collect()

        # Expected: [0,2,4,6,8] (x<5) + [15,18,21,24,27] (x>=5) sorted
        expected = sorted([0, 2, 4, 6, 8, 15, 18, 21, 24, 27])
        assert result == expected

    @pytest.mark.asyncio
    async def test_batched_processing(self):
        """Test batched processing patterns."""
        data = list(range(20))

        def process_batch(batch):
            return sum(batch)  # Sum each batch

        pipeline = r.Batch(5) | r.Map(process_batch)
        result = await (data | pipeline).collect()

        # Should have 4 batches: [0,1,2,3,4], [5,6,7,8,9], [10,11,12,13,14], [15,16,17,18,19]
        expected = [10, 35, 60, 85]  # Sums of each batch
        assert result == expected

    @pytest.mark.asyncio
    async def test_complex_transformation_chain(self):
        """Test complex transformation chains."""

        async def fetch_data(x):
            await asyncio.sleep(0.001)
            return {"id": x, "value": x * 10}

        async def enrich_data(item):
            await asyncio.sleep(0.001)
            return {**item, "enriched": item["value"] > 50}

        def format_result(item):
            return (
                f"ID:{item['id']}, Value:{item['value']}, Enriched:{item['enriched']}"
            )

        data = list(range(10))
        pipeline = (
            r.Map(fetch_data)
            | r.Map(enrich_data)
            | r.Filter[dict[str, Any]](lambda x: cast(bool, x["enriched"]))
            | r.Map(format_result)
            | r.Take(3)
        )

        result = await (data | pipeline).collect()

        assert len(result) == 3
        # Should include items with value > 50 (i.e., id >= 6)
        assert all("Enriched:True" in item for item in result)
        assert "ID:6" in result[0]


class TestErrorRecoveryPatterns:
    """Test error recovery and resilience patterns."""

    @pytest.mark.asyncio
    async def test_retry_pattern(self):
        """Test retry pattern implementation."""
        call_count = {}

        async def flaky_operation(x):
            if x not in call_count:
                call_count[x] = 0
            call_count[x] += 1

            # Fail on first attempt for certain values
            if x in [2, 5] and call_count[x] == 1:
                raise RuntimeError(f"Temporary failure for {x}")

            return x * 2

        async def retry_wrapper(x: int) -> int:
            for attempt in range(3):  # Up to 3 attempts
                try:
                    return await flaky_operation(x)
                except RuntimeError:
                    if attempt == 2:  # Last attempt
                        raise
                    await asyncio.sleep(0.001)  # Brief delay before retry
            raise ValueError(f"Failed to retry for {x}")

        data = list(range(8))
        pipeline = r.Pipeline([r.Map(retry_wrapper)])

        result = await (data | pipeline).collect()

        # All items should succeed after retries
        expected = [i * 2 for i in range(8)]
        assert sorted(result) == expected

        # Verify retry attempts
        assert call_count[2] >= 2  # Should have retried
        assert call_count[5] >= 2  # Should have retried

    @pytest.mark.asyncio
    async def test_circuit_breaker_pattern(self):
        """Test circuit breaker pattern."""
        failure_count = 0

        async def unreliable_service(x):
            nonlocal failure_count

            # Simulate service that fails after some successes
            if x > 3:
                failure_count += 1
                if failure_count > 2:  # Circuit opens after 2 failures
                    raise RuntimeError("Circuit breaker open")
                raise RuntimeError(f"Service failure {failure_count}")

            return x * 10

        data = list(range(8))
        pipeline = r.Pipeline(
            [r.Map(unreliable_service)], error_policy=ErrorPolicy.IGNORE
        )

        result = await pipeline.collect(data)

        # Should only have results from before circuit opened
        assert len(result) <= 4  # First few items before failures
        assert all(x in [0, 10, 20, 30] for x in result)

    @pytest.mark.asyncio
    async def test_graceful_degradation(self):
        """Test graceful degradation patterns."""

        async def primary_service(x):
            if x > 5:  # Primary service fails for larger values
                raise RuntimeError("Primary service unavailable")
            return {"source": "primary", "value": x * 100}

        async def fallback_service(x):
            await asyncio.sleep(0.001)  # Slightly slower
            return {"source": "fallback", "value": x * 10}

        async def resilient_transform(x):
            try:
                return await primary_service(x)
            except RuntimeError:
                return await fallback_service(x)

        data = list(range(10))
        pipeline = r.Map(resilient_transform)

        result = await (data | pipeline).collect()

        assert len(result) == 10

        # Check that primary was used for small values
        primary_results = [r for r in result if r["source"] == "primary"]
        fallback_results = [r for r in result if r["source"] == "fallback"]

        assert len(primary_results) == 6  # 0-5
        assert len(fallback_results) == 4  # 6-9

        # Verify values
        assert all(r["value"] == i * 100 for i, r in enumerate(primary_results))
        assert all(r["value"] in [60, 70, 80, 90] for r in fallback_results)


if __name__ == "__main__":
    # Run a quick validation test
    async def quick_test():
        # Test basic streaming
        data = [1, 2, 3, 4, 5]
        pipeline = r.Pipeline([r.Map[int, int](lambda x: x * 2)])

        results = []
        async for item in (data | pipeline).stream():
            results.append(item)

        assert results == [2, 4, 6, 8, 10]

        # Test context manager
        async with await pipeline.run(data) as stream:
            ctx_results = []
            async for event in stream:
                if isinstance(event, StreamItemEvent):
                    ctx_results.append(event.item)

        assert sorted(ctx_results) == [2, 4, 6, 8, 10]

        print("✅ Streaming patterns tests validated!")

    asyncio.run(quick_test())
