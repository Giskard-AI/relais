import asyncio
import pytest
import relais as r


async def test_chained_operations():
    """Test: [3, 1, 4, 2] | r.Sort() | r.Map(lambda x: x * 2) | r.Batch(2)"""
    pipeline = [3, 1, 4, 2] | r.Sort() | r.Map(lambda x: x * 2) | r.Batch(2)
    result = await pipeline.collect()
    assert result == [[2, 4], [6, 8]]


async def test_pipeline_as_step():
    """Test using a pipeline as a step in another pipeline"""
    # First pipeline that sums a range
    pipeline1 = (
        range(3) | r.Map(lambda x: x * 2) | r.Reduce(lambda acc, x: acc + x, initial=0)
    )

    # Second pipeline that generates a range from a number
    pipeline2 = (lambda n: range(n)) | r.Map(lambda x: x + 1)

    # Compose them
    pipeline3 = pipeline1 | pipeline2
    result = await pipeline3.collect()
    assert result == [1, 2, 3, 4, 5, 6]  # range(6) + 1 each


async def test_stream_method_exists():
    """Test that stream method works"""
    pipeline = range(3) | r.Map(lambda x: x * 2)

    results = []
    async for item in pipeline.stream():
        results.append(item)

    assert results == [0, 2, 4]


async def test_concurrent_processing():
    """Test that async operations are processed concurrently"""
    start_time = asyncio.get_event_loop().time()

    async def slow_operation(x):
        await asyncio.sleep(0.1)  # 100ms delay
        return x * 2

    # If processed sequentially, this would take ~500ms
    # If concurrent, should be much faster
    pipeline = range(5) | r.Map(slow_operation)
    result = await pipeline.collect()

    end_time = asyncio.get_event_loop().time()
    elapsed = end_time - start_time

    assert result == [0, 2, 4, 6, 8]
    # Should complete in roughly 100ms (concurrent) rather than 500ms (sequential)
    assert elapsed < 0.3  # Allow some margin but should be much less than 500ms


async def test_sync_function_error():
    """Test error handling with sync functions"""

    def failing_func(x):
        if x == 2:
            raise ValueError("Test error")
        return x * 2

    pipeline = range(4) | r.Map(failing_func)

    with pytest.raises(ValueError, match="Test error"):
        await pipeline.collect()


async def test_async_function_error():
    """Test error handling with async functions"""

    async def failing_async_func(x):
        await asyncio.sleep(0.001)
        if x == 2:
            raise ValueError("Async test error")
        return x * 2

    pipeline = range(4) | r.Map(failing_async_func)

    with pytest.raises(ValueError, match="Async test error"):
        await pipeline.collect() 