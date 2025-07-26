import asyncio
import random
import pytest

import relais as r


async def test_simple_map_with_collect():
    """Test: range(3) | r.Map(lambda x: x * 2) with collect()"""
    pipeline = range(3) | r.Map(lambda x: x * 2) | r.Map(lambda x: x + 1)
    assert isinstance(pipeline, r.Pipeline)
    result = await pipeline.collect()
    assert result == [1, 3, 5]


async def test_simple_map_with_runtime_argument():
    pipeline = r.Map(lambda x: x * 2) | r.Map(lambda x: x + 1)
    result = await pipeline.collect(range(3))
    assert result == [1, 3, 5]


async def test_simple_map_with_list():
    """Test: range(3) | r.Map(lambda x: x * 2) | list"""
    pipeline = range(3) | r.Map(lambda x: x * 2) | list
    result = await pipeline.run()
    assert result == [0, 2, 4]


async def test_chained_operations():
    """Test: [3, 1, 4, 2] | r.Sort() | r.Map(lambda x: x * 2) | r.Batch(2)"""
    pipeline = [3, 1, 4, 2] | r.Sort() | r.Map(lambda x: x * 2) | r.Batch(2)
    result = await pipeline.collect()
    assert result == [[2, 4], [6, 8]]


async def test_async_map_function():
    """Test Map with async function"""

    async def async_square(x):
        await asyncio.sleep(0.001)  # Minimal delay
        return x * x

    pipeline = range(4) | r.Map(async_square)
    result = await pipeline.collect()
    assert result == [0, 1, 4, 9]


async def test_streaming_with_async_functions():
    """Test streaming with async functions that complete at different times"""

    async def async_square_random_delay(x):
        await asyncio.sleep(random.random() * 0.01)  # Small random delay
        return x * x

    pipeline = range(4) | r.Map(async_square_random_delay) | r.Batch(2)

    results = []
    async for batch in pipeline.stream():
        results.append(batch)

    # Should have 2 batches
    assert len(results) == 2
    # Flatten and sort to compare regardless of order
    flattened = sorted([item for batch in results for item in batch])
    assert flattened == [0, 1, 4, 9]


async def test_filter_operation():
    """Test Filter operation"""
    pipeline = range(10) | r.Filter(lambda x: x % 2 == 0)
    result = await pipeline.collect()
    assert result == [0, 2, 4, 6, 8]


async def test_flatmap_operation():
    """Test FlatMap operation"""
    pipeline = [1, 2, 3] | r.FlatMap(lambda x: [x, x * 2])
    result = await pipeline.collect()
    assert result == [1, 2, 2, 4, 3, 6]


async def test_reduce_operation():
    """Test Reduce operation"""
    pipeline = range(5) | r.Reduce(lambda acc, x: acc + x, initial=0)
    result = await pipeline.collect()
    assert result == 10  # 0+1+2+3+4


async def test_take_operation():
    """Test Take operation"""
    pipeline = range(10) | r.Take(3)
    result = await pipeline.collect()
    assert result == [0, 1, 2]


async def test_skip_operation():
    """Test Skip operation"""
    pipeline = range(5) | r.Skip(2)
    result = await pipeline.collect()
    assert result == [2, 3, 4]


async def test_unique_operation():
    """Test Unique operation"""
    pipeline = [1, 2, 2, 3, 1, 4] | r.Unique()
    result = await pipeline.collect()
    assert result == [1, 2, 3, 4]


async def test_unique_with_key_function():
    """Test Unique with key function"""
    pipeline = ["a", "bb", "c", "dd", "eee"] | r.Unique(key_fn=len)
    result = await pipeline.collect()
    assert result == ["a", "bb", "eee"]  # Unique by length


async def test_sort_operation():
    """Test Sort operation"""
    pipeline = [3, 1, 4, 1, 5] | r.Sort()
    result = await pipeline.collect()
    assert result == [1, 1, 3, 4, 5]


async def test_sort_with_key_function():
    """Test Sort with key function"""
    pipeline = ["apple", "pie", "washington", "book"] | r.Sort(key=len)
    result = await pipeline.collect()
    assert result == ["pie", "book", "apple", "washington"]


async def test_groupby_operation():
    """Test GroupBy operation"""
    pipeline = ["apple", "pie", "book", "cat"] | r.GroupBy(key_fn=len)
    result = await pipeline.collect()
    expected = {3: ["pie", "cat"], 4: ["book"], 5: ["apple"]}
    assert result == expected


async def test_batch_operation():
    """Test Batch operation"""
    pipeline = range(7) | r.Batch(3)
    result = await pipeline.collect()
    assert result == [[0, 1, 2], [3, 4, 5], [6]]


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


async def test_nested_pipeline_composition():
    """Test more complex pipeline composition"""
    # Transform numbers
    transform_pipeline = r.Map(lambda x: x * 3) | r.Filter(lambda x: x > 5)

    # Use it in a larger pipeline
    pipeline = range(5) | transform_pipeline | r.Take(2)
    result = await pipeline.collect()
    assert result == [6, 9]  # 0*3=0(filtered), 1*3=3(filtered), 2*3=6, 3*3=9, take 2


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
