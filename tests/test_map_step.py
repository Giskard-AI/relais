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


async def test_simple_map_with_stream():
    pipeline = range(3) | r.Map(lambda x: x * 2) | r.Map(lambda x: x + 1)
    assert isinstance(pipeline, r.Pipeline)
    result = []
    async for item in pipeline.stream():
        result.append(item)
    assert set(result) == set([1, 3, 5])


async def test_async_map_function():
    """Test Map with async function"""

    async def async_square(x):
        await asyncio.sleep(0.001)  # Minimal delay
        return x * x

    pipeline = range(4) | r.Map(async_square)
    result = await pipeline.collect()
    assert result == [0, 1, 4, 9]


async def test_simple_map_with_list():
    pipeline = range(3) | r.Map(lambda x: x * 2) | list
    result = await pipeline.run()
    assert result == [0, 2, 4]


async def test_async_generator_input():
    """Test how pipeline handles async generator input."""

    # Create async generator
    async def async_number_stream():
        """Async generator that yields numbers with delays."""
        for i in range(5):
            await asyncio.sleep(0.01)  # Small delay to simulate async work
            yield i

    async_stream = async_number_stream()

    # Try to use it in pipeline
    pipeline = async_stream | r.Map(lambda x: x * 2)
    result = await pipeline.collect()

    # Should get the same result as regular iterable
    assert result == [0, 2, 4, 6, 8]
