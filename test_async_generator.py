import asyncio
import pytest
import relais as r

async def async_number_stream():
    """Async generator that yields numbers with delays."""
    for i in range(5):
        await asyncio.sleep(0.01)  # Small delay to simulate async work
        yield i

async def test_regular_iterable_baseline():
    """Test with regular iterable for comparison."""
    pipeline = range(5) | r.Map(lambda x: x * 2)
    result = await pipeline.collect()
    assert result == [0, 2, 4, 6, 8]

async def test_async_generator_input():
    """Test how pipeline handles async generator input."""
    # Create async generator
    async_stream = async_number_stream()
    
    # Use pipe syntax for single step
    pipeline = async_stream | r.Map(lambda x: x * 2)
    result = await pipeline.collect()
    
    # Should get the same result as regular iterable
    assert result == [0, 2, 4, 6, 8]

async def test_async_generator_with_chained_maps():
    """Test async generator with multiple chained Map operations."""
    async_stream = async_number_stream()
    
    pipeline = r.Map(lambda x: x * 2) | r.Map(lambda x: x + 1)
    result = await pipeline.collect(async_stream)
    
    # (0*2)+1=1, (1*2)+1=3, (2*2)+1=5, (3*2)+1=7, (4*2)+1=9
    assert result == [1, 3, 5, 7, 9]

async def test_async_generator_with_pipe_syntax():
    """Test async generator with pipe syntax."""
    async_stream = async_number_stream()
    
    # This should work if async generators are properly handled
    pipeline = async_stream | r.Map(lambda x: x * 2)
    result = await pipeline.collect()
    
    assert result == [0, 2, 4, 6, 8] 