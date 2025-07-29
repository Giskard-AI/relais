"""Test directional cancellation implementation."""

import asyncio
import pytest
import sys
import os

# Add src to path for testing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import relais as r
from relais.base import ErrorPolicy


class SlowProducer:
    """Test helper for simulating slow data production."""
    
    def __init__(self, max_items: int = 1000, delay: float = 0.1):
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


@pytest.mark.asyncio
async def test_take_cancels_upstream():
    """Test that take() cancels upstream production when it has enough items."""
    producer = SlowProducer(max_items=100, delay=0.05)  # Would take 5 seconds to complete
    
    # Take only 3 items
    pipeline = producer | r.take(3, ordered=False)
    
    start_time = asyncio.get_event_loop().time()
    result = await pipeline.collect()
    end_time = asyncio.get_event_loop().time()
    
    # Verify results
    assert len(result) == 3
    assert result == [0, 1, 2]
    
    # Should complete quickly (much less than 5 seconds)
    assert end_time - start_time < 1.0
    
    # Verify producer was stopped early
    assert producer.produced_count <= 5  # Should stop producing after take() gets its items


@pytest.mark.asyncio
async def test_downstream_continues_after_upstream_cancellation():
    """Test that downstream processing continues when upstream is cancelled."""
    producer = SlowProducer(max_items=100, delay=0.05)
    
    # Pipeline: producer -> take(3) -> map(lambda x: x * 2)
    pipeline = producer | r.take(3, ordered=False) | r.map(lambda x: x * 2)
    
    start_time = asyncio.get_event_loop().time()
    result = await pipeline.collect()
    end_time = asyncio.get_event_loop().time()
    
    # Verify all items were processed by downstream map
    assert len(result) == 3
    assert result == [0, 2, 4]
    
    # Should still complete quickly
    assert end_time - start_time < 1.0


@pytest.mark.asyncio
async def test_error_propagation_through_cancellation():
    """Test that errors propagate correctly through the cancellation system."""
    
    def failing_processor(x):
        if x == 2:
            raise ValueError(f"Intentional error for item {x}")
        return x * 2
    
    # Test FAIL_FAST error policy
    producer = SlowProducer(max_items=10, delay=0.01)
    pipeline = producer | r.map(failing_processor)
    
    with pytest.raises(Exception):  # Should raise PipelineError containing ValueError
        await pipeline.collect()


@pytest.mark.asyncio
async def test_error_policy_ignore_with_cancellation():
    """Test IGNORE error policy continues processing despite errors."""
    
    def failing_processor(x):
        if x == 2:
            raise ValueError(f"Intentional error for item {x}")
        return x * 2
    
    producer = SlowProducer(max_items=5, delay=0.01)
    pipeline = producer | r.map(failing_processor).with_error_policy(ErrorPolicy.IGNORE)
    
    result = await pipeline.collect()
    
    # Should have processed all items except the one that failed
    # Note: exact result depends on implementation details of IGNORE policy
    assert len(result) <= 4  # At most 4 items (excluding the failed one)
    assert 4 not in result  # The failed item (2 * 2) shouldn't be in results


@pytest.mark.asyncio
async def test_multiple_takes_in_pipeline():
    """Test multiple take operations in the same pipeline."""
    producer = SlowProducer(max_items=100, delay=0.01)
    
    # Take 10, then take 3 of those
    pipeline = producer | r.take(10, ordered=False) | r.take(3, ordered=False)
    
    start_time = asyncio.get_event_loop().time()
    result = await pipeline.collect()
    end_time = asyncio.get_event_loop().time()
    
    assert len(result) == 3
    assert result == [0, 1, 2]
    
    # Should complete very quickly since both takes limit the processing
    assert end_time - start_time < 0.5


@pytest.mark.asyncio  
async def test_cancellation_with_stateful_operations():
    """Test cancellation works correctly with stateful operations like sort."""
    producer = SlowProducer(max_items=100, delay=0.02)
    
    # Take 5 items, sort them (stateful), then take 3
    pipeline = (producer | 
                r.take(5, ordered=False) | 
                r.sort() |  # This is stateful 
                r.take(3, ordered=False))
    
    start_time = asyncio.get_event_loop().time()
    result = await pipeline.collect()
    end_time = asyncio.get_event_loop().time()
    
    assert len(result) == 3
    assert result == [0, 1, 2]  # Should be sorted
    
    # Should complete relatively quickly
    assert end_time - start_time < 1.0


@pytest.mark.asyncio
async def test_stream_cancellation_cleanup():
    """Test that streams are properly cleaned up when cancelled."""
    producer = SlowProducer(max_items=100, delay=0.01)
    
    pipeline = producer | r.take(2, ordered=False)
    
    # Run the pipeline
    result = await pipeline.collect()
    assert result == [0, 1]
    
    # The producer should have been told to stop early
    assert producer.produced_count <= 4  # Some buffer allowed for async timing


if __name__ == "__main__":
    # Run tests directly for debugging
    asyncio.run(test_take_cancels_upstream())
    asyncio.run(test_downstream_continues_after_upstream_cancellation()) 
    print("✅ All directional cancellation tests passed!")