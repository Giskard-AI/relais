import asyncio
import random
import relais as r


async def test_batch_operation():
    """Test Batch operation"""
    pipeline = range(7) | r.Batch(3)
    result = await pipeline.collect()
    assert result == [[0, 1, 2], [3, 4, 5], [6]]


async def test_streaming_with_async_functions_and_batch():
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