#!/usr/bin/env python3
"""
Basic Pipeline Example

Shows core relais concepts with simple operations:
- Concurrent processing with map()
- Filtering data
- Batching results
- Error handling

Run with: python examples/basic_pipeline.py
"""

import asyncio
import random
import time

import relais as r


async def slow_square(x: int) -> int:
    """Simulate slow computation (like API call)."""
    delay = random.uniform(0.1, 0.5)
    await asyncio.sleep(delay)
    return x * x


async def slow_add_ten(x: int) -> int:
    """Another slow operation."""
    await asyncio.sleep(random.uniform(0.05, 0.2))
    return x + 10


def is_large(x: int) -> bool:
    """Filter for large numbers."""
    return x > 50


async def demonstrate_basic_pipeline():
    """Show basic pipeline operations."""
    print("🔢 Basic Pipeline Demo")
    print("=" * 30)

    numbers = list(range(1, 11))  # [1, 2, 3, ..., 10]
    print(f"Input: {numbers}")

    start = time.time()

    # Simple pipeline: square numbers, add 10, filter large ones
    result = await (
        numbers
        | r.Map[int, int](slow_square)  # [1, 4, 9, 16, ..., 100]
        | r.Map[int, int](slow_add_ten)  # [11, 14, 19, 26, ..., 110]
        | r.Filter[int](is_large)  # [61, 66, 76, 86, 96, 110]
    ).collect()

    elapsed = time.time() - start
    print(f"Result: {result}")
    print(f"Completed in {elapsed:.2f}s (concurrent processing)")
    print()


async def demonstrate_batching():
    """Show batching operations."""
    print("📦 Batching Demo")
    print("=" * 30)

    items = list(range(1, 13))  # 12 items
    print(f"Input: {items}")

    # Process and batch into groups of 3
    batches = await (
        items
        | r.Map[int, int](lambda x: x * 2)  # Double each number
        | r.Batch(3)  # Group into batches of 3
    ).collect()

    print(f"Batches: {batches}")
    print()


async def demonstrate_streaming():
    """Show streaming results as they arrive."""
    print("🌊 Streaming Demo")
    print("=" * 30)

    items = range(1, 8)
    print(f"Processing {list(items)} with streaming...")

    pipeline = items | r.Map[int, int](slow_square) | r.Batch(2)

    # Stream results as they become available
    async for batch in pipeline.stream():
        print(f"  Received batch: {batch}")

    print()


async def demonstrate_error_handling():
    """Show error handling in pipelines."""
    print("⚠️  Error Handling Demo")
    print("=" * 30)

    async def sometimes_fails(x: int) -> int:
        """Function that randomly fails."""
        if random.random() < 0.3:  # 30% chance of failure
            raise ValueError(f"Failed processing {x}")
        await asyncio.sleep(0.1)
        return x * 2

    numbers = list(range(1, 11))
    print(f"Input: {numbers} (some operations will fail)")

    # Use IGNORE error policy to skip failed items
    from relais import ErrorPolicy

    pipeline = r.Pipeline(
        [r.Map[int, int](sometimes_fails)], error_policy=ErrorPolicy.IGNORE
    )

    results = await pipeline.collect(numbers)
    print(f"Successful results: {results}")
    print(f"Processed {len(results)} out of {len(numbers)} items")
    print()


async def main():
    """Run all demonstrations."""
    await demonstrate_basic_pipeline()
    await demonstrate_batching()
    await demonstrate_streaming()
    await demonstrate_error_handling()

    print("✅ All demos completed!")


if __name__ == "__main__":
    asyncio.run(main())
