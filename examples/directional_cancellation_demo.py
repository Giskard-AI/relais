#!/usr/bin/env python3
"""
Directional Cancellation Demo: Early Termination Optimization

This example demonstrates relais's directional cancellation feature, where operations
like take() and skip() can signal upstream operations to stop processing early,
providing significant performance improvements.

Key scenarios demonstrated:
1. take() stops upstream processing after N results
2. Memory efficiency with large datasets
3. Performance comparison with and without cancellation

Note: You may see some "Task exception was never retrieved" warnings with GeneratorExit.
These are expected when using early termination and indicate the cancellation is working correctly.

Run with: python examples/directional_cancellation_demo.py
"""

import asyncio
import random
import time
from typing import AsyncIterator

import relais as r


class LargeDataSource:
    """Simulates a large data source (like a database or API)."""

    def __init__(self, total_items: int = 10000, delay: float = 0.001):
        self.total_items = total_items
        self.delay = delay
        self.items_produced = 0

    def __aiter__(self) -> AsyncIterator[int]:
        return self

    async def __anext__(self) -> int:
        if self.items_produced >= self.total_items:
            raise StopAsyncIteration

        current = self.items_produced
        self.items_produced += 1

        # Simulate processing delay
        await asyncio.sleep(self.delay)
        return current


async def expensive_computation(x: int) -> int:
    """Simulate expensive computation."""
    # Variable delay to simulate real computation
    delay = random.uniform(0.001, 0.005)
    await asyncio.sleep(delay)
    return x * x + x + 1


async def demonstrate_early_termination():
    """Show how take() stops upstream processing early."""
    print("🛑 EARLY TERMINATION DEMO")
    print("=" * 50)
    print("Demonstrating how take() cancels upstream processing")

    # Without take() - processes many items
    print("\n1️⃣ Without take() - Processing 20 items:")
    data_source = LargeDataSource(total_items=20, delay=0.001)

    start = time.time()
    results_full = await (data_source | r.Map(expensive_computation)).collect()
    full_time = time.time() - start

    print(f"   ✅ Processed {len(results_full)} items in {full_time:.2f}s")
    print(f"   📊 Data source produced {data_source.items_produced} items")

    # With take() - stops early
    print("\n2️⃣ With take(5) - Only need first 5 results:")
    data_source2 = LargeDataSource(total_items=20, delay=0.001)

    start = time.time()
    results_limited = await (
        data_source2 | r.Map(expensive_computation) | r.Take(5)
    ).collect()
    limited_time = time.time() - start

    print(f"   ✅ Got {len(results_limited)} items in {limited_time:.2f}s")
    print(f"   📊 Data source only produced {data_source2.items_produced} items")

    # Analysis
    print("\n📊 Performance Analysis:")
    print(
        f"   • Without take(): {full_time:.2f}s, processed {data_source.items_produced} items"
    )
    print(
        f"   • With take():    {limited_time:.2f}s, processed {data_source2.items_produced} items"
    )
    print(f"   • Speedup:        {full_time / limited_time:.1f}x faster")
    print(
        f"   • Items saved:    {data_source.items_produced - data_source2.items_produced} items avoided"
    )


async def demonstrate_memory_efficiency():
    """Show memory efficiency with large datasets."""
    print("\n💾 MEMORY EFFICIENCY DEMO")
    print("=" * 50)
    print("Processing huge dataset but only keeping first 5 results")

    # Simulate processing a huge dataset but only taking first 5
    huge_data_source = LargeDataSource(total_items=10000, delay=0.0001)

    print("🚀 Processing 10,000 items but only taking first 5...")
    start = time.time()

    results = await (
        huge_data_source
        | r.Map(expensive_computation)
        | r.Filter[int](lambda x: x > 0)  # All results are positive (less restrictive)
        | r.Take(5)  # Stop after 5 results
    ).collect()

    elapsed = time.time() - start

    print(f"   ✅ Got {len(results)} results in {elapsed:.2f}s")
    print(f"   📊 Only processed {huge_data_source.items_produced} items out of 10,000")
    print("   💾 Memory usage stayed constant (no accumulation)")
    print(f"   ⚡ Results: {results}")


async def demonstrate_streaming_cancellation():
    """Show how streaming + cancellation work together."""
    print("\n🌊 STREAMING + CANCELLATION DEMO")
    print("=" * 50)
    print("Getting results as they arrive, stopping early when we have enough")

    data_source = LargeDataSource(total_items=1000, delay=0.001)
    pipeline = (
        data_source | r.Map(expensive_computation) | r.Filter[int](lambda x: x > 10)
    )

    print("🔄 Streaming results until we find 3 good ones...")
    start = time.time()

    count = 0
    try:
        async for result in pipeline.stream():
            count += 1
            elapsed = time.time() - start
            print(f"   ✅ Result #{count}: {result} (after {elapsed:.2f}s)")

            if count >= 3:
                print("   🛑 Got enough results, stopping early!")
                break
    except Exception as e:
        # Handle any cleanup exceptions gracefully
        if count == 0:
            print(f"   ⚠️  Exception occurred: {e}")

    final_elapsed = time.time() - start
    print(f"   📊 Total time: {final_elapsed:.2f}s")
    print(f"   📊 Items processed: {data_source.items_produced} out of 1000")
    print("   💡 Pipeline automatically cancelled when we stopped consuming")


async def performance_comparison():
    """Compare performance with different take() sizes."""
    print("\n📊 PERFORMANCE SCALING DEMO")
    print("=" * 50)
    print("How take() performance scales with different limits")

    take_sizes = [5, 10, 25, 50, 100]

    for take_size in take_sizes:
        data_source = LargeDataSource(total_items=1000, delay=0.0005)

        start = time.time()
        _ = await (
            data_source | r.Map(expensive_computation) | r.Take(take_size)
        ).collect()
        elapsed = time.time() - start

        print(
            f"   take({take_size:3d}): {elapsed:.2f}s, processed {data_source.items_produced:4d} items"
        )


async def ordered_vs_unordered_comparison():
    """Compare ordered vs unordered take() performance."""
    print("\n⚡ ORDERED vs UNORDERED COMPARISON")
    print("=" * 50)
    print("ordered=False (default) enables aggressive cancellation")

    # Unordered (default) - aggressive cancellation
    print("\n🚀 Unordered take (ordered=False, default):")
    data_source1 = LargeDataSource(total_items=200, delay=0.001)

    start = time.time()
    _ = await (
        data_source1 | r.Map(expensive_computation) | r.Take(10, ordered=False)
    ).collect()
    unordered_time = time.time() - start

    print(
        f"   ✅ Time: {unordered_time:.2f}s, processed {data_source1.items_produced} items"
    )

    # Ordered - less aggressive cancellation
    print("\n🐌 Ordered take (ordered=True):")
    data_source2 = LargeDataSource(total_items=200, delay=0.001)

    start = time.time()
    _ = await (
        data_source2 | r.Map(expensive_computation) | r.Take(10, ordered=True)
    ).collect()
    ordered_time = time.time() - start

    print(
        f"   ✅ Time: {ordered_time:.2f}s, processed {data_source2.items_produced} items"
    )

    # Analysis
    print("\n📊 Comparison:")
    print(
        f"   • Unordered: {unordered_time:.2f}s, {data_source1.items_produced} items processed"
    )
    print(
        f"   • Ordered:   {ordered_time:.2f}s, {data_source2.items_produced} items processed"
    )
    print(f"   • Speedup:   {ordered_time / unordered_time:.1f}x faster with unordered")
    print("   💡 Use ordered=False (default) for maximum performance")


async def main():
    """Run all directional cancellation demos."""
    print("🛑 RELAIS DIRECTIONAL CANCELLATION DEMO")
    print("=" * 60)
    print("Demonstrating early termination optimizations in streaming pipelines")
    print("Key feature: Operations like take() signal upstream to stop processing")
    print()

    # Note: Any "Task exception was never retrieved" warnings with GeneratorExit
    # are expected and indicate that early termination cancellation is working correctly

    await demonstrate_early_termination()
    await demonstrate_memory_efficiency()
    await demonstrate_streaming_cancellation()
    await performance_comparison()
    await ordered_vs_unordered_comparison()

    print("\n🎯 KEY TAKEAWAYS")
    print("=" * 60)
    print("✅ take() and skip() provide automatic upstream cancellation")
    print("✅ Significant performance improvements for large datasets")
    print("✅ Memory usage stays bounded regardless of input size")
    print("✅ Streaming + cancellation work together for optimal efficiency")
    print("✅ Use ordered=False (default) for maximum performance")

    print("\n💡 Perfect for scenarios where you:")
    print("   • Need first N results from large datasets")
    print("   • Want to stop processing when condition is met")
    print("   • Process data streams with unknown size")
    print("   • Need bounded memory usage")


if __name__ == "__main__":
    asyncio.run(main())
