#!/usr/bin/env python3
"""
Simple Performance Benchmark: Sequential vs AsyncIO vs Relais

A straightforward comparison showing the performance benefits of different approaches
for I/O-bound operations (like API calls).

Run with: python examples/simple_benchmark.py
"""

import asyncio
import random
import time

import relais as r


async def slow_operation(x: int) -> int:
    """Simulate LLM API call with realistic, variable delays."""
    # Simulate real LLM API response time distribution:
    # - 70% fast responses: 0.2-1.5s (typical)
    # - 20% medium responses: 1.5-4s (complex queries)
    # - 10% slow responses: 4-8s (very complex/retry/rate limits)

    rand = random.random()
    if rand < 0.7:  # 70% fast responses
        delay = random.uniform(0.2, 1.5)
    elif rand < 0.9:  # 20% medium responses
        delay = random.uniform(1.5, 4.0)
    else:  # 10% slow responses
        delay = random.uniform(4.0, 8.0)

    await asyncio.sleep(delay)
    return x * 2


def process_result(x: int) -> int:
    """Simple processing step."""
    return x + 10


def filter_large(x: int) -> bool:
    """Filter to keep only large numbers."""
    return x > 50


async def sequential_approach(data: list[int]) -> list[int]:
    """Process data sequentially - one item at a time."""
    results = []
    for item in data:
        # Each item waits for previous to complete
        processed = await slow_operation(item)
        transformed = process_result(processed)
        if filter_large(transformed):
            results.append(transformed)
    return results


async def asyncio_approach(data: list[int]) -> list[int]:
    """Process data with pure asyncio concurrency."""
    # Step 1: Process all items concurrently
    tasks = [slow_operation(item) for item in data]
    processed = await asyncio.gather(*tasks)

    # Step 2: Transform and filter
    results = []
    for item in processed:
        transformed = process_result(item)
        if filter_large(transformed):
            results.append(transformed)

    return results


async def relais_approach(data: list[int]) -> list[int]:
    """Process data with relais pipeline."""
    return await (
        data
        | r.Map(slow_operation)  # Concurrent I/O operations
        | r.Map(process_result)  # Transform results
        | r.Filter(filter_large)  # Filter large values
    ).collect()


async def benchmark_approaches(data_size: int = 20) -> None:
    """Compare all three approaches."""
    print(f"🏁 Benchmarking with {data_size} items")
    print("=" * 50)
    print("⚠️  Using realistic LLM API delays:")
    print("   • 70% fast responses (0.2-1.5s)")
    print("   • 20% medium responses (1.5-4s)")
    print("   • 10% slow responses (4-8s)")
    print("   This variability is key to showing concurrency benefits!")
    print()

    # Generate test data
    data = list(range(1, data_size + 1))

    approaches = [
        ("Sequential", sequential_approach),
        ("Pure AsyncIO", asyncio_approach),
        ("Relais Pipeline", relais_approach),
    ]

    results = {}

    for name, func in approaches:
        print(f"⏱️  Running {name}...")

        start = time.time()
        result = await func(data.copy())
        duration = time.time() - start

        results[name] = {
            "time": duration,
            "items": len(result),
            "rate": len(result) / duration,
        }

        print(
            f"   ✅ {duration:.2f}s, {len(result)} items, {len(result) / duration:.1f} items/sec"
        )

    # Analysis
    print("\n📊 PERFORMANCE COMPARISON")
    print("=" * 50)

    sequential_time = results["Sequential"]["time"]

    for name, data in results.items():
        speedup = sequential_time / data["time"] if name != "Sequential" else 1.0
        print(f"{name:15} | {data['time']:6.2f}s | {speedup:5.1f}x speedup")

    # Key insights
    relais_time = results["Relais Pipeline"]["time"]
    asyncio_time = results["Pure AsyncIO"]["time"]

    print("\n💡 Key Insights:")
    print(f"   • Relais: {sequential_time / relais_time:.1f}x faster than sequential")
    print(f"   • AsyncIO: {sequential_time / asyncio_time:.1f}x faster than sequential")

    if relais_time < asyncio_time:
        print(
            f"   • Relais is {asyncio_time / relais_time:.1f}x faster than pure AsyncIO (streaming advantage)"
        )
    else:
        print(
            f"   • Pure AsyncIO is {relais_time / asyncio_time:.1f}x faster than Relais"
        )

    print("   • Code complexity: Sequential < Relais < Pure AsyncIO")
    print("   • Variable delays make concurrency benefits more pronounced")


async def demo_streaming_advantage():
    """Demonstrate relais streaming advantage."""
    print("\n🌊 STREAMING ADVANTAGE DEMO")
    print("=" * 50)
    print("Relais processes items as they complete, not in batches")

    data = list(range(1, 8))

    print("Sequential processing:")
    start = time.time()
    for i, item in enumerate(data):
        result = await slow_operation(item)
        elapsed = time.time() - start
        print(f"  Item {item}: {result} (after {elapsed:.1f}s)")

    print("\nRelais streaming:")
    start = time.time()
    pipeline = data | r.Map(slow_operation)

    async for result in pipeline.stream():
        elapsed = time.time() - start
        print(f"  Item result: {result} (after {elapsed:.1f}s)")


async def main():
    """Run all benchmarks."""
    print("🚀 Simple Performance Benchmark")
    print("Comparing Sequential vs AsyncIO vs Relais")
    print("=" * 50)

    # Main benchmark
    await benchmark_approaches(20)

    # Streaming demo
    await demo_streaming_advantage()

    # Different scales
    print("\n📏 SCALING ANALYSIS")
    print("=" * 50)

    for size in [10, 25, 50]:
        print(f"\nDataset size: {size}")

        data = list(range(1, size + 1))

        # Quick comparison of just relais vs sequential
        start = time.time()
        _ = await sequential_approach(data.copy())
        seq_time = time.time() - start

        start = time.time()
        _ = await relais_approach(data.copy())
        relais_time = time.time() - start

        speedup = seq_time / relais_time
        print(f"  Sequential: {seq_time:.2f}s")
        print(f"  Relais:     {relais_time:.2f}s ({speedup:.1f}x speedup)")

    print("\n✅ Benchmark completed!")
    print("💡 Relais provides significant speedups for I/O-bound operations")
    print("   with much simpler code than manual asyncio coordination.")


if __name__ == "__main__":
    asyncio.run(main())
