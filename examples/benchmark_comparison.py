#!/usr/bin/env python3
"""
Performance Benchmark: Relais vs AsyncIO vs Sequential

This benchmark compares three approaches for processing a pipeline of I/O-bound operations:
1. Sequential processing (synchronous, one at a time)
2. Pure asyncio with manual concurrency management
3. Relais pipeline with automatic concurrency

The benchmark simulates a typical LLM evaluation workflow:
- Fetch data (simulate API call)
- Process data (simulate computation)
- Validate results (simulate evaluation)

Run with: python examples/benchmark_comparison.py
"""

import asyncio
import random
import time
from dataclasses import dataclass
from typing import Any, List

import relais as r


@dataclass
class BenchmarkResult:
    """Results from a benchmark run."""

    approach: str
    total_time: float
    items_processed: int
    items_per_second: float
    memory_overhead: str
    code_complexity: str


# Simulated workload functions with realistic LLM-like delays
async def fetch_data(item_id: int) -> dict[str, Any]:
    """Simulate LLM API call with realistic, highly variable delays."""
    # Real LLM APIs have very variable response times:
    # - Most responses: 200ms - 2s
    # - Some slow responses: 2s - 10s
    # - Rare very slow responses: 10s+

    rand = random.random()
    if rand < 0.8:  # 80% normal responses
        delay = random.uniform(0.2, 2.0)
    elif rand < 0.95:  # 15% slow responses
        delay = random.uniform(2.0, 5.0)
    else:  # 5% very slow responses
        delay = random.uniform(5.0, 8.0)

    await asyncio.sleep(delay)
    return {"id": item_id, "data": f"llm_response_{item_id}", "fetch_time": delay}


async def process_data(item: dict[str, Any]) -> dict[str, Any]:
    """Simulate evaluation with variable processing time."""
    # Evaluation can also be variable:
    # - Simple checks: fast
    # - Complex analysis: slower
    # - LLM-as-judge: very slow

    rand = random.random()
    if rand < 0.7:  # 70% simple evaluation
        delay = random.uniform(0.05, 0.2)
    elif rand < 0.9:  # 20% complex evaluation
        delay = random.uniform(0.2, 1.0)
    else:  # 10% LLM-as-judge evaluation
        delay = random.uniform(1.0, 3.0)

    await asyncio.sleep(delay)
    return {
        **item,
        "processed": True,
        "result": item["data"].upper(),
        "process_time": delay,
    }


async def validate_result(item: dict[str, Any]) -> dict[str, Any]:
    """Simulate final validation with some randomness."""
    # Final validation is usually quick but can vary
    delay = random.choice(
        [
            random.uniform(0.01, 0.05),  # Quick validation
            random.uniform(0.05, 0.2),  # Thorough validation
            random.uniform(0.2, 0.5),  # Deep validation (rare)
        ]
    )

    await asyncio.sleep(delay)

    # Simulate some validation failures
    is_valid = random.random() > 0.1  # 90% success rate

    return {**item, "valid": is_valid, "validation_time": delay}


def filter_valid_items(item: dict[str, Any]) -> bool:
    """Filter to keep only valid items."""
    return item.get("valid", False)


# Sequential Implementation
async def benchmark_sequential(items: list[int]) -> BenchmarkResult:
    """Sequential processing - one item at a time."""
    print("🐌 Running Sequential Benchmark...")

    start_time = time.time()
    results = []

    for item_id in items:
        # Process each item completely before moving to next
        try:
            fetched = await fetch_data(item_id)
            processed = await process_data(fetched)
            validated = await validate_result(processed)

            if filter_valid_items(validated):
                results.append(validated)
        except Exception:
            continue  # Skip failed items

    total_time = time.time() - start_time

    return BenchmarkResult(
        approach="Sequential",
        total_time=total_time,
        items_processed=len(results),
        items_per_second=len(results) / total_time,
        memory_overhead="Low",
        code_complexity="Simple",
    )


# Pure AsyncIO Implementation
async def benchmark_pure_asyncio(items: list[int]) -> BenchmarkResult:
    """Pure asyncio with manual concurrency management."""
    print("⚡ Running Pure AsyncIO Benchmark...")

    start_time = time.time()

    # Step 1: Fetch all data concurrently
    fetch_tasks = [fetch_data(item_id) for item_id in items]
    fetched_items = []
    for task in asyncio.as_completed(fetch_tasks):
        try:
            result = await task
            fetched_items.append(result)
        except Exception:
            continue

    # Step 2: Process all data concurrently
    process_tasks = [process_data(item) for item in fetched_items]
    processed_items = []
    for task in asyncio.as_completed(process_tasks):
        try:
            result = await task
            processed_items.append(result)
        except Exception:
            continue

    # Step 3: Validate all results concurrently
    validate_tasks = [validate_result(item) for item in processed_items]
    validated_items = []
    for task in asyncio.as_completed(validate_tasks):
        try:
            result = await task
            validated_items.append(result)
        except Exception:
            continue

    # Step 4: Filter valid items
    results = [item for item in validated_items if filter_valid_items(item)]

    total_time = time.time() - start_time

    return BenchmarkResult(
        approach="Pure AsyncIO",
        total_time=total_time,
        items_processed=len(results),
        items_per_second=len(results) / total_time,
        memory_overhead="Medium",
        code_complexity="Complex",
    )


# Relais Implementation
async def benchmark_relais(items: list[int]) -> BenchmarkResult:
    """Relais pipeline with automatic concurrency."""
    print("🚀 Running Relais Benchmark...")

    start_time = time.time()

    # Build pipeline - items flow through each stage concurrently
    pipeline = (
        items
        | r.Map(fetch_data)  # Fetch data concurrently
        | r.Map(process_data)  # Process concurrently as data arrives
        | r.Map(validate_result)  # Validate concurrently as processing completes
        | r.Filter(filter_valid_items)  # Filter valid items
    )

    try:
        results = await pipeline.collect()
    except Exception:
        results = []

    total_time = time.time() - start_time

    return BenchmarkResult(
        approach="Relais Pipeline",
        total_time=total_time,
        items_processed=len(results),
        items_per_second=len(results) / total_time,
        memory_overhead="Medium",
        code_complexity="Simple",
    )


async def run_benchmark_suite(num_items: int = 50) -> List[BenchmarkResult]:
    """Run all benchmarks with the same dataset."""
    print(f"🏁 Running Benchmark Suite with {num_items} items")
    print("=" * 60)
    print("⚠️  Using realistic LLM-like delays:")
    print("   • LLM API: 80% fast (0.2-2s), 15% slow (2-5s), 5% very slow (5-8s)")
    print(
        "   • Evaluation: 70% simple (50-200ms), 20% complex (0.2-1s), 10% LLM-judge (1-3s)"
    )
    print("   • This simulates real-world variability in LLM evaluation pipelines")
    print()

    # Generate test data
    items = list(range(1, num_items + 1))

    # Run each benchmark
    results = []

    # Sequential
    seq_result = await benchmark_sequential(items)
    results.append(seq_result)
    print(
        f"✅ Sequential: {seq_result.total_time:.2f}s, {seq_result.items_per_second:.1f} items/sec"
    )

    # Pure AsyncIO
    asyncio_result = await benchmark_pure_asyncio(items)
    results.append(asyncio_result)
    print(
        f"✅ Pure AsyncIO: {asyncio_result.total_time:.2f}s, {asyncio_result.items_per_second:.1f} items/sec"
    )

    # Relais
    relais_result = await benchmark_relais(items)
    results.append(relais_result)
    print(
        f"✅ Relais: {relais_result.total_time:.2f}s, {relais_result.items_per_second:.1f} items/sec"
    )

    return results


def analyze_results(results: List[BenchmarkResult]) -> None:
    """Analyze and display benchmark results."""
    print("\n" + "=" * 60)
    print("📊 BENCHMARK ANALYSIS")
    print("=" * 60)

    # Find fastest approach
    fastest = min(results, key=lambda x: x.total_time)
    print(f"🏆 Fastest: {fastest.approach} ({fastest.total_time:.2f}s)")

    # Calculate speedup ratios
    sequential_time = next(r.total_time for r in results if r.approach == "Sequential")

    print("\n📈 Performance Comparison (vs Sequential):")
    for result in results:
        if result.approach == "Sequential":
            speedup = 1.0
        else:
            speedup = sequential_time / result.total_time

        print(
            f"  {result.approach:15} | {result.total_time:6.2f}s | {speedup:5.1f}x speedup | {result.items_per_second:6.1f} items/sec"
        )

    # Detailed comparison table
    print("\n📋 Detailed Comparison:")
    print(
        f"{'Approach':<15} | {'Time':<8} | {'Items':<6} | {'Rate':<10} | {'Memory':<8} | {'Complexity'}"
    )
    print("-" * 70)

    for result in results:
        print(
            f"{result.approach:<15} | "
            f"{result.total_time:<8.2f} | "
            f"{result.items_processed:<6} | "
            f"{result.items_per_second:<10.1f} | "
            f"{result.memory_overhead:<8} | "
            f"{result.code_complexity}"
        )

    # Key insights
    print("\n💡 Key Insights:")

    relais_result = next(r for r in results if r.approach == "Relais Pipeline")
    asyncio_result = next(r for r in results if r.approach == "Pure AsyncIO")
    _ = next(r for r in results if r.approach == "Sequential")

    relais_speedup = sequential_time / relais_result.total_time
    asyncio_speedup = sequential_time / asyncio_result.total_time

    print(
        f"  • Relais provides {relais_speedup:.1f}x speedup with simpler code than manual asyncio"
    )
    print(
        f"  • Pure AsyncIO achieves {asyncio_speedup:.1f}x speedup but requires complex coordination"
    )
    print(
        f"  • Sequential processing is simplest but {relais_speedup:.1f}x slower for I/O-bound tasks"
    )

    if relais_result.total_time < asyncio_result.total_time:
        print(
            f"  • Relais is {asyncio_result.total_time / relais_result.total_time:.1f}x faster than manual asyncio due to streaming"
        )
    else:
        print(
            f"  • Pure AsyncIO is {relais_result.total_time / asyncio_result.total_time:.1f}x faster but much more complex"
        )


async def run_multiple_benchmarks():
    """Run benchmarks with different dataset sizes."""
    print("🔬 Running Multi-Scale Benchmarks")
    print("=" * 60)

    dataset_sizes = [10, 25, 50, 100]
    all_results = {}

    for size in dataset_sizes:
        print(f"\n📏 Dataset Size: {size} items")
        results = await run_benchmark_suite(size)
        all_results[size] = results

        # Quick summary
        relais = next(r for r in results if r.approach == "Relais Pipeline")
        sequential = next(r for r in results if r.approach == "Sequential")
        speedup = sequential.total_time / relais.total_time
        print(f"   Relais speedup: {speedup:.1f}x")

    # Overall analysis
    print("\n📊 Scalability Analysis:")
    print(
        f"{'Size':<6} | {'Sequential':<12} | {'AsyncIO':<12} | {'Relais':<12} | {'Speedup'}"
    )
    print("-" * 60)

    for size, results in all_results.items():
        seq = next(r for r in results if r.approach == "Sequential")
        asyncio_r = next(r for r in results if r.approach == "Pure AsyncIO")
        relais = next(r for r in results if r.approach == "Relais Pipeline")
        speedup = seq.total_time / relais.total_time

        print(
            f"{size:<6} | {seq.total_time:<12.2f} | {asyncio_r.total_time:<12.2f} | {relais.total_time:<12.2f} | {speedup:.1f}x"
        )


async def main():
    """Main benchmark execution."""
    print("🚀 Relais Performance Benchmark Suite")
    print("=" * 60)
    print("Comparing Sequential vs Pure AsyncIO vs Relais Pipeline")
    print("Simulating LLM evaluation workflow: Fetch → Process → Validate → Filter")
    print()

    # Single benchmark run
    results = await run_benchmark_suite(50)
    analyze_results(results)

    # Multiple scale benchmarks
    print("\n" + "=" * 60)
    await run_multiple_benchmarks()

    print("\n✅ Benchmark suite completed!")
    print(
        "\n💡 Takeaway: Relais provides excellent performance with minimal code complexity"
    )
    print("   Perfect for I/O-bound pipelines like LLM evaluation workflows.")


if __name__ == "__main__":
    asyncio.run(main())
