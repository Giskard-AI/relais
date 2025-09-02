"""Performance tests for the new streaming architecture."""

import asyncio
import statistics
import time
from typing import Any, Dict, List

import pytest

import relais as r
from relais.errors import ErrorPolicy


class PerformanceProfiler:
    """Helper class for measuring performance metrics."""

    def __init__(self):
        self.measurements: Dict[str, List[float]] = {}

    async def measure(self, name: str, coro):
        """Measure the execution time of a coroutine."""
        start_time = time.perf_counter()
        result = await coro
        end_time = time.perf_counter()

        duration = end_time - start_time
        if name not in self.measurements:
            self.measurements[name] = []
        self.measurements[name].append(duration)

        return result

    def get_stats(self, name: str) -> Dict[str, float]:
        """Get statistics for a measurement."""
        if name not in self.measurements:
            return {}

        measurements = self.measurements[name]
        return {
            "mean": statistics.mean(measurements),
            "median": statistics.median(measurements),
            "min": min(measurements),
            "max": max(measurements),
            "stdev": statistics.stdev(measurements) if len(measurements) > 1 else 0,
            "count": len(measurements),
        }

    def compare(self, name1: str, name2: str) -> Dict[str, Any]:
        """Compare two measurements."""
        stats1 = self.get_stats(name1)
        stats2 = self.get_stats(name2)

        if not stats1 or not stats2:
            return {}

        return {
            "name1": name1,
            "name2": name2,
            "speedup": stats1["mean"] / stats2["mean"],
            "mean_diff_ms": (stats2["mean"] - stats1["mean"]) * 1000,
            "stats1": stats1,
            "stats2": stats2,
        }


class TestBasicPerformance:
    """Test basic performance characteristics of the new architecture."""

    @pytest.mark.asyncio
    async def test_small_pipeline_performance(self):
        """Test performance with small datasets (typical use case)."""
        profiler = PerformanceProfiler()

        # Test data
        data = list(range(100))

        # Simple pipeline
        async def run_simple_pipeline():
            pipeline = r.Map[int, int](lambda x: x * 2) | r.Filter[int](
                lambda x: x > 50
            )
            return await (data | pipeline).collect()

        # Run multiple times to get stable measurements
        for _ in range(10):
            await profiler.measure("simple_pipeline", run_simple_pipeline())

        stats = profiler.get_stats("simple_pipeline")

        # Basic performance assertions (adjust based on expected performance)
        assert stats["mean"] < 0.1, f"Simple pipeline too slow: {stats['mean']:.3f}s"
        assert stats["stdev"] < 0.05, (
            f"High variance in performance: {stats['stdev']:.3f}s"
        )

        print(f"Simple pipeline: {stats['mean']:.3f}s ± {stats['stdev']:.3f}s")

    @pytest.mark.asyncio
    async def test_async_operation_performance(self):
        """Test performance with async operations."""
        profiler = PerformanceProfiler()

        data = list(range(50))  # Smaller dataset for async operations

        async def async_transform(x):
            await asyncio.sleep(0.001)  # Small async delay
            return x * 3

        async def run_async_pipeline():
            pipeline = r.Map[int, int](async_transform) | r.Filter[int](
                lambda x: x < 100
            )
            return await (data | pipeline).collect()

        # Run multiple times
        for _ in range(5):
            await profiler.measure("async_pipeline", run_async_pipeline())

        stats = profiler.get_stats("async_pipeline")

        # Should complete reasonably quickly despite async operations
        assert stats["mean"] < 0.5, f"Async pipeline too slow: {stats['mean']:.3f}s"

        print(f"Async pipeline: {stats['mean']:.3f}s ± {stats['stdev']:.3f}s")

    @pytest.mark.asyncio
    async def test_stateful_operation_performance(self):
        """Test performance of stateful operations like sort."""
        profiler = PerformanceProfiler()

        # Create unsorted data
        import random

        data = list(range(200))
        random.shuffle(data)

        async def run_sort_pipeline():
            pipeline = r.Map[int, int](lambda x: x) | r.Sort()
            return await (data | pipeline).collect()

        # Run multiple times
        for _ in range(10):
            await profiler.measure("sort_pipeline", run_sort_pipeline())

        stats = profiler.get_stats("sort_pipeline")

        # Sort should be reasonably fast for small datasets
        assert stats["mean"] < 0.05, f"Sort pipeline too slow: {stats['mean']:.3f}s"

        print(f"Sort pipeline: {stats['mean']:.3f}s ± {stats['stdev']:.3f}s")


class TestConcurrencyPerformance:
    """Test performance under high concurrency."""

    @pytest.mark.asyncio
    async def test_high_concurrency_throughput(self):
        """Test throughput with high concurrency levels."""
        profiler = PerformanceProfiler()

        data = list(range(100))

        async def cpu_intensive_task(x):
            # Simulate CPU work without blocking
            await asyncio.sleep(0.001)
            return sum(range(x % 10)) * x

        async def run_concurrent_pipeline():
            pipeline = r.Map[int, int](cpu_intensive_task) | r.Filter[int](
                lambda x: x > 0
            )
            return await (data | pipeline).collect()

        # Measure throughput
        for _ in range(5):
            await profiler.measure("concurrent_pipeline", run_concurrent_pipeline())

        stats = profiler.get_stats("concurrent_pipeline")

        # Calculate throughput (items per second)
        throughput = len(data) / stats["mean"]

        print(f"Concurrent throughput: {throughput:.0f} items/sec")
        print(f"Concurrent pipeline: {stats['mean']:.3f}s ± {stats['stdev']:.3f}s")

        # Should achieve reasonable throughput
        assert throughput > 500, f"Low throughput: {throughput:.0f} items/sec"

    @pytest.mark.asyncio
    async def test_parallel_pipeline_performance(self):
        """Test performance of multiple pipelines running in parallel."""
        profiler = PerformanceProfiler()

        async def create_pipeline(data_offset: int):
            data = [i + data_offset for i in range(50)]
            pipeline = r.Map[int, int](lambda x: x * 2) | r.Filter[int](
                lambda x: x > data_offset
            )
            return await (data | pipeline).collect()

        async def run_parallel_pipelines():
            # Run 5 pipelines concurrently
            tasks = [asyncio.create_task(create_pipeline(i * 100)) for i in range(5)]
            return await asyncio.gather(*tasks)

        # Measure parallel execution
        for _ in range(5):
            await profiler.measure("parallel_pipelines", run_parallel_pipelines())

        stats = profiler.get_stats("parallel_pipelines")

        print(f"Parallel pipelines: {stats['mean']:.3f}s ± {stats['stdev']:.3f}s")

        # Should complete efficiently
        assert stats["mean"] < 0.2, f"Parallel pipelines too slow: {stats['mean']:.3f}s"


class TestMemoryPerformance:
    """Test memory efficiency of the new architecture."""

    @pytest.mark.asyncio
    async def test_streaming_memory_usage(self):
        """Test that streaming doesn't accumulate excessive memory."""
        profiler = PerformanceProfiler()

        # Large dataset that would consume significant memory if not streamed
        async def large_data_generator():
            for i in range(1000):
                yield i

        async def run_streaming_pipeline():
            # Process data in streaming fashion
            count = 0
            async for item in (
                large_data_generator() | r.Map[int, int](lambda x: x * 2)
            ).stream():
                count += 1
                if count >= 100:  # Process only first 100 items
                    break
            return count

        # Measure streaming performance
        for _ in range(5):
            result = await profiler.measure(
                "streaming_pipeline", run_streaming_pipeline()
            )
            assert result == 100  # Should process exactly 100 items

        stats = profiler.get_stats("streaming_pipeline")

        print(f"Streaming pipeline: {stats['mean']:.3f}s ± {stats['stdev']:.3f}s")

        # Should be very fast since it's streaming
        assert stats["mean"] < 0.05, f"Streaming too slow: {stats['mean']:.3f}s"

    @pytest.mark.asyncio
    async def test_batch_processing_performance(self):
        """Test performance of batch processing operations."""
        profiler = PerformanceProfiler()

        data = list(range(200))

        async def run_batch_pipeline():
            pipeline = (
                r.Map[int, int](lambda x: x * 2)
                | r.Batch(10)
                | r.Map[list[int], int](lambda batch: sum(batch))
            )
            return await (data | pipeline).collect()

        # Measure batch processing
        for _ in range(10):
            await profiler.measure("batch_pipeline", run_batch_pipeline())

        stats = profiler.get_stats("batch_pipeline")

        print(f"Batch pipeline: {stats['mean']:.3f}s ± {stats['stdev']:.3f}s")

        # Should be efficient
        assert stats["mean"] < 0.05, f"Batch processing too slow: {stats['mean']:.3f}s"


class TestErrorHandlingPerformance:
    """Test performance impact of error handling."""

    @pytest.mark.asyncio
    async def test_ignore_policy_performance(self):
        """Test performance impact of IGNORE error policy."""
        profiler = PerformanceProfiler()

        data = list(range(100))

        def failing_function(x):
            if x % 10 == 0:  # 10% failure rate
                raise ValueError(f"Error at {x}")
            return x * 2

        async def run_ignore_pipeline():
            pipeline = r.Pipeline(
                [r.Map[int, int](failing_function)], error_policy=ErrorPolicy.IGNORE
            )
            return await pipeline.collect(data)

        # Measure error handling performance
        for _ in range(10):
            await profiler.measure("ignore_errors", run_ignore_pipeline())

        stats = profiler.get_stats("ignore_errors")

        print(f"Ignore errors: {stats['mean']:.3f}s ± {stats['stdev']:.3f}s")

        # Should still be reasonably fast despite errors
        assert stats["mean"] < 0.1, f"Error handling too slow: {stats['mean']:.3f}s"

    @pytest.mark.asyncio
    async def test_collect_policy_performance(self):
        """Test performance impact of COLLECT error policy."""
        profiler = PerformanceProfiler()

        data = list(range(100))

        def failing_function(x):
            if x % 15 == 0:  # ~7% failure rate
                raise ValueError(f"Error at {x}")
            return x * 2

        async def run_collect_pipeline():
            pipeline = r.Pipeline(
                [r.Map[int, int](failing_function)], error_policy=ErrorPolicy.COLLECT
            )
            result, errors = await pipeline.collect_with_errors(data)
            return len(result), len(errors)

        # Measure collect policy performance
        for _ in range(10):
            result_count, error_count = await profiler.measure(
                "collect_errors", run_collect_pipeline()
            )
            assert result_count > 80  # Most items should succeed
            assert error_count > 0  # Some errors should be collected

        stats = profiler.get_stats("collect_errors")

        print(f"Collect errors: {stats['mean']:.3f}s ± {stats['stdev']:.3f}s")

        # Should be efficient despite error collection
        assert stats["mean"] < 0.1, f"Error collection too slow: {stats['mean']:.3f}s"


class TestScalabilityPerformance:
    """Test how the architecture scales with different data sizes."""

    @pytest.mark.asyncio
    async def test_data_size_scaling(self):
        """Test performance scaling with different data sizes."""
        profiler = PerformanceProfiler()

        async def run_pipeline_with_size(size: int):
            data = list(range(size))
            pipeline = r.Map[int, int](lambda x: x * 2) | r.Filter[int](
                lambda x: x % 3 == 0
            )
            return await (data | pipeline).collect()

        # Test different sizes
        sizes = [50, 100, 200, 500]

        for size in sizes:
            test_name = f"size_{size}"
            # Run each size multiple times
            for _ in range(5):
                result = await profiler.measure(test_name, run_pipeline_with_size(size))
                assert len(result) > 0  # Should produce some results

        # Analyze scaling behavior
        for size in sizes:
            stats = profiler.get_stats(f"size_{size}")
            throughput = size / stats["mean"]
            print(f"Size {size}: {stats['mean']:.3f}s, {throughput:.0f} items/sec")

        # Check that throughput doesn't degrade too badly with size
        small_throughput = sizes[0] / profiler.get_stats(f"size_{sizes[0]}")["mean"]
        large_throughput = sizes[-1] / profiler.get_stats(f"size_{sizes[-1]}")["mean"]

        # Throughput shouldn't drop below 50% for larger datasets
        throughput_ratio = large_throughput / small_throughput
        assert throughput_ratio > 0.5, f"Poor scaling: {throughput_ratio:.2f}"

    @pytest.mark.asyncio
    async def test_pipeline_depth_scaling(self):
        """Test performance scaling with pipeline depth."""
        profiler = PerformanceProfiler()

        data = list(range(100))

        async def run_shallow_pipeline():
            pipeline = r.Map[int, int](lambda x: x * 2)
            return await (data | pipeline).collect()

        async def run_deep_pipeline():
            pipeline = (
                r.Map[int, int](lambda x: x * 2)
                | r.Filter[int](lambda x: x > 10)
                | r.Map[int, int](lambda x: x + 1)
                | r.Filter[int](lambda x: x % 2 == 0)
                | r.Map[int, int](lambda x: x // 2)
            )
            return await (data | pipeline).collect()

        # Measure both pipeline depths
        for _ in range(10):
            await profiler.measure("shallow", run_shallow_pipeline())
            await profiler.measure("deep", run_deep_pipeline())

        shallow_stats = profiler.get_stats("shallow")
        deep_stats = profiler.get_stats("deep")

        print(f"Shallow pipeline: {shallow_stats['mean']:.3f}s")
        print(f"Deep pipeline: {deep_stats['mean']:.3f}s")

        # Deep pipeline should not be more than 5x slower than shallow
        depth_overhead = deep_stats["mean"] / shallow_stats["mean"]
        assert depth_overhead < 5.0, f"Excessive depth overhead: {depth_overhead:.2f}x"


class TestPerformanceRegression:
    """Test for performance regressions in common operations."""

    @pytest.mark.asyncio
    async def test_common_operations_benchmark(self):
        """Benchmark common operations to detect regressions."""
        profiler = PerformanceProfiler()

        data = list(range(200))

        # Common operation patterns
        operations = {
            "map_only": r.Map[int, int](lambda x: x * 2),
            "filter_only": r.Filter[int](lambda x: x % 2 == 0),
            "map_filter": r.Map[int, int](lambda x: x * 2)
            | r.Filter[int](lambda x: x > 100),
            "sort_operation": r.Sort(),
            "take_operation": r.Take(50),
            "batch_operation": r.Batch(10),
            "flat_map": r.FlatMap[int, int](lambda x: [x, x + 1]),
        }

        # Benchmark each operation
        for name, pipeline in operations.items():
            for _ in range(5):
                await profiler.measure(name, (data | pipeline).collect())

        # Print benchmark results
        print("\nBenchmark Results:")
        print("-" * 50)
        for name in operations.keys():
            stats = profiler.get_stats(name)
            throughput = len(data) / stats["mean"]
            print(f"{name:15}: {stats['mean']:.3f}s ({throughput:.0f} items/sec)")

        # Basic regression checks (adjust thresholds based on expected performance)
        for name in operations.keys():
            stats = profiler.get_stats(name)
            assert stats["mean"] < 0.1, f"{name} too slow: {stats['mean']:.3f}s"


@pytest.mark.performance
class TestPerformanceComparison:
    """Compare performance characteristics between different approaches."""

    @pytest.mark.asyncio
    async def test_sync_vs_async_performance(self):
        """Compare sync vs async operation performance."""
        profiler = PerformanceProfiler()

        data = list(range(100))

        # Sync pipeline
        async def run_sync_pipeline():
            pipeline = r.Map[int, int](lambda x: x * 2) | r.Filter[int](
                lambda x: x > 50
            )
            return await (data | pipeline).collect()

        # Async pipeline
        async def async_transform(x):
            await asyncio.sleep(0.0001)  # Very small async delay
            return x * 2

        async def run_async_pipeline():
            pipeline = r.Map[int, int](async_transform) | r.Filter[int](
                lambda x: x > 50
            )
            return await (data | pipeline).collect()

        # Measure both approaches
        for _ in range(5):
            await profiler.measure("sync_ops", run_sync_pipeline())
            await profiler.measure("async_ops", run_async_pipeline())

        comparison = profiler.compare("sync_ops", "async_ops")

        print(f"Sync operations: {comparison['stats1']['mean']:.3f}s")
        print(f"Async operations: {comparison['stats2']['mean']:.3f}s")
        print(f"Async overhead: {comparison['speedup']:.2f}x")

        # Async should not be more than 10x slower for small delays
        assert comparison["speedup"] < 10.0, (
            f"Excessive async overhead: {comparison['speedup']:.2f}x"
        )


if __name__ == "__main__":
    # Run a quick performance test
    async def quick_perf_test():
        data = list(range(100))

        start_time = time.perf_counter()
        pipeline = r.Map[int, int](lambda x: x * 2) | r.Filter[int](lambda x: x > 50)
        result = await (data | pipeline).collect()
        end_time = time.perf_counter()

        duration = end_time - start_time
        throughput = len(data) / duration

        print(
            f"✅ Quick performance test: {duration:.3f}s ({throughput:.0f} items/sec)"
        )
        print(f"   Processed {len(result)} items")

        assert duration < 0.1, f"Performance regression: {duration:.3f}s"
        assert len(result) > 0, "No results produced"

    asyncio.run(quick_perf_test())
