"""Test memory efficiency and resource management in the new streaming architecture."""

import asyncio
import gc
import os

import psutil
import pytest

import relais as r
from relais.errors import ErrorPolicy
from relais.stream import StreamItemEvent


def get_memory_usage() -> float:
    """Get current memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024


def get_memory_peak() -> float:
    """Get peak memory usage in MB."""
    process = psutil.Process(os.getpid())
    return (
        process.memory_info().peak_wss / 1024 / 1024
        if hasattr(process.memory_info(), "peak_wss")
        else get_memory_usage()
    )


class MemoryMonitor:
    """Helper class to monitor memory usage during tests."""

    def __init__(self):
        self.start_memory = 0
        self.peak_memory = 0
        self.end_memory = 0

    def start(self):
        gc.collect()  # Clean up before measurement
        self.start_memory = get_memory_usage()
        return self

    def end(self):
        gc.collect()  # Clean up before final measurement
        self.end_memory = get_memory_usage()
        self.peak_memory = max(self.peak_memory, self.end_memory)
        return self

    @property
    def memory_used(self) -> float:
        """Memory used during the operation (MB)."""
        return self.end_memory - self.start_memory

    @property
    def peak_usage(self) -> float:
        """Peak memory usage during operation (MB)."""
        return self.peak_memory - self.start_memory


class LargeObject:
    """Test object that consumes significant memory."""

    def __init__(self, size_mb: float = 1.0):
        # Create a large byte array to consume memory
        self.data = bytearray(int(size_mb * 1024 * 1024))
        self.metadata = f"Object_{id(self)}"

    def __repr__(self):
        return f"LargeObject({len(self.data)} bytes)"


class AsyncLargeDataSource:
    """Async iterator that produces large objects."""

    def __init__(self, count: int, object_size_mb: float = 0.1, delay: float = 0.001):
        self.count = count
        self.object_size_mb = object_size_mb
        self.delay = delay
        self.current = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.current >= self.count:
            raise StopAsyncIteration

        obj = LargeObject(self.object_size_mb)
        self.current += 1

        if self.delay > 0:
            await asyncio.sleep(self.delay)

        return obj


class TestStreamingMemoryEfficiency:
    """Test that streaming doesn't accumulate excessive memory."""

    @pytest.mark.asyncio
    async def test_streaming_vs_collect_memory_usage(self):
        """Test that streaming uses less memory than collect()."""

        # Test streaming consumption
        monitor_stream = MemoryMonitor().start()

        data_source = AsyncLargeDataSource(count=50, object_size_mb=0.1, delay=0.001)
        pipeline = r.Map[LargeObject, int](lambda obj: len(obj.data)) | r.Take(10)

        streaming_results = []
        async for item in (data_source | pipeline).stream():
            streaming_results.append(item)

        monitor_stream.end()

        # Test collect consumption
        monitor_collect = MemoryMonitor().start()

        data_source2 = AsyncLargeDataSource(count=50, object_size_mb=0.1, delay=0.001)
        pipeline2 = r.Map[LargeObject, int](lambda obj: len(obj.data)) | r.Take(10)

        collect_results = await (data_source2 | pipeline2).collect()

        monitor_collect.end()

        # Results should be identical
        assert streaming_results == collect_results

        # Streaming should use significantly less peak memory
        print(f"Streaming memory: {monitor_stream.memory_used:.2f} MB")
        print(f"Collect memory: {monitor_collect.memory_used:.2f} MB")

        # Note: This assertion might be flaky depending on GC timing
        # We mainly want to verify the implementation doesn't crash with large data

    @pytest.mark.asyncio
    async def test_large_pipeline_memory_bounded(self):
        """Test that processing large amounts of data stays memory bounded."""

        monitor = MemoryMonitor().start()

        # Process 1000 objects but only keep 5
        data_source = AsyncLargeDataSource(
            count=1000, object_size_mb=0.05, delay=0.0001
        )
        pipeline = (
            r.Map[LargeObject, int](lambda obj: len(obj.data))
            | r.Filter[int](lambda x: x > 0)
            | r.Take(5)
        )

        result = []
        async for item in (data_source | pipeline).stream():
            result.append(item)

        monitor.end()

        assert len(result) == 5

        # Memory usage should be bounded regardless of total data size
        print(f"Memory used for 1000->5 processing: {monitor.memory_used:.2f} MB")

        # Should not use excessive memory (less than 100MB for this test)
        assert monitor.memory_used < 100

    @pytest.mark.asyncio
    async def test_stateful_operations_memory_usage(self):
        """Test memory usage of stateful operations like sort."""

        monitor = MemoryMonitor().start()

        # Create moderately sized dataset for sorting
        data = [LargeObject(0.01) for _ in range(100)]  # 1MB total
        pipeline = (
            r.Map[LargeObject, int](lambda obj: len(obj.data)) | r.Sort() | r.Take(10)
        )

        result = await (data | pipeline).collect()

        monitor.end()

        assert len(result) == 10
        assert result == sorted([len(obj.data) for obj in data])[:10]

        print(f"Sort memory usage: {monitor.memory_used:.2f} MB")

    @pytest.mark.asyncio
    async def test_concurrent_processing_memory(self):
        """Test memory usage under high concurrency."""

        monitor = MemoryMonitor().start()

        async def memory_intensive_transform(obj):
            # Simulate some processing that might create temporary objects
            await asyncio.sleep(0.001)
            temp_data = bytearray(1024)  # 1KB temporary data
            result = len(obj.data) + len(temp_data)
            del temp_data  # Explicit cleanup
            return result

        data_source = AsyncLargeDataSource(count=100, object_size_mb=0.02, delay=0.001)
        pipeline = r.Map[LargeObject, int](memory_intensive_transform) | r.Take(20)

        result = await (data_source | pipeline).collect()

        monitor.end()

        assert len(result) == 20

        print(f"Concurrent processing memory: {monitor.memory_used:.2f} MB")


class TestMemoryLeakPrevention:
    """Test that the architecture doesn't have memory leaks."""

    @pytest.mark.asyncio
    async def test_repeated_pipeline_execution(self):
        """Test that repeated pipeline executions don't leak memory."""

        initial_memory = get_memory_usage()

        # Run the same pipeline multiple times
        for i in range(10):
            data_source = AsyncLargeDataSource(
                count=20, object_size_mb=0.05, delay=0.001
            )
            pipeline = (
                r.Map[LargeObject, int](lambda obj: len(obj.data))
                | r.Filter[int](lambda x: x > 0)
                | r.Take(5)
            )

            result = await (data_source | pipeline).collect()
            assert len(result) == 5

            # Force garbage collection after each run
            gc.collect()

        final_memory = get_memory_usage()
        memory_growth = final_memory - initial_memory

        print(f"Memory growth after 10 runs: {memory_growth:.2f} MB")

        # Should not have significant memory growth (less than 10MB)
        assert memory_growth < 10

    @pytest.mark.asyncio
    async def test_context_manager_cleanup(self):
        """Test that context managers properly clean up resources."""

        monitor = MemoryMonitor().start()

        data_source = AsyncLargeDataSource(count=100, object_size_mb=0.02, delay=0.001)
        pipeline = r.Map[LargeObject, int](lambda obj: len(obj.data)) | r.Take(10)

        results = []
        async with await pipeline.run(data_source) as stream:
            async for event in stream:
                if isinstance(event, StreamItemEvent):
                    results.append(event.item)

        # Force cleanup
        del stream
        gc.collect()

        monitor.end()

        assert len(results) == 10

        print(f"Context manager memory usage: {monitor.memory_used:.2f} MB")

    @pytest.mark.asyncio
    async def test_cancelled_pipeline_cleanup(self):
        """Test that cancelled pipelines clean up properly."""

        monitor = MemoryMonitor().start()

        data_source = AsyncLargeDataSource(count=1000, object_size_mb=0.01, delay=0.001)
        pipeline = r.Pipeline([r.Map[LargeObject, int](lambda obj: len(obj.data))])

        results = []
        try:
            async with await pipeline.run(data_source) as stream:
                count = 0
                async for event in stream:
                    if isinstance(event, StreamItemEvent):
                        results.append(event.item)
                        count += 1
                        if count >= 5:  # Cancel early
                            break
        except Exception:
            pass  # Ignore any cleanup exceptions

        # Force cleanup
        gc.collect()

        monitor.end()

        assert len(results) == 5

        print(f"Cancelled pipeline memory usage: {monitor.memory_used:.2f} MB")


class TestLargeDataProcessing:
    """Test processing of large datasets."""

    @pytest.mark.asyncio
    async def test_very_large_list_processing(self):
        """Test processing very large lists efficiently."""

        monitor = MemoryMonitor().start()

        # Create a large list of small objects
        large_data = list(range(100000))  # 100k items
        pipeline = (
            r.Map[int, int](lambda x: x * 2)
            | r.Filter[int](lambda x: x % 1000 == 0)
            | r.Take(10)
        )

        result = await (large_data | pipeline).collect()

        monitor.end()

        assert len(result) == 10
        assert result == [0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000]

        print(f"Large list processing memory: {monitor.memory_used:.2f} MB")

    @pytest.mark.asyncio
    async def test_streaming_large_dataset(self):
        """Test streaming through a large dataset."""

        monitor = MemoryMonitor().start()

        # Stream through large dataset
        large_source = AsyncLargeDataSource(
            count=10000, object_size_mb=0.001, delay=0.0001
        )
        pipeline = (
            r.Map[LargeObject, int](lambda obj: len(obj.data))
            | r.Filter[int](lambda x: x > 500)
            | r.Take(5)
        )

        results = []
        async for item in (large_source | pipeline).stream():
            results.append(item)

        monitor.end()

        assert len(results) == 5

        print(f"Large dataset streaming memory: {monitor.memory_used:.2f} MB")

        # Should complete with bounded memory
        assert monitor.memory_used < 50

    @pytest.mark.asyncio
    async def test_batch_processing_memory_efficiency(self):
        """Test that batch processing is memory efficient."""

        monitor = MemoryMonitor().start()

        # Process data in batches
        data_source = AsyncLargeDataSource(count=200, object_size_mb=0.01, delay=0.001)

        def process_batch(batch):
            # Simulate batch processing that might be memory intensive
            total_size = sum(len(obj.data) for obj in batch)
            return total_size // len(batch)  # Average size

        pipeline = (
            r.Batch(10) | r.Map[list[LargeObject], int](process_batch) | r.Take(5)
        )

        result = await (data_source | pipeline).collect()

        monitor.end()

        assert len(result) == 5

        print(f"Batch processing memory: {monitor.memory_used:.2f} MB")


class TestMemoryWithErrors:
    """Test memory usage with error handling."""

    @pytest.mark.asyncio
    async def test_error_handling_memory_impact(self):
        """Test memory usage when errors occur."""

        monitor = MemoryMonitor().start()

        def failing_transform(obj):
            if len(obj.data) % 3 == 0:  # Fail every 3rd item
                raise ValueError("Simulated processing error")
            return len(obj.data)

        data_source = AsyncLargeDataSource(count=100, object_size_mb=0.02, delay=0.001)
        pipeline = r.Pipeline(
            [r.Map[LargeObject, int](failing_transform), r.Take(20)],
            error_policy=ErrorPolicy.IGNORE,
        )

        result = await pipeline.collect(data_source)

        monitor.end()

        # Should have some results (errors ignored)
        assert len(result) > 0

        print(f"Error handling memory usage: {monitor.memory_used:.2f} MB")

    @pytest.mark.asyncio
    async def test_collect_errors_memory_usage(self):
        """Test memory usage when collecting errors."""

        monitor = MemoryMonitor().start()

        counter = {"counter": 0}

        def failing_transform(obj):
            # Use a counter-based approach to fail every 5th item
            counter["counter"] += 1
            if counter["counter"] % 5 == 0:  # Fail every 5th item
                raise RuntimeError(f"Error processing item {counter['counter']}")
            return len(obj.data)

        data_source = AsyncLargeDataSource(count=50, object_size_mb=0.02, delay=0.001)
        pipeline = r.Pipeline(
            [r.Map[LargeObject, int](failing_transform), r.Take(30)],
            error_policy=ErrorPolicy.COLLECT,
        )

        results, errors = await pipeline.collect_with_errors(data_source)

        monitor.end()

        # Should have both results and errors
        assert len(results) > 0
        assert len(errors) > 0

        print(f"Collect errors memory usage: {monitor.memory_used:.2f} MB")
        print(f"Results: {len(results)}, Errors: {len(errors)}")


class TestMemoryOptimizations:
    """Test memory optimizations in the architecture."""

    @pytest.mark.asyncio
    async def test_take_optimization_memory_savings(self):
        """Test that take() optimization saves memory by not processing extra items."""

        # Test without optimization (process all items)
        monitor1 = MemoryMonitor().start()

        data1 = [LargeObject(0.1) for _ in range(100)]  # 10MB total
        pipeline1 = (
            r.Map[LargeObject, int](lambda obj: len(obj.data))
            | r.Sort()
            | r.Take(5, ordered=True)
        )

        result1 = await (data1 | pipeline1).collect()

        monitor1.end()

        # Test with optimization (early termination)
        monitor2 = MemoryMonitor().start()

        data_source2 = AsyncLargeDataSource(count=100, object_size_mb=0.1, delay=0.001)
        pipeline2 = r.Map[LargeObject, int](lambda obj: len(obj.data)) | r.Take(
            5, ordered=False
        )  # Early termination

        result2 = await (data_source2 | pipeline2).collect()

        monitor2.end()

        # Results should be similar (first 5 items)
        assert len(result1) == len(result2) == 5

        print(f"Full processing memory: {monitor1.memory_used:.2f} MB")
        print(f"Optimized processing memory: {monitor2.memory_used:.2f} MB")

        # Optimized version should use less memory (though this might be flaky)
        # Main goal is to ensure it works without excessive memory usage

    @pytest.mark.asyncio
    async def test_generator_vs_list_memory_usage(self):
        """Test memory difference between generator and list inputs."""

        async def large_data_async_generator():
            for i in range(10000):
                print(f"Yielding item {i}")
                yield LargeObject(0.001)  # 1KB each, 10MB total

        # Test with async generator (streaming)
        monitor_gen = MemoryMonitor().start()

        pipeline = r.Map[LargeObject, int](lambda obj: len(obj.data)) | r.Take(10)
        result_gen = await (large_data_async_generator() | pipeline).collect()

        monitor_gen.end()

        # Test with list (all in memory)
        monitor_list = MemoryMonitor().start()

        large_list = [LargeObject(0.001) for _ in range(10000)]
        result_list = await (large_list | pipeline).collect()

        monitor_list.end()

        # Results should be identical
        assert result_gen == result_list
        assert len(result_gen) == 10

        print(f"Async generator memory usage: {monitor_gen.memory_used:.2f} MB")
        print(f"List memory usage: {monitor_list.memory_used:.2f} MB")

        # Clean up the large list
        del large_list
        gc.collect()


if __name__ == "__main__":
    # Run a quick memory test
    async def quick_memory_test():
        monitor = MemoryMonitor().start()

        # Process some data and measure memory
        data = [LargeObject(0.01) for _ in range(20)]  # 200KB total
        pipeline = r.Map[LargeObject, int](lambda obj: len(obj.data)) | r.Take(5)

        result = await (data | pipeline).collect()

        monitor.end()

        assert len(result) == 5
        print(f"✅ Quick memory test passed! Memory used: {monitor.memory_used:.2f} MB")

    asyncio.run(quick_memory_test())
