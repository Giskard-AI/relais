import asyncio
import pytest
import time
from typing import List
from src.relais.poc import Stream, StatelessPipelineStep, StatefulPipelineStep, EndEvent


class TestStream:
    
    @pytest.mark.asyncio
    async def test_stream_basic_operations(self):
        stream = Stream[int]()
        
        # Test putting items
        await stream.put(1)
        await stream.put(2)
        await stream.put(3)
        await stream.end()
        
        # Test iteration
        items = []
        async for item in stream:
            items.append(item)
        
        assert items == [1, 2, 3]
    
    @pytest.mark.asyncio
    async def test_stream_early_end(self):
        stream = Stream[int]()
        
        await stream.put(1)
        await stream.end()
        
        # Should raise error when trying to put after end
        with pytest.raises(ValueError):
            await stream.put(2)
    
    @pytest.mark.asyncio
    async def test_stream_empty(self):
        stream = Stream[int]()
        await stream.end()
        
        items = []
        async for item in stream:
            items.append(item)
        
        assert items == []


class MapStep(StatelessPipelineStep[int, int, int]):
    def __init__(self, func):
        self.func = func
        self.next = None
    
    async def _process(self, item: int) -> int:
        # Simulate some processing time to test parallelism
        await asyncio.sleep(0.01)
        return self.func(item)


class FilterStep(StatelessPipelineStep[int, int, int]):
    def __init__(self, predicate):
        self.predicate = predicate
        self.next = None
    
    async def _process(self, item: int) -> int:
        await asyncio.sleep(0.01)
        if self.predicate(item):
            return item
        # In a real implementation, we'd need a way to skip items
        # For now, we'll return None and filter it out later
        return None


class SortStep(StatefulPipelineStep[int, int, int]):
    def __init__(self, reverse=False):
        self.reverse = reverse
        self.next = None
    
    async def _process(self, stream_data: List[int]) -> List[int]:
        await asyncio.sleep(0.01)  # Simulate processing time
        return sorted(stream_data, reverse=self.reverse)


class TestStatelessPipelineStep:
    
    @pytest.mark.asyncio
    async def test_map_step_basic(self):
        # Create input stream
        input_stream = Stream[int]()
        
        # Create map step
        map_step = MapStep(lambda x: x * 2)
        
        # Process in background
        result_generator = map_step.process(input_stream)
        
        # Add data to input stream
        asyncio.create_task(self._populate_stream(input_stream, [1, 2, 3, 4, 5]))
        
        # Collect results
        results = []
        async for item in result_generator:
            results.append(item)
        
        assert sorted(results) == [2, 4, 6, 8, 10]
    
    @pytest.mark.asyncio
    async def test_parallelism_performance(self):
        input_stream = Stream[int]()
        map_step = MapStep(lambda x: x * 2)
        
        # Test with more items to see parallelism benefits
        test_data = list(range(20))
        
        start_time = time.time()
        
        result_generator = map_step.process(input_stream)
        asyncio.create_task(self._populate_stream(input_stream, test_data))
        
        results = []
        async for item in result_generator:
            results.append(item)
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # With 20 items and 0.01s sleep each, sequential would take ~0.2s
        # Parallel should be much faster
        assert processing_time < 0.15  # Should be much less than sequential
        assert len(results) == 20
    
    async def _populate_stream(self, stream: Stream[int], data: List[int]):
        for item in data:
            await stream.put(item)
        await stream.end()


class TestStatefulPipelineStep:
    
    @pytest.mark.asyncio
    async def test_sort_step_basic(self):
        input_stream = Stream[int]()
        sort_step = SortStep()
        
        result_generator = sort_step.process(input_stream)
        
        # Add unordered data
        asyncio.create_task(self._populate_stream(input_stream, [5, 2, 8, 1, 9, 3]))
        
        results = []
        async for item in result_generator:
            results.append(item)
        
        assert results == [1, 2, 3, 5, 8, 9]
    
    @pytest.mark.asyncio
    async def test_sort_step_reverse(self):
        input_stream = Stream[int]()
        sort_step = SortStep(reverse=True)
        
        result_generator = sort_step.process(input_stream)
        
        asyncio.create_task(self._populate_stream(input_stream, [5, 2, 8, 1, 9, 3]))
        
        results = []
        async for item in result_generator:
            results.append(item)
        
        assert results == [9, 8, 5, 3, 2, 1]
    
    async def _populate_stream(self, stream: Stream[int], data: List[int]):
        for item in data:
            await stream.put(item)
        await stream.end()


class TestPipelineChaining:
    
    @pytest.mark.asyncio
    async def test_simple_chain(self):
        """Test chaining map -> sort"""
        input_stream = Stream[int]()
        
        # Create pipeline: map(x * 2) -> sort
        map_step = MapStep(lambda x: x * 2)
        sort_step = SortStep()
        map_step.next = sort_step
        
        result_generator = map_step.process(input_stream)
        
        asyncio.create_task(self._populate_stream(input_stream, [3, 1, 4, 1, 5]))
        
        results = []
        async for item in result_generator:
            results.append(item)
        
        # Should be [3,1,4,1,5] -> [6,2,8,2,10] -> [2,2,6,8,10]
        assert results == [2, 2, 6, 8, 10]
    
    @pytest.mark.asyncio
    async def test_complex_chain_performance(self):
        """Test performance of chained operations with parallelism"""
        input_stream = Stream[int]()
        
        # Create pipeline: map(x * 2) -> map(x + 1) -> sort
        map1 = MapStep(lambda x: x * 2)
        map2 = MapStep(lambda x: x + 1)
        sort_step = SortStep()
        
        map1.next = map2
        map2.next = sort_step
        
        test_data = list(range(15))
        
        start_time = time.time()
        
        result_generator = map1.process(input_stream)
        asyncio.create_task(self._populate_stream(input_stream, test_data))
        
        results = []
        async for item in result_generator:
            results.append(item)
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Verify correctness: [0,1,2,3,4...] -> [0,2,4,6,8...] -> [1,3,5,7,9...] -> sorted
        expected = sorted([x * 2 + 1 for x in test_data])
        assert results == expected
        
        # Performance check - should benefit from parallelism in map steps
        assert processing_time < 0.25  # Much less than sequential would take
    
    async def _populate_stream(self, stream: Stream[int], data: List[int]):
        for item in data:
            await stream.put(item)
        await stream.end()


class TestConcurrency:
    
    @pytest.mark.asyncio
    async def test_concurrent_processing_order(self):
        """Test that items can be processed out of order due to parallelism"""
        input_stream = Stream[int]()
        
        # Create a map step with variable processing time
        class VariableTimeMapStep(StatelessPipelineStep[int, int, int]):
            async def _process(self, item: int) -> int:
                # Larger numbers take longer to process
                await asyncio.sleep(item * 0.01)
                return item * 10
        
        map_step = VariableTimeMapStep()
        result_generator = map_step.process(input_stream)
        
        # Add items where larger numbers are added first
        asyncio.create_task(self._populate_stream(input_stream, [5, 4, 3, 2, 1]))
        
        results = []
        async for item in result_generator:
            results.append(item)
        
        # Results should be present but may be out of order due to parallelism
        assert sorted(results) == [10, 20, 30, 40, 50]
        # First result should likely be the fastest (smallest number)
        assert results[0] == 10  # Item 1 processes fastest
    
    async def _populate_stream(self, stream: Stream[int], data: List[int]):
        for item in data:
            await stream.put(item)
        await stream.end()


class TestParallelismDepth:
    """Tests to ensure parallelism is truly concurrent and in-depth"""
    
    @pytest.mark.asyncio
    async def test_true_parallel_execution_timing(self):
        """Verify that processing happens truly in parallel, not sequentially"""
        input_stream = Stream[int]()
        
        # Track when each task starts and ends
        execution_log = []
        
        class TimingMapStep(StatelessPipelineStep[int, int, int]):
            async def _process(self, item: int) -> int:
                start_time = time.time()
                execution_log.append(f"Task {item} started at {start_time:.3f}")
                
                # All tasks take the same time to process
                await asyncio.sleep(0.1)  # 100ms processing time
                
                end_time = time.time()
                execution_log.append(f"Task {item} ended at {end_time:.3f}")
                return item * 2
        
        map_step = TimingMapStep()
        result_generator = map_step.process(input_stream)
        
        # Start timing
        overall_start = time.time()
        
        # Add multiple items quickly
        asyncio.create_task(self._populate_stream(input_stream, [1, 2, 3, 4, 5]))
        
        results = []
        async for item in result_generator:
            results.append(item)
        
        overall_end = time.time()
        total_time = overall_end - overall_start
        
        # Print execution log for debugging
        print("\nExecution log:")
        for log_entry in execution_log:
            print(log_entry)
        
        # Verify results are correct
        assert sorted(results) == [2, 4, 6, 8, 10]
        
        # Key test: If sequential, would take 5 * 0.1 = 0.5 seconds
        # If parallel, should take roughly 0.1 seconds (plus overhead)
        print(f"Total execution time: {total_time:.3f}s")
        assert total_time < 0.25, f"Expected parallel execution (~0.1s), got {total_time:.3f}s"
        
        # Verify overlapping execution by checking start/end times
        start_times = []
        end_times = []
        for log_entry in execution_log:
            if "started" in log_entry:
                start_times.append(float(log_entry.split()[-1]))
            elif "ended" in log_entry:
                end_times.append(float(log_entry.split()[-1]))
        
        # All tasks should start before any task ends (true parallelism)
        min_end_time = min(end_times)
        max_start_time = max(start_times)
        assert max_start_time < min_end_time, "Tasks are not executing in parallel!"
    
    @pytest.mark.asyncio
    async def test_concurrent_capacity_limits(self):
        """Test behavior with high concurrency to verify true parallelism"""
        input_stream = Stream[int]()
        
        # Track active concurrent tasks
        active_tasks = set()
        max_concurrent = 0
        concurrent_log = []
        
        class ConcurrencyTrackingStep(StatelessPipelineStep[int, int, int]):
            async def _process(self, item: int) -> int:
                nonlocal max_concurrent
                
                # Track this task as active
                task_id = f"task_{item}"
                active_tasks.add(task_id)
                current_concurrent = len(active_tasks)
                max_concurrent = max(max_concurrent, current_concurrent)
                
                concurrent_log.append(f"Task {item}: {current_concurrent} concurrent tasks active")
                
                # Simulate work
                await asyncio.sleep(0.05)
                
                # Remove from active tasks
                active_tasks.discard(task_id)
                return item
        
        map_step = ConcurrencyTrackingStep()
        result_generator = map_step.process(input_stream)
        
        # Add many items to test concurrency depth
        test_data = list(range(20))
        asyncio.create_task(self._populate_stream(input_stream, test_data))
        
        results = []
        async for item in result_generator:
            results.append(item)
        
        print(f"\nMax concurrent tasks: {max_concurrent}")
        print("Concurrency log (first 10 entries):")
        for log_entry in concurrent_log[:10]:
            print(log_entry)
        
        # Verify all items processed
        assert sorted(results) == test_data
        
        # Should achieve significant concurrency with 20 items
        assert max_concurrent >= 10, f"Expected high concurrency, got max {max_concurrent}"
    
    @pytest.mark.asyncio
    async def test_processing_order_randomness(self):
        """Verify that processing order varies due to true parallelism"""
        input_stream = Stream[int]()
        
        # Items with random processing times to ensure order variation
        import random
        
        class RandomTimingStep(StatelessPipelineStep[int, int, int]):
            async def _process(self, item: int) -> int:
                # Random delay between 0.01 and 0.05 seconds
                delay = random.uniform(0.01, 0.05)
                await asyncio.sleep(delay)
                return item
        
        map_step = RandomTimingStep()
        result_generator = map_step.process(input_stream)
        
        test_data = list(range(15))  # [0, 1, 2, ..., 14]
        asyncio.create_task(self._populate_stream(input_stream, test_data))
        
        results = []
        async for item in result_generator:
            results.append(item)
        
        print(f"\nInput order:  {test_data}")
        print(f"Output order: {results}")
        
        # All items should be present
        assert sorted(results) == test_data
        
        # Due to random timing, output order should differ from input order
        # (This test might occasionally fail due to randomness, but very unlikely)
        differences = sum(1 for i, (a, b) in enumerate(zip(test_data, results)) if a != b)
        print(f"Order differences: {differences}/15")
        
        # Expect some order changes due to parallel processing
        assert differences > 0, "Expected some reordering due to parallelism"
    
    @pytest.mark.asyncio
    async def test_chain_parallelism_depth(self):
        """Test that parallelism works correctly in chained pipeline steps"""
        input_stream = Stream[int]()
        
        # Track execution across the pipeline
        execution_tracker = {"step1": [], "step2": []}
        
        class TrackingStep1(StatelessPipelineStep[int, int, int]):
            async def _process(self, item: int) -> int:
                start = time.time()
                execution_tracker["step1"].append(f"Step1 processing {item} at {start:.3f}")
                await asyncio.sleep(0.05)  # 50ms processing
                return item * 2
        
        class TrackingStep2(StatelessPipelineStep[int, int, int]):
            async def _process(self, item: int) -> int:
                start = time.time()
                execution_tracker["step2"].append(f"Step2 processing {item} at {start:.3f}")
                await asyncio.sleep(0.05)  # 50ms processing
                return item + 1
        
        step1 = TrackingStep1()
        step2 = TrackingStep2()
        step1.next = step2
        
        overall_start = time.time()
        
        result_generator = step1.process(input_stream)
        asyncio.create_task(self._populate_stream(input_stream, [1, 2, 3, 4, 5]))
        
        results = []
        async for item in result_generator:
            results.append(item)
        
        overall_end = time.time()
        total_time = overall_end - overall_start
        
        print(f"\nStep 1 execution log:")
        for log in execution_tracker["step1"]:
            print(log)
        print(f"Step 2 execution log:")
        for log in execution_tracker["step2"]:
            print(log)
        print(f"Total pipeline time: {total_time:.3f}s")
        
        # Expected results: [1,2,3,4,5] -> [2,4,6,8,10] -> [3,5,7,9,11]
        assert sorted(results) == [3, 5, 7, 9, 11]
        
        # Both steps should show parallel execution
        assert len(execution_tracker["step1"]) == 5
        assert len(execution_tracker["step2"]) == 5
        
        # Total time should be much less than sequential (5 * 0.05 * 2 = 0.5s per step)
        assert total_time < 0.3, f"Pipeline not parallel enough: {total_time:.3f}s"
    
    @pytest.mark.asyncio
    async def test_simultaneous_task_execution(self):
        """Test that multiple tasks genuinely execute simultaneously"""
        input_stream = Stream[int]()
        
        # Use a shared counter to track simultaneous execution
        simultaneous_counter = {"count": 0, "max_simultaneous": 0}
        
        class SimultaneousTrackingStep(StatelessPipelineStep[int, int, int]):
            async def _process(self, item: int) -> int:
                # Increment counter at start
                simultaneous_counter["count"] += 1
                current_count = simultaneous_counter["count"]
                simultaneous_counter["max_simultaneous"] = max(
                    simultaneous_counter["max_simultaneous"], 
                    current_count
                )
                
                print(f"Task {item}: {current_count} tasks running simultaneously")
                
                # Hold the task for a bit to ensure overlap
                await asyncio.sleep(0.1)
                
                # Decrement counter at end
                simultaneous_counter["count"] -= 1
                
                return item
        
        map_step = SimultaneousTrackingStep()
        result_generator = map_step.process(input_stream)
        
        # Add items quickly
        asyncio.create_task(self._populate_stream(input_stream, [1, 2, 3, 4, 5, 6, 7, 8]))
        
        results = []
        async for item in result_generator:
            results.append(item)
        
        print(f"\nMax simultaneous tasks: {simultaneous_counter['max_simultaneous']}")
        
        # Verify all items processed
        assert sorted(results) == [1, 2, 3, 4, 5, 6, 7, 8]
        
        # Should have multiple tasks running simultaneously
        assert simultaneous_counter["max_simultaneous"] >= 5, \
            f"Expected multiple simultaneous tasks, got max {simultaneous_counter['max_simultaneous']}"
    
    async def _populate_stream(self, stream: Stream[int], data: List[int]):
        for item in data:
            await stream.put(item)
        await stream.end()
