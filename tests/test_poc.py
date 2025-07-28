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


if __name__ == "__main__":
    # Simple test runner for manual execution
    async def run_tests():
        print("Running basic stream test...")
        test = TestStream()
        await test.test_stream_basic_operations()
        print("✓ Basic stream test passed")
        
        print("Running parallelism test...")
        test = TestStatelessPipelineStep()
        await test.test_parallelism_performance()
        print("✓ Parallelism test passed")
        
        print("Running pipeline chaining test...")
        test = TestPipelineChaining()
        await test.test_simple_chain()
        print("✓ Pipeline chaining test passed")
        
        print("All tests completed successfully!")
    
    asyncio.run(run_tests())