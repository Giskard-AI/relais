#!/usr/bin/env python3
"""Tests for complex pipeline operations and integrations."""

import asyncio
import time
import pytest
from typing import List
from src.relais import Pipeline, Stream, map, filter, sort, flat_map

class TestPipelines:
    
    @pytest.mark.asyncio
    async def test_simple_pipeline_chaining(self):
        """Test chaining multiple operations."""
        # Chain: map -> filter -> sort
        pipeline = (
            map(lambda x: x * 2) |
            filter(lambda x: x > 5) |
            sort(reverse=True)
        )
        
        actual = await ([1, 2, 3, 4, 5, 6] | pipeline).collect()
        
        # [1,2,3,4,5,6] -> [2,4,6,8,10,12] -> [6,8,10,12] -> [12,10,8,6]
        expected = [12, 10, 8, 6]
        assert actual == expected

    @pytest.mark.asyncio
    async def test_complex_pipeline_all_operations(self):
        """Test a complex pipeline with all operations."""
        async def async_multiply(x: int) -> int:
            await asyncio.sleep(0.01)
            return x * 3
            
        def duplicate_and_increment(x: int) -> List[int]:
            return [x, x + 1]
        
        pipeline = (
            map(async_multiply) |           # Multiply by 3 async
            filter(lambda x: x < 20) |      # Keep values < 20
            flat_map(duplicate_and_increment) |  # Duplicate and add 1
            sort()                          # Sort final result
        )
        
        actual = await ([1, 2, 3, 4, 5, 6, 7] | pipeline).collect()
        
        # [1,2,3,4,5,6,7] -> [3,6,9,12,15,18,21] 
        # -> [3,6,9,12,15,18] -> [3,4,6,7,9,10,12,13,15,16,18,19]
        expected = [3, 4, 6, 7, 9, 10, 12, 13, 15, 16, 18, 19]
        assert actual == expected

    @pytest.mark.asyncio
    async def test_data_processing_pipeline(self):
        """Test a realistic data processing pipeline."""
        # Simulate processing user data
        users = [
            {"name": "Alice", "age": 25, "active": True},
            {"name": "Bob", "age": 30, "active": False},
            {"name": "Charlie", "age": 35, "active": True},
            {"name": "David", "age": 20, "active": True},
            {"name": "Eve", "age": 40, "active": False},
        ]
        
        async def enrich_user(user):
            await asyncio.sleep(0.01)  # Simulate API call
            return {**user, "category": "senior" if user["age"] >= 30 else "junior"}
        
        def extract_tags(user):
            tags = []
            tags.append(user["category"])
            if user["active"]:
                tags.append("active")
            return tags
        
        pipeline = (
            filter(lambda user: user["active"]) |      # Only active users
            map(enrich_user) |                         # Add category
            flat_map(extract_tags) |                   # Extract tags
            sort()                                     # Sort tags
        )
        
        actual = await (users | pipeline).collect()
        
        # Active users: Alice(junior), Charlie(senior), David(junior)
        # Tags: [junior, active, senior, active, junior, active]
        expected = ["active", "active", "active", "junior", "junior", "senior"]
        assert actual == expected

    @pytest.mark.asyncio
    async def test_streaming_vs_collect(self):
        """Test streaming results vs collecting all."""
        pipeline = (
            map(lambda x: x * 2) | 
            filter(lambda x: x > 4) |
            sort()
        )
        
        # Test collect (get all at once)
        actual = await ([1, 2, 3, 4, 5] | pipeline).collect()
        
        # Test streaming (get one by one)
        streamed_items = []
        async for item in ([1, 2, 3, 4, 5] | pipeline).stream():
            streamed_items.append(item.item)
        
        # Should get same results
        expected = [6, 8, 10]  # [2,4,6,8,10] -> [6,8,10] -> [6,8,10]
        assert actual == expected
        assert streamed_items == expected  # sort ensures order

    @pytest.mark.asyncio
    async def test_parallel_performance(self):
        """Test that parallel processing provides speedup."""
        async def slow_operation(x: int) -> int:
            await asyncio.sleep(0.1)  # 100ms delay per item
            return x * 2
            
        data = [1, 2, 3, 4, 5]
        
        # Sequential would take ~500ms, parallel should be much faster
        start_time = time.time()
        pipeline = map(slow_operation)
        actual = await (data | pipeline).collect()
        end_time = time.time()
        
        # Should complete in roughly the time of the slowest operation
        # plus some overhead, not the sum of all operations
        duration = end_time - start_time
        assert duration < 0.3  # Much less than 5 * 0.1 = 0.5s
        
        expected = [2, 4, 6, 8, 10]
        assert actual == expected

    @pytest.mark.asyncio
    async def test_mixed_sync_async_pipeline(self):
        """Test mixing synchronous and asynchronous operations."""
        async def async_add_one(x: int) -> int:
            await asyncio.sleep(0.01)
            return x + 1
            
        def sync_multiply_two(x: int) -> int:
            return x * 2
            
        async def async_is_even(x: int) -> bool:
            await asyncio.sleep(0.01)
            return x % 2 == 0
        
        pipeline = (
            map(async_add_one) |      # async
            map(sync_multiply_two) |  # sync  
            filter(async_is_even) |   # async
            sort()                    # stateful
        )
        
        actual = await ([1, 2, 3, 4, 5] | pipeline).collect()
        
        # [1,2,3,4,5] -> [2,3,4,5,6] -> [4,6,8,10,12] -> [4,6,8,10,12] -> [4,6,8,10,12]
        expected = [4, 6, 8, 10, 12]
        actual = [item.item for item in result]
        assert actual == expected

    @pytest.mark.asyncio
    async def test_empty_pipeline_stages(self):
        """Test pipeline where intermediate stages produce empty results."""
        pipeline = (
            map(lambda x: x * 2) |
            filter(lambda x: x > 100) |  # Will filter everything out
            map(lambda x: x + 10) |      # This won't process anything
            sort()
        )
        
        result = await ([1, 2, 3, 4, 5] | pipeline).collect()
        assert result == []

    @pytest.mark.asyncio
    async def test_index_tracking_through_pipeline(self):
        """Test that indexes are tracked correctly through complex pipelines."""
        # Use operations that preserve, modify, or create indexes
        pipeline = (
            map(lambda x: x * 10) |      # Preserves indexes: [10,20,30]
            filter(lambda x: x >= 20) |  # Filters but preserves original indexes: [20,30] at [1,2]
            flat_map(lambda x: [x, x+1]) # Creates sub-indexes
        )
        
        result = await ([1, 2, 3] | pipeline).collect()
        
        # Expected: [20, 21, 30, 31]
        expected_items = [20, 21, 30, 31]
        actual_items = [item.item for item in result]
        assert actual_items == expected_items
        
        # Check hierarchical indexing from flat_map
        # First filtered item (20) came from original index 1
        assert result[0].index.index == 1
        assert result[0].index.sub_index.index == 0
        assert result[1].index.index == 1
        assert result[1].index.sub_index.index == 1
        
        # Second filtered item (30) came from original index 2
        assert result[2].index.index == 2
        assert result[2].index.sub_index.index == 0
        assert result[3].index.index == 2
        assert result[3].index.sub_index.index == 1

    @pytest.mark.asyncio
    async def test_error_handling_in_pipeline(self):
        """Test error handling in complex pipelines."""
        def failing_at_three(x: int) -> int:
            if x == 3:
                raise ValueError("Error at 3")
            return x * 2
            
        pipeline = (
            map(lambda x: x + 1) |    # [2,3,4,5]
            map(failing_at_three) |   # Will fail at 3
            filter(lambda x: x > 0)
        )
        
        with pytest.raises(ValueError, match="Error at 3"):
            await ([1, 2, 3, 4] | pipeline).collect()

    @pytest.mark.asyncio
    async def test_large_dataset_processing(self):
        """Test processing larger datasets efficiently."""
        # Create larger dataset
        data = list(range(100))
        
        pipeline = (
            map(lambda x: x * 2) |
            filter(lambda x: x % 3 == 0) |
            map(lambda x: x // 2) |
            sort(reverse=True)
        )
        
        result = await (data | pipeline).collect()
        
        # Should get multiples of 3 from 0-99, sorted in reverse
        expected = list(range(99, -1, -3))  # [99, 96, 93, ..., 3, 0]
        actual = [item.item for item in result]
        assert actual == expected

    @pytest.mark.asyncio
    async def test_nested_pipeline_composition(self):
        """Test composing pipelines from other pipelines."""
        # Create sub-pipelines
        preprocessing = map(lambda x: x * 2) | filter(lambda x: x > 5)
        postprocessing = sort() | map(lambda x: x + 100)
        
        # Combine them
        full_pipeline = preprocessing | postprocessing
        
        result = await ([1, 2, 3, 4, 5] | full_pipeline).collect()
        
        # [1,2,3,4,5] -> [2,4,6,8,10] -> [6,8,10] -> [6,8,10] -> [106,108,110]
        expected = [106, 108, 110]
        actual = [item.item for item in result]
        assert actual == expected

async def run_manual_tests():
    """Run tests manually without pytest."""
    test_instance = TestPipelines()
    
    print("🧪 Running Complex Pipeline Tests")
    print("=" * 50)
    
    tests = [
        ("Simple Pipeline Chaining", test_instance.test_simple_pipeline_chaining),
        ("Complex Pipeline All Operations", test_instance.test_complex_pipeline_all_operations),
        ("Data Processing Pipeline", test_instance.test_data_processing_pipeline),
        ("Streaming vs Collect", test_instance.test_streaming_vs_collect),
        ("Parallel Performance", test_instance.test_parallel_performance),
        ("Mixed Sync/Async Pipeline", test_instance.test_mixed_sync_async_pipeline),
        ("Empty Pipeline Stages", test_instance.test_empty_pipeline_stages),
        ("Index Tracking", test_instance.test_index_tracking_through_pipeline),
        ("Large Dataset Processing", test_instance.test_large_dataset_processing),
        ("Nested Pipeline Composition", test_instance.test_nested_pipeline_composition),
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        try:
            print(f"Running {test_name}...", end=" ")
            await test_func()
            print("✅ PASSED")
            passed += 1
        except Exception as e:
            print(f"❌ FAILED: {e}")
            failed += 1
    
    print("\n" + "=" * 50)
    print(f"Results: {passed} passed, {failed} failed")
    
    if failed == 0:
        print("🎉 All tests passed!")
    else:
        print("❌ Some tests failed")
