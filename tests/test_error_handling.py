"""Tests for error handling functionality."""
import pytest
import asyncio
import relais as r
from relais import ErrorPolicy, PipelineError


class TestErrorHandling:
    """Test suite for error handling in pipelines."""

    @pytest.mark.asyncio
    async def test_fail_fast_policy_with_map(self):
        """Test that fail-fast policy stops pipeline on first error."""
        def failing_function(x):
            if x == 2:
                raise ValueError(f"Intentional error for x={x}")
            return x * 2
        
        pipeline = r.Pipeline([r.map(failing_function)], error_policy=ErrorPolicy.FAIL_FAST)
        
        with pytest.raises(PipelineError) as exc_info:
            await pipeline.collect([1, 2, 3, 4])
        
        assert "Pipeline execution failed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_ignore_policy_skips_errors(self):
        """Test that ignore policy skips failing items and continues."""
        def failing_function(x):
            if x == 2:
                raise ValueError(f"Intentional error for x={x}")
            return x * 2
        
        pipeline = r.Pipeline([r.map(failing_function)], error_policy=ErrorPolicy.IGNORE)
        
        result = await pipeline.collect([1, 2, 3, 4])
        expected = [2, 6, 8]  # x=1->2, x=3->6, x=4->8, x=2 skipped
        assert sorted(result) == sorted(expected)

    @pytest.mark.asyncio
    async def test_collect_policy_continues_with_errors(self):
        """Test that collect policy processes all items despite errors."""
        def failing_function(x):
            if x == 2 or x == 4:
                raise ValueError(f"Intentional error for x={x}")
            return x * 2
        
        pipeline = r.Pipeline([r.map(failing_function)], error_policy=ErrorPolicy.COLLECT)
        
        # With collect policy, we should get the successful results
        result = await pipeline.collect([1, 2, 3, 4])
        expected = [2, 6]  # x=1->2, x=3->6, x=2 and x=4 failed
        assert sorted(result) == sorted(expected)
        
        # TODO: In the future, we could check that errors were collected
        # This would require extending the collect() method to return error info

    @pytest.mark.asyncio
    async def test_error_policy_inheritance_in_chained_steps(self):
        """Test that error policy is inherited through chained steps."""
        def failing_function(x):
            if x == 2:
                raise ValueError(f"Intentional error for x={x}")
            return x * 2
        
        # Create pipeline with ignore policy
        pipeline = r.Pipeline([
            r.map(failing_function),
            r.filter(lambda x: x > 0)  # Should inherit ignore policy
        ], error_policy=ErrorPolicy.IGNORE)
        
        result = await pipeline.collect([1, 2, 3, 4])
        expected = [2, 6, 8]  # Error in first step ignored, all pass filter
        assert sorted(result) == sorted(expected)

    @pytest.mark.asyncio
    async def test_with_error_policy_method(self):
        """Test using with_error_policy method to set error handling."""
        def failing_function(x):
            if x == 2:
                raise ValueError(f"Intentional error for x={x}")
            return x * 2
        
        pipeline = (r.map(failing_function)
                   .with_error_policy(ErrorPolicy.IGNORE))
        
        result = await pipeline.collect([1, 2, 3, 4])
        expected = [2, 6, 8]  # x=2 error ignored
        assert sorted(result) == sorted(expected)

    @pytest.mark.asyncio
    async def test_stateful_processor_error_handling(self):
        """Test error handling in stateful processors like sort."""
        # This tests a different error path since stateful processors
        # collect all data before processing
        
        # Create a custom stateful operation that might fail
        def custom_key_function(x):
            if x == 2:
                raise ValueError("Sort key error")
            return x
        
        pipeline = r.Pipeline([r.sort(key=custom_key_function)], 
                             error_policy=ErrorPolicy.FAIL_FAST)
        
        with pytest.raises(PipelineError):
            await pipeline.collect([1, 2, 3])

    @pytest.mark.asyncio
    async def test_async_function_error_handling(self):
        """Test error handling with async functions."""
        async def async_failing_function(x):
            await asyncio.sleep(0.01)  # Simulate async work
            if x == 2:
                raise ValueError(f"Async error for x={x}")
            return x * 2
        
        pipeline = r.Pipeline([r.map(async_failing_function)], 
                             error_policy=ErrorPolicy.IGNORE)
        
        result = await pipeline.collect([1, 2, 3, 4])
        expected = [2, 6, 8]  # x=2 error ignored
        assert sorted(result) == sorted(expected)

    @pytest.mark.asyncio
    async def test_multiple_errors_in_batch(self):
        """Test handling multiple errors in the same batch."""
        def failing_function(x):
            if x in [2, 4, 6]:
                raise ValueError(f"Error for x={x}")
            return x * 2
        
        pipeline = r.Pipeline([r.map(failing_function)], 
                             error_policy=ErrorPolicy.IGNORE)
        
        result = await pipeline.collect([1, 2, 3, 4, 5, 6, 7, 8])
        expected = [2, 6, 10, 14, 16]  # 1,3,5,7,8 succeed, 2,4,6 fail
        assert sorted(result) == sorted(expected)

    @pytest.mark.asyncio
    async def test_error_in_complex_pipeline(self):
        """Test error handling in a complex multi-step pipeline."""
        def failing_map(x):
            if x == 4:  # This will be 2*2 from the first map
                raise ValueError("Error in second map")
            return x + 1
        
        pipeline = r.Pipeline([
            r.map(lambda x: x * 2),     # 1->2, 2->4, 3->6
            r.map(failing_map),         # 2->3, 4->error, 6->7
            r.filter(lambda x: x > 5)   # 3->filtered, 7->pass
        ], error_policy=ErrorPolicy.IGNORE)
        
        result = await pipeline.collect([1, 2, 3])
        expected = [7]  # Only x=3 -> 6 -> 7 -> passes filter
        assert result == expected

    @pytest.mark.asyncio  
    async def test_default_error_policy_is_fail_fast(self):
        """Test that default error policy is fail-fast."""
        def failing_function(x):
            if x == 2:
                raise ValueError("Error")
            return x
        
        # Default pipeline should fail fast
        pipeline = [1, 2, 3] | r.map(failing_function)
        
        with pytest.raises(PipelineError):
            await pipeline.collect()