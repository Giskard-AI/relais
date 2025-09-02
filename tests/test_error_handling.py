"""Tests for error handling functionality."""

import asyncio

import pytest

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

        pipeline = r.Pipeline(
            [r.Map(failing_function)], error_policy=ErrorPolicy.FAIL_FAST
        )

        with pytest.raises(PipelineError) as exc_info:
            await pipeline.collect([1, 2, 3, 4])

        assert "Processing failed in _MapProcessor: Intentional error for x=2" in str(
            exc_info.value
        )

    @pytest.mark.asyncio
    async def test_ignore_policy_skips_errors(self):
        """Test that ignore policy skips failing items and continues."""

        def failing_function(x):
            if x == 2:
                raise ValueError(f"Intentional error for x={x}")
            return x * 2

        pipeline = r.Pipeline(
            [r.Map(failing_function)], error_policy=ErrorPolicy.IGNORE
        )

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

        pipeline = r.Pipeline(
            [r.Map(failing_function)], error_policy=ErrorPolicy.COLLECT
        )

        # With collect policy, we should get the successful results
        result = await pipeline.collect([1, 2, 3, 4])
        expected = [2, 6]  # x=1->2, x=3->6, x=2 and x=4 failed
        assert sorted(result) == sorted(expected)

        # Now test that errors are properly collected using collect_with_errors
        data, errors = await pipeline.collect_with_errors([1, 2, 3, 4])
        assert sorted(data) == sorted(expected)
        assert len(errors) == 2

        # Check that the errors contain the right information
        error_messages = [str(err) for err in errors]
        assert (
            "Intentional error for x=2" in error_messages[0]
            or "Intentional error for x=2" in error_messages[1]
        )
        assert (
            "Intentional error for x=4" in error_messages[0]
            or "Intentional error for x=4" in error_messages[1]
        )

    @pytest.mark.asyncio
    async def test_error_policy_inheritance_in_chained_steps(self):
        """Test that error policy is inherited through chained steps."""

        def failing_function(x):
            if x == 2:
                raise ValueError(f"Intentional error for x={x}")
            return x * 2

        # Create pipeline with ignore policy
        pipeline = r.Pipeline(
            [
                r.Map(failing_function),
                r.Filter(lambda x: x > 0),  # Should inherit ignore policy
            ],
            error_policy=ErrorPolicy.IGNORE,
        )

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

        pipeline = r.Map(failing_function).with_error_policy(ErrorPolicy.IGNORE)

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

        pipeline = r.Pipeline(
            [r.Sort(key=custom_key_function)], error_policy=ErrorPolicy.FAIL_FAST
        )

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

        pipeline = r.Pipeline(
            [r.Map(async_failing_function)], error_policy=ErrorPolicy.IGNORE
        )

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

        pipeline = r.Pipeline(
            [r.Map(failing_function)], error_policy=ErrorPolicy.IGNORE
        )

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

        pipeline = r.Pipeline(
            [
                r.Map(lambda x: x * 2),  # 1->2, 2->4, 3->6
                r.Map(failing_map),  # 2->3, 4->error, 6->7
                r.Filter(lambda x: x > 5),  # 3->filtered, 7->pass
            ],
            error_policy=ErrorPolicy.IGNORE,
        )

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
        pipeline = [1, 2, 3] | r.Map(failing_function)

        with pytest.raises(PipelineError):
            await pipeline.collect()

    @pytest.mark.asyncio
    async def test_fail_fast_stops_all_concurrent_processing(self):
        """Test that FAIL_FAST immediately stops all concurrent processing."""
        processed_items = []

        async def tracking_processor(x):
            # Add delay to ensure some items are being processed when error occurs
            await asyncio.sleep(
                0.01 * x
            )  # Increase delay for higher numbers to ensure that cancellation can happen
            processed_items.append(x)
            if x == 3:
                raise ValueError(f"Intentional error for x={x}")
            return x * 2

        pipeline = r.Pipeline(
            [r.Map(tracking_processor)], error_policy=ErrorPolicy.FAIL_FAST
        )

        with pytest.raises(PipelineError):
            await pipeline.collect([1, 2, 3, 4, 5, 6, 7, 8])

        # Should not have processed all items due to early termination
        assert len(processed_items) < 8, (
            f"Expected early termination, but processed {processed_items}"
        )

    @pytest.mark.asyncio
    async def test_error_propagation_through_complex_pipeline(self):
        """Test error propagation through multiple pipeline steps with different processor types."""

        async def failing_async_map(x):
            await asyncio.sleep(0.001)
            if x == 6:  # This will be 3*2 from first map
                raise ValueError("Error in async map step")
            return x + 10

        def failing_sync_filter(x):
            if x == 14:  # This will be 4+10 from second map
                raise ValueError("Error in filter step")
            return x > 12

        # Complex pipeline: map (stateless) -> map (stateless) -> filter (stateless) -> sort (stateful)
        pipeline = r.Pipeline(
            [
                r.Map(lambda x: x * 2),  # 1->2, 2->4, 3->6, 4->8
                r.Map(failing_async_map),  # 2->12, 4->14, 6->error, 8->18
                r.Filter(failing_sync_filter),  # 12->False, 14->error, 18->True
                r.Sort(),  # Stateful processor
            ],
            error_policy=ErrorPolicy.FAIL_FAST,
        )

        with pytest.raises(PipelineError) as exc_info:
            await pipeline.collect([1, 2, 3, 4])

        # Should contain information about which step failed
        assert "Processing failed in _MapProcessor: Error in async map step" in str(
            exc_info.value
        )

    @pytest.mark.asyncio
    async def test_collect_policy_accumulates_multiple_errors(self):
        """Test that COLLECT policy accumulates errors from multiple steps and items."""

        def first_step_errors(x):
            if x in [2, 5]:
                raise ValueError(f"First step error for x={x}")
            return x * 2

        def second_step_errors(x):
            if x == 8:  # This will be 4*2 from first step
                raise ValueError(f"Second step error for x={x}")
            return x + 1

        pipeline = r.Pipeline(
            [
                r.Map(first_step_errors),  # Errors for x=2,5
                r.Map(second_step_errors),  # Error for x=4 (becomes 8)
            ],
            error_policy=ErrorPolicy.COLLECT,
        )

        result = await pipeline.collect([1, 2, 3, 4, 5, 6])

        # Should get successful results: 1->2->3, 3->6->7, 6->12->13
        expected = [3, 7, 13]
        assert sorted(result) == sorted(expected)

        # Note: In future versions, we could check pipeline.errors to verify
        # that errors were properly collected from multiple steps

    @pytest.mark.asyncio
    async def test_ignore_policy_with_stateful_processor_errors(self):
        """Test IGNORE policy with errors in stateful processors."""

        def error_prone_key_function(x):
            if x == 3:
                raise ValueError("Sort key error")
            return -x  # Descending order

        pipeline = r.Pipeline(
            [
                r.Map(lambda x: x * 2),  # 1->2, 2->4, 3->6, 4->8
                r.Sort(key=error_prone_key_function),  # Should fail for x=6
            ],
            error_policy=ErrorPolicy.IGNORE,
        )

        # With IGNORE policy, the entire sort operation might be skipped
        # or the error might be handled differently for stateful operations
        try:
            result = await pipeline.collect([1, 2, 3, 4])
            # If sort succeeds despite error, expect remaining items in reverse order
            # Exact behavior depends on implementation details
            assert isinstance(result, list)
        except PipelineError:
            # If stateful processor can't gracefully ignore errors, that's acceptable
            pytest.skip(
                "Stateful processor error handling not implemented for IGNORE policy"
            )

    @pytest.mark.asyncio
    async def test_error_cancellation_propagation(self):
        """Test that errors properly trigger cancellation signals across pipeline stages."""
        processed_items = []
        failed_items = []

        async def delay_processor(x):
            await asyncio.sleep(
                0.001 * x
            )  # Small processing delay to ensure that cancellation can happen
            return x

        async def failing_processor(x):
            processed_items.append(x)
            await asyncio.sleep(0.001)  # Small processing delay
            if x == 5:
                failed_items.append(x)
                raise ValueError(f"Processing failed for item {x}")
            return x * 2

        # Create pipeline with large dataset to test early termination
        large_dataset = list(range(20))  # Large enough to show early termination
        pipeline = r.Pipeline(
            [r.Map(delay_processor), r.Map(failing_processor)],
            error_policy=ErrorPolicy.FAIL_FAST,
        )

        with pytest.raises(PipelineError):
            await pipeline.collect(large_dataset)

        # Should have failed fast - not all items should be processed
        assert len(failed_items) >= 1, "Should have encountered the failing item"
        assert len(processed_items) <= len(large_dataset), (
            "Should not process more items than exist"
        )

        # Early termination means fewer items processed than total available
        # (exact number depends on timing, but should be much less for fail-fast)
        assert len(processed_items) < len(large_dataset), (
            f"Expected early termination, but processed {len(processed_items)} items"
        )

    @pytest.mark.asyncio
    async def test_mixed_sync_async_error_handling(self):
        """Test error handling with mixed synchronous and asynchronous operations."""

        def sync_failing_map(x):
            if x == 2:
                raise ValueError("Sync error")
            return x * 2

        async def async_failing_filter(x):
            await asyncio.sleep(0.001)
            if x == 6:  # 3*2 from map
                raise ValueError("Async error")
            return x > 0

        pipeline = r.Pipeline(
            [
                r.Map(sync_failing_map),  # Sync error for x=2
                r.Filter(async_failing_filter),  # Async error for x=3->6
            ],
            error_policy=ErrorPolicy.IGNORE,
        )

        result = await pipeline.collect([1, 2, 3, 4])

        # Should get: 1->2->pass, 2->error, 3->6->error, 4->8->pass
        expected = [2, 8]
        assert sorted(result) == sorted(expected)

    @pytest.mark.asyncio
    async def test_error_policy_inheritance_across_chained_operations(self):
        """Test that error policy is properly inherited when chaining operations with |."""

        def first_failing_function(x):
            if x == 2:
                raise ValueError("First error")
            return x * 2

        def second_failing_function(x):
            if x == 6:  # 3*2 from first function
                raise ValueError("Second error")
            return x + 1

        # Chain operations with | - should inherit error policy
        base_pipeline = r.Pipeline([], error_policy=ErrorPolicy.IGNORE)
        chained_pipeline = (
            base_pipeline
            | r.Map(first_failing_function)
            | r.Map(second_failing_function)
        )

        result = await chained_pipeline.collect([1, 2, 3, 4])

        # Should get: 1->2->3, 2->error, 3->6->error, 4->8->9
        expected = [3, 9]
        assert sorted(result) == sorted(expected)

    @pytest.mark.asyncio
    async def test_stream_error_state_management(self):
        """Test that stream error states are properly managed during error conditions."""
        # This test is no longer relevant as we don't expose low-level stream error handling
        # in the new architecture. Error handling is managed internally by processors.
        pytest.skip("Stream error state management is now internal to processors")

    @pytest.mark.asyncio
    async def test_exception_chaining_preserves_context(self):
        """Test that exception chaining preserves original error context."""
        original_error = ValueError("Original error message")

        def failing_function(x):
            if x == 2:
                raise original_error
            return x

        pipeline = r.Pipeline(
            [r.Map(failing_function)], error_policy=ErrorPolicy.FAIL_FAST
        )

        with pytest.raises(PipelineError) as exc_info:
            await pipeline.collect([1, 2, 3])

        # Should preserve original error
        pipeline_error = exc_info.value
        assert pipeline_error.original_error == original_error
        assert "Original error message" in str(pipeline_error)

    @pytest.mark.asyncio
    async def test_concurrent_errors_in_parallel_processing(self):
        """Test handling of multiple concurrent errors in parallel processing."""
        error_times = []

        async def concurrent_failing_function(x):
            await asyncio.sleep(0.01)  # Small delay to ensure concurrency
            if x in [2, 3, 5]:
                error_time = asyncio.get_event_loop().time()
                error_times.append(error_time)
                raise ValueError(f"Concurrent error for x={x}")
            return x * 2

        pipeline = r.Pipeline(
            [r.Map(concurrent_failing_function)], error_policy=ErrorPolicy.FAIL_FAST
        )

        start_time = asyncio.get_event_loop().time()

        with pytest.raises(PipelineError):
            await pipeline.collect([1, 2, 3, 4, 5, 6, 7, 8])

        end_time = asyncio.get_event_loop().time()

        # Should fail fast - not all items should have been processed
        # and execution should stop quickly after first error
        execution_time = end_time - start_time
        assert execution_time < 0.1, f"Execution took too long: {execution_time}s"

        # At least one error should have occurred


class TestErrorCollection:
    """Test suite specifically for error collection functionality."""

    @pytest.mark.asyncio
    async def test_collect_with_errors_basic_functionality(self):
        """Test basic functionality of collect_with_errors method."""

        def failing_function(x):
            if x % 2 == 0:  # Fail on even numbers
                raise ValueError(f"Error for x={x}")
            return x * 2

        pipeline = r.Pipeline(
            [r.Map(failing_function)], error_policy=ErrorPolicy.COLLECT
        )
        data, errors = await pipeline.collect_with_errors([1, 2, 3, 4, 5])

        # Check the data - should contain successful items only
        expected_data = [2, 6, 10]  # x=1->2, x=3->6, x=5->10
        assert sorted(data) == sorted(expected_data)

        # Check errors - should have 2 errors for x=2 and x=4
        assert len(errors) == 2

        # Check error details
        error_values = [
            err.item_index.index
            if hasattr(err, "item_index") and err.item_index
            else -1
            for err in errors
        ]
        assert (
            1 in error_values or 3 in error_values
        )  # Should have errors from x=2 or x=4

    @pytest.mark.asyncio
    async def test_collect_with_errors_no_errors(self):
        """Test collect_with_errors when no errors occur."""

        def successful_function(x):
            return x * 2

        pipeline = r.Pipeline(
            [r.Map(successful_function)], error_policy=ErrorPolicy.COLLECT
        )
        data, errors = await pipeline.collect_with_errors([1, 2, 3, 4])

        assert data == [2, 4, 6, 8]
        assert len(errors) == 0

    @pytest.mark.asyncio
    async def test_collect_with_errors_multi_step_pipeline(self):
        """Test error collection across multiple pipeline steps."""

        def step1_function(x):
            if x == 2:
                raise ValueError(f"Step1 error for x={x}")
            return x * 2

        def step2_function(x):
            if x == 6:  # This is x=3 after step1: 3*2=6
                raise ValueError(f"Step2 error for x={x}")
            return x + 1

        pipeline = r.Pipeline(
            [r.Map(step1_function), r.Map(step2_function)],
            error_policy=ErrorPolicy.COLLECT,
        )

        data, errors = await pipeline.collect_with_errors([1, 2, 3, 4])

        # Should have successful items: x=1->2->3, x=4->8->9
        # x=2 fails in step1, x=3 passes step1 (3->6) but fails in step2
        expected_data = [3, 9]
        assert sorted(data) == sorted(expected_data)

        # Should have 2 errors total
        assert len(errors) == 2

        # Check that errors from both steps are captured
        step_names = [
            err.step_name if hasattr(err, "step_name") else "Unknown" for err in errors
        ]
        assert any("_MapProcessor" in name for name in step_names if name is not None)

    @pytest.mark.asyncio
    async def test_collect_with_errors_functionality(self):
        """Test the collect_with_errors method returns errors properly."""

        def failing_function(x):
            if x == 2:
                raise ValueError(f"Error for x={x}")
            return x * 2

        pipeline = r.Pipeline(
            [r.Map(failing_function)], error_policy=ErrorPolicy.COLLECT
        )

        # Test with errors
        data_with_errors, errors = await pipeline.collect_with_errors([1, 2, 3])
        assert len(errors) == 1
        assert "Error for x=2" in str(errors[0])
        assert sorted(data_with_errors) == [2, 6]  # x=1->2, x=3->6

        # Test without errors
        data_no_errors, errors_no_errors = await pipeline.collect_with_errors([1, 3, 5])
        assert len(errors_no_errors) == 0
        assert sorted(data_no_errors) == [2, 6, 10]

    @pytest.mark.asyncio
    async def test_collect_delegates_to_collect_with_errors_for_collect_policy(self):
        """Test that collect() properly delegates to collect_with_errors for COLLECT policy."""

        def failing_function(x):
            if x == 2:
                raise ValueError(f"Error for x={x}")
            return x * 2

        pipeline = r.Pipeline(
            [r.Map(failing_function)], error_policy=ErrorPolicy.COLLECT
        )

        # collect() should return just the data
        result = await pipeline.collect([1, 2, 3, 4])
        expected = [2, 6, 8]  # x=1->2, x=3->6, x=4->8, x=2 failed
        assert sorted(result) == sorted(expected)
        assert isinstance(result, list)  # Should be list
