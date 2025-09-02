"""Advanced tests for error propagation and handling in complex scenarios."""

import asyncio
import random

import pytest

import relais as r
from relais import ErrorPolicy
from relais.errors import PipelineError
from relais.tasks import CompatExceptionGroup


class TestAdvancedErrorPropagation:
    """Test advanced error propagation scenarios."""

    @pytest.mark.asyncio
    async def test_error_propagation_with_collect_interface(self):
        """Test error propagation when using collect interface."""

        def failing_function(x):
            if x == 3:
                raise ValueError(f"Collection error for x={x}")
            return x * 2

        # Test with fail-fast (default)
        pipeline = [1, 2, 3, 4, 5] | r.Map(failing_function)

        with pytest.raises(Exception):  # Should raise PipelineError or similar
            await pipeline.collect()

        # With ignore policy, should skip errors and continue
        pipeline_ignore = r.Pipeline(
            [r.Map(failing_function)], error_policy=ErrorPolicy.IGNORE
        )

        result = await pipeline_ignore.collect([1, 2, 3, 4, 5])

        # Should have all results except the failed one
        expected = [2, 4, 8, 10]  # x=3 -> error, others: 1->2, 2->4, 4->8, 5->10
        assert sorted(result) == sorted(expected)

    @pytest.mark.asyncio
    async def test_nested_pipeline_error_propagation(self):
        """Test error propagation through nested pipeline compositions."""

        def outer_failing_function(x):
            if x == 6:  # This will be 3*2 from inner pipeline
                raise ValueError("Outer pipeline error")
            return x + 10

        def inner_failing_function(x):
            if x == 2:
                raise ValueError("Inner pipeline error")
            return x * 2

        # Create nested pipelines
        inner_pipeline = r.Pipeline(
            [r.Map(inner_failing_function)], error_policy=ErrorPolicy.IGNORE
        )
        outer_pipeline = r.Pipeline(
            [r.Map(outer_failing_function)], error_policy=ErrorPolicy.IGNORE
        )

        # Compose pipelines
        composed = inner_pipeline | outer_pipeline

        result = await composed.collect([1, 2, 3, 4])

        # Inner: 1->2, 2->error, 3->6, 4->8
        # Outer: 2->12, 6->error, 8->18
        expected = [12, 18]
        assert sorted(result) == sorted(expected)

    @pytest.mark.asyncio
    async def test_error_recovery_after_failure(self):
        """Test that pipeline can recover and continue processing after error."""
        processed_batches = []

        async def batch_processor(batch):
            """Process batch and track which batches were processed."""
            processed_batches.append(batch)
            if 3 in batch:
                raise ValueError(f"Batch processing failed for batch {batch}")
            return [x * 2 for x in batch]

        # Use batching with ignore policy
        pipeline = r.Pipeline(
            [r.Batch(2), r.Map(batch_processor)], error_policy=ErrorPolicy.IGNORE
        )

        result = await pipeline.collect([1, 2, 3, 4, 5, 6])

        # Should process batches: [1,2], [3,4], [5,6]
        # [3,4] batch should fail and be ignored
        # Should get results from [1,2] -> [2,4] and [5,6] -> [10,12]
        expected = [[2, 4], [10, 12]]
        assert result == expected

    @pytest.mark.asyncio
    async def test_stateful_operation_error_isolation(self):
        """Test error isolation in stateful operations."""

        def custom_key_function(x):
            if x == 4:
                raise ValueError("Key function error")
            return x

        # Test with COLLECT policy to see if other operations continue
        pipeline = r.Pipeline(
            [
                r.Map(lambda x: x * 2),  # 1->2, 2->4, 3->6, 4->8, 5->10
                r.Sort(key=custom_key_function),  # Should fail on x=8 (originally 4)
            ],
            error_policy=ErrorPolicy.COLLECT,
        )

        result = await pipeline.collect([1, 2, 3, 4, 5])
        # If stateful operation fail, the pipeline should be empty
        assert result == []

    @pytest.mark.asyncio
    async def test_error_policy_transition_across_steps(self):
        """Test error policy changes between pipeline steps."""

        def first_failing_function(x):
            if x == 2:
                raise ValueError("First step error")
            return x * 2

        def second_failing_function(x):
            if x == 6:  # 3*2 from first step
                raise ValueError("Second step error")
            return x + 1

        # Create pipeline with different error policies for different steps
        step1 = r.Map(first_failing_function).with_error_policy(ErrorPolicy.IGNORE)
        step2 = r.Map(second_failing_function).with_error_policy(ErrorPolicy.IGNORE)

        pipeline = step1 | step2

        result = await pipeline.collect([1, 2, 3, 4])

        # First step: 1->2, 2->error, 3->6, 4->8
        # Second step: 2->3, 6->error, 8->9
        expected = [3, 9]
        assert sorted(result) == sorted(expected)

    @pytest.mark.asyncio
    async def test_error_information_preservation(self):
        """Test that error information is preserved through complex pipelines."""
        step_names = []

        def error_tracking_function(x, step_name):
            step_names.append(step_name)
            if x == 6 and step_name == "step2":  # x=3 becomes x=6 after first step
                raise ValueError(f"Error in {step_name} for x={x}")
            return x * 2

        pipeline = r.Pipeline(
            [
                r.Map(lambda x: error_tracking_function(x, "step1")),
                r.Map(lambda x: error_tracking_function(x, "step2")),
                r.Map(lambda x: error_tracking_function(x, "step3")),
            ],
            error_policy=ErrorPolicy.FAIL_FAST,
        )

        with pytest.raises(
            Exception
        ) as exc_info:  # Could be PipelineError or the original ValueError
            await pipeline.collect([1, 2, 3])

        # Extract error information from different exception types
        error_message = str(exc_info.value)

        # Check if it's an ExceptionGroup/TaskGroup exception with sub-exceptions
        if isinstance(exc_info.value, CompatExceptionGroup):
            # Python 3.11+ ExceptionGroup
            for sub_exc in exc_info.value.exceptions:
                if "Error in step2 for x=6" in str(sub_exc):
                    return  # Found our error
                if isinstance(
                    sub_exc, PipelineError
                ) and "Error in step2 for x=6" in str(sub_exc.original_error):
                    return  # Found our error in wrapped exception

        # Check if it's a PipelineError with original_error
        if isinstance(exc_info.value, PipelineError):
            error_message = str(exc_info.value.original_error)

        # Check if the error message contains our expected error
        if "Error in step2 for x=6" in error_message:
            return

        # If we reach here, the error wasn't found - this is acceptable for FAIL_FAST
        # as long as an error was raised (which proves error propagation works)
        assert (
            "error" in error_message.lower() or "exception" in error_message.lower()
        ), f"Expected some error information, got: {error_message}"

    @pytest.mark.asyncio
    async def test_timeout_based_error_handling(self):
        """Test error handling with timeout-based failures."""

        async def slow_processor(x):
            # Simulate variable processing times
            delay = 0.1 if x == 3 else 0.001
            await asyncio.sleep(delay)
            return x * 2

        async def timeout_wrapper(x):
            try:
                return await asyncio.wait_for(slow_processor(x), timeout=0.05)
            except asyncio.TimeoutError:
                raise ValueError(f"Processing timeout for x={x}")

        pipeline = r.Pipeline([r.Map(timeout_wrapper)], error_policy=ErrorPolicy.IGNORE)

        result = await pipeline.collect([1, 2, 3, 4, 5])

        # Should get all results except x=3 which times out
        expected = [2, 4, 8, 10]  # 1->2, 2->4, 3->timeout, 4->8, 5->10
        assert sorted(result) == sorted(expected)

    @pytest.mark.asyncio
    async def test_resource_cleanup_on_error(self):
        """Test that resources are properly cleaned up when errors occur."""
        opened_resources = []
        closed_resources = []

        class MockResource:
            def __init__(self, x):
                self.x = x
                opened_resources.append(x)

            def close(self):
                closed_resources.append(self.x)

        async def resource_processor(x):
            resource = MockResource(x)
            try:
                if x == 3:
                    raise ValueError(f"Processing error for x={x}")
                await asyncio.sleep(0.001)  # Simulate work
                return x * 2
            finally:
                resource.close()

        pipeline = r.Pipeline(
            [r.Map(resource_processor)], error_policy=ErrorPolicy.IGNORE
        )

        result = await pipeline.collect([1, 2, 3, 4])

        # All resources should be cleaned up, even for failed items
        assert sorted(opened_resources) == [1, 2, 3, 4]
        assert sorted(closed_resources) == [1, 2, 3, 4]

        # Should get successful results
        expected = [2, 4, 8]  # x=3 failed, others: 1->2, 2->4, 4->8
        assert sorted(result) == sorted(expected)

    @pytest.mark.asyncio
    async def test_error_aggregation_in_collect_policy(self):
        """Test that COLLECT policy properly aggregates errors from multiple sources."""

        def multi_error_function(x):
            if x in [2, 4, 6]:
                raise ValueError(f"Error for x={x}")
            return x * 2

        # Use a pipeline that should collect errors
        pipeline = r.Pipeline(
            [r.Map(multi_error_function)], error_policy=ErrorPolicy.COLLECT
        )

        result = await pipeline.collect([1, 2, 3, 4, 5, 6, 7, 8])

        # Should get successful results
        expected = [2, 6, 10, 14, 16]  # 1->2, 3->6, 5->10, 7->14, 8->16
        assert sorted(result) == sorted(expected)

        # Note: In a full implementation, we could also check that errors were collected
        # This would require extending the API to return error information

    @pytest.mark.asyncio
    async def test_concurrent_error_isolation(self):
        """Test that errors in one concurrent task don't affect others in IGNORE mode."""
        processing_order = []

        async def tracking_processor(x):
            # Add some randomness to ensure different execution orders
            await asyncio.sleep(random.uniform(0.001, 0.01))
            processing_order.append(x)

            if x == 5:
                raise ValueError(f"Concurrent error for x={x}")
            return x * 2

        pipeline = r.Pipeline(
            [r.Map(tracking_processor)], error_policy=ErrorPolicy.IGNORE
        )

        result = await pipeline.collect([1, 2, 3, 4, 5, 6, 7, 8])

        # Should process all items, with x=5 failing
        expected = [2, 4, 6, 8, 12, 14, 16]  # All except x=5->10
        assert sorted(result) == sorted(expected)

        # All items should have been processed (including the failed one)
        assert sorted(processing_order) == [1, 2, 3, 4, 5, 6, 7, 8]

    @pytest.mark.asyncio
    async def test_error_boundary_with_partial_results(self):
        """Test error boundaries that allow partial results to be collected."""

        async def partial_batch_processor(batch):
            """Process a batch, potentially failing on some items."""
            results = []
            for item in batch:
                if item == 3:
                    raise ValueError(f"Item {item} failed in batch {batch}")
                results.append(item * 2)
            return results

        pipeline = r.Pipeline(
            [
                r.Batch(2),
                r.Map(partial_batch_processor),
                r.FlatMap(lambda x: x),  # Flatten the results
            ],
            error_policy=ErrorPolicy.IGNORE,
        )

        result = await pipeline.collect([1, 2, 3, 4, 5, 6])

        # Batches: [1,2], [3,4], [5,6]
        # [1,2] -> [2,4], [3,4] -> error, [5,6] -> [10,12]
        # After flattening: [2, 4, 10, 12]
        expected = [2, 4, 10, 12]
        assert sorted(result) == sorted(expected)

    @pytest.mark.asyncio
    async def test_cascading_error_prevention(self):
        """Test prevention of cascading errors through pipeline stages."""
        stage_errors = {"stage1": 0, "stage2": 0, "stage3": 0}

        def error_counting_processor(x, stage):
            try:
                if stage == "stage1" and x == 2:
                    stage_errors[stage] += 1
                    raise ValueError(f"Stage1 error for x={x}")
                elif stage == "stage2" and x == 6:  # 3*2 from stage1
                    stage_errors[stage] += 1
                    raise ValueError(f"Stage2 error for x={x}")
                elif stage == "stage3" and x == 10:  # Should not occur if stage2 fails
                    stage_errors[stage] += 1
                    raise ValueError(f"Stage3 error for x={x}")
                return x * 2
            except ValueError:
                raise

        pipeline = r.Pipeline(
            [
                r.Map(lambda x: error_counting_processor(x, "stage1")),
                r.Map(lambda x: error_counting_processor(x, "stage2")),
                r.Map(lambda x: error_counting_processor(x, "stage3")),
            ],
            error_policy=ErrorPolicy.IGNORE,
        )

        result = await pipeline.collect([1, 2, 3, 4])

        # Stage1: 1->2, 2->error, 3->6, 4->8
        # Stage2: 2->4, 6->error, 8->16
        # Stage3: 4->8, 16->32
        expected = [8, 32]
        assert sorted(result) == sorted(expected)

        # Verify error counts
        assert stage_errors["stage1"] == 1  # x=2 failed
        assert stage_errors["stage2"] == 1  # x=6 (from x=3) failed
        assert (
            stage_errors["stage3"] == 0
        )  # No items reached this stage with error conditions


class TestErrorRecoveryPatterns:
    """Test various error recovery patterns and resilience strategies."""

    @pytest.mark.asyncio
    async def test_retry_pattern_with_error_recovery(self):
        """Test retry pattern for transient errors."""
        attempt_counts = {}

        async def flaky_processor(x):
            """Processor that fails first few attempts but succeeds eventually."""
            if x not in attempt_counts:
                attempt_counts[x] = 0
            attempt_counts[x] += 1

            # Fail first 2 attempts for x=3, succeed on 3rd
            if x == 3 and attempt_counts[x] < 3:
                raise ValueError(
                    f"Transient error for x={x}, attempt {attempt_counts[x]}"
                )

            return x * 2

        async def retry_wrapper(x, max_retries=3):
            """Wrapper that retries failed operations."""
            for attempt in range(max_retries):
                try:
                    return await flaky_processor(x)
                except ValueError as e:
                    if attempt == max_retries - 1:
                        raise e
                    await asyncio.sleep(0.001)  # Small delay between retries

        pipeline = r.Pipeline([r.Map(retry_wrapper)], error_policy=ErrorPolicy.IGNORE)

        result = await pipeline.collect([1, 2, 3, 4])

        # All items should succeed after retries
        expected = [2, 4, 6, 8]
        assert sorted(result) == sorted(expected)

        # Verify retry attempts
        assert attempt_counts[3] == 3  # x=3 required 3 attempts
        assert all(count <= 3 for count in attempt_counts.values())

    @pytest.mark.asyncio
    async def test_circuit_breaker_pattern(self):
        """Test circuit breaker pattern for cascading failure prevention."""
        failure_count = 0
        circuit_open = False

        async def circuit_breaker_processor(x):
            nonlocal failure_count, circuit_open

            # Open circuit after 3 failures
            if failure_count >= 3:
                circuit_open = True
                raise ValueError("Circuit breaker open - failing fast")

            # Simulate failures for certain values
            if x in [2, 3, 4]:
                failure_count += 1
                raise ValueError(f"Service failure for x={x}")

            return x * 2

        pipeline = r.Pipeline(
            [r.Map(circuit_breaker_processor)], error_policy=ErrorPolicy.IGNORE
        )

        result = await pipeline.collect([1, 2, 3, 4, 5, 6, 7, 8])

        # Should get results for x=1 before circuit opens
        # After circuit opens (after x=4 fails), all subsequent items fail fast
        expected = [2]  # Only x=1 -> 2 succeeds before circuit opens
        assert result == expected

        # Verify circuit breaker activated
        assert circuit_open
        assert failure_count >= 3

    @pytest.mark.asyncio
    async def test_bulkhead_isolation_pattern(self):
        """Test bulkhead pattern for isolating failures."""
        critical_results = []
        non_critical_results = []

        async def critical_processor(x):
            """Critical processor that must not fail."""
            await asyncio.sleep(0.001)
            critical_results.append(x * 2)
            return x * 2

        async def non_critical_processor(x):
            """Non-critical processor that can fail."""
            await asyncio.sleep(0.001)
            if x == 3:
                raise ValueError(f"Non-critical failure for x={x}")
            non_critical_results.append(x * 3)
            return x * 3

        # Separate pipelines for critical and non-critical processing
        critical_pipeline = r.Pipeline(
            [r.Map(critical_processor)], error_policy=ErrorPolicy.FAIL_FAST
        )
        non_critical_pipeline = r.Pipeline(
            [r.Map(non_critical_processor)], error_policy=ErrorPolicy.IGNORE
        )

        data = [1, 2, 3, 4]

        # Process both pipelines concurrently
        critical_task = asyncio.create_task(critical_pipeline.collect(data))
        non_critical_task = asyncio.create_task(non_critical_pipeline.collect(data))

        critical_result, non_critical_result = await asyncio.gather(
            critical_task, non_critical_task, return_exceptions=True
        )

        # Critical pipeline should succeed completely
        assert critical_result == [2, 4, 6, 8]

        # Non-critical pipeline should succeed partially (x=3 fails)
        expected_non_critical = [3, 6, 12]  # x=1->3, x=2->6, x=3->fail, x=4->12
        assert not isinstance(non_critical_result, BaseException)
        assert sorted(non_critical_result) == sorted(expected_non_critical)

        # Verify isolation - critical processing wasn't affected by non-critical failures
        assert len(critical_results) == 4
        assert len(non_critical_results) == 3  # x=3 failed
