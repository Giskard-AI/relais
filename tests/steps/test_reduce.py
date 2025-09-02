#!/usr/bin/env python3
"""Tests for reduce operation."""

import asyncio

import pytest

import relais as r


class TestReduce:
    @pytest.mark.asyncio
    async def test_reduce_with_initial_value(self):
        """Test reduce with initial value."""
        pipeline = r.Reduce[int, int](lambda acc, x: acc + x, 0)
        result = await ([1, 2, 3, 4, 5] | pipeline).collect()

        expected = [15]  # 0 + 1 + 2 + 3 + 4 + 5
        assert result == expected

    @pytest.mark.asyncio
    async def test_reduce_without_initial_value(self):
        """Test reduce without initial value (uses first item)."""
        pipeline = r.Reduce[int, int](lambda acc, x: acc + x)
        result = await ([1, 2, 3, 4, 5] | pipeline).collect()

        expected = [15]  # 1 + 2 + 3 + 4 + 5
        assert result == expected

    @pytest.mark.asyncio
    async def test_reduce_with_none_as_initial_value(self):
        """Test reduce with None as valid initial value."""
        pipeline = r.Reduce[int, int](lambda acc, x: (acc or 0) + x)
        result = await ([1, 2, 3] | pipeline).collect()

        expected = [6]  # 0 + 1 + 2 + 3
        assert result == expected

    @pytest.mark.asyncio
    async def test_reduce_empty_sequence_with_initial(self):
        """Test reduce with empty sequence but with initial value."""
        pipeline = r.Reduce[int, int](lambda acc, x: acc + x, 10)
        result = await ([] | pipeline).collect()

        expected = [10]  # Just the initial value
        assert result == expected

    @pytest.mark.asyncio
    async def test_reduce_empty_sequence_without_initial_raises_error(self):
        """Test reduce with empty sequence and no initial value raises error."""
        pipeline = r.Reduce[int, int](lambda acc, x: acc + x)

        # With error handling, this now raises a PipelineError
        with pytest.raises(r.PipelineError) as exc_info:
            await ([] | pipeline).collect()

        # Check that this raises the expected PipelineError (specific message check is complex due to nesting)
        assert isinstance(exc_info.value, r.PipelineError)
        assert (
            "Processing failed in _ReduceProcessor: Cannot reduce empty sequence without initial value"
            in str(exc_info.value)
        )

    @pytest.mark.asyncio
    async def test_reduce_single_item_with_initial(self):
        """Test reduce with single item and initial value."""
        pipeline = r.Reduce[int, int](lambda acc, x: acc * x, 2)
        result = await ([5] | pipeline).collect()

        expected = [10]  # 2 * 5
        assert result == expected

    @pytest.mark.asyncio
    async def test_reduce_single_item_without_initial(self):
        """Test reduce with single item and no initial value."""
        pipeline = r.Reduce[int, int](lambda acc, x: acc * x)
        result = await ([5] | pipeline).collect()

        expected = [5]  # Just the single item
        assert result == expected

    @pytest.mark.asyncio
    async def test_async_reduce_function(self):
        """Test reduce with asynchronous reducer function."""

        async def async_add(acc: int, x: int) -> int:
            await asyncio.sleep(0.01)
            return acc + x

        pipeline = r.Reduce[int, int](async_add, 0)
        result = await ([1, 2, 3, 4] | pipeline).collect()

        expected = [10]  # 0 + 1 + 2 + 3 + 4
        assert result == expected

    @pytest.mark.asyncio
    async def test_reduce_string_concatenation(self):
        """Test reduce for string concatenation."""
        pipeline = r.Reduce[str, str](lambda acc, x: acc + x, "")
        result = await (["hello", " ", "world", "!"] | pipeline).collect()

        expected = ["hello world!"]
        assert result == expected

    @pytest.mark.asyncio
    async def test_reduce_list_accumulation(self):
        """Test reduce for list accumulation."""
        pipeline = r.Reduce[int, list[int]](lambda acc, x: acc + [x], [])
        result = await ([1, 2, 3] | pipeline).collect()

        expected = [[1, 2, 3]]
        assert result == expected

    @pytest.mark.asyncio
    async def test_reduce_multiplication(self):
        """Test reduce for multiplication."""
        pipeline = r.Reduce[int, int](lambda acc, x: acc * x, 1)
        result = await ([2, 3, 4] | pipeline).collect()

        expected = [24]  # 1 * 2 * 3 * 4
        assert result == expected

    @pytest.mark.asyncio
    async def test_reduce_max_value(self):
        """Test reduce to find maximum value."""
        pipeline = r.Reduce[int, int](lambda acc, x: max(acc, x))
        result = await ([3, 1, 4, 1, 5, 9, 2, 6] | pipeline).collect()

        expected = [9]  # Maximum value
        assert result == expected

    @pytest.mark.asyncio
    async def test_reduce_complex_objects(self):
        """Test reduce with complex objects."""
        data = [
            {"value": 10, "weight": 2},
            {"value": 20, "weight": 3},
            {"value": 15, "weight": 1},
        ]

        def weighted_sum(acc: int, item: dict[str, int]) -> int:
            return acc + (item["value"] * item["weight"])

        pipeline = r.Reduce[dict[str, int], int](weighted_sum, 0)
        result = await (data | pipeline).collect()

        expected = [95]  # 0 + (10*2) + (20*3) + (15*1) = 95
        assert result == expected
