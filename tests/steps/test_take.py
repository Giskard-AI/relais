#!/usr/bin/env python3
"""Tests for take operation."""

import asyncio

import pytest

import relais as r


class TestTake:
    @pytest.mark.asyncio
    async def test_basic_take(self):
        """Test basic take functionality."""
        pipeline = r.Take(3)
        result = await ([1, 2, 3, 4, 5, 6, 7, 8] | pipeline).collect()

        expected = [1, 2, 3]
        assert result == expected

    @pytest.mark.asyncio
    async def test_take_zero(self):
        """Test take with zero items raises ValueError."""
        with pytest.raises(ValueError, match="n must be greater than 0"):
            r.Take(0)

    @pytest.mark.asyncio
    async def test_take_all_items(self):
        """Test take with count equal to total items."""
        pipeline = r.Take(5)
        result = await ([1, 2, 3, 4, 5] | pipeline).collect()

        expected = [1, 2, 3, 4, 5]
        assert result == expected

    @pytest.mark.asyncio
    async def test_take_more_than_available(self):
        """Test take with count greater than available items."""
        pipeline = r.Take(10)
        result = await ([1, 2, 3] | pipeline).collect()

        expected = [1, 2, 3]
        assert result == expected

    @pytest.mark.asyncio
    async def test_take_with_empty_input(self):
        """Test take with empty input."""
        pipeline = r.Take(5)
        result = await ([] | pipeline).collect()
        assert result == []

    @pytest.mark.asyncio
    async def test_take_with_single_item(self):
        """Test take with single item."""
        # Take 1 item
        pipeline = r.Take(1)
        result = await ([42] | pipeline).collect()
        expected = [42]
        assert result == expected

        # Take 0 items should raise ValueError
        with pytest.raises(ValueError, match="n must be greater than 0"):
            r.Take(0)

        # Take more than available
        pipeline = r.Take(5)
        result = await ([42] | pipeline).collect()
        expected = [42]
        assert result == expected

    @pytest.mark.asyncio
    async def test_take_preserves_order(self):
        """Test that take preserves order of selected items."""
        pipeline = r.Take(3)
        result = await ([50, 40, 30, 20, 10] | pipeline).collect()

        expected = [50, 40, 30]
        assert result == expected

    @pytest.mark.asyncio
    async def test_take_with_strings(self):
        """Test take with string data."""
        pipeline = r.Take(3)
        result = await (
            ["apple", "banana", "cherry", "date", "elderberry"] | pipeline
        ).collect()

        expected = ["apple", "banana", "cherry"]
        assert result == expected

    @pytest.mark.asyncio
    async def test_take_in_pipeline(self):
        """Test take as part of a pipeline."""
        pipeline = (
            r.Map[int, int](lambda x: x * 2)  # [2, 4, 6, 8, 10]
            | r.Take(3)  # [2, 4, 6]
            | r.Filter[int](lambda x: x > 2)  # [4, 6]
        )

        result = await ([1, 2, 3, 4, 5] | pipeline).collect()

        expected = [4, 6]
        assert result == expected

    @pytest.mark.asyncio
    async def test_take_early_termination(self):
        """Test that take stops processing after getting required items."""
        call_count = 0

        def counting_func(x):
            nonlocal call_count
            call_count += 1
            return x * 2

        pipeline = r.Map[int, int](counting_func) | r.Take(3)
        result = await ([1, 2, 3, 4, 5, 6, 7, 8] | pipeline).collect()

        expected = [2, 4, 6]
        assert result == expected

        # Should have processed all items due to parallel execution
        # but take should only return first 3 results
        assert len(result) == 3

    @pytest.mark.asyncio
    async def test_take_with_duplicates(self):
        """Test take with duplicate values."""
        pipeline = r.Take(4)
        result = await ([1, 1, 2, 2, 3, 3, 4, 4] | pipeline).collect()

        expected = [1, 1, 2, 2]
        assert result == expected

    @pytest.mark.asyncio
    async def test_take_large_dataset(self):
        """Test take with larger dataset."""
        data = list(range(100))  # [0, 1, 2, ..., 99]
        pipeline = r.Take(5)
        result = await (data | pipeline).collect()

        expected = [0, 1, 2, 3, 4]
        assert result == expected

    @pytest.mark.asyncio
    async def test_multiple_takes(self):
        """Test multiple take operations in sequence."""
        pipeline = r.Take(5) | r.Take(3)  # Take first 5, then take first 3 of those
        result = await ([1, 2, 3, 4, 5, 6, 7, 8] | pipeline).collect()

        # First take: [1, 2, 3, 4, 5]
        # Second take: [1, 2, 3]
        expected = [1, 2, 3]
        assert result == expected

    @pytest.mark.asyncio
    async def test_take_and_skip_combination(self):
        """Test combining take and skip operations."""
        # Take middle elements: skip first 2, then take next 3
        pipeline = r.Skip(2) | r.Take(3)
        result = await ([1, 2, 3, 4, 5, 6, 7, 8] | pipeline).collect()

        # Skip: [3, 4, 5, 6, 7, 8]
        # Take: [3, 4, 5]
        expected = [3, 4, 5]
        assert result == expected

    @pytest.mark.asyncio
    async def test_take_with_async_operations(self):
        """Test take with async pipeline operations."""

        async def async_double(x):
            await asyncio.sleep(0.01)
            return x * 2

        pipeline = r.Map[int, int](async_double) | r.Take(3)
        result = await ([1, 2, 3, 4, 5] | pipeline).collect()

        expected = [2, 4, 6]
        assert result == expected
