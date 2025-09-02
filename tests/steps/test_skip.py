#!/usr/bin/env python3
"""Tests for skip operation."""

import pytest

import relais as r


class TestSkip:
    @pytest.mark.asyncio
    async def test_basic_skip(self):
        """Test basic skip functionality."""
        pipeline = r.Skip(3)
        result = await ([1, 2, 3, 4, 5, 6, 7, 8] | pipeline).collect()

        expected = [4, 5, 6, 7, 8]
        assert result == expected

    @pytest.mark.asyncio
    async def test_skip_zero(self):
        """Test skip with zero items."""
        pipeline = r.Skip(0)
        result = await ([1, 2, 3, 4, 5] | pipeline).collect()

        expected = [1, 2, 3, 4, 5]
        assert result == expected

    @pytest.mark.asyncio
    async def test_skip_all_items(self):
        """Test skip with count equal to total items."""
        pipeline = r.Skip(5)
        result = await ([1, 2, 3, 4, 5] | pipeline).collect()

        expected = []
        assert result == expected

    @pytest.mark.asyncio
    async def test_skip_more_than_available(self):
        """Test skip with count greater than available items."""
        pipeline = r.Skip(10)
        result = await ([1, 2, 3] | pipeline).collect()

        expected = []
        assert result == expected

    @pytest.mark.asyncio
    async def test_skip_with_empty_input(self):
        """Test skip with empty input."""
        pipeline = r.Skip(5)
        result = await ([] | pipeline).collect()
        assert result == []

    @pytest.mark.asyncio
    async def test_skip_with_single_item(self):
        """Test skip with single item."""
        # Skip 0 items
        pipeline = r.Skip(0)
        result = await ([42] | pipeline).collect()
        expected = [42]
        assert result == expected

        # Skip 1 item
        pipeline = r.Skip(1)
        result = await ([42] | pipeline).collect()
        expected = []
        assert result == expected

    @pytest.mark.asyncio
    async def test_skip_preserves_order(self):
        """Test that skip preserves order of remaining items."""
        pipeline = r.Skip(2)
        result = await ([10, 20, 30, 40, 50] | pipeline).collect()

        expected = [30, 40, 50]
        assert result == expected

    @pytest.mark.asyncio
    async def test_skip_with_strings(self):
        """Test skip with string data."""
        pipeline = r.Skip(2)
        result = await (
            ["apple", "banana", "cherry", "date", "elderberry"] | pipeline
        ).collect()

        expected = ["cherry", "date", "elderberry"]
        assert result == expected

    @pytest.mark.asyncio
    async def test_skip_in_pipeline(self):
        """Test skip as part of a pipeline."""
        pipeline = (
            r.Map[int, int](lambda x: x * 2)  # [2, 4, 6, 8, 10]
            | r.Skip(2)  # [6, 8, 10]
            | r.Filter[int](lambda x: x > 6)  # [8, 10]
        )

        result = await ([1, 2, 3, 4, 5] | pipeline).collect()

        expected = [8, 10]
        assert result == expected

    @pytest.mark.asyncio
    async def test_skip_large_dataset(self):
        """Test skip with larger dataset."""
        data = list(range(100))  # [0, 1, 2, ..., 99]
        pipeline = r.Skip(95)
        result = await (data | pipeline).collect()

        expected = [95, 96, 97, 98, 99]
        assert result == expected

    @pytest.mark.asyncio
    async def test_skip_with_duplicates(self):
        """Test skip with duplicate values."""
        pipeline = r.Skip(3)
        result = await ([1, 1, 2, 2, 3, 3, 4, 4] | pipeline).collect()

        expected = [2, 3, 3, 4, 4]
        assert result == expected

    @pytest.mark.asyncio
    async def test_multiple_skips(self):
        """Test multiple skip operations in sequence."""
        pipeline = r.Skip(2) | r.Skip(2)  # Skip first 2, then skip next 2
        result = await ([1, 2, 3, 4, 5, 6, 7, 8] | pipeline).collect()

        # First skip: [3, 4, 5, 6, 7, 8]
        # Second skip: [5, 6, 7, 8]
        expected = [5, 6, 7, 8]
        assert result == expected
