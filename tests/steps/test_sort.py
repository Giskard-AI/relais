#!/usr/bin/env python3
"""Tests for sort operation."""

from typing import Any

import pytest

import relais as r


class TestSort:
    @pytest.mark.asyncio
    async def test_basic_sort(self):
        """Test basic sorting without key or reverse."""
        pipeline = r.Sort()
        result = await ([3, 1, 4, 1, 5, 9, 2, 6] | pipeline).collect()

        expected = [1, 1, 2, 3, 4, 5, 6, 9]
        assert result == expected

    @pytest.mark.asyncio
    async def test_sort_with_key_function(self):
        """Test sorting with custom key function."""
        pipeline = r.Sort[int](
            key=lambda x: -x
        )  # Sort by negative value (reverse order)
        result = await ([3, 1, 4, 2] | pipeline).collect()

        expected = [4, 3, 2, 1]
        assert result == expected

    @pytest.mark.asyncio
    async def test_sort_with_reverse(self):
        """Test sorting in reverse order."""
        pipeline = r.Sort(reverse=True)
        result = await ([3, 1, 4, 2] | pipeline).collect()

        expected = [4, 3, 2, 1]
        assert result == expected

    @pytest.mark.asyncio
    async def test_sort_with_key_and_reverse(self):
        """Test sorting with both key function and reverse."""
        # Sort strings by length, reversed
        pipeline = r.Sort(key=len, reverse=True)
        result = await (["a", "abc", "ab", "abcd"] | pipeline).collect()

        expected = ["abcd", "abc", "ab", "a"]
        assert result == expected

    @pytest.mark.asyncio
    async def test_sort_already_sorted(self):
        """Test sorting already sorted data."""
        pipeline = r.Sort()
        result = await ([1, 2, 3, 4, 5] | pipeline).collect()

        expected = [1, 2, 3, 4, 5]
        assert result == expected

    @pytest.mark.asyncio
    async def test_sort_with_empty_input(self):
        """Test sort with empty input."""
        pipeline = r.Sort()
        result = await ([] | pipeline).collect()
        assert result == []

    @pytest.mark.asyncio
    async def test_sort_with_single_item(self):
        """Test sort with single item."""
        pipeline = r.Sort()
        result = await ([42] | pipeline).collect()

        expected = [42]
        assert result == expected

    @pytest.mark.asyncio
    async def test_sort_with_duplicates(self):
        """Test sort with duplicate values."""
        pipeline = r.Sort()
        result = await ([3, 1, 2, 1, 3, 2] | pipeline).collect()

        expected = [1, 1, 2, 2, 3, 3]
        assert result == expected

    @pytest.mark.asyncio
    async def test_sort_creates_new_indexes(self):
        """Test that sort creates new sequential indexes for sorted items."""
        pipeline = r.Sort()
        result = await ([30, 10, 20] | pipeline).collect()

        # Items should be [10, 20, 30] with new indexes [0, 1, 2]
        expected = [10, 20, 30]

        assert result == expected

    @pytest.mark.asyncio
    async def test_sort_complex_objects(self):
        """Test sorting complex objects."""
        data = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35},
        ]

        pipeline = r.Sort[dict[str, Any]](key=lambda x: x["age"])
        result = await (data | pipeline).collect()

        expected = [data[1], data[0], data[2]]
        assert result == expected
