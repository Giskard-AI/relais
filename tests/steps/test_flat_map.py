#!/usr/bin/env python3
"""Tests for flat_map operation."""

import asyncio
from typing import List

import pytest

import relais as r


class TestFlatMap:
    @pytest.mark.asyncio
    async def test_sync_flat_map(self):
        """Test flat_map with synchronous function."""
        pipeline = r.FlatMap[int, int](lambda x: [x, x * 2])
        result = await ([1, 2, 3] | pipeline).collect()

        expected = [1, 2, 2, 4, 3, 6]
        assert result == expected

    @pytest.mark.asyncio
    async def test_async_flat_map(self):
        """Test flat_map with asynchronous function."""

        async def async_duplicate(x: int) -> List[int]:
            await asyncio.sleep(0.01)
            return [x, x + 10]

        pipeline = r.FlatMap[int, int](async_duplicate)
        result = await ([1, 2, 3] | pipeline).collect()

        expected = [1, 11, 2, 12, 3, 13]
        assert result == expected

    @pytest.mark.asyncio
    async def test_flat_map_empty_results(self):
        """Test flat_map with function that returns empty lists for some items."""

        def conditional_expand(x: int) -> List[int]:
            if x % 2 == 0:
                return [x, x * 2]
            else:
                return []  # Odd numbers produce no results

        pipeline = r.FlatMap[int, int](conditional_expand)
        result = await ([1, 2, 3, 4, 5, 6] | pipeline).collect()

        expected = [2, 4, 4, 8, 6, 12]  # Only even numbers produce results
        assert result == expected

    @pytest.mark.asyncio
    async def test_flat_map_single_item_expansion(self):
        """Test flat_map that expands single items to multiple."""
        pipeline = r.FlatMap[int, int](lambda x: [x] * x)  # Repeat each item x times
        result = await ([1, 2, 3] | pipeline).collect()

        expected = [1, 2, 2, 3, 3, 3]
        assert result == expected

    @pytest.mark.asyncio
    async def test_flat_map_with_empty_input(self):
        """Test flat_map with empty input."""
        pipeline = r.FlatMap[int, int](lambda x: [x, x * 2])
        result = await ([] | pipeline).collect()
        assert result == []

    @pytest.mark.asyncio
    async def test_flat_map_with_strings(self):
        """Test flat_map with string operations."""
        pipeline = r.FlatMap[str, str](
            lambda word: list(word)
        )  # Split word into characters
        result = await (["hi", "bye"] | pipeline).collect()

        expected = ["h", "i", "b", "y", "e"]
        assert result == expected

    @pytest.mark.asyncio
    async def test_flat_map_preserves_order(self):
        """Test that flat_map preserves order even with async operations."""

        async def slow_for_even(x: int) -> List[int]:
            if x % 2 == 0:
                await asyncio.sleep(0.1)  # Even numbers take longer
            return [x, x + 100]

        pipeline = r.FlatMap[int, int](slow_for_even)
        result = await ([1, 2, 3, 4] | pipeline).collect()

        expected = [1, 101, 2, 102, 3, 103, 4, 104]
        assert result == expected

    @pytest.mark.asyncio
    async def test_flat_map_nested_structure_flattening(self):
        """Test flat_map flattening nested structures."""
        data = [[1, 2], [3, 4], [5]]
        pipeline = r.FlatMap[list[int], int](
            lambda sublist: sublist
        )  # Flatten one level
        result = await (data | pipeline).collect()

        expected = [1, 2, 3, 4, 5]
        assert result == expected
