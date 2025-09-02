#!/usr/bin/env python3
"""Tests for map operation."""

import asyncio

import pytest

import relais as r


class TestMap:
    @pytest.mark.asyncio
    async def test_sync_map(self):
        """Test map with synchronous function."""
        pipeline = r.Map[int, int](lambda x: x * 2)
        result = await ([1, 2, 3, 4, 5] | pipeline).collect()

        expected = [2, 4, 6, 8, 10]
        assert result == expected

    @pytest.mark.asyncio
    async def test_async_map(self):
        """Test map with asynchronous function."""

        async def async_square(x: int) -> int:
            await asyncio.sleep(0.01)
            return x * x

        pipeline = r.Map[int, int](async_square)
        result = await ([1, 2, 3, 4] | pipeline).collect()

        expected = [1, 4, 9, 16]
        assert result == expected

    @pytest.mark.asyncio
    async def test_map_preserves_order(self):
        """Test that map preserves original order."""

        async def slow_for_even(x: int) -> int:
            if x % 2 == 0:
                await asyncio.sleep(0.1)  # Even numbers take longer
            return x * 2

        pipeline = r.Map[int, int](slow_for_even)
        result = await ([1, 2, 3, 4, 5] | pipeline).collect()

        expected = [2, 4, 6, 8, 10]
        assert result == expected

    @pytest.mark.asyncio
    async def test_map_with_empty_input(self):
        """Test map with empty input."""
        pipeline = r.Map[int, int](lambda x: x * 2)
        result = await ([] | pipeline).collect()
        assert result == []

    @pytest.mark.asyncio
    async def test_map_with_single_item(self):
        """Test map with single item."""
        pipeline = r.Map[int, int](lambda x: x * 3)
        result = await ([5] | pipeline).collect()

        expected = [15]
        assert result == expected

    @pytest.mark.asyncio
    async def test_map_index_preservation(self):
        """Test that map preserves original indexes."""
        pipeline = r.Map[int, int](lambda x: x * 2)
        result = await ([10, 20, 30] | pipeline).collect()

        expected = [20, 40, 60]
        assert result == expected
