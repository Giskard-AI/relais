#!/usr/bin/env python3
"""Tests for filter operation."""

import asyncio

import pytest

import relais as r


class TestFilter:
    @pytest.mark.asyncio
    async def test_sync_filter(self):
        """Test filter with synchronous predicate."""
        pipeline = r.Filter[int](lambda x: x % 2 == 0)
        result = await ([1, 2, 3, 4, 5, 6] | pipeline).collect()

        expected = [2, 4, 6]
        assert result == expected

    @pytest.mark.asyncio
    async def test_async_filter(self):
        """Test filter with asynchronous predicate."""

        async def is_greater_than_three(x: int) -> bool:
            await asyncio.sleep(0.01)
            return x > 3

        pipeline = r.Filter[int](is_greater_than_three)
        result = await ([1, 2, 3, 4, 5] | pipeline).collect()

        expected = [4, 5]
        assert result == expected

    @pytest.mark.asyncio
    async def test_filter_preserves_order(self):
        """Test that filter preserves original order of matching items."""

        async def slow_for_large(x: int) -> bool:
            if x > 5:
                await asyncio.sleep(0.1)  # Large numbers take longer to check
            return x % 2 == 0

        pipeline = r.Filter[int](slow_for_large)
        result = await ([1, 2, 3, 4, 5, 6, 7, 8] | pipeline).collect()

        expected = [2, 4, 6, 8]
        assert result == expected

    @pytest.mark.asyncio
    async def test_filter_all_pass(self):
        """Test filter where all items pass."""
        pipeline = r.Filter[int](lambda x: x > 0)
        result = await ([1, 2, 3, 4, 5] | pipeline).collect()

        expected = [1, 2, 3, 4, 5]
        assert result == expected

    @pytest.mark.asyncio
    async def test_filter_none_pass(self):
        """Test filter where no items pass."""
        pipeline = r.Filter[int](lambda x: x > 10)
        result = await ([1, 2, 3, 4, 5] | pipeline).collect()

        assert result == []

    @pytest.mark.asyncio
    async def test_filter_with_empty_input(self):
        """Test filter with empty input."""
        pipeline = r.Filter[int](lambda x: x > 0)
        result = await ([] | pipeline).collect()
        assert result == []
