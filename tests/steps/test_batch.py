#!/usr/bin/env python3
"""Tests for batch operation."""

import asyncio

import pytest

import relais as r


class TestBatch:
    @pytest.mark.asyncio
    async def test_batch_even_division(self):
        """Test batch with data that divides evenly."""
        pipeline = r.Batch(2)
        result = await ([1, 2, 3, 4, 5, 6] | pipeline).collect()

        expected = [[1, 2], [3, 4], [5, 6]]
        assert result == expected

    @pytest.mark.asyncio
    async def test_batch_uneven_division(self):
        """Test batch with data that doesn't divide evenly."""
        pipeline = r.Batch(3)
        result = await ([1, 2, 3, 4, 5, 6, 7] | pipeline).collect()

        expected = [[1, 2, 3], [4, 5, 6], [7]]  # Last batch has only 1 item
        assert result == expected

    @pytest.mark.asyncio
    async def test_batch_size_one(self):
        """Test batch with size of 1."""
        pipeline = r.Batch(1)
        result = await ([1, 2, 3] | pipeline).collect()

        expected = [[1], [2], [3]]
        assert result == expected

    @pytest.mark.asyncio
    async def test_batch_larger_than_input(self):
        """Test batch size larger than input data."""
        pipeline = r.Batch(10)
        result = await ([1, 2, 3] | pipeline).collect()

        expected = [[1, 2, 3]]  # Single batch with all items
        assert result == expected

    @pytest.mark.asyncio
    async def test_batch_empty_input(self):
        """Test batch with empty input."""
        pipeline = r.Batch(3)
        result = await ([] | pipeline).collect()

        expected = []  # No batches created
        assert result == expected

    @pytest.mark.asyncio
    async def test_batch_single_item(self):
        """Test batch with single item."""
        pipeline = r.Batch(2)
        result = await ([42] | pipeline).collect()

        expected = [[42]]  # Single batch with one item
        assert result == expected

    @pytest.mark.asyncio
    async def test_batch_preserves_order(self):
        """Test that batch preserves order of items."""
        pipeline = r.Batch(2)
        result = await ([10, 20, 30, 40, 50] | pipeline).collect()

        expected = [[10, 20], [30, 40], [50]]
        assert result == expected

    @pytest.mark.asyncio
    async def test_batch_with_strings(self):
        """Test batch with string data."""
        pipeline = r.Batch(3)
        result = await (["a", "b", "c", "d", "e", "f", "g"] | pipeline).collect()

        expected = [["a", "b", "c"], ["d", "e", "f"], ["g"]]
        assert result == expected

    @pytest.mark.asyncio
    async def test_batch_with_complex_objects(self):
        """Test batch with complex objects."""
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
            {"id": 4, "name": "Diana"},
            {"id": 5, "name": "Eve"},
        ]

        pipeline = r.Batch(2)
        result = await (data | pipeline).collect()

        expected = [
            [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
            [{"id": 3, "name": "Charlie"}, {"id": 4, "name": "Diana"}],
            [{"id": 5, "name": "Eve"}],
        ]
        assert result == expected

    @pytest.mark.asyncio
    async def test_batch_streaming_behavior(self):
        """Test that batch emits batches as they are ready (streaming)."""
        pipeline = r.Batch(2)

        # Use streaming to verify batches are emitted as they become available
        results = []
        async for indexed_batch in ([1, 2, 3, 4, 5] | pipeline).stream():
            results.append(
                indexed_batch
            )  # Extract the actual batch from Indexed wrapper

        expected = [[1, 2], [3, 4], [5]]
        assert results == expected

    @pytest.mark.asyncio
    async def test_batch_with_async_processing(self):
        """Test batch combined with async processing."""

        async def async_double(x):
            await asyncio.sleep(0.01)
            return x * 2

        pipeline = r.Map(async_double) | r.Batch(2)
        result = await ([1, 2, 3, 4, 5] | pipeline).collect()

        expected = [[2, 4], [6, 8], [10]]
        assert result == expected

    @pytest.mark.asyncio
    async def test_batch_size_validation(self):
        """Test that batch raises error for invalid size."""
        with pytest.raises(ValueError, match="Batch size must be greater than 0"):
            r.Batch(0)

        with pytest.raises(ValueError, match="Batch size must be greater than 0"):
            r.Batch(-1)

    @pytest.mark.asyncio
    async def test_batch_large_size(self):
        """Test batch with large batch size."""
        pipeline = r.Batch(100)
        result = await (list(range(50)) | pipeline).collect()

        expected = [list(range(50))]  # Single batch with all 50 items
        assert result == expected

    @pytest.mark.asyncio
    async def test_batch_mixed_types(self):
        """Test batch with mixed data types."""
        data = [1, "hello", 3.14, True, None, [1, 2, 3]]
        pipeline = r.Batch(3)
        result = await (data | pipeline).collect()

        expected = [[1, "hello", 3.14], [True, None, [1, 2, 3]]]
        assert result == expected

    @pytest.mark.asyncio
    async def test_batch_final_cleanup_behavior(self):
        """Test that final partial batch is emitted in cleanup."""
        # This test specifically verifies the cleanup behavior
        # by ensuring the final partial batch is included
        pipeline = r.Batch(4)
        result = await ([1, 2, 3, 4, 5, 6, 7] | pipeline).collect()

        expected = [[1, 2, 3, 4], [5, 6, 7]]  # Last batch from cleanup
        assert result == expected
