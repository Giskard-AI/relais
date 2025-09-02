#!/usr/bin/env python3
"""Tests for distinct operation."""

from typing import Any

import pytest

import relais as r


class TestDistinct:
    @pytest.mark.asyncio
    async def test_basic_distinct(self):
        """Test basic distinct functionality."""
        pipeline = r.Distinct()
        result = await ([1, 2, 2, 3, 1, 4, 3, 5] | pipeline).collect()

        expected = [1, 2, 3, 4, 5]
        assert result == expected

    @pytest.mark.asyncio
    async def test_distinct_preserves_order(self):
        """Test that distinct preserves first occurrence order."""
        pipeline = r.Distinct()
        result = await ([3, 1, 4, 1, 5, 9, 2, 6, 5, 3] | pipeline).collect()

        # Should keep first occurrence of each item in original order
        expected = [3, 1, 4, 5, 9, 2, 6]
        assert result == expected

    @pytest.mark.asyncio
    async def test_distinct_with_custom_key(self):
        """Test distinct with custom key function."""
        data = [
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": 30},
            {"name": "Charlie", "age": 25},  # Same age as Alice
            {"name": "David", "age": 35},
            {"name": "Eve", "age": 30},  # Same age as Bob
        ]

        pipeline = r.Distinct[dict[str, Any]](key=lambda x: x["age"])
        result = await (data | pipeline).collect()

        # Should keep first occurrence of each age
        expected = [data[0], data[1], data[3]]  # Alice(25), Bob(30), David(35)
        assert result == expected

    @pytest.mark.asyncio
    async def test_distinct_with_strings(self):
        """Test distinct with string data."""
        pipeline = r.Distinct()
        result = await (
            ["apple", "banana", "apple", "cherry", "banana", "date"] | pipeline
        ).collect()

        expected = ["apple", "banana", "cherry", "date"]
        assert result == expected

    @pytest.mark.asyncio
    async def test_distinct_with_empty_input(self):
        """Test distinct with empty input."""
        pipeline = r.Distinct()
        result = await ([] | pipeline).collect()
        assert result == []

    @pytest.mark.asyncio
    async def test_distinct_with_single_item(self):
        """Test distinct with single item."""
        pipeline = r.Distinct()
        result = await ([42] | pipeline).collect()

        expected = [42]
        assert result == expected

    @pytest.mark.asyncio
    async def test_distinct_all_unique(self):
        """Test distinct with all unique items."""
        pipeline = r.Distinct()
        result = await ([1, 2, 3, 4, 5] | pipeline).collect()

        expected = [1, 2, 3, 4, 5]
        assert result == expected

    @pytest.mark.asyncio
    async def test_distinct_all_same(self):
        """Test distinct with all identical items."""
        pipeline = r.Distinct()
        result = await ([7, 7, 7, 7, 7] | pipeline).collect()

        expected = [7]
        assert result == expected

    @pytest.mark.asyncio
    async def test_distinct_with_none_values(self):
        """Test distinct with None values."""
        pipeline = r.Distinct()
        result = await ([1, None, 2, None, 3, 1] | pipeline).collect()

        expected = [1, None, 2, 3]
        assert result == expected

    @pytest.mark.asyncio
    async def test_distinct_case_sensitive_strings(self):
        """Test distinct with case-sensitive string comparison."""
        pipeline = r.Distinct()
        result = await (
            ["Apple", "apple", "APPLE", "banana", "Banana"] | pipeline
        ).collect()

        # Should treat different cases as different items
        expected = ["Apple", "apple", "APPLE", "banana", "Banana"]
        assert result == expected

    @pytest.mark.asyncio
    async def test_distinct_with_complex_objects(self):
        """Test distinct with complex objects (value comparison)."""
        obj1 = {"x": 1, "y": 2}
        obj2 = {"x": 1, "y": 2}  # Same content as obj1
        obj3 = {"x": 3, "y": 4}
        obj4 = {"x": 1, "y": 2}  # Same content as obj1 and obj2

        pipeline = r.Distinct()
        result = await ([obj1, obj2, obj3, obj4, obj1] | pipeline).collect()

        # Should keep first occurrence of each unique value
        # obj2 and obj4 have same content as obj1, so only obj1 and obj3 remain
        expected = [obj1, obj3]
        assert result == expected

    @pytest.mark.asyncio
    async def test_distinct_with_complex_objects_warning_on_max_unhashable_items(self):
        """Test distinct with complex objects (value comparison)."""
        obj1 = {"x": 1, "y": 2}
        obj2 = {"x": 1, "y": 2}  # Same content as obj1
        obj3 = {"x": 3, "y": 4}
        obj4 = {"x": 1, "y": 2}  # Same content as obj1 and obj2

        with pytest.warns(
            UserWarning,
            match="Distinct processor reached max unhashable items limit. Consider using a different key function to avoid performance degradation.",
        ):
            pipeline = r.Distinct(max_unhashable_items=1)
            result = await ([obj1, obj2, obj3, obj4, obj1] | pipeline).collect()

        # Should keep first occurrence of each unique value
        # obj2 and obj4 have same content as obj1, so only obj1 and obj3 remain
        expected = [obj1, obj3]
        assert result == expected

    @pytest.mark.asyncio
    async def test_distinct_with_object_identity(self):
        """Test distinct with object identity using id() as key."""
        obj1 = {"x": 1, "y": 2}
        obj2 = {"x": 1, "y": 2}  # Same content but different object
        obj3 = {"x": 3, "y": 4}

        pipeline = r.Distinct(key=id)  # Use object identity
        result = await ([obj1, obj2, obj1, obj3, obj2] | pipeline).collect()

        # Should keep first occurrence of each object reference
        expected = [obj1, obj2, obj3]
        assert result == expected
