#!/usr/bin/env python3
"""Tests for group_by operation."""

from typing import Any

import pytest

import relais as r


class TestGroupBy:
    @pytest.mark.asyncio
    async def test_group_by_simple_key(self):
        """Test group_by with simple key function."""
        pipeline = r.GroupBy[int](lambda x: x % 2)
        result = await ([1, 2, 3, 4, 5, 6] | pipeline).collect()

        expected = [
            {
                0: [2, 4, 6],  # Even numbers
                1: [1, 3, 5],  # Odd numbers
            }
        ]
        assert result == expected

    @pytest.mark.asyncio
    async def test_group_by_string_length(self):
        """Test group_by with string length as key."""
        pipeline = r.GroupBy[str](len)
        result = await (["a", "bb", "ccc", "dd", "e"] | pipeline).collect()

        expected = [{1: ["a", "e"], 2: ["bb", "dd"], 3: ["ccc"]}]
        assert result == expected

    @pytest.mark.asyncio
    async def test_group_by_first_letter(self):
        """Test group_by with first letter as key."""
        pipeline = r.GroupBy[str](lambda word: word[0].upper())
        result = await (
            ["apple", "banana", "cherry", "apricot", "blueberry"] | pipeline
        ).collect()

        expected = [
            {"A": ["apple", "apricot"], "B": ["banana", "blueberry"], "C": ["cherry"]}
        ]
        assert result == expected

    @pytest.mark.asyncio
    async def test_group_by_empty_input(self):
        """Test group_by with empty input."""
        pipeline = r.GroupBy[int](lambda x: x)
        result = await ([] | pipeline).collect()

        expected = [{}]  # Empty dictionary
        assert result == expected

    @pytest.mark.asyncio
    async def test_group_by_single_item(self):
        """Test group_by with single item."""
        pipeline = r.GroupBy[int](lambda x: x % 2)
        result = await ([5] | pipeline).collect()

        expected = [{1: [5]}]
        assert result == expected

    @pytest.mark.asyncio
    async def test_group_by_all_same_key(self):
        """Test group_by where all items have the same key."""
        pipeline = r.GroupBy[int](lambda x: "same")
        result = await ([1, 2, 3, 4] | pipeline).collect()

        expected = [{"same": [1, 2, 3, 4]}]
        assert result == expected

    @pytest.mark.asyncio
    async def test_group_by_complex_objects(self):
        """Test group_by with complex objects."""
        data = [
            {"name": "Alice", "department": "Engineering", "age": 30},
            {"name": "Bob", "department": "Sales", "age": 25},
            {"name": "Charlie", "department": "Engineering", "age": 35},
            {"name": "Diana", "department": "Sales", "age": 28},
            {"name": "Eve", "department": "Marketing", "age": 32},
        ]

        pipeline = r.GroupBy[dict[str, Any]](lambda person: person["department"])
        result = await (data | pipeline).collect()

        expected = [
            {
                "Engineering": [
                    {"name": "Alice", "department": "Engineering", "age": 30},
                    {"name": "Charlie", "department": "Engineering", "age": 35},
                ],
                "Sales": [
                    {"name": "Bob", "department": "Sales", "age": 25},
                    {"name": "Diana", "department": "Sales", "age": 28},
                ],
                "Marketing": [{"name": "Eve", "department": "Marketing", "age": 32}],
            }
        ]
        assert result == expected

    @pytest.mark.asyncio
    async def test_group_by_age_ranges(self):
        """Test group_by with age ranges."""
        data = [
            {"name": "Alice", "age": 22},
            {"name": "Bob", "age": 35},
            {"name": "Charlie", "age": 17},
            {"name": "Diana", "age": 28},
            {"name": "Eve", "age": 45},
        ]

        def age_group(person):
            age = person["age"]
            if age < 20:
                return "teen"
            elif age < 30:
                return "twenties"
            elif age < 40:
                return "thirties"
            else:
                return "forties+"

        pipeline = r.GroupBy[dict[str, Any]](age_group)
        result = await (data | pipeline).collect()

        expected = [
            {
                "twenties": [
                    {"name": "Alice", "age": 22},
                    {"name": "Diana", "age": 28},
                ],
                "thirties": [{"name": "Bob", "age": 35}],
                "teen": [{"name": "Charlie", "age": 17}],
                "forties+": [{"name": "Eve", "age": 45}],
            }
        ]
        assert result == expected

    @pytest.mark.asyncio
    async def test_group_by_preserves_order_within_groups(self):
        """Test that group_by preserves order within groups."""
        data = [1, 3, 2, 5, 4, 7, 6, 9, 8]
        pipeline = r.GroupBy[int](lambda x: x % 2)
        result = await (data | pipeline).collect()

        expected = [
            {
                1: [1, 3, 5, 7, 9],  # Odd numbers in original order
                0: [2, 4, 6, 8],  # Even numbers in original order
            }
        ]
        assert result == expected

    @pytest.mark.asyncio
    async def test_group_by_none_as_key(self):
        """Test group_by where some items have None as key."""

        def get_key(x):
            return x if x % 2 == 0 else None

        pipeline = r.GroupBy[int](get_key)
        result = await ([1, 2, 3, 4, 5, 6] | pipeline).collect()

        expected = [
            {
                None: [1, 3, 5],  # Odd numbers
                2: [2],
                4: [4],
                6: [6],
            }
        ]
        assert result == expected

    @pytest.mark.asyncio
    async def test_group_by_with_duplicates(self):
        """Test group_by with duplicate items."""
        pipeline = r.GroupBy[int](lambda x: x % 3)
        result = await ([1, 4, 1, 7, 4, 10, 1] | pipeline).collect()

        # 1%3=1, 4%3=1, 1%3=1, 7%3=1, 4%3=1, 10%3=1, 1%3=1
        expected = [
            {
                1: [1, 4, 1, 7, 4, 10, 1]  # All have remainder 1
            }
        ]
        assert result == expected
