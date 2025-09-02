"""Tests for the new Index class functionality."""

import pytest

from relais.index import Index


class TestIndexBasics:
    """Test basic Index functionality."""

    def test_index_creation(self):
        """Test basic index creation."""
        idx = Index(5)
        assert idx.index == 5
        assert idx.sub_index is None

    def test_index_with_sub_index(self):
        """Test index creation with sub-index."""
        sub_idx = Index(2)
        idx = Index(5, sub_idx)
        assert idx.index == 5
        assert idx.sub_index is not None
        assert idx.sub_index.index == 2

    def test_index_validation(self):
        """Test index validation."""
        # Negative indices should raise error (except -1)
        with pytest.raises(ValueError):
            Index(-2)

        # -1 should be allowed (sentinel value)
        idx = Index(-1)
        assert idx.index == -1

    def test_index_comparison_simple(self):
        """Test simple index comparison."""
        idx1 = Index(1)
        idx2 = Index(2)
        idx3 = Index(1)

        assert idx1 < idx2
        assert not idx2 < idx1
        assert not idx1 < idx3  # Equal indices


class TestIndexOrdering:
    """Test Index ordering behavior."""

    def test_primary_index_ordering(self):
        """Test ordering by primary index."""
        indices = [Index(3), Index(1), Index(5), Index(2)]
        sorted_indices = sorted(indices)

        expected_order = [1, 2, 3, 5]
        actual_order = [idx.index for idx in sorted_indices]
        assert actual_order == expected_order

    def test_sub_index_ordering(self):
        """Test ordering with sub-indices."""
        # Same primary index, different sub-indices
        idx1 = Index(1, Index(5))
        idx2 = Index(1, Index(3))
        idx3 = Index(1, Index(7))

        indices = [idx1, idx2, idx3]
        sorted_indices = sorted(indices)

        # Should be ordered by sub-index: 3, 5, 7
        expected_sub_order = [3, 5, 7]
        actual_sub_order = [
            idx.sub_index.index for idx in sorted_indices if idx.sub_index is not None
        ]
        assert actual_sub_order == expected_sub_order

    def test_mixed_ordering(self):
        """Test ordering with mix of sub-indices and no sub-indices."""
        idx1 = Index(1)  # No sub-index
        idx2 = Index(1, Index(5))  # With sub-index
        idx3 = Index(2)  # No sub-index, higher primary
        idx4 = Index(1, Index(2))  # With sub-index

        indices = [idx3, idx2, idx1, idx4]
        sorted_indices = sorted(indices)

        # Expected order:
        # 1. Index(1) - no sub-index sorts first among same primary
        # 2. Index(1, Index(2)) - sub-index 2
        # 3. Index(1, Index(5)) - sub-index 5
        # 4. Index(2) - higher primary index

        assert sorted_indices[0] == idx1  # Index(1)
        assert sorted_indices[1] == idx4  # Index(1, Index(2))
        assert sorted_indices[2] == idx2  # Index(1, Index(5))
        assert sorted_indices[3] == idx3  # Index(2)

    def test_none_sub_index_sorts_first(self):
        """Test that None sub-index sorts before any actual sub-index."""
        idx_none = Index(5)  # No sub-index
        idx_zero = Index(5, Index(0))  # Sub-index 0
        idx_positive = Index(5, Index(10))  # Sub-index 10

        indices = [idx_positive, idx_zero, idx_none]
        sorted_indices = sorted(indices)

        # None should sort first, then by sub-index value
        assert sorted_indices[0] == idx_none
        assert sorted_indices[1] == idx_zero
        assert sorted_indices[2] == idx_positive


class TestIndexEquality:
    """Test Index equality behavior."""

    def test_index_equality(self):
        """Test index equality."""
        idx1 = Index(5)
        idx2 = Index(5)
        idx3 = Index(6)

        # Equality is based on __lt__ returning False in both directions
        assert not idx1 < idx2 and not idx2 < idx1  # Equal
        assert idx1 < idx3 or idx3 < idx1  # Not equal

    def test_sub_index_equality(self):
        """Test equality with sub-indices."""
        idx1 = Index(5, Index(3))
        idx2 = Index(5, Index(3))
        idx3 = Index(5, Index(4))

        assert not idx1 < idx2 and not idx2 < idx1  # Equal
        assert idx1 < idx3  # Different sub-indices


class TestIndexHierarchy:
    """Test hierarchical index structures."""

    def test_nested_sub_indices(self):
        """Test deeply nested sub-indices."""
        # Create nested structure: Index(1, Index(2, Index(3)))
        deep_sub = Index(3)
        mid_sub = Index(2, deep_sub)
        top_idx = Index(1, mid_sub)

        assert top_idx.index == 1
        assert top_idx.sub_index is not None
        assert top_idx.sub_index.index == 2
        assert top_idx.sub_index.sub_index is not None
        assert top_idx.sub_index.sub_index.index == 3

    def test_with_sub_index_method(self):
        """Test the with_sub_index method for creating hierarchical indices."""
        base_idx = Index(10)

        # Add sub-index when none exists
        new_idx = base_idx.with_sub_index(5)
        assert new_idx.index == 10
        assert new_idx.sub_index is not None
        assert new_idx.sub_index.index == 5
        assert new_idx.sub_index.sub_index is None

        # Add sub-index when one already exists (should create deeper nesting)
        deeper_idx = new_idx.with_sub_index(3)
        assert deeper_idx.index == 10
        assert deeper_idx.sub_index is not None
        assert deeper_idx.sub_index.index == 5
        assert deeper_idx.sub_index.sub_index is not None
        assert deeper_idx.sub_index.sub_index.index == 3

    def test_hierarchical_ordering(self):
        """Test ordering with hierarchical indices."""
        # Create indices with different nesting levels
        idx1 = Index(1)
        idx2 = Index(1, Index(0))
        idx3 = Index(1, Index(0, Index(5)))
        idx4 = Index(1, Index(1))

        indices = [idx4, idx3, idx1, idx2]
        sorted_indices = sorted(indices)

        # Expected order:
        # 1. Index(1) - no sub-index
        # 2. Index(1, Index(0)) - sub-index 0
        # 3. Index(1, Index(0, Index(5))) - sub-index 0 with sub-sub-index 5
        # 4. Index(1, Index(1)) - sub-index 1

        assert sorted_indices[0] == idx1
        assert sorted_indices[1] == idx2
        assert sorted_indices[2] == idx3
        assert sorted_indices[3] == idx4


class TestIndexEdgeCases:
    """Test edge cases and error conditions."""

    def test_sentinel_value_handling(self):
        """Test handling of -1 as sentinel value."""
        sentinel = Index(-1)
        normal = Index(0)

        assert sentinel < normal
        assert sentinel.index == -1

    def test_large_index_values(self):
        """Test handling of large index values."""
        large_idx = Index(1000000)
        normal_idx = Index(5)

        assert normal_idx < large_idx
        assert large_idx.index == 1000000

    def test_recursive_comparison_depth(self):
        """Test that recursive comparison works for reasonable depths."""
        # Create a chain of nested indices
        current = Index(5)
        for i in range(10):  # Create 10 levels of nesting
            current = Index(1, current)

        # Should be able to compare without issues
        other = Index(2)
        assert current < other  # Primary index 1 < 2

    def test_comparison_with_complex_hierarchy(self):
        """Test comparison with complex hierarchical structures."""
        # Create two complex hierarchical indices
        idx1 = Index(1, Index(2, Index(3, Index(4))))
        idx2 = Index(1, Index(2, Index(3, Index(5))))

        # Should compare based on the deepest difference
        assert idx1 < idx2  # 4 < 5 at the deepest level


class TestIndexUsagePatterns:
    """Test common usage patterns for indices."""

    def test_flat_map_index_pattern(self):
        """Test index pattern typical of flat_map operations."""
        # Item 0 expansions
        item0_expansions = [
            Index(0, Index(0)),  # First expansion of item 0
            Index(0, Index(1)),  # Second expansion of item 0
        ]

        # Item 1 expansions
        item1_expansions = [
            Index(1, Index(0)),  # First expansion of item 1
            Index(1, Index(1)),  # Second expansion of item 1
            Index(1, Index(2)),  # Third expansion of item 1
        ]

        all_indices = item0_expansions + item1_expansions
        sorted_indices = sorted(all_indices)

        # Should maintain proper ordering despite expansions
        expected_order = [
            Index(0, Index(0)),
            Index(0, Index(1)),
            Index(1, Index(0)),
            Index(1, Index(1)),
            Index(1, Index(2)),
        ]

        for actual, expected in zip(sorted_indices, expected_order):
            assert actual.index == expected.index
            assert actual.sub_index is not None
            assert expected.sub_index is not None
            assert actual.sub_index.index == expected.sub_index.index

    def test_concurrent_processing_ordering(self):
        """Test that indices maintain order even when processed out of sequence."""
        # Simulate items completing out of order due to concurrent processing
        completed_order = [Index(3), Index(1), Index(0), Index(4), Index(2)]

        # Sort to restore original order
        restored_order = sorted(completed_order)

        expected_indices = [0, 1, 2, 3, 4]
        actual_indices = [idx.index for idx in restored_order]

        assert actual_indices == expected_indices


if __name__ == "__main__":
    # Run basic tests for quick validation
    def quick_test():
        # Basic creation and comparison
        idx1 = Index(1)
        idx2 = Index(2)
        assert idx1 < idx2

        # Sub-index creation
        idx_with_sub = Index(5, Index(3))
        assert idx_with_sub.index == 5
        assert idx_with_sub.sub_index is not None
        assert idx_with_sub.sub_index.sub_index is None
        assert idx_with_sub.sub_index.index == 3

        # Ordering
        indices = [Index(3), Index(1), Index(2)]
        sorted_indices = sorted(indices)
        assert [idx.index for idx in sorted_indices] == [1, 2, 3]

        print("✅ Quick Index tests passed!")

    quick_test()
