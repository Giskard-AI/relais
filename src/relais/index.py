from typing import Optional


class Index:
    """Index of an item in a stream for maintaining order during parallel processing.

    The Index class is used to track the position of items as they flow through
    the pipeline, enabling proper ordering of results even when items are processed
    concurrently across multiple steps.

    Attributes:
        index: The primary index position of the item
        sub_index: Optional nested index for operations that expand items (like flat_map)

    Example:
        >>> idx1 = Index(0)  # First item
        >>> idx2 = Index(1, Index(2))  # Second item with sub-index 2
        >>> idx1 < idx2  # True - maintains ordering
    """

    index: int
    sub_index: Optional["Index"] = None

    def __init__(self, index: int, sub_index: Optional["Index"] = None):
        """Initialize an Index.

        Args:
            index: The primary index position
            sub_index: Optional nested index for expanded items
        """
        if not isinstance(index, int):
            raise TypeError(f"Index must be an integer, got {type(index).__name__}")
        if index < -1:  # Allow -1 for sentinel values
            raise ValueError(f"Index must be >= -1, got {index}")

        self.index = index
        self.sub_index = sub_index

    def __lt__(self, other: "Index") -> bool:
        # First compare primary indices
        if self.index != other.index:
            return self.index < other.index

        # If primary indices are equal, compare sub_indices
        # None sub_index sorts before any actual sub_index
        if self.sub_index is None and other.sub_index is None:
            return False
        elif self.sub_index is None:
            return True  # None sorts first
        elif other.sub_index is None:
            return False
        else:
            return self.sub_index < other.sub_index  # Recursive comparison

    def with_sub_index(self, sub_index: int) -> "Index":
        return Index(
            self.index,
            Index(sub_index)
            if self.sub_index is None
            else self.sub_index.with_sub_index(sub_index),
        )
