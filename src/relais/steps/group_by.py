import asyncio
from typing import Any, Callable, Dict, List
from collections import defaultdict

from ..base import Step, Stream, StatefulStreamProcessor, T

class _GroupByProcessor(StatefulStreamProcessor[T, Dict[Any, List[T]]]):
    """Processor that groups items by a key function into a dictionary.
    
    This processor collects all items and groups them by the result of applying
    a key function to each item. The output is a single dictionary mapping
    keys to lists of items.
    """

    def __init__(self, input_stream: Stream[T], output_stream: Stream[Dict[Any, List[T]]], key_func: Callable[[T], Any]):
        """Initialize the group_by processor.
        
        Args:
            input_stream: Stream to read items from
            output_stream: Stream to write the grouped dictionary to
            key_func: Function to extract grouping key from each item
        """
        super().__init__(input_stream, output_stream)
        self.key_func = key_func

    async def _process_items(self, items: List[T]) -> List[Dict[Any, List[T]]]:
        """Group items by the result of the key function.
        
        Args:
            items: All items from the input stream
            
        Returns:
            List containing a single dictionary mapping keys to item lists
        """
        groups: Dict[Any, List[T]] = defaultdict(list)
        
        for item in items:
            key = self.key_func(item)
            groups[key].append(item)
        
        # Return the groups dictionary as a single-item list
        return [dict(groups)]

class GroupBy(Step[T, Dict[Any, List[T]]]):
    """Pipeline step that groups items by a key function into a dictionary.
    
    The GroupBy step collects all items and organizes them into groups based on
    the result of applying a key function to each item. The output is a single
    dictionary where keys are the grouping values and values are lists of items
    that share that key.
    
    This is a stateful operation that requires all items to be collected before
    grouping can occur.
    
    Example:
        >>> # Group numbers by remainder when divided by 3
        >>> numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        >>> pipeline = numbers | group_by(lambda x: x % 3)
        >>> await pipeline.collect()
        >>> # [{0: [3, 6, 9], 1: [1, 4, 7], 2: [2, 5, 8]}]
        
        >>> # Group words by length
        >>> words = ["cat", "dog", "bird", "elephant", "ant"]
        >>> pipeline = words | group_by(len)
        >>> await pipeline.collect()
        >>> # [{3: ["cat", "dog", "ant"], 4: ["bird"], 8: ["elephant"]}]
        
        >>> # Group objects by attribute
        >>> users = [
        ...     {"name": "Alice", "dept": "eng"},
        ...     {"name": "Bob", "dept": "sales"},
        ...     {"name": "Charlie", "dept": "eng"}
        ... ]
        >>> pipeline = users | group_by(lambda u: u["dept"])
        >>> # {"eng": [{"name": "Alice", ...}, {"name": "Charlie", ...}], "sales": [...]} 
    
    Note:
        The result is always a list with a single dictionary. This allows
        consistent chaining with other pipeline operations.
    """

    def __init__(self, key_func: Callable[[T], Any]):
        """Initialize the GroupBy step.
        
        Args:
            key_func: Function to extract grouping key from each item
        """
        self.key_func = key_func

    def _build_processor(self, input_stream: Stream[T], output_stream: Stream[Dict[Any, List[T]]]) -> _GroupByProcessor[T]:
        """Build the processor for this group_by step.
        
        Args:
            input_stream: Stream to read from
            output_stream: Stream to write to
            
        Returns:
            A configured group_by processor
        """
        return _GroupByProcessor(input_stream, output_stream, self.key_func)

def group_by(key_func: Callable[[T], Any]) -> GroupBy[T]:
    """Create a group_by step that groups items by a key function.
    
    This function creates a grouping operation that organizes items into
    a dictionary based on the result of applying a key function to each item.
    Items with the same key are grouped together in lists.
    
    Args:
        key_func: Function to extract the grouping key from each item.
                 Should return a hashable value.
    
    Returns:
        A GroupBy step that can be used in pipelines
    
    Examples:
        >>> # Group numbers by even/odd
        >>> numbers = range(10)
        >>> groups = await (numbers | group_by(lambda x: x % 2)).collect()
        >>> # [{0: [0, 2, 4, 6, 8], 1: [1, 3, 5, 7, 9]}]
        
        >>> # Group strings by first letter
        >>> words = ["apple", "banana", "cherry", "apricot", "blueberry"]
        >>> by_first_letter = await (words | group_by(lambda w: w[0])).collect()
        >>> # [{"a": ["apple", "apricot"], "b": ["banana", "blueberry"], "c": ["cherry"]}]
        
        >>> # Group users by department
        >>> employees = [
        ...     {"name": "Alice", "dept": "Engineering", "salary": 100000},
        ...     {"name": "Bob", "dept": "Sales", "salary": 80000},
        ...     {"name": "Charlie", "dept": "Engineering", "salary": 95000},
        ...     {"name": "Diana", "dept": "Marketing", "salary": 85000}
        ... ]
        >>> by_dept = await (employees | group_by(lambda e: e["dept"])).collect()
        >>> # Groups employees by department
        
        >>> # Group by multiple criteria (using tuple)
        >>> def group_key(person):
        ...     return (person["dept"], person["salary"] > 90000)
        >>> 
        >>> complex_groups = await (employees | group_by(group_key)).collect()
        >>> # Groups by department AND high/low salary
        
        >>> # Chain with other operations
        >>> # Get count of items in each group
        >>> group_counts = await (
        ...     numbers 
        ...     | group_by(lambda x: x % 3)
        ...     | map(lambda groups: {k: len(v) for k, v in groups.items()})
        ... ).collect()
        >>> # [{0: 4, 1: 3, 2: 3}] - counts per remainder group
    
    Use Cases:
        - Data analysis and aggregation
        - Categorizing items by properties
        - Preparing data for further processing by groups
        - Creating lookup tables and indices
        - Statistical grouping operations
    
    Note:
        - The key function should return hashable values
        - This is a stateful operation that loads all items into memory
        - The result is a list with one dictionary for consistent pipeline chaining
        - Items maintain their original order within each group
    """
    return GroupBy(key_func)