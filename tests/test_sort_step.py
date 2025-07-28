import relais as r


async def test_sort_operation():
    """Test Sort operation"""
    pipeline = [3, 1, 4, 1, 5] | r.Sort()
    result = await pipeline.collect()
    assert result == [1, 1, 3, 4, 5]


async def test_sort_with_key_function():
    """Test Sort with key function"""
    pipeline = ["apple", "pie", "washington", "book"] | r.Sort(key=len)
    result = await pipeline.collect()
    assert result == ["pie", "book", "apple", "washington"] 