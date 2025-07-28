import relais as r


async def test_unique_operation():
    """Test Unique operation"""
    pipeline = [1, 2, 2, 3, 1, 4] | r.Unique()
    result = await pipeline.collect()
    assert result == [1, 2, 3, 4]


async def test_unique_with_key_function():
    """Test Unique with key function"""
    pipeline = ["a", "bb", "c", "dd", "eee"] | r.Unique(key_fn=len)
    result = await pipeline.collect()
    assert result == ["a", "bb", "eee"]  # Unique by length 