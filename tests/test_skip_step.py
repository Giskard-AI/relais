import relais as r


async def test_skip_operation():
    """Test Skip operation"""
    pipeline = range(5) | r.Skip(2)
    result = await pipeline.collect()
    assert result == [2, 3, 4] 