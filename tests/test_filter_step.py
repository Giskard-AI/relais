import relais as r


async def test_filter_operation():
    """Test Filter operation"""
    pipeline = range(10) | r.Filter(lambda x: x % 2 == 0)
    result = await pipeline.collect()
    assert result == [0, 2, 4, 6, 8] 