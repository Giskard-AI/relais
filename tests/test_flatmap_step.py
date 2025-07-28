import relais as r


async def test_flatmap_operation():
    """Test FlatMap operation"""
    pipeline = [1, 2, 3] | r.FlatMap(lambda x: [x, x * 2])
    result = await pipeline.collect()
    assert result == [1, 2, 2, 4, 3, 6] 