import relais as r


async def test_reduce_operation():
    """Test Reduce operation"""
    pipeline = range(5) | r.Reduce(lambda acc, x: acc + x, initial=0)
    result = await pipeline.collect()
    assert result == 10  # 0+1+2+3+4 