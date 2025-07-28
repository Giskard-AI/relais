import relais as r


async def test_take_operation():
    """Test Take operation"""
    pipeline = range(10) | r.Take(3)
    result = await pipeline.collect()
    assert result == [0, 1, 2]


async def test_nested_pipeline_with_take():
    """Test more complex pipeline composition with Take"""
    # Transform numbers
    transform_pipeline = r.Map(lambda x: x * 3) | r.Filter(lambda x: x > 5)

    # Use it in a larger pipeline
    pipeline = range(5) | transform_pipeline | r.Take(2)
    result = await pipeline.collect()
    assert result == [6, 9]  # 0*3=0(filtered), 1*3=3(filtered), 2*3=6, 3*3=9, take 2
