import relais as r


async def test_groupby_operation():
    """Test GroupBy operation"""
    pipeline = ["apple", "pie", "book", "cat"] | r.GroupBy(key_fn=len)
    result = await pipeline.collect()
    expected = {3: ["pie", "cat"], 4: ["book"], 5: ["apple"]}
    assert result == expected 