import asyncio
from typing import Iterable

import relais as r


async def test_iterable():
    sync_iterable = range(3)
    step = r.Iterable(sync_iterable)

    assert step.data == sync_iterable

    async with asyncio.TaskGroup() as tg:
        result = await step.process(None, tg)
    assert isinstance(result, Iterable)

    for t in result:
        assert isinstance(t, asyncio.Task)
        res = await t
