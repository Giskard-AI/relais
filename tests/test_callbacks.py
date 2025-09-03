import pytest

import relais as r
from relais import ErrorPolicy, PipelineError


@pytest.mark.asyncio
async def test_on_error_callback_called_ignore_and_collect():
    def failing(x: int) -> int:
        if x in {2, 4}:
            raise ValueError(f"boom {x}")
        return x * 2

    # IGNORE: errors are skipped but callback must still be invoked
    pipeline_ignore = r.Pipeline([r.Map(failing)], error_policy=ErrorPolicy.IGNORE)

    ignore_errors: list[str] = []

    def on_error_ignore(err: PipelineError):
        ignore_errors.append(str(err))

    result_ignore = await pipeline_ignore.collect(
        [1, 2, 3, 4], on_error=on_error_ignore
    )

    assert sorted(result_ignore) == sorted([2, 6])
    assert len(ignore_errors) == 2

    # COLLECT: errors are returned and callback must be invoked as well
    pipeline_collect = r.Pipeline([r.Map(failing)], error_policy=ErrorPolicy.COLLECT)

    collect_errors: list[str] = []

    def on_error_collect(err: PipelineError):
        collect_errors.append(str(err))

    combined = await pipeline_collect.collect(
        [1, 2, 3, 4], error_policy=ErrorPolicy.COLLECT, on_error=on_error_collect
    )

    data = [x for x in combined if not isinstance(x, PipelineError)]
    errors = [x for x in combined if isinstance(x, PipelineError)]

    assert sorted(data) == sorted([2, 6])
    assert len(errors) == 2
    assert len(collect_errors) == 2


@pytest.mark.asyncio
async def test_on_result_called_only_for_final_outputs():
    pipeline = r.Pipeline(
        [
            r.Map(lambda x: x * 2),
            r.Filter(lambda y: y % 3 == 0),
            r.Map(lambda z: z + 1),
        ],
        error_policy=ErrorPolicy.IGNORE,
    )

    captured: list[int] = []

    def on_result(value: int):
        captured.append(value)

    inputs = [1, 2, 3, 4, 5, 6]
    results = await pipeline.collect(inputs, on_result=on_result)

    # Map -> Filter(multiples of 3) -> Map(+1)
    # 1..6 -> 2,4,6,8,10,12 -> 6,12 -> 7,13
    expected = [7, 13]

    assert sorted(results) == sorted(expected)
    assert sorted(captured) == sorted(expected)

    # Ensure intermediate values (6, 12) are not reported by on_result
    assert 6 not in captured and 12 not in captured
