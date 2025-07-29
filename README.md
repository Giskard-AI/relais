# relais

A practical tool for managing async pipelines.

# usage

```py
import relais as r

pipeline = range(10) | r.map(lambda x: x * 2)
result = await pipeline.collect()
```

# docs

## Pipeline operations

- `r.map(fn)` - apply a function to each item
- `r.filter(fn)` - filter items based on a condition
- `r.flat_map(fn)` - for operations that return iterables
- `r.reduce(fn, initial)` - accumulate values
- `r.take(n)` / `r.skip(n)` - limit/offset operations
- `r.distinct(key_fn)` - deduplicate items with optional key function
- `r.sort(key_fn)` - sorting with custom key functions
- `r.group_by(key_fn)` - group items by key (returns dict)
- `r.batch(size)` - batch items into chunks

All functions can be async. Our interface is async-first.

## Pipeline composition

Pipeline steps can be composed using the `|` operator. This should also support simple Python objects.

**Examples:**

Simple map:

```py
pipeline = range(3) | r.map(lambda x: x * 2) | list
result = await pipeline.run()
# [0, 2, 4]
```

We can also replace the `list` step with `collect`:

```py
pipeline = range(3) | r.map(lambda x: x * 2)
result = await pipeline.collect()
# [0, 2, 4]
```

We can also pass the argument to the pipeline at runtime:

```py
pipeline = r.map(lambda x: x * 2) | r.map(lambda x: x + 1)
result = await pipeline.collect(range(3))
# [1, 3, 5]
```

We can chain multiple steps:

```py
pipeline = [3, 1, 4, 2] | r.sort() | r.map(lambda x: x * 2) | r.batch(2)
result = await pipeline.collect()
# [[2, 4], [6, 8]]
```

Computation must be efficient with async functions. Everything should be processed concurrently, that is as soon as there is a result from a step, it should be processed by the next step. Like queues.

The `stream` function can be used to get results as they are available.

```py
async def async_square(x):
    await asyncio.sleep(random.random() * 5)
    return x * x

pipeline = range(4) | r.map(async_square) | r.batch(2)

async for result in pipeline.stream():
    print(result)
# Prints [0, 1], [4, 9], [16, 25], [36, 49] not necessarily in that order.
```

We can also compose pipelines. We won't be simply concatenating the steps, but considering a whole pipeline as a step itself. This is important since each pipeline could have a slightly different configuration (e.g. maximum number of concurrent tasks, what to do with exceptions, etc.).

```py
pipeline1 = range(3) | r.map(lambda x: x * 2) | r.reduce(lambda acc, x: acc + x, initial=0)
pipeline2 = (lambda n: range(n)) | r.map(lambda x: x + 1)

pipeline3 = pipeline1 | pipeline2
# pipeline3 is composed of two PipelineStep, one for pipeline1 and one for pipeline2.

result = await pipeline3.collect()
# [1, 2, 3, 4, 5, 6]
```
