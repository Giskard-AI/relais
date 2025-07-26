# relais

A practical tool for managing async pipelines.

# usage

```py
import relais as r

pipeline = r.P(range(10)) | r.Map(lambda x: x * 2)
result = await pipeline.run()
```

# docs

## Pipeline operations

`r.Map(fn)` - apply a function to each item
`r.Filter(fn)` - filter items based on a condition
`r.FlatMap(fn)` - for operations that return iterables
`r.Reduce(fn)` - accumulate values
`r.Take(n)` / `r.Skip(n)` - limit/offset operations
`r.Unique(key_fn)` - deduplicate items with optional key function
`r.Sort(key_fn)` - sorting with custom key functions
`r.GroupBy(key_fn)` - group items by key
`r.Batch(size)` - batch items into chunks

All functions can be async. Our interface is async-first.

## Pipeline composition

Pipeline steps can be composed using the `|` operator. This should also support simple Python objects.

**Examples:**

Simple map:

```py
pipeline = range(3) | r.Map(lambda x: x * 2) | list
result = await pipeline.run()
# [0, 2, 4]
```

We can also replace the `list` step with `collect`:

```py
pipeline = range(3) | r.Map(lambda x: x * 2)
result = await pipeline.collect()
# [0, 2, 4]
```

We can also pass the argument to the pipeline at runtime:

```py
pipeline = r.Map(lambda x: x * 2) | r.Map(lambda x: x + 1)
result = await pipeline.collect(range(3))
# [1, 3, 5]
```

We can chain multiple steps:

```py
pipeline = [3, 1, 4, 2] | r.Sort() | r.Map(lambda x: x * 2) | r.Batch(2)
result = await pipeline.collect()
# [[2, 4], [6, 8]]
```

Computation must be efficient with async functions. Everything should be processed concurrently, that is as soon as there is a result from a step, it should be processed by the next step. Like queues.

The `stream` function can be used to get results as they are available.

```py
async def async_square(x):
    await asyncio.sleep(random.random() * 5)
    return x * x

pipeline = range(4) | r.Map(async_square) | r.Batch(2)

async for result in pipeline.stream():
    print(result)
# Prints [0, 1], [4, 9], [16, 25], [36, 49] not necessarily in that order.
```

We can also compose pipelines. We won't be simply concatenating the steps, but considering a whole pipeline as a step itself. This is important since each pipeline could have a slightly different configuration (e.g. maximum number of concurrent tasks, what to do with exceptions, etc.).

```py
pipeline1 = range(3) | r.Map(lambda x: x * 2) | r.Reduce(lambda acc, x: acc + x, initial=0)
pipeline2 = (lambda n: range(n)) | r.Map(lambda x: x + 1)

pipeline3 = pipeline1 | pipeline2
# pipeline3 is composed of two PipelineStep, one for pipeline1 and one for pipeline2.

result = await pipeline3.collect()
# [1, 2, 3, 4, 5, 6]
```
