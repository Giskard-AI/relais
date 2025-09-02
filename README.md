# relais

[![PyPI version](https://badge.fury.io/py/relais.svg)](https://badge.fury.io/py/relais)
[![Python versions](https://img.shields.io/pypi/pyversions/relais.svg)](https://pypi.org/project/relais/)
[![License](https://img.shields.io/pypi/l/relais.svg)](https://pypi.org/project/relais/)
[![CI](https://github.com/Giskard-AI/relais/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/Giskard-AI/relais/actions/workflows/ci.yml)
[![Code Coverage](https://img.shields.io/codecov/c/github/Giskard-AI/relais?branch=main)](https://codecov.io/gh/Giskard-AI/relais)

A high-performance async streaming pipeline library for Python.

**Key Features:**
- 🚀 **True Streaming**: Process data as it flows through the pipeline
- ⚡ **Directional Cancellation**: Early termination optimizations (e.g., `take(5)` stops upstream processing)
- 🔄 **Concurrent Processing**: All operations run concurrently with proper backpressure
- 🛡️ **Memory Efficient**: Bounded memory usage with configurable stream buffers
- 🎯 **Flexible Ordering**: Choose between ordered and unordered processing for optimal performance

**Perfect for:**
- LLM evaluation pipelines
- API data processing
- Real-time data transformation
- I/O-bound concurrent operations

# usage

```py
import relais as r

# Simple pipeline
pipeline = range(10) | r.Map(lambda x: x * 2) | r.Take(5)
result = await pipeline.collect()  # [0, 2, 4, 6, 8]

# Streaming processing
async for item in (range(100) | r.Map(lambda x: x * 2) | r.Take(5)).stream():
    print(item)  # Prints results as they become available
```

## Installation

```bash
pip install relais
```

## Requirements
- Python 3.10+

## Quick Start

```py
import asyncio
import relais as r

async def main():
    # Transform and filter data
    result = await (
        range(20)
        | r.Map(lambda x: x * 2)
        | r.Filter(lambda x: x > 10)
        | r.Take(5)
    ).collect()
    print(result)  # [12, 14, 16, 18, 20]

    # Stream processing with async operations
    async def slow_square(x):
        await asyncio.sleep(0.1)  # Simulate I/O
        return x * x

    # Process items as they complete
    async for item in (range(5) | r.Map(slow_square)).stream():
        print(f"Completed: {item}")

asyncio.run(main())
```

# API Reference

## Core Operations

### Transform Operations
- `r.Map(fn)` - Apply function to each item (supports async functions)
- `r.Filter(fn)` - Filter items based on condition
- `r.FlatMap(fn)` - Flatten iterables returned by function

### Collection Operations
- `r.Take(n, ordered=False)` - Take first N items (with early cancellation)
- `r.Skip(n, ordered=False)` - Skip first N items
- `r.Distinct(key_fn=None)` - Remove duplicates
- `r.Sort(key_fn=None)` - Sort items (stateful operation)
- `r.Batch(size)` - Group items into batches
- `r.Reduce(fn, initial)` - Accumulate values

### Processing Modes
- **Unordered** (default): Maximum performance, items processed as available
- **Ordered**: Preserves input order, may be slower for some operations

**All operations support async functions and run concurrently by default.**

## Performance Features

### Directional Cancellation
```py
# Only processes first 5 items, cancels upstream automatically
result = await (large_data_source | r.expensive_operation() | r.Take(5)).collect()
```

### Memory Efficiency
```py
# Streams through millions of items with bounded memory
async for batch in (huge_dataset | r.Map(transform) | r.Batch(100)).stream():
    process_batch(batch)  # Constant memory usage
```

### Concurrent Processing
```py
# All async operations run concurrently
pipeline = (
    data_source
    | r.Map(async_api_call)  # Multiple concurrent API calls
    | r.Filter(validate)     # Filters results as they arrive
    | r.Take(10)            # Stops processing after 10 valid results
)
```

## Pipeline Composition

Pipelines are built using the intuitive `|` operator for chaining operations:

### Basic Usage

```py
# Data source | operations | collection
result = await (range(5) | r.Map(lambda x: x * 2) | r.Filter(lambda x: x > 4)).collect()
# [6, 8]
```

### Runtime Input

```py
# Define pipeline without input data
pipeline = r.Map(lambda x: x * 2) | r.Take(3)

# Apply to different data sources
result1 = await pipeline.collect(range(10))  # [0, 2, 4]
result2 = await pipeline.collect([5, 6, 7, 8])  # [10, 12, 14]
```

### Streaming Results

```py
import asyncio
import random

async def slow_process(x):
    await asyncio.sleep(random.uniform(0.1, 0.5))
    return x * x

pipeline = range(10) | r.Map(slow_process) | r.Filter(lambda x: x % 2 == 0)

# Process results as they become available
async for result in pipeline.stream():
    print(f"Got result: {result}")
    # Results appear in completion order, not input order
```

### Error Handling

```py
from relais.errors import ErrorPolicy

# Fail fast (default) - stops on first error
pipeline = r.Pipeline(
    [r.Map(might_fail), r.Filter(lambda x: x > 0)],
    error_policy=ErrorPolicy.FAIL_FAST
)

# Collect errors for later inspection
pipeline = r.Pipeline(
    [r.Map(might_fail), r.Take(10)],
    error_policy=ErrorPolicy.COLLECT
)
results, errors = await pipeline.collect_with_errors(data)

# Ignore errors and continue processing
pipeline = r.Pipeline(
    [r.Map(might_fail), r.Take(10)],
    error_policy=ErrorPolicy.IGNORE
)
```

### Advanced: Context Manager Usage

```py
# For fine-grained control over pipeline execution
async with await pipeline.run(data_source) as stream:
    async for event in stream:
        if isinstance(event, StreamItemEvent):
            print(f"Item: {event.item}")
        elif isinstance(event, StreamErrorEvent):
            print(f"Error: {event.error}")

        # Early termination
        if some_condition:
            break
# Pipeline automatically cleans up resources
```

## Architecture & Performance

### Streaming Architecture

Relais uses a true streaming architecture where:
- **Data flows through bounded queues** between pipeline steps
- **Operations run concurrently** - each step processes items as they arrive
- **Memory usage is bounded** - configurable queue sizes prevent memory explosions
- **Backpressure handling** - upstream producers slow down when downstream is busy

### Directional Cancellation

Optimizations flow backwards through the pipeline:

```py
# take(5) signals upstream to stop after 5 items
# This prevents processing millions of unnecessary items
huge_dataset | expensive_computation | r.Take(5)
```

### Memory Efficiency

- **Bounded queues**: Default 1000 items per stream (configurable)
- **Streaming processing**: Items are processed and released immediately
- **Resource cleanup**: Automatic cleanup via context managers

### Performance Characteristics

- **Best for**: I/O-bound operations with 100-100K items
- **Concurrent**: All async operations run in parallel
- **Memory bounded**: Constant memory usage regardless of input size
- **Early termination**: Operations like `take()` provide significant optimizations

## Use Cases

### LLM Evaluation Pipeline
```py
# Generate test cases → Run model → Evaluate results
test_cases | r.Map(run_llm_async) | r.Map(evaluate_response) | r.Take(100)
```

### API Data Processing
```py
# Fetch → Transform → Validate → Store
api_endpoints | r.Map(fetch_async) | r.Map(transform) | r.Filter(validate) | r.Batch(10)
```

### Real-time Stream Processing
```py
# Process events as they arrive
event_stream | r.Filter(important) | r.Map(enrich) | r.Batch(5)
```

## Support
- 📝 [Issues & Bug Reports](https://github.com/Giskard-AI/relais/issues)
- 💡 [Feature Requests](https://github.com/Giskard-AI/relais/discussions)
- 📚 [Documentation](https://github.com/Giskard-AI/relais/blob/main/README.md)
- 🤝 [Contributing](https://github.com/Giskard-AI/relais/blob/main/CONTRIBUTING.md)
