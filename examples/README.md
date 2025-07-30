# Relais Examples

This directory contains practical examples showing how to use relais for concurrent pipeline processing.

## Examples

### 🔢 `basic_pipeline.py` - Core Concepts
**Best starting point for new users**

Demonstrates fundamental relais operations:
- Concurrent processing with `map()`
- Filtering data with `filter()`
- Batching results with `batch()`
- Streaming results as they arrive
- Error handling policies

```bash
python examples/basic_pipeline.py
```

### 🤖 `simple_llm_eval.py` - LLM Evaluation
**Real-world use case example**

Shows a typical LLM evaluation pipeline:
- Simulate LLM API calls with realistic delays
- Concurrent response evaluation
- Quality filtering
- Performance reporting

```bash
python examples/simple_llm_eval.py
```

### 📋 `llm_evaluation_pipeline.py` - Advanced LLM Evaluation
**Comprehensive production-style example**

Full-featured evaluation system with:
- Structured test cases and data models
- Mock LLM client with realistic behavior
- Detailed evaluation metrics
- Category-based analysis
- Performance benchmarking

```bash
python examples/llm_evaluation_pipeline.py
```

### ⚡ `simple_benchmark.py` - Performance Comparison
**Quick performance demonstration**

Compares three approaches for I/O-bound processing:
- Sequential processing (one at a time)
- Pure asyncio with manual concurrency
- Relais pipeline with automatic concurrency
- Shows streaming advantages and scaling analysis

```bash
python examples/simple_benchmark.py
```

### 🏁 `benchmark_comparison.py` - Comprehensive Benchmarks
**Detailed performance analysis**

Full benchmark suite with:
- Multi-step pipeline comparisons
- Memory usage analysis
- Code complexity evaluation
- Scalability testing with different dataset sizes
- Detailed performance metrics and insights

```bash
python examples/benchmark_comparison.py
```

## Key Concepts Demonstrated

### Concurrent Processing
All examples show how relais enables true concurrent processing - items flow through pipeline stages in parallel rather than being processed in batches.

### Error Handling
Examples demonstrate different error policies:
- `FAIL_FAST`: Stop on first error (default)
- `IGNORE`: Skip failed items, continue processing
- `COLLECT`: Collect errors, return at end

### Streaming vs Collecting
- `collect()`: Wait for all results, return as list
- `stream()`: Get results as they become available (async iterator)

### Typical Use Cases
These examples represent ideal relais use cases:
- I/O-bound operations (API calls, file processing)
- Moderate data volumes (hundreds to thousands of items)
- Need for concurrent processing to improve throughput
- Pipelines with multiple transformation steps

## Running Examples

All examples are self-contained and only require:
- Python 3.10+
- The relais library (installed from source)

From the project root:
```bash
# Install in development mode
uv pip install -e .

# Run any example
python examples/basic_pipeline.py
python examples/simple_llm_eval.py
python examples/llm_evaluation_pipeline.py

# Run benchmarks
python examples/simple_benchmark.py
python examples/benchmark_comparison.py
```

## Understanding the Output

Each example shows:
- **Execution time**: How long the pipeline took to run
- **Concurrency benefits**: Processing happens in parallel, not sequentially
- **Item throughput**: Number of items processed successfully
- **Error handling**: How failures are managed based on error policy

### Performance Expectations

The benchmarks typically show:
- **2-5x speedup** for I/O-bound operations compared to sequential processing
- **Streaming advantage**: Relais processes items as they complete, not in batches
- **Code simplicity**: Much cleaner than manual asyncio coordination
- **Memory efficiency**: Moderate overhead for small-medium pipelines

## Performance Characteristics

### When Relais Excels
- **I/O-bound operations**: API calls, file processing, database queries
- **Moderate concurrency**: 10-1000 concurrent operations
- **Multi-step pipelines**: 2+ processing stages
- **Mixed operation types**: Some fast, some slow operations

### When to Consider Alternatives
- **CPU-bound operations**: Heavy computation (use multiprocessing)
- **Very large datasets**: Millions of items (consider streaming solutions)
- **Simple single-step operations**: Minimal benefit from pipeline overhead

The examples are designed to demonstrate relais' strength in concurrent, I/O-bound processing workloads typical of modern data pipelines and AI applications.
