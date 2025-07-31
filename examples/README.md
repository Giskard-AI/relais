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

### 🌊 `streaming_advantage_benchmark.py` - True Streaming Benefits
**Demonstrates relais's core advantage**

Shows where relais truly excels over pure asyncio:
- Multi-stage pipeline streaming (items flow through stages independently)
- Early results availability (don't wait for all items to complete)
- Mixed-speed operations (fast items don't wait for slow ones)
- Time-to-first-result advantages (5-10x faster)

```bash
python examples/streaming_advantage_benchmark.py
```

### 🎯 `real_world_advantage.py` - Realistic LLM Pipeline
**Real-world content moderation scenario**

5-stage content moderation pipeline showing 2-4x speedups:
- Generate inputs → Moderation API → LLM Classification → Safety Check → Report
- Realistic variable delays (moderation APIs: 0.5s-12s, LLM: 1s-6s)
- Time-to-first-result comparisons
- Resource utilization analysis

```bash
python examples/real_world_advantage.py
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

# Run performance benchmarks
python examples/simple_benchmark.py
python examples/benchmark_comparison.py

# Run streaming advantage demos
python examples/streaming_advantage_benchmark.py
python examples/real_world_advantage.py
```

## Understanding the Output

Each example shows:
- **Execution time**: How long the pipeline took to run
- **Concurrency benefits**: Processing happens in parallel, not sequentially
- **Item throughput**: Number of items processed successfully
- **Error handling**: How failures are managed based on error policy

### Performance Expectations

The benchmarks show different advantages depending on the scenario:

**Simple pipelines** (`simple_benchmark.py`):
- **2-3x speedup** vs sequential processing
- Similar performance to pure asyncio (both do concurrent processing)

**Multi-stage pipelines** (`streaming_advantage_benchmark.py`, `real_world_advantage.py`):
- **2-4x speedup** vs pure asyncio batch processing
- **5-10x faster time-to-first-result** (streaming advantage)
- **Much simpler code** than manual asyncio stage coordination
- **Better resource utilization** (parallel stage execution)

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
