"""
Relais: High-Performance Async Streaming Pipelines

A modern Python library for building concurrent, streaming data processing pipelines.
Optimized for I/O-bound operations with true streaming architecture and directional
cancellation for maximum performance.

Key Features:
- 🚀 True streaming with bounded memory usage
- ⚡ Directional cancellation (take() stops upstream processing)
- 🔄 Concurrent processing with proper backpressure
- 🛡️ Flexible error handling (FAIL_FAST, IGNORE, COLLECT)
- 📊 Perfect for LLM evaluation, API processing, data enrichment

Quick Start:
    import relais as r

    # Basic pipeline
    result = await (range(10) | r.Map(lambda x: x * 2) | r.Take(5)).collect()

    # Streaming processing
    async for item in (data | r.Map(async_transform) | r.Filter(validate)).stream():
        process(item)

    # Error handling
    pipeline = r.Pipeline([r.Map(might_fail)], error_policy=r.ErrorPolicy.IGNORE)
    results = await pipeline.collect(data)
"""

from .steps import (
    Batch,
    Distinct,
    Filter,
    FlatMap,
    GroupBy,
    Map,
    Reduce,
    Skip,
    Sort,
    Take,
    AsyncIteratorStep,
)

from .errors import (
    ErrorPolicy,
    PipelineError,
)

from .base import Pipeline

__all__ = [
    "Batch",
    "Distinct",
    "Filter",
    "FlatMap",
    "GroupBy",
    "Map",
    "Reduce",
    "Skip",
    "Sort",
    "Take",
    "AsyncIteratorStep",
    "ErrorPolicy",
    "PipelineError",
    "Pipeline",
]
