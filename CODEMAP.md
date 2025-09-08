# CODEMAP

This codemap explains how Relais is structured and how the main parts interact so you can navigate the codebase quickly, extend it safely, and reason about performance and behavior.

- Source: `src/relais/`
- Tests: `tests/`
- Examples: `examples/`

## High-level Architecture

Relais implements a high-performance async streaming pipeline. Data flows through a chain of steps connected by bounded streams. Each step is implemented by a processor that reads from an input `StreamReader[T]`, performs work (possibly concurrently), and writes to an output `StreamWriter[U]`.

Key modules:
- `base.py`: public pipeline API (`Step`, `Pipeline`, `PipelineSession`) and chaining (`|`).
- `stream.py`: streaming primitives (`Stream`, `StreamReader`, `StreamWriter`) and events.
- `processors.py`: processor base classes (`StreamProcessor`, `StatelessStreamProcessor`, `StatefulStreamProcessor`).
- `errors.py`: `ErrorPolicy` and `PipelineError`.
- `tasks.py`: concurrency helpers (`BlockingTaskLimiter`, `CancellationScope`).
- `steps/`: built-in steps (map, filter, flat_map, take, skip, sort, distinct, batch, reduce, group_by, async iterator source).
- `index.py`: `Index` for deterministic ordering across concurrent work.

## Core Concepts

### Pipeline and Step (`base.py`)
- `Step[T, U]`: abstract building block of the pipeline. Each step implements `_build_processor(reader, writer)` to return a `StreamProcessor[T, U]`.
- `Pipeline[T, U]`: a sequence of `Step`s. Constructed implicitly with `|` or explicitly. Provides:
  - `collect(...) -> list[...]` to run to completion and gather results.
  - `stream(...) -> AsyncIterator[...]` to consume results as they become available.
  - `open(...) -> PipelineSession[...]` for fine-grained control.
- Chaining:
  - `step1 | step2` returns a `Pipeline`.
  - `data | step` binds input data.
  - `pipeline.then(step)` adds steps to an existing pipeline.
- Input handling: `_get_input_stream(...)` converts input into a `Stream`. Sync iterables use `Stream.from_iterable`; async iterables go through `AsyncIteratorStep` and start a background processor.

### Streams and Events (`stream.py`)
- `Stream[T]`: holds a bounded `asyncio.Queue` of events and coordinates cancellation/completion for a whole chain via shared events. Children created with `pipe()` share cancellation.
- Events:
  - `StreamItemEvent[T]` (item, `Index`)
  - `StreamErrorEvent` (`PipelineError`, `Index`)
  - `StreamEndEvent`
- `StreamReader[T]` and `StreamWriter[T]` wrap read/write operations and expose helpers like `collect`, `handle_error`, `complete`, `cancellation_scope()`.
- Backpressure: queue `max_size` bounds pending events; writers await when downstream is slow.
- Directional completion: `Stream.complete(clear_queue=True)` on a child will signal completion upstream (parent), enabling early stop.

### Processors (`processors.py`)
- `StreamProcessor[T, U]`: base interface. Subclasses implement `process_stream()`.
- `StatelessStreamProcessor[T, U]`:
  - Processes items as they arrive using a `BlockingTaskLimiter` to cap concurrency.
  - Each item is handled in `_process_item(item)` inside a `CancellationScope` and error wrapper `_safe_process_item`.
  - Ideal for: `Map`, `Filter`, `FlatMap`, `Batch` (emits batches on the fly), etc.
- `StatefulStreamProcessor[T, U]`:
  - Waits for input completion, collects with `ErrorPolicy.COLLECT`, processes all items in `_process_items(items)`, then emits outputs and replays errors.
  - Ideal for: `Sort`, `GroupBy`, some `Reduce` variants, `Distinct` when global state is required.

### Concurrency & Cancellation (`tasks.py`)
- `BlockingTaskLimiter(max_tasks)`: schedules coroutines with a concurrency cap via a `TaskGroup` and a semaphore.
- `CancellationScope(cancelled_events)`: background watcher cancels work when any of the provided events (e.g., stream cancelled/completed) is set. Processors use this to promptly stop per-item tasks.

### Error Handling (`errors.py`)
- `ErrorPolicy`:
  - `FAIL_FAST`: first error cancels the stream and raises.
  - `IGNORE`: skip errors; successful items continue.
  - `COLLECT`: include errors as `StreamErrorEvent`s; collection/stream returns both.
- `PipelineError`: wraps the original exception and includes context: step name and item index where available.

### Ordering (`index.py`)
- `Index` tracks logical position for items. Supports nested `sub_index` (e.g., `FlatMap`).
- Readers can sort collected events by `Index` to provide deterministic ordering when needed.

## Data Flow and Lifecycle

1) Build pipeline
- `data | StepA | StepB | StepC` constructs a `Pipeline` where each step’s `_build_processor()` connects `StreamReader`→`StreamWriter` pairs.

2) Execute
- `Pipeline.collect(...)`: opens a `PipelineSession`, starts all processors in a `TaskGroup`, then collects from the final `StreamReader` using the requested `ErrorPolicy`.
- `Pipeline.stream(...)`: yields items as `StreamItemEvent`s arrive; can optionally yield `PipelineError`s if `ErrorPolicy.COLLECT` is active.

3) Backpressure
- Each link is a bounded queue. When a downstream queue is full, upstream writes await, naturally throttling production.

4) Directional cancellation & early termination
- Steps like `Take(…)` and `Skip(…)` may call `output_stream.complete()` early; processors check `output_stream.is_completed()` and stop scheduling/processing.
- `CancellationScope` propagates completion/cancellation to in-flight per-item tasks which raise `CancellationError` and exit quickly.
- `Stream.cancel()` sets a shared cancellation event for the entire chain.

5) Errors
- Processors surface item failures via `StreamErrorEvent(PipelineError, Index)`; `Stream.handle_error` applies the policy and may cancel the chain (fail-fast).

## Built-in Steps (overview)

- `Map(fn)`: transform each item (sync or async). Stateless; preserves item association by `Index`.
- `Filter(predicate)`: pass through items for which predicate returns truthy. Stateless.
- `FlatMap(fn)`: map each item to an iterable/async-iterable and flatten results; uses `Index.with_sub_index` for stable ordering when collected.
- `Batch(size)`: group items into fixed-size lists and emit as they fill; the final batch may be smaller. Stateless buffer per step.
- `Reduce(fn, initial)`: accumulate items into a single value or rolling values. (See implementation for specifics.)
- `GroupBy(key_fn)`: collect to completion and emit a dict of key→list of items. Stateful.
- `Distinct(key_fn=None)`: remove duplicates across the stream; stateful across all items.
- `Sort(key_fn=None, reverse=False)`: collect all items and emit sorted output. Stateful.
- `Take(n, ordered=False)`: output first n items; unordered mode favors performance and stops upstream early; ordered mode collects to preserve input order.
- `Skip(n, ordered=False)`: skip first n items; unordered mode processes as items arrive; ordered mode collects to preserve order.
- `AsyncIteratorStep(async_iterable)`: source step to adapt async iterables into a stream.

For exact semantics, see `src/relais/steps/*.py` and the tests in `tests/steps/`.

## Extension Guide: Adding a New Step

1) Choose processor type
- If processing is per-item without global state, subclass `StatelessStreamProcessor` and implement `_process_item(item)`.
- If you need all input to compute outputs (e.g., global aggregation), subclass `StatefulStreamProcessor` and implement `_process_items(items)`.

2) Implement the `Step`
- Create `class MyStep(Step[T, U])` with `_build_processor(self, input_stream, output_stream)` returning your processor.
- Prefer pure functions, no shared global state.

3) Handle indices and outputs
- Preserve `Index` when emitting single outputs. For multi-outputs per input (flat-map patterns), use `item.index.with_sub_index(k)` to create deterministic order when collecting.

4) Respect cancellation and backpressure
- Wrap item work with `async with output_stream.cancellation_scope():` (already done by `StatelessStreamProcessor._safe_process_item`).
- Avoid blocking calls; await I/O; bound concurrency with `BlockingTaskLimiter` if you spawn tasks yourself.

5) Surface errors correctly
- Raise exceptions; the processor wrappers convert them to `PipelineError` and route via `StreamErrorEvent` according to policy.
- For known error contexts, populate `PipelineError(step_name=self.__class__.__name__, item_index=item.index)`.

6) Tests
- Mirror examples in `tests/` and verify: success path, error policy behavior, cancellation, performance characteristics for large inputs.

## Error Policies in Practice

- Fail fast (default): first failure cancels the chain; `collect()`/`stream()` raises `PipelineError`.
- Ignore: errors are dropped; successful results continue to flow.
- Collect: errors are yielded/returned interleaved with successful results; callers separate data from errors.

## Performance Notes

- Best for I/O-bound workloads with 10²–10⁵ items.
- Memory is bounded by stream queue sizes and any state accumulated by stateful steps.
- Unordered modes (`Take`, `Skip`) enable early termination and lower memory use.

## Public API Surface (`__init__.py`)

Top-level imports for end users:
- `Pipeline`, `ErrorPolicy`, `PipelineError`
- Steps: `Map`, `Filter`, `FlatMap`, `Reduce`, `Batch`, `GroupBy`, `Distinct`, `Sort`, `Take`, `Skip`, `AsyncIteratorStep`

## Cross-References

- Examples (`examples/`): end-to-end usage, streaming patterns, LLM evaluation.
- Tests (`tests/`):
  - Concurrency and streaming: `test_concurrency.py`, `test_streaming_patterns.py`
  - Error handling & policies: `test_error_handling.py`, `test_advanced_error_propagation.py`
  - Steps behavior: `tests/steps/*`
  - Directional cancellation: `test_directional_cancellation.py`
  - Memory and performance: `test_memory_efficiency.py`, `test_performance.py`
  - Reader/writer semantics: `test_stream_reader_writer.py`

## Troubleshooting

- Hanging or slow: check for blocking code in steps; ensure async I/O is awaited; reduce `max_concurrent_tasks` if needed.
- Unexpected ordering: collecting with `ErrorPolicy.COLLECT` preserves event order by `Index`; `stream()` yields in completion order.
- Early stop not working: ensure the terminating step completes its output stream; confirm processors check `is_completed()`/use `CancellationScope`.
- Out-of-memory: prefer unordered `Take`/`Skip`, avoid stateful steps on huge inputs, and keep queue sizes bounded.

---
For deeper details, see module docstrings and inline comments in `src/relais/` and open the corresponding tests to validate assumptions.
