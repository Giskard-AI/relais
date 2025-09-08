# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.1] - 2025-09-04

### Added
- Callbacks for `collect()` and `stream()` ([#24](https://github.com/Giskard-AI/relais/pull/24)) by @kevinmessiaen

### Changed
- Renamed pipeline run method and context manager API ([#22](https://github.com/Giskard-AI/relais/pull/22)) by @kevinmessiaen
- Updated configurations and tooling following Python 3.10 deprecation ([#21](https://github.com/Giskard-AI/relais/pull/21)) by @kevinmessiaen
- Streamlined error handling; unified around error policies ([#23](https://github.com/Giskard-AI/relais/pull/23)) by @kevinmessiaen

### Removed
- Dropped Python 3.10 support ([#21](https://github.com/Giskard-AI/relais/pull/21))
- Removed `collect_with_error` helper in favor of unified error handling ([#23](https://github.com/Giskard-AI/relais/pull/23))

## [0.2.0] - 2025-08-06

### Added
- Initial release of relais streaming pipeline library
- Core streaming operations: Map, Filter, FlatMap, Take, Skip, Distinct, Sort, Batch, Reduce
- Directional cancellation support for early termination optimizations
- Concurrent processing with proper backpressure handling
- Memory-efficient bounded queues for streaming data
- True streaming architecture with async/await support
- Flexible ordering: choose between ordered and unordered processing
- Comprehensive error handling with configurable error policies
- Rich examples demonstrating real-world usage patterns
- Full async compatibility with proper resource cleanup
- Type hints and py.typed marker for full type checking support

### Technical Features
- Pipeline composition using intuitive `|` operator
- Context manager support for fine-grained control
- Streaming and collection modes for different use cases
- Backpressure and flow control mechanisms
- Cross-platform compatibility (Windows, macOS, Linux)
- Zero runtime dependencies for maximum compatibility

[Unreleased]: https://github.com/Giskard-AI/relais/compare/v0.2.1...HEAD
[0.2.1]: https://github.com/Giskard-AI/relais/releases/tag/v0.2.1
[0.2.0]: https://github.com/Giskard-AI/relais/releases/tag/v0.2.0
