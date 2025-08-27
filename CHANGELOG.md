# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - 2025-08-06

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

[Unreleased]: https://github.com/Giskard-AI/relais/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/Giskard-AI/relais/releases/tag/v1.0.0
