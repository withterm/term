# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2025-09-11

### Added

#### Advanced Analytics (TER-162)

- **KllSketchAnalyzer**: Approximate quantile estimation with O(k log n) memory complexity
  - Configurable accuracy/memory tradeoff via k parameter
  - Custom quantile selection
  - Streaming quantile computation for large datasets
- **CorrelationAnalyzer**: Multi-method correlation analysis
  - Pearson correlation for linear relationships
  - Spearman rank correlation for monotonic relationships
  - Covariance measurement for joint variability
- **MutualInformationAnalyzer**: Information-theoretic dependency detection
  - Detects both linear and non-linear relationships
  - Normalized mutual information scoring
  - Configurable discretization for continuous data

#### Incremental Computation Framework (TER-160)

- **IncrementalAnalysisRunner**: Orchestrates stateful incremental analysis
  - Efficient append-only data processing
  - Partition-based computation
  - Batch processing for large partition sets
- **StateStore Trait**: Pluggable state persistence interface
  - FilesystemStateStore implementation
  - Binary serialization with bincode
  - Partition-level state management
- **TypeErasedAnalyzer**: Dynamic analyzer dispatch for incremental processing
  - State merging across partitions
  - Serialization/deserialization support

#### Metrics Repository Framework

- **MetricsRepository Trait**: Flexible storage and query interface
  - Time-series metric storage
  - Tag-based organization
  - Flexible query API with time ranges and filters
- **InMemoryRepository**: Thread-safe in-memory implementation
  - Arc<RwLock> for concurrent access
  - O(1) save/load operations
- **ResultKey**: Collision-resistant metric identification
  - SHA256 hashing for complex keys
  - Tag-based matching
- **DataFusion Integration**: Optimized query execution
  - Arrow columnar format
  - SQL support for complex analytics

#### Testing & Benchmarking Infrastructure (TER-164)

- **Comprehensive Benchmark Suite**: Six categories of performance tests
  - Individual constraint benchmarks
  - Suite scaling tests (10-500 checks)
  - Data scaling tests (100-10M rows)
  - Advanced analytics benchmarks
  - Memory usage profiling
  - Concurrent validation benchmarks
- **TPC-H Integration Tests**: Industry-standard test data
  - Coverage for all constraint types
  - Worst-case scenario testing
- **CI Optimizations**:
  - cargo-nextest for 30-50% faster test execution
  - SessionContext caching with once_cell
  - Native rust-cache integration

#### Documentation Improvements

- **Diátaxis-Compliant Documentation**: Complete reorganization following Diátaxis framework
  - Tutorials for learning-oriented content
  - How-to guides for task-oriented content
  - Reference for information-oriented content
  - Explanation for understanding-oriented content
- **New Documentation**:
  - Advanced analyzer guides
  - Incremental analysis documentation
  - Metrics repository reference
  - Benchmarking guide

#### Constraint Suggestion System

- **SuggestionEngine**: Rule-based system for automatic constraint recommendations
- **Six Suggestion Rules**:
  - `CompletenessRule`: Suggests null checks based on data completeness
  - `UniquenessRule`: Identifies potential primary keys and unique constraints
  - `PatternRule`: Detects common formats (email, date, phone)
  - `RangeRule`: Recommends min/max bounds for numeric data
  - `DataTypeRule`: Ensures data type consistency
  - `CardinalityRule`: Identifies categorical columns
- **Confidence Scoring**: Each suggestion includes confidence score and rationale
- **Priority System**: Critical/High/Medium/Low priority for implementation guidance
- **Batch Processing**: Analyze multiple columns simultaneously

### Changed

- Updated dependencies to latest compatible versions
- Improved error messages with more context
- Enhanced OpenTelemetry instrumentation coverage

### Fixed

- Batch merging bug in incremental analysis that was overwriting accumulated states
- Memory leak in long-running analyzer processes
- SQL injection vulnerabilities in predicate validation

### Performance

- 30-50% faster test execution with cargo-nextest
- Reduced memory usage in KLL sketch implementation
- Optimized Spearman correlation using SQL RANK() instead of loading all data
- SessionContext caching reduces test setup overhead by 40%

## [0.0.1] - 2024-01-24

### Added

#### Core Features

- **ValidationSuite**: Core abstraction for grouping related validation checks
- **Check**: Container for related constraints with builder pattern API
- **Constraint**: Trait-based system for extensible validation rules
- **Unified Constraint Architecture**: Consolidated 30+ constraint types into 8 unified families

#### Unified Constraints

- **UnifiedCompletenessConstraint**: Validates data completeness with logical operators
- **UnifiedStatisticalConstraint**: Performs statistical validations in optimized queries
- **FormatConstraint**: Validates data formats (email, URL, phone, regex, etc.)
- **UnifiedUniquenessConstraint**: Checks uniqueness with flexible null handling
- **LengthConstraint**: Validates string lengths with flexible assertions
- **UnifiedDataTypeConstraint**: Validates data types and type consistency
- **UnifiedQuantileConstraint**: Computes quantiles and distribution properties
- **UnifiedCorrelationConstraint**: Analyzes correlations between columns

#### Data Sources

- CSV file support with schema inference
- Parquet file support
- JSON file support
- Cloud storage integration (S3, GCS, Azure) via feature flags
- In-memory data support
- Database support (PostgreSQL, MySQL, SQLite) - _Coming in v0.0.2_

#### Performance Features

- Query optimizer for batching similar constraints
- Pattern caching for regex-based validations
- Achieved 14-27% performance improvement over individual constraints
- Memory usage reduced by 44%

#### Observability

- OpenTelemetry integration for distributed tracing
- Structured logging with tracing crate
- Performance metrics and timing information

#### Developer Experience

- Comprehensive builder pattern APIs
- Backward compatibility with deprecation warnings
- Rich error messages with thiserror
- Extensive documentation with examples
- Property-based testing with proptest

### Technical Stack

- Built on Apache DataFusion 48.0 and Arrow 55.0
- Async-first design with Tokio runtime
- Zero-copy data processing where possible
- Type-safe SQL generation with security validation

### Migration from Deequ

- Drop-in replacement for common Deequ checks
- No Spark dependencies required
- Better performance for single-node workloads
- Native Rust safety and performance

## [Unreleased]

### Planned

- Python bindings via PyO3
- WebAssembly support
- Streaming data validation
- Custom data source plugins
- Interactive CLI tool
- Web UI for validation results

[0.0.1]: https://github.com/withterm/term/releases/tag/v0.0.1
