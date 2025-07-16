# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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
