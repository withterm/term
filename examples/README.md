# Term Examples

This directory contains runnable examples demonstrating various features of the Term data validation library.

## Running Examples

All examples can be run using `cargo run --example <example_name>`. Some examples require specific features to be enabled.

## Available Examples

### 1. Basic Validation
**File:** `src/basic_validation.rs`

Demonstrates core Term functionality including:
- Creating validation suites with multiple checks
- Running completeness, uniqueness, and statistical constraints
- Interpreting validation results

```bash
cargo run --example basic_validation
```

### 2. TPC-H Validation
**File:** `src/tpc_h_validation.rs`

Shows data quality validation on TPC-H benchmark data:
- Using Term's test utilities to generate TPC-H data
- Validating multiple TPC-H tables
- Checking referential integrity between tables
- Business rule compliance validation

```bash
cargo run --example tpc_h_validation --features test-utils
```

### 3. Cloud Storage Integration
**File:** `src/cloud_storage_example.rs`

Demonstrates validating data from cloud storage services:
- AWS S3 integration (with IAM and access keys)
- Google Cloud Storage (GCS) integration
- Azure Blob Storage integration
- Running validation on cloud-hosted data

```bash
# For S3
cargo run --example cloud_storage_example --features s3

# For all cloud providers
cargo run --example cloud_storage_example --features "s3 gcs azure"
```

### 4. OpenTelemetry Integration
**File:** `src/telemetry_example.rs`

Shows how to integrate Term with OpenTelemetry for observability:
- Configuring OpenTelemetry with console exporters
- Creating TermTelemetry configuration
- Running validation with telemetry spans and metrics
- Analyzing performance and validation metrics

```bash
cargo run --example telemetry_example --features telemetry
```

### 5. Migration from Deequ
**File:** `src/deequ_migration.rs`

Provides side-by-side comparison for users migrating from Deequ:
- Basic validation suite comparison
- Constraint suggestions (profiling) equivalent
- Anomaly detection patterns
- Custom assertions and business rules

```bash
cargo run --example deequ_migration
```

### 6. Structured Logging
**File:** `src/structured_logging_example.rs`

Demonstrates Term's logging capabilities:
- JSON-formatted structured logs
- Different log levels and filtering
- Integration with tracing ecosystem

```bash
cargo run --example structured_logging_example --features telemetry
```

### 7. Result Formatters
**File:** `src/result_formatters_example.rs`

Shows different ways to format and display validation results:
- Human-readable console output
- JSON format for programmatic processing
- Markdown format for documentation
- JUnit XML format for CI integration

```bash
cargo run --example result_formatters_example
```

## Example Data

Most examples use simple CSV data created on-the-fly. The TPC-H example generates standard benchmark data using Term's test utilities.

For cloud storage examples, you'll need:
- Appropriate credentials configured (AWS credentials, GCP service account, Azure connection string)
- Access to cloud storage buckets/containers with sample data

## Contributing

When adding new examples:
1. Create a new `.rs` file in the `src/` directory
2. Add an entry to `Cargo.toml` with any required features
3. Update this README with a description and usage instructions
4. Ensure the example is self-contained and well-commented