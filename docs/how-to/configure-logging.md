# Structured Logging in Term

Term provides comprehensive structured logging with OpenTelemetry integration for observability and debugging.

## Features

- **Structured Fields**: All logs use structured fields for easy querying and analysis
- **Trace Correlation**: Automatic trace ID propagation through OpenTelemetry integration
- **Performance-Sensitive**: Configurable logging levels to minimize overhead
- **JSON Format**: Machine-readable JSON output for log aggregation systems

## Quick Start

### Basic Setup

```rust
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// Initialize structured logging
tracing_subscriber::fmt()
    .json()  // Use JSON format
    .with_env_filter("info,term_core=debug")
    .init();
```

### With OpenTelemetry Integration

```rust
use tracing_opentelemetry::OpenTelemetryLayer;

// Create OpenTelemetry layer for trace correlation
let telemetry_layer = OpenTelemetryLayer::new(tracer);

// Initialize with trace correlation
tracing_subscriber::fmt()
    .json()
    .finish()
    .with(telemetry_layer)
    .init();
```

## Log Levels

Term uses the following log levels:

- **ERROR**: Critical failures that prevent validation from completing
- **WARN**: Constraint failures and validation issues
- **INFO**: High-level operation status (suite start/end, data source registration)
- **DEBUG**: Detailed operation information (constraint evaluation, metrics)

## Structured Fields

### Suite-Level Fields

```json
{
  "level": "INFO",
  "message": "Starting validation suite",
  "suite.name": "data_quality",
  "suite.checks": 5,
  "suite.description": "Daily data quality checks",
  "telemetry.enabled": true,
  "trace_id": "7b3f1a2d4e5f6789"
}
```

### Check-Level Fields

```json
{
  "level": "DEBUG",
  "message": "Running validation check",
  "check.name": "user_completeness",
  "check.level": "Warning",
  "check.constraints": 3,
  "trace_id": "7b3f1a2d4e5f6789"
}
```

### Constraint-Level Fields

```json
{
  "level": "DEBUG",
  "message": "Completeness constraint passed",
  "constraint.name": "completeness",
  "constraint.column": "user_id",
  "constraint.threshold": "0.95",
  "result.completeness": "0.9832",
  "result.non_null_count": 9832,
  "result.total_count": 10000,
  "result.status": "success",
  "trace_id": "7b3f1a2d4e5f6789"
}
```

### Failure Fields

```json
{
  "level": "WARN",
  "message": "Constraint failed",
  "constraint.name": "completeness",
  "check.name": "email_quality",
  "check.level": "Error",
  "failure.message": "Completeness 0.7523 is below threshold 0.9",
  "constraint.metric": 0.7523,
  "trace_id": "7b3f1a2d4e5f6789"
}
```

## Performance Configuration

Term provides a `LogConfig` for performance-sensitive environments:

```rust
use term_core::logging::LogConfig;

// Production configuration (minimal logging)
let config = LogConfig::production();

// Verbose configuration (detailed debugging)
let config = LogConfig::verbose();

// Custom configuration
let config = LogConfig {
    base_level: Level::INFO,
    log_constraint_details: false,
    log_data_operations: true,
    log_metrics: true,
    max_field_length: 256,
};
```

## Log Analysis

### Using jq for JSON logs

```bash
# Run with JSON output
cargo run --example validation 2>&1 | jq

# Filter by level
cargo run --example validation 2>&1 | jq 'select(.level == "WARN")'

# Extract specific fields
cargo run --example validation 2>&1 | jq '{time, level, message, trace_id}'

# Group by trace ID
cargo run --example validation 2>&1 | jq -s 'group_by(.trace_id)'

# Find all failed constraints
cargo run --example validation 2>&1 | jq 'select(.message == "Constraint failed")'
```

### Log Aggregation Systems

Term's structured logs are compatible with popular log aggregation systems:

- **Elasticsearch/Kibana**: Index on fields like `suite.name`, `check.name`, `constraint.name`
- **Datadog**: Use facets for `level`, `result.status`, `check.level`
- **CloudWatch Insights**: Query using fields like `trace_id`, `metrics.duration_ms`

## Best Practices

### 1. Use Structured Fields

Instead of:
```rust
info!("Starting validation for {}", suite_name);
```

Use:
```rust
info!(
    suite.name = %suite_name,
    suite.checks = check_count,
    "Starting validation suite"
);
```

### 2. Include Context

Always include relevant context in logs:
```rust
warn!(
    constraint.name = %constraint.name(),
    check.name = %check.name(),
    failure.reason = %error,
    "Constraint evaluation failed"
);
```

### 3. Use Appropriate Levels

- ERROR: Only for unrecoverable errors
- WARN: For failures that don't stop execution
- INFO: For major operations
- DEBUG: For detailed debugging information

### 4. Leverage Trace Correlation

The trace ID is automatically included when using OpenTelemetry integration:
```rust
// All logs within this span will have the same trace_id
let span = telemetry.start_suite_span(&suite_name, check_count);
info!("This log will include the trace_id automatically");
```

### 5. Performance Considerations

- Use conditional logging for hot paths
- Avoid logging large data structures
- Use the `max_field_length` configuration to truncate long values
- Consider using sampling in high-throughput scenarios

### 6. Term-Specific Best Practices

#### Use #[instrument] for Automatic Spans

The `#[instrument]` macro automatically creates spans with structured fields:

```rust
#[instrument(skip(self, ctx), fields(
    constraint.name = %self.name(),
    constraint.column = %self.column
))]
async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult> {
    // Logs within this function automatically include the span fields
    debug!("Evaluating constraint");
}
```

#### Log at the Right Level for Constraints

```rust
// Success - use DEBUG
debug!(
    constraint.name = %self.name(),
    result.metric = %metric,
    "Constraint passed"
);

// Failure - use WARN
warn!(
    constraint.name = %self.name(),
    failure.message = %message,
    "Constraint failed"
);

// Error - use ERROR
error!(
    constraint.name = %self.name(),
    error = %e,
    "Error evaluating constraint"
);
```

#### Include Metrics in Logs

Always log quantitative results for analysis:

```rust
info!(
    suite.name = %self.name,
    metrics.passed = passed,
    metrics.failed = failed,
    metrics.total = total,
    metrics.duration_ms = duration_ms,
    metrics.success_rate = %format!("{:.2}%", success_rate),
    "Validation suite completed"
);
```

#### Structured Field Naming Convention

Use dot notation for hierarchical data:

- `suite.*` - Suite-level information
- `check.*` - Check-level information  
- `constraint.*` - Constraint-level information
- `result.*` - Result information
- `metrics.*` - Performance metrics
- `source.*` - Data source information

#### Data Source Logging

For data sources, include relevant connection details:

```rust
info!(
    table.name = %table_name,
    source.type = "csv",
    source.paths = ?self.paths,
    csv.delimiter = %delimiter,
    csv.has_header = has_header,
    "Registering CSV data source"
);
```

## Example Output

Running a validation suite produces structured logs like:

```json
{"timestamp":"2024-01-20T10:30:45.123Z","level":"INFO","message":"Starting validation suite","suite.name":"daily_quality_checks","suite.checks":5,"telemetry.enabled":true,"trace_id":"a1b2c3d4e5f6"}
{"timestamp":"2024-01-20T10:30:45.125Z","level":"DEBUG","message":"Running validation check","check.name":"user_completeness","check.level":"Warning","check.constraints":1,"trace_id":"a1b2c3d4e5f6"}
{"timestamp":"2024-01-20T10:30:45.234Z","level":"DEBUG","message":"Completeness constraint passed","constraint.name":"completeness","constraint.column":"user_id","result.completeness":"0.9923","result.status":"success","trace_id":"a1b2c3d4e5f6"}
{"timestamp":"2024-01-20T10:30:45.567Z","level":"INFO","message":"Validation suite completed","suite.name":"daily_quality_checks","metrics.passed":4,"metrics.failed":1,"metrics.total":5,"metrics.duration_ms":444,"metrics.success_rate":"80.00%","suite.result":"failed","trace_id":"a1b2c3d4e5f6"}
```

## Troubleshooting

### No Logs Appearing

Check your environment filter:
```bash
# Enable all Term logs
RUST_LOG=term_core=debug cargo run

# Enable specific modules
RUST_LOG=term_core::core=debug,term_core::constraints=info cargo run
```

### Too Many Logs

Use `LogConfig::production()` or adjust the environment filter:
```bash
# Only warnings and errors
RUST_LOG=warn cargo run
```

### Missing Trace IDs

Ensure OpenTelemetry is properly configured:
```rust
let telemetry_layer = OpenTelemetryLayer::new(tracer);
let subscriber = tracing_subscriber::fmt()
    .json()
    .finish()
    .with(telemetry_layer);
```