# Production Pipeline Example

This example demonstrates how to integrate Term into a production data pipeline with:
- Multiple data sources
- Incremental validation
- Metrics export
- Alerting on failures
- OpenTelemetry integration

## Features Demonstrated

- **Multi-source validation**: Validate data from Parquet, CSV, and database sources
- **Incremental processing**: Handle streaming data with state management
- **Observability**: Export metrics to monitoring systems
- **Error handling**: Robust error recovery and alerting
- **Performance optimization**: Parallel execution and caching

## Running the Example

```bash
cd docs/examples/production-pipeline
cargo run --release
```

## Configuration

Edit `config.yaml` to configure:
- Data source connections
- Validation thresholds
- Alert destinations
- Metric export endpoints

## Files

- `main.rs` - Production pipeline implementation
- `config.yaml` - Configuration file
- `src/pipeline.rs` - Pipeline orchestration logic
- `src/monitoring.rs` - Metrics and alerting
- `src/sources.rs` - Data source management