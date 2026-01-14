# Nexus Repository Example

This example demonstrates how to persist validation metrics to Term Nexus and query historical results, similar to Deequ's MetricsRepository.

## What This Example Shows

- Connecting to Term Nexus with API key authentication
- Running validation checks and storing metrics with tags
- Querying historical metrics by time range and tags
- Comparing metrics across validation runs for anomaly detection

## Prerequisites

- Term Nexus API running locally at `http://localhost:8080`
- Set the `TERM_API_KEY` environment variable

## Running the Example

```bash
# Start the Nexus API (if not already running)
# cd nexus && cargo run

# Run the example
cd docs/examples/nexus-repository
TERM_API_KEY=your-api-key cargo run
```

## Key Concepts Demonstrated

1. **NexusRepository**: Implements `MetricsRepository` for cloud persistence
2. **ResultKey**: Unique identifier with timestamp and tags for organization
3. **Tag-based Filtering**: Query metrics by environment, pipeline, or custom tags
4. **Historical Comparison**: Load previous metrics for trend analysis
