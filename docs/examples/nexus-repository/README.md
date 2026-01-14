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

## Files

- `src/main.rs` - Nexus repository integration logic
- `data/items.csv` - Sample item inventory data
- `Cargo.toml` - Dependencies

## Running the Example

```bash
# Start the Nexus API (if not already running)
# cd nexus && cargo run

# Run the example
cd docs/examples/nexus-repository
TERM_API_KEY=your-api-key cargo run
```

## Example Output

```
=== Term Nexus Repository Example ===

Connecting to Term Nexus...
Connected to Term Nexus v0.1.0

Loading item data...
Loaded items table

Running validation checks...

Validation complete: 5/6 checks passed

[X] completeness: Completeness check on 'name' failed (0.90 < 1.00)
    metric: 0.90

--- Storing Metrics to Nexus ---
Result Key: 1736870400000
Tags: {"dataset": "items", "environment": "development", "pipeline": "daily-inventory"}
Metrics queued for upload
Metrics uploaded to Nexus

--- Querying Historical Metrics ---

Found 2 historical result(s):

  Timestamp: 1736870400000
  Tags: {"dataset": "items", "environment": "development", "pipeline": "daily-inventory"}
  Passed checks: Long(5)

  Timestamp: 1736784000000
  Tags: {"dataset": "items", "environment": "development", "pipeline": "daily-inventory"}
  Passed checks: Long(5)

--- Comparison with Previous Run ---

Previous run timestamp: 1736784000000
No change in passing checks

Worker stats: 5 uploaded, 0 failed

Example complete! Metrics are now stored in Term Nexus.
You can query them using the Nexus API or run this example again
to see historical comparison.
```

## Key Concepts Demonstrated

1. **NexusRepository**: Implements `MetricsRepository` for cloud persistence
2. **ResultKey**: Unique identifier with timestamp and tags for organization
3. **Tag-based Filtering**: Query metrics by environment, pipeline, or custom tags
4. **Historical Comparison**: Load previous metrics for trend analysis
