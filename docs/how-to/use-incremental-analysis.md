# How to Use Incremental Analysis

This guide shows you how to use Term's incremental analysis framework to efficiently process append-only and partitioned datasets without recomputing historical data.

## Prerequisites

- Term v0.2.0 or later
- Basic understanding of [analyzers](use-analyzers.md)
- Partitioned or time-series data

## When to Use Incremental Analysis

Use incremental analysis when you have:
- **Append-only data** that grows over time
- **Partitioned datasets** (by date, region, etc.)
- **Large historical data** that's expensive to reprocess
- **Regular updates** that need efficient computation

## Basic Setup

### 1. Create State Store

First, set up a state store to persist analyzer states between runs:

```rust
use term_guard::analyzers::incremental::{
    FilesystemStateStore,
    IncrementalAnalysisRunner,
};

// Use filesystem-based state persistence
let state_store = Box::new(
    FilesystemStateStore::new("./analyzer_state")
);
```

### 2. Configure Analyzers

Create analyzers that support incremental computation:

```rust
use term_guard::analyzers::{
    basic::{SizeAnalyzer, CompletenessAnalyzer, MeanAnalyzer},
    advanced::KllSketchAnalyzer,
};

let analyzers: Vec<Box<dyn TypeErasedAnalyzer>> = vec![
    Box::new(SizeAnalyzer::new().into_type_erased()),
    Box::new(CompletenessAnalyzer::new("user_id").into_type_erased()),
    Box::new(MeanAnalyzer::new("revenue").into_type_erased()),
    Box::new(KllSketchAnalyzer::new("response_time").into_type_erased()),
];
```

### 3. Create Runner

Initialize the incremental analysis runner:

```rust
let runner = IncrementalAnalysisRunner::new(analyzers, state_store)
    .with_batch_size(100); // Process 100 partitions at a time
```

## Processing Partitioned Data

### Time-Based Partitions

For daily partitioned data:

```rust
use datafusion::prelude::*;

async fn analyze_daily_data(
    runner: &IncrementalAnalysisRunner,
    base_path: &str,
) -> Result<HashMap<String, MetricValue>> {
    // Create context with partitioned data
    let ctx = SessionContext::new();
    
    // Register each partition
    let partitions = vec![
        "2024-01-01".to_string(),
        "2024-01-02".to_string(),
        "2024-01-03".to_string(),
    ];
    
    for partition in &partitions {
        let path = format!("{}/date={}/data.parquet", base_path, partition);
        ctx.register_parquet("data", &path, ParquetReadOptions::default())
            .await?;
    }
    
    // Run incremental analysis
    runner.run_incremental(&ctx, partitions).await
}
```

### Adding New Partitions

When new data arrives, only process the new partitions:

```rust
async fn process_new_day(
    runner: &IncrementalAnalysisRunner,
    all_partitions: Vec<String>,
) -> Result<HashMap<String, MetricValue>> {
    // State store automatically loads previous states
    // Only new partitions are computed
    runner.run_incremental(&ctx, all_partitions).await
}

// Day 1: Process January data
let jan_metrics = process_new_day(&runner, vec![
    "2024-01-01".to_string(),
    "2024-01-02".to_string(),
]).await?;

// Day 2: Add February - only February is computed
let jan_feb_metrics = process_new_day(&runner, vec![
    "2024-01-01".to_string(),
    "2024-01-02".to_string(),
    "2024-02-01".to_string(), // New partition
]).await?;
```

## Handling Different Partition Schemes

### By Region

```rust
let partitions = vec![
    "region=us-west".to_string(),
    "region=us-east".to_string(),
    "region=eu-central".to_string(),
];

let metrics = runner.run_incremental(&ctx, partitions).await?;
```

### By Customer

```rust
let partitions = customer_ids.iter()
    .map(|id| format!("customer_id={}", id))
    .collect();

let metrics = runner.run_incremental(&ctx, partitions).await?;
```

### Multi-Level Partitions

```rust
let partitions = vec![
    "year=2024/month=01/day=01".to_string(),
    "year=2024/month=01/day=02".to_string(),
];

let metrics = runner.run_incremental(&ctx, partitions).await?;
```

## Managing State

### Inspecting Stored State

```rust
let state_store = FilesystemStateStore::new("./analyzer_state");

// List all partitions for an analyzer
let partitions = state_store
    .list_partitions("completeness_analyzer")
    .await?;

println!("Stored partitions: {:?}", partitions);
```

### Cleaning Old State

```rust
async fn cleanup_old_partitions(
    state_store: &FilesystemStateStore,
    analyzer_name: &str,
    days_to_keep: i64,
) -> Result<()> {
    let cutoff = Utc::now() - Duration::days(days_to_keep);
    let partitions = state_store.list_partitions(analyzer_name).await?;
    
    for partition in partitions {
        // Parse partition date
        if let Ok(date) = partition.parse::<NaiveDate>() {
            if date < cutoff.date_naive() {
                state_store.delete_state(analyzer_name, &partition).await?;
            }
        }
    }
    Ok(())
}
```

### Resetting State

To recompute from scratch:

```rust
// Delete all state for specific analyzer
for partition in state_store.list_partitions("mean_analyzer").await? {
    state_store.delete_state("mean_analyzer", &partition).await?;
}

// Or delete entire state directory
std::fs::remove_dir_all("./analyzer_state")?;
```

## Advanced Configuration

### Custom Batch Size

Optimize for memory and performance:

```rust
let runner = IncrementalAnalysisRunner::new(analyzers, state_store)
    .with_batch_size(match total_partitions {
        n if n < 100 => n,        // Small dataset: process all at once
        n if n < 1000 => 100,     // Medium: batches of 100
        _ => 500,                  // Large: bigger batches
    });
```

### Parallel Processing

Process multiple analyzers concurrently:

```rust
use futures::future::join_all;

async fn parallel_incremental_analysis(
    analyzers: Vec<Vec<Box<dyn TypeErasedAnalyzer>>>,
    ctx: &SessionContext,
    partitions: Vec<String>,
) -> Result<Vec<HashMap<String, MetricValue>>> {
    let tasks = analyzers.into_iter().map(|analyzer_group| {
        let ctx = ctx.clone();
        let partitions = partitions.clone();
        
        async move {
            let runner = IncrementalAnalysisRunner::new(
                analyzer_group,
                Box::new(FilesystemStateStore::new("./state")),
            );
            runner.run_incremental(&ctx, partitions).await
        }
    });
    
    join_all(tasks).await.into_iter().collect()
}
```

## Error Recovery

### Handling Partial Failures

```rust
async fn robust_incremental_analysis(
    runner: &IncrementalAnalysisRunner,
    ctx: &SessionContext,
    partitions: Vec<String>,
) -> Result<HashMap<String, MetricValue>> {
    let mut successful_partitions = vec![];
    let mut failed_partitions = vec![];
    
    for partition in partitions {
        match runner.run_on_partition(ctx, &partition).await {
            Ok(state) => successful_partitions.push((partition, state)),
            Err(e) => {
                eprintln!("Failed to process partition {}: {}", partition, e);
                failed_partitions.push(partition);
            }
        }
    }
    
    if !failed_partitions.is_empty() {
        eprintln!("Warning: {} partitions failed", failed_partitions.len());
    }
    
    // Compute metrics from successful partitions
    let states = successful_partitions.into_iter()
        .map(|(_, state)| state)
        .collect();
    
    runner.compute_metrics_from_states(states).await
}
```

### State Corruption Recovery

```rust
async fn recover_from_corruption(
    runner: &IncrementalAnalysisRunner,
    ctx: &SessionContext,
    partition: &str,
) -> Result<()> {
    // Delete corrupted state
    runner.state_store.delete_state("*", partition).await?;
    
    // Recompute for this partition
    runner.run_on_partition(ctx, partition).await?;
    
    Ok(())
}
```

## Monitoring and Debugging

### Tracking Progress

```rust
use indicatif::{ProgressBar, ProgressStyle};

async fn analyze_with_progress(
    runner: &IncrementalAnalysisRunner,
    ctx: &SessionContext,
    partitions: Vec<String>,
) -> Result<HashMap<String, MetricValue>> {
    let pb = ProgressBar::new(partitions.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40} {pos}/{len} {msg}")
            .unwrap()
    );
    
    let mut results = HashMap::new();
    
    for partition in partitions {
        pb.set_message(format!("Processing {}", partition));
        let state = runner.run_on_partition(ctx, &partition).await?;
        results.insert(partition, state);
        pb.inc(1);
    }
    
    pb.finish_with_message("Analysis complete");
    
    runner.compute_metrics_from_states(
        results.into_values().collect()
    ).await
}
```

### Logging State Operations

```rust
use tracing::{info, debug};

#[derive(Debug)]
struct LoggingStateStore {
    inner: FilesystemStateStore,
}

#[async_trait]
impl StateStore for LoggingStateStore {
    async fn save_state(
        &self,
        analyzer: &str,
        partition: &str,
        state: &[u8],
    ) -> AnalyzerResult<()> {
        info!(
            analyzer = analyzer,
            partition = partition,
            size = state.len(),
            "Saving state"
        );
        self.inner.save_state(analyzer, partition, state).await
    }
    
    async fn load_state(
        &self,
        analyzer: &str,
        partition: &str,
    ) -> AnalyzerResult<Option<Vec<u8>>> {
        debug!(
            analyzer = analyzer,
            partition = partition,
            "Loading state"
        );
        self.inner.load_state(analyzer, partition).await
    }
}
```

## Performance Tips

1. **Batch Size**: Start with 100 partitions per batch and adjust based on memory
2. **State Cleanup**: Implement retention policies to prevent unbounded state growth
3. **Compression**: Consider compressing state files for large datasets
4. **Caching**: Use in-memory caching for frequently accessed partitions
5. **Parallel Loading**: Load multiple partition states concurrently

## Troubleshooting

### State Not Persisting

Check file permissions:
```bash
ls -la ./analyzer_state/
chmod -R 755 ./analyzer_state/
```

### Out of Memory

Reduce batch size:
```rust
.with_batch_size(10) // Process fewer partitions at once
```

### Slow State Loading

Enable state compression:
```rust
use flate2::{Compression, write::GzEncoder};

let compressed = GzEncoder::new(Vec::new(), Compression::default());
```

## Next Steps

- Learn about [advanced analyzers](use-advanced-analyzers.md)
- Explore [metrics repository](../reference/metrics-repository.md) for storing results
- See [performance optimization](optimize-performance.md) guide