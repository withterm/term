# How to Analyze Large Datasets Efficiently

<!-- 
This is a HOW-TO GUIDE following Diátaxis principles.
It provides practical steps to accomplish specific tasks.
-->

## Overview

When analyzing large datasets, you need to optimize for memory usage and performance. This guide shows you how to use Term's analyzers efficiently for datasets that don't fit in memory.

**Use this guide when you need to:**
- Analyze datasets larger than available RAM
- Optimize analysis performance
- Handle analysis failures gracefully
- Monitor long-running analyses

## Prerequisites

- Term configured in your project
- Basic understanding of analyzers (see [Understanding Analyzers tutorial](../tutorials/03-understanding-analyzers.md))
- Access to a large dataset (CSV, Parquet, or database)

## Analyzing Partitioned Data

### Problem
Your dataset is split across multiple files or partitions.

### Solution
Use analyzer state merging to combine results from partitions:

```rust
use term_guard::analyzers::{basic::*, AnalyzerState};
use datafusion::prelude::*;

async fn analyze_partitioned_data(partition_paths: Vec<&str>) -> Result<()> {
    let analyzer = SizeAnalyzer::new();
    let mut states = Vec::new();
    
    // Process each partition
    for path in partition_paths {
        let ctx = SessionContext::new();
        let df = ctx.read_parquet(path, ParquetReadOptions::default()).await?;
        ctx.register_table("data", df)?;
        
        // Compute state for this partition
        let state = analyzer.compute_state_from_data(&ctx).await?;
        states.push(state);
    }
    
    // Merge all states
    let final_state = SizeState::merge(states)?;
    let total_size = analyzer.compute_metric_from_state(&final_state)?;
    
    println!("Total size across all partitions: {:?}", total_size);
    Ok(())
}
```

## Optimizing Memory Usage

### Problem
DataFusion runs out of memory during analysis.

### Solution
Configure memory limits and use streaming execution:

```rust
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::execution::memory_pool::FairSpillPool;

async fn create_memory_limited_context(memory_limit: usize) -> SessionContext {
    let runtime_config = RuntimeConfig::new()
        .with_memory_pool(Arc::new(FairSpillPool::new(memory_limit)));
    
    let runtime = Arc::new(RuntimeEnv::new(runtime_config).unwrap());
    let session_config = SessionConfig::new()
        .with_batch_size(8192); // Smaller batches = less memory
    
    SessionContext::new_with_config_rt(session_config, runtime)
}

// Use the memory-limited context
let ctx = create_memory_limited_context(2_000_000_000).await; // 2GB limit
```

## Implementing Progress Monitoring

### Problem
Long-running analyses provide no feedback.

### Solution
Use progress callbacks and logging:

```rust
use std::time::Instant;
use tracing::{info, warn};

async fn analyze_with_monitoring(ctx: &SessionContext) -> Result<AnalyzerContext> {
    let start = Instant::now();
    let total_analyzers = 10;
    
    let runner = AnalysisRunner::new()
        .add(SizeAnalyzer::new())
        .add(CompletenessAnalyzer::new("customer_id"))
        .add(DistinctnessAnalyzer::new("customer_id"))
        .add(MeanAnalyzer::new("amount"))
        .add(StandardDeviationAnalyzer::new("amount"))
        // ... more analyzers ...
        .on_progress(move |progress| {
            let elapsed = start.elapsed();
            let percent = (progress * 100.0) as u32;
            let remaining = if progress > 0.0 {
                let total_estimate = elapsed.as_secs_f64() / progress;
                let remaining_secs = total_estimate - elapsed.as_secs_f64();
                format!("{:.0}s remaining", remaining_secs)
            } else {
                "calculating...".to_string()
            };
            
            info!(
                percent,
                elapsed_secs = elapsed.as_secs(),
                remaining,
                "Analysis progress"
            );
        })
        .continue_on_error(true); // Don't fail if one analyzer fails
    
    let context = runner.run(ctx).await?;
    
    // Log summary
    info!(
        total_metrics = context.all_metrics().len(),
        errors = context.errors().len(),
        duration_secs = start.elapsed().as_secs(),
        "Analysis complete"
    );
    
    // Log any errors
    for error in context.errors() {
        warn!(
            analyzer = error.analyzer_name,
            error = %error.error,
            "Analyzer failed"
        );
    }
    
    Ok(context)
}
```

## Handling Analysis Failures

### Problem
Some analyzers fail on specific data types or malformed data.

### Solution
Use error recovery and type-specific analyzers:

```rust
async fn robust_analysis(ctx: &SessionContext) -> Result<()> {
    // First, probe the data types
    let schema_df = ctx.sql("SELECT * FROM data LIMIT 1").await?;
    let schema = schema_df.schema();
    
    let mut runner = AnalysisRunner::new()
        .add(SizeAnalyzer::new())
        .continue_on_error(true); // Continue even if some analyzers fail
    
    // Add analyzers based on column types
    for field in schema.fields() {
        match field.data_type() {
            DataType::Int64 | DataType::Float64 => {
                runner = runner
                    .add(CompletenessAnalyzer::new(field.name()))
                    .add(MeanAnalyzer::new(field.name()))
                    .add(MinAnalyzer::new(field.name()))
                    .add(MaxAnalyzer::new(field.name()));
            }
            DataType::Utf8 => {
                runner = runner
                    .add(CompletenessAnalyzer::new(field.name()))
                    .add(DistinctnessAnalyzer::new(field.name()));
            }
            _ => {
                // Skip unsupported types
                info!(column = field.name(), dtype = ?field.data_type(), "Skipping column");
            }
        }
    }
    
    let context = runner.run(ctx).await?;
    
    // Check what succeeded and what failed
    println!("Successful metrics: {}", context.all_metrics().len());
    println!("Failed analyzers: {}", context.errors().len());
    
    Ok(())
}
```

## Analyzing Streaming Data

### Problem
Data arrives continuously and you need up-to-date metrics.

### Solution
Use incremental state updates:

```rust
use tokio::time::{interval, Duration};

async fn streaming_analysis(
    data_source: impl Stream<Item = RecordBatch>
) -> Result<()> {
    let analyzer = CompletenessAnalyzer::new("user_id");
    let mut current_state: Option<CompletenessState> = None;
    let mut batch_count = 0;
    
    // Process batches as they arrive
    pin_mut!(data_source);
    while let Some(batch) = data_source.next().await {
        batch_count += 1;
        
        // Create temporary context for this batch
        let ctx = SessionContext::new();
        ctx.register_batch("data", batch)?;
        
        // Compute state for new batch
        let batch_state = analyzer.compute_state_from_data(&ctx).await?;
        
        // Merge with existing state
        current_state = match current_state {
            Some(existing) => Some(CompletenessState::merge(vec![existing, batch_state])?),
            None => Some(batch_state),
        };
        
        // Compute current metric
        if let Some(ref state) = current_state {
            let metric = analyzer.compute_metric_from_state(state)?;
            info!(
                batch_count,
                completeness = ?metric,
                "Updated completeness"
            );
        }
    }
    
    Ok(())
}
```

## Performance Tips

### 1. Choose the Right Analyzer

```rust
// For exact counts on high-cardinality columns
let exact = DistinctnessAnalyzer::new("user_id"); // Slow, exact

// For approximate counts (much faster)
let approx = ApproxCountDistinctAnalyzer::new("user_id"); // Fast, ~2% error
```

### 2. Batch Column Analysis

```rust
// Instead of multiple passes:
// DON'T do this - makes multiple scans
let r1 = AnalysisRunner::new().add(MeanAnalyzer::new("col1")).run(&ctx).await?;
let r2 = AnalysisRunner::new().add(MeanAnalyzer::new("col2")).run(&ctx).await?;

// DO this - single scan
let runner = AnalysisRunner::new()
    .add(MeanAnalyzer::new("col1"))
    .add(MeanAnalyzer::new("col2"));
let results = runner.run(&ctx).await?;
```

### 3. Use Appropriate Data Formats

```rust
// Parquet is much faster than CSV for analysis
let df = ctx.read_parquet("data.parquet", ParquetReadOptions::default()).await?;

// For CSV, configure optimal settings
let csv_options = CsvReadOptions::new()
    .has_header(true)
    .delimiter(b',')
    .file_compression_type(FileCompressionType::GZIP);
```

## Troubleshooting

### Out of Memory Errors

**Symptom**: `Resources exhausted: Failed to allocate additional`

**Solution**:
1. Reduce batch size
2. Enable disk spilling
3. Process partitions separately

### Slow Analysis

**Symptom**: Analysis takes hours on moderate datasets

**Solution**:
1. Check for missing statistics on source files
2. Use columnar formats (Parquet)
3. Enable parallelism in DataFusion

### Type Conversion Errors

**Symptom**: `Invalid data: Expected Float64 array for min`

**Solution**:
1. Check column data types match analyzer expectations
2. Use CAST in SQL views if needed
3. Implement custom type handling

## Complete Example

Here's a complete example analyzing a large dataset efficiently:

```rust
use term_guard::prelude::*;
use term_guard::analyzers::{AnalysisRunner, basic::*, advanced::*};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::execution::memory_pool::FairSpillPool;
use tracing::{info, error};

async fn analyze_large_dataset(file_path: &str) -> Result<()> {
    // Configure memory-limited context
    let runtime_config = RuntimeConfig::new()
        .with_memory_pool(Arc::new(FairSpillPool::new(4_000_000_000))); // 4GB
    
    let runtime = Arc::new(RuntimeEnv::new(runtime_config)?);
    let session_config = SessionConfig::new()
        .with_batch_size(8192)
        .with_target_partitions(num_cpus::get());
    
    let ctx = SessionContext::new_with_config_rt(session_config, runtime);
    
    // Read data
    info!(path = file_path, "Loading dataset");
    let df = ctx.read_parquet(file_path, ParquetReadOptions::default()).await?;
    ctx.register_table("data", df)?;
    
    // Build comprehensive analysis
    let runner = AnalysisRunner::new()
        // Basic metrics
        .add(SizeAnalyzer::new())
        // Completeness for key columns
        .add(CompletenessAnalyzer::new("customer_id"))
        .add(CompletenessAnalyzer::new("transaction_date"))
        .add(CompletenessAnalyzer::new("amount"))
        // Statistical analysis
        .add(MeanAnalyzer::new("amount"))
        .add(StandardDeviationAnalyzer::new("amount"))
        .add(MinAnalyzer::new("amount"))
        .add(MaxAnalyzer::new("amount"))
        // Cardinality analysis
        .add(ApproxCountDistinctAnalyzer::new("customer_id"))
        .add(DistinctnessAnalyzer::new("transaction_type"))
        // Advanced analysis
        .add(HistogramAnalyzer::new("amount", 20))
        .add(DataTypeAnalyzer::new("transaction_date"))
        // Configuration
        .continue_on_error(true)
        .on_progress(|p| info!(progress = format!("{:.1}%", p * 100.0)));
    
    // Run analysis
    info!("Starting analysis");
    let start = std::time::Instant::now();
    let context = runner.run(&ctx).await?;
    let duration = start.elapsed();
    
    // Report results
    info!(
        duration_secs = duration.as_secs(),
        metrics = context.all_metrics().len(),
        errors = context.errors().len(),
        "Analysis complete"
    );
    
    // Save results
    let results = serde_json::to_string_pretty(&context)?;
    std::fs::write("analysis_results.json", results)?;
    
    Ok(())
}
```

## See Also

- [Analyzer Architecture](../explanation/analyzer-architecture.md) - Understanding the two-phase computation model
- [API Reference: AnalysisRunner](../reference/analysis-runner.md) - Complete API documentation
- [Performance Optimization Guide](optimize-performance.md) - General optimization techniques