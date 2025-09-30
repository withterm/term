# How to Use KLL Sketches for Large-Scale Quantile Estimation

## Problem
You need to compute quantiles (percentiles, median, etc.) on datasets too large to fit in memory, or you need to continuously update quantile estimates as new data arrives.

## Solution
Use Term's KLL sketch implementation to maintain a compact summary that provides approximate quantiles with guaranteed error bounds.

## Basic Usage

### Computing Quantiles on Large Datasets

```rust
use term_guard::analyzers::advanced::KLLSketchAnalyzer;
use term_guard::prelude::*;

async fn analyze_large_dataset(ctx: &SessionContext) -> Result<()> {
    // Configure KLL sketch for your dataset
    let analyzer = KLLSketchAnalyzer::builder()
        .column("transaction_amount")
        .sketch_size(200)  // Default: good for most use cases
        .build();
    
    // Run analysis - processes data in streaming fashion
    let result = analyzer.analyze(ctx).await?;
    
    // Extract quantiles
    if let MetricValue::KLLSketch(sketch) = result.metric_value() {
        let p50 = sketch.quantile(0.50)?;  // Median
        let p95 = sketch.quantile(0.95)?;  // 95th percentile
        let p99 = sketch.quantile(0.99)?;  // 99th percentile
        
        println!("P50: {:.2}, P95: {:.2}, P99: {:.2}", p50, p95, p99);
    }
    
    Ok(())
}
```

### Configuring Accuracy vs Memory Trade-offs

```rust
// Higher accuracy (more memory)
let high_accuracy = KLLSketchAnalyzer::builder()
    .column("response_time")
    .sketch_size(400)  // ~0.8% error
    .shrinking_factor(0.9)  // Less aggressive compaction
    .build();

// Lower memory usage (less accuracy)
let low_memory = KLLSketchAnalyzer::builder()
    .column("response_time")
    .sketch_size(100)  // ~3.3% error
    .shrinking_factor(0.75)  // More aggressive compaction
    .build();
```

## Advanced Techniques

### Merging Sketches from Multiple Partitions

```rust
use term_guard::analyzers::incremental::{IncrementalAnalysisRunner, FileSystemStateStore};

async fn analyze_partitioned_data(ctx: &SessionContext) -> Result<()> {
    // Set up incremental analysis with state persistence
    let state_store = FileSystemStateStore::new("./sketch_states")?;
    let mut runner = IncrementalAnalysisRunner::new(Box::new(state_store))
        .add_analyzer(Box::new(
            KLLSketchAnalyzer::builder()
                .column("metric_value")
                .build()
        ));
    
    // Process each partition
    for partition in ["2024-01-01", "2024-01-02", "2024-01-03"] {
        // Register partition data
        ctx.register_parquet(
            "metrics",
            &format!("s3://bucket/data/partition={}/", partition),
            ParquetReadOptions::default()
        ).await?;
        
        // Analyze and merge with previous results
        let results = runner.analyze_partition(ctx, partition).await?;
        
        // Get cumulative sketch
        if let Some(sketch) = results.get_kll_sketch("metric_value") {
            println!("Cumulative P50 including {}: {:.2}", 
                     partition, sketch.quantile(0.5)?);
        }
    }
    
    Ok(())
}
```

### Creating Validation Checks with Quantile Thresholds

```rust
async fn setup_sla_monitoring(ctx: &SessionContext) -> Result<()> {
    // First, establish baseline quantiles
    let baseline_analyzer = KLLSketchAnalyzer::builder()
        .column("api_latency_ms")
        .build();
    
    let baseline = baseline_analyzer.analyze(ctx).await?;
    let baseline_p99 = baseline.get_quantile(0.99)?;
    
    // Create validation suite with quantile-based checks
    let monitoring_suite = ValidationSuite::builder("sla_monitoring")
        .add_check(
            Check::builder("latency_p50_check")
                .description("Median latency should be under 100ms")
                .has_approx_quantile(
                    "api_latency_ms",
                    0.50,
                    Assertion::LessThan(100.0)
                )
                .build()
        )
        .add_check(
            Check::builder("latency_p99_check")
                .description("P99 latency should not exceed baseline by >20%")
                .has_approx_quantile(
                    "api_latency_ms",
                    0.99,
                    Assertion::LessThan(baseline_p99 * 1.2)
                )
                .build()
        )
        .build();
    
    // Run validation
    let results = monitoring_suite.run(ctx).await?;
    for check_result in results.check_results() {
        println!("{}: {}", check_result.check.name(), check_result.status);
    }
    
    Ok(())
}
```

### Streaming Updates with KLL Sketches

```rust
use tokio::time::{interval, Duration};

async fn continuous_monitoring(ctx: &SessionContext) -> Result<()> {
    let mut sketch_state: Option<KLLSketch> = None;
    let mut interval = interval(Duration::from_secs(60));
    
    loop {
        interval.tick().await;
        
        // Get new data batch
        ctx.register_csv(
            "latest_metrics",
            "data/latest_batch.csv",
            CsvReadOptions::default()
        ).await?;
        
        // Create analyzer
        let analyzer = KLLSketchAnalyzer::builder()
            .column("value")
            .existing_sketch(sketch_state.clone())  // Continue from previous state
            .build();
        
        // Update sketch
        let result = analyzer.analyze(ctx).await?;
        if let MetricValue::KLLSketch(new_sketch) = result.metric_value() {
            // Report current quantiles
            println!("Current P50: {:.2}", new_sketch.quantile(0.5)?);
            println!("Current P95: {:.2}", new_sketch.quantile(0.95)?);
            
            // Save state for next iteration
            sketch_state = Some(new_sketch.clone());
        }
    }
}
```

## Performance Optimization

### Batch Processing Multiple Columns

```rust
use term_guard::analyzers::runner::AnalysisRunner;

async fn analyze_multiple_metrics(ctx: &SessionContext) -> Result<()> {
    // Create analyzers for multiple columns
    let analyzers = vec![
        Box::new(KLLSketchAnalyzer::builder()
            .column("cpu_usage")
            .build()) as Box<dyn Analyzer>,
        Box::new(KLLSketchAnalyzer::builder()
            .column("memory_usage")
            .build()) as Box<dyn Analyzer>,
        Box::new(KLLSketchAnalyzer::builder()
            .column("disk_io")
            .build()) as Box<dyn Analyzer>,
    ];
    
    // Run all analyzers in a single scan
    let runner = AnalysisRunner::new()
        .with_analyzers(analyzers);
    
    let results = runner.run(ctx).await?;
    
    // Process results
    for (column, sketch) in results.kll_sketches() {
        println!("{} P95: {:.2}", column, sketch.quantile(0.95)?);
    }
    
    Ok(())
}
```

### Memory-Efficient Configuration

```rust
fn configure_for_memory_constraints(memory_budget_mb: usize) -> KLLSketchAnalyzer {
    // Estimate sketch size based on memory budget
    // Each sketch level uses ~8 bytes per item
    let items_per_mb = 1024 * 1024 / 8;
    let sketch_size = (memory_budget_mb * items_per_mb / 10) as u32;
    
    KLLSketchAnalyzer::builder()
        .column("large_column")
        .sketch_size(sketch_size.min(1000))  // Cap at reasonable maximum
        .shrinking_factor(0.7)  // Aggressive compaction for memory efficiency
        .build()
}
```

## Common Pitfalls and Solutions

### Handling Skewed Distributions

```rust
// For highly skewed data, use larger sketch size
let skewed_data_analyzer = KLLSketchAnalyzer::builder()
    .column("power_law_distribution")
    .sketch_size(400)  // Larger size for better accuracy on tails
    .build();
```

### Dealing with Null Values

```rust
// Combine with completeness check
let suite = ValidationSuite::builder("quantile_with_nulls")
    .add_check(
        Check::builder("completeness")
            .has_completeness("amount", 0.95)  // Ensure <5% nulls
            .build()
    )
    .add_check(
        Check::builder("quantiles")
            .has_approx_quantile("amount", 0.5, Assertion::Between(100.0, 1000.0))
            .build()
    )
    .build();
```

## Testing and Validation

### Comparing Approximate vs Exact Quantiles

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_sketch_accuracy() {
        let ctx = create_test_context().await.unwrap();
        
        // Compute exact quantile on sample
        let exact = compute_exact_quantile(&ctx, "value", 0.95).await.unwrap();
        
        // Compute approximate with KLL
        let analyzer = KLLSketchAnalyzer::builder()
            .column("value")
            .sketch_size(200)
            .build();
        
        let result = analyzer.analyze(&ctx).await.unwrap();
        let approx = result.get_quantile(0.95).unwrap();
        
        // Verify accuracy within expected bounds
        let error = (approx - exact).abs() / exact;
        assert!(error < 0.02, "Error {} exceeds 2% threshold", error);
    }
}
```

## Troubleshooting

**Problem**: Sketch using too much memory
- **Solution**: Reduce sketch_size or increase shrinking_factor

**Problem**: Quantile estimates not accurate enough
- **Solution**: Increase sketch_size (double it for ~half the error)

**Problem**: Merge operations are slow
- **Solution**: Use incremental analysis runner with state persistence

**Problem**: Different results on same data
- **Solution**: Ensure deterministic data ordering or use seeded random sampling

## Related Documentation
- [Tutorial: KLL Sketches](../tutorials/07-kll-sketches-quantiles.md)
- [Reference: KLLSketchAnalyzer API](../reference/analyzers/kll-sketch.md)
- [Explanation: KLL Algorithm](../explanation/kll-algorithm.md)