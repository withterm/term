# Performance Tuning Guide for Term Validation Library

This guide provides best practices and optimization techniques for achieving optimal performance with the Term validation library.

## Table of Contents
1. [Optimize Statistical Constraints](#optimize-statistical-constraints)
2. [Batch Related Validations](#batch-related-validations)
3. [Memory Optimization](#memory-optimization)
4. [DataFusion Tuning](#datafusion-tuning)
5. [Large Dataset Considerations](#large-dataset-considerations)
6. [Performance Monitoring](#performance-monitoring)

## Optimize Statistical Constraints

### Combine Related Statistics
When validating multiple statistics on the same column, always use `StatisticalOptions` to combine them into a single query:

```rust
let check = Check::builder("stats")
    .statistics(
        "response_time",
        StatisticalOptions::new()
            .min(Assertion::GreaterThanOrEqual(0.0))
            .max(Assertion::LessThan(5000.0))
            .mean(Assertion::Between(100.0, 500.0))
            .percentile(0.95, Assertion::LessThan(1000.0))
            .percentile(0.99, Assertion::LessThan(2000.0))
    )?
    .build();
```

This executes a single optimized query instead of multiple separate queries, resulting in **~27% performance improvement**.

### Use Appropriate Statistics
- **Median vs Mean**: Median is more robust to outliers
- **Percentiles**: Use for SLA validation (p95, p99)
- **Standard Deviation**: Good for detecting anomalies

## Batch Related Validations

Group related constraints in the same `Check` to enable optimization:

```rust
let suite = ValidationSuite::builder("user_data")
    .check(
        Check::builder("critical_fields")
            // These constraints can be optimized together
            .primary_key(vec!["user_id"])
            .completeness("email", CompletenessOptions::threshold(0.99).into_constraint_options())
            .email("email", 0.95)
            .build()
    )
    .build();
```

## Memory Optimization

### 1. Use Builder Pattern Efficiently
```rust
// ✅ GOOD: Direct chaining
let check = Check::builder("validation")
    .completeness("id", CompletenessOptions::full().into_constraint_options())
    .statistics("value", stats_options)?
    .build();

// ❌ AVOID: Unnecessary intermediate variables
let builder = Check::builder("validation");
let builder = builder.completeness("id", CompletenessOptions::full().into_constraint_options());
let builder = builder.statistics("value", stats_options)?;
let check = builder.build();
```

### 2. Reuse Validation Suites
For repeated validations, create the suite once:

```rust
// Create once
let suite = create_validation_suite();

// Reuse many times
for batch in data_batches {
    let ctx = create_context(batch);
    let result = suite.run(&ctx).await?;
}
```

## DataFusion Tuning

### 1. Configure Memory Pool
```rust
use datafusion::execution::memory_pool::{FairSpillPool, MemoryPool};

let config = SessionConfig::new()
    .with_batch_size(8192)  // Larger batches for better performance
    .with_target_partitions(num_cpus::get());

let runtime = RuntimeEnv::new(config)?
    .with_memory_pool(Arc::new(FairSpillPool::new(1_000_000_000))); // 1GB

let ctx = SessionContext::with_config_rt(config, Arc::new(runtime));
```

### 2. Optimize Table Registration
```rust
// For large datasets, use Parquet format
ctx.register_parquet("data", "path/to/data.parquet", ParquetReadOptions::default()).await?;

// For in-memory data, batch appropriately
let batches = create_batches_with_size(data, 10_000); // 10k rows per batch
```

## Large Dataset Considerations

### 1. Partition Your Data
For datasets > 1M rows, partition by a logical key:

```rust
// Partition by date for time-series data
let partitions = partition_by_date(data);
for (date, partition_data) in partitions {
    let ctx = create_context(partition_data);
    let result = suite.run(&ctx).await?;
}
```

### 2. Use Sampling for Approximate Validation
```rust
// For very large datasets, validate a sample
let sample_check = Check::builder("sample_validation")
    .custom_sql(
        "data_sample",
        "SELECT * FROM data TABLESAMPLE (10 PERCENT)",
        |df| async move {
            // Validate sampled data
            Ok(())
        }
    )
    .build();
```

### 3. Parallel Validation
```rust
use tokio::task::JoinSet;

let mut tasks = JoinSet::new();

for partition in data_partitions {
    let suite = suite.clone();
    tasks.spawn(async move {
        let ctx = create_context(partition);
        suite.run(&ctx).await
    });
}

// Collect results
while let Some(result) = tasks.join_next().await {
    let validation_result = result??;
    // Process result
}
```

## Performance Monitoring

### 1. Enable Tracing
```rust
use tracing_subscriber::prelude::*;

tracing_subscriber::registry()
    .with(tracing_subscriber::fmt::layer())
    .with(tracing_subscriber::EnvFilter::from_default_env())
    .init();

// Set RUST_LOG=term_guard=debug for detailed timing
```

### 2. Measure Constraint Performance
```rust
use std::time::Instant;

let start = Instant::now();
let result = suite.run(&ctx).await?;
let duration = start.elapsed();

println!("Validation took: {:?}", duration);

// Access individual constraint timings from OpenTelemetry spans
```

### 3. Profile SQL Queries
```rust
// Enable DataFusion query logging
std::env::set_var("DATAFUSION_EXPLAIN", "1");

// Analyze query plans
let df = ctx.sql("EXPLAIN SELECT MIN(col), MAX(col) FROM data").await?;
df.show().await?;
```

## Quick Performance Checklist

- [ ] Combining related statistics with `StatisticalOptions`
- [ ] Grouping related constraints in same `Check`
- [ ] Appropriate batch sizes (8192 for DataFusion)
- [ ] Memory pool configured for dataset size
- [ ] Partitioning for datasets > 1M rows
- [ ] Parallel validation for independent checks
- [ ] Tracing enabled for performance monitoring

## Common Performance Pitfalls

1. **Multiple statistics as separate constraints**: Use `StatisticalOptions` instead
2. **Not batching data**: Use appropriate batch sizes (8192 recommended)
3. **Validating entire dataset at once**: Partition large datasets
4. **Creating validation suite repeatedly**: Create once, reuse many times
5. **Not using convenience methods**: `primary_key()`, `email()` are optimized

## Expected Performance

Based on benchmarks:

| Dataset Size | Validation Type | Expected Time |
|-------------|----------------|---------------|
| 10K rows | 3 statistics | ~5.6ms |
| 10K rows | Complex (6 constraints) | ~10ms |
| 100K rows | 3 statistics | ~57ms |
| 1M rows | 3 statistics | ~570ms |

Performance scales linearly with data size and number of constraints.