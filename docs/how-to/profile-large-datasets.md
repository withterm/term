# How to Profile Large Datasets Efficiently

This guide shows you how to profile large datasets without running out of memory or time.

## Problem

You need to profile a dataset with millions of rows and hundreds of columns, but:
- Full scans take too long
- Memory usage grows beyond available resources
- You need results quickly for exploratory analysis

## Solution

Use Term's adaptive profiling strategies and memory management features.

### Configure Memory-Aware Profiling

```rust
use term_guard::analyzers::{ColumnProfiler, MemoryPoolConfig, MemoryPoolType};

let profiler = ColumnProfiler::builder()
    // Limit sample size for initial type detection
    .sample_size(10_000)
    
    // Configure memory pool
    .memory_pool_config(MemoryPoolConfig {
        size: Some(2 * 1024 * 1024 * 1024), // 2GB limit
        pool_type: MemoryPoolType::Fair,     // Fair allocation
        max_partitions: Some(16),            // Limit parallelism
        enable_memory_manager: true,         // Enable monitoring
    })
    
    // Use higher cardinality threshold
    .cardinality_threshold(10_000)
    
    // Enable parallel processing
    .enable_parallel(true)
    .build();
```

### Use Sampling Strategies

```rust
// For initial exploration, use aggressive sampling
let quick_profile = profiler
    .profile_column_with_options(&ctx, "large_table", "column_name")
    .sample_ratio(0.01)  // Sample 1% of data
    .timeout_seconds(30) // Abort if takes too long
    .await?;

// For production validation, use adaptive sampling
let production_profile = profiler
    .profile_column_with_options(&ctx, "large_table", "column_name")
    .adaptive_sampling(true)    // Automatically adjust sample size
    .min_sample_size(1000)      // At least 1k rows
    .max_sample_size(1_000_000) // At most 1M rows
    .await?;
```

### Profile in Batches

```rust
use futures::stream::{self, StreamExt};

// Get column list
let columns = get_table_columns(&ctx, "large_table").await?;

// Process in batches to control memory
let batch_size = 10;
let mut all_profiles = Vec::new();

for chunk in columns.chunks(batch_size) {
    let batch_profiles: Vec<_> = stream::iter(chunk)
        .map(|col| {
            let ctx = ctx.clone();
            let profiler = profiler.clone();
            async move {
                profiler.profile_column(&ctx, "large_table", col).await
            }
        })
        .buffer_unordered(5) // Process 5 columns concurrently
        .collect()
        .await;
    
    all_profiles.extend(batch_profiles.into_iter().filter_map(Result::ok));
    
    // Optional: Clear memory between batches
    ctx.clear_cache().await?;
}
```

### Use Progress Callbacks

```rust
use std::sync::{Arc, Mutex};
use std::time::Instant;

let progress = Arc::new(Mutex::new(0.0));
let start = Instant::now();

let profiler = ColumnProfiler::builder()
    .with_progress_callback({
        let progress = progress.clone();
        move |p| {
            let mut prog = progress.lock().unwrap();
            *prog = p;
            println!("Progress: {:.1}% | Elapsed: {:?}", 
                     p * 100.0, start.elapsed());
        }
    })
    .build();
```

### Optimize for Specific Data Types

```rust
// For numeric columns, skip expensive string analysis
let numeric_profiler = ColumnProfiler::builder()
    .skip_pattern_analysis(true)
    .skip_categorical_analysis(true)
    .enable_numeric_histogram(true)
    .build();

// For categorical columns, limit unique value tracking
let categorical_profiler = ColumnProfiler::builder()
    .max_categorical_values(100)  // Track top 100 only
    .skip_numeric_analysis(true)
    .build();

// For text columns, use sampling for pattern detection
let text_profiler = ColumnProfiler::builder()
    .pattern_sample_size(1000)    // Sample for patterns
    .skip_numeric_analysis(true)
    .build();
```

### Handle Partitioned Data

```rust
// Profile partitioned tables efficiently
async fn profile_partitioned_table(
    ctx: &SessionContext,
    table: &str,
    partition_column: &str,
) -> Result<Vec<ColumnProfile>> {
    let partitions = get_partitions(ctx, table, partition_column).await?;
    let profiler = ColumnProfiler::new();
    
    // Profile one partition as representative sample
    let sample_partition = &partitions[0];
    let filter = format!("{} = '{}'", partition_column, sample_partition);
    
    let sample_profiles = profiler
        .profile_table_with_filter(ctx, table, &filter)
        .await?;
    
    // Extrapolate statistics
    let total_partitions = partitions.len() as f64;
    sample_profiles.into_iter().map(|mut profile| {
        profile.basic_stats.row_count *= total_partitions as u64;
        profile.basic_stats.null_count *= total_partitions as u64;
        profile
    }).collect()
}
```

### Save and Resume Profiling

```rust
use serde_json;
use std::fs;

// Save intermediate results
let checkpoint_dir = "profile_checkpoints";
fs::create_dir_all(checkpoint_dir)?;

for (idx, profile) in all_profiles.iter().enumerate() {
    let path = format!("{}/profile_{}.json", checkpoint_dir, idx);
    let json = serde_json::to_string_pretty(profile)?;
    fs::write(path, json)?;
}

// Resume from checkpoint
let saved_profiles: Vec<ColumnProfile> = fs::read_dir(checkpoint_dir)?
    .filter_map(|entry| entry.ok())
    .filter_map(|entry| {
        fs::read_to_string(entry.path()).ok()
            .and_then(|content| serde_json::from_str(&content).ok())
    })
    .collect();
```

## Troubleshooting

### Out of Memory Errors

1. Reduce `sample_size` and `max_memory_bytes`
2. Use `MemoryPoolType::Fair` instead of `Greedy`
3. Process fewer columns in parallel
4. Enable disk spilling in DataFusion

### Slow Performance

1. Check if statistics are available in metadata
2. Use column pruning to skip unnecessary data
3. Ensure proper indexing on filter columns
4. Consider pre-aggregating data

### Inaccurate Results

1. Increase sample size for better accuracy
2. Use stratified sampling for skewed data
3. Profile multiple partitions and aggregate
4. Validate samples are representative

## Example: Complete Large Dataset Profile

```rust
use term_guard::analyzers::*;
use std::time::Instant;

async fn profile_large_dataset(
    ctx: &SessionContext,
    table: &str,
) -> Result<DatasetProfile> {
    let start = Instant::now();
    
    // Configure for large dataset
    let profiler = ColumnProfiler::builder()
        .sample_size(100_000)
        .cardinality_threshold(10_000)
        .memory_pool_config(MemoryPoolConfig {
            size: Some(4 * 1024 * 1024 * 1024), // 4GB
            pool_type: MemoryPoolType::Fair,
            max_partitions: Some(32),
            enable_memory_manager: true,
        })
        .enable_parallel(true)
        .with_progress_callback(|p| {
            println!("Profiling: {:.1}%", p * 100.0);
        })
        .build();
    
    // Get table metadata
    let row_count = get_table_row_count(ctx, table).await?;
    println!("Table has {} rows", row_count);
    
    // Determine sampling strategy
    let sample_ratio = if row_count > 10_000_000 {
        0.01  // 1% for very large tables
    } else if row_count > 1_000_000 {
        0.1   // 10% for large tables
    } else {
        1.0   // Full scan for smaller tables
    };
    
    // Profile with appropriate strategy
    let profiles = profiler
        .profile_table_with_options(ctx, table)
        .sample_ratio(sample_ratio)
        .await?;
    
    println!("Profiled {} columns in {:?}", 
             profiles.len(), start.elapsed());
    
    Ok(DatasetProfile {
        table_name: table.to_string(),
        profiles,
        sample_ratio,
        profiling_time: start.elapsed(),
    })
}
```

## Related

- [How to use constraint suggestions](use-constraint-suggestions.md)
- [How to optimize performance](optimize-performance.md)
- [Tutorial: Column profiling](../tutorials/04-column-profiling.md)
- [Reference: Profiler API](../reference/profiler.md)