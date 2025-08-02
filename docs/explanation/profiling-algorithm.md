# Understanding Term's Column Profiling Algorithm

<!-- 
This is an EXPLANATION document following Diátaxis principles.
It explains the concepts and design decisions behind the profiling system.
-->

## Why Three-Pass Profiling?

Term's ColumnProfiler uses a three-pass algorithm to balance accuracy, performance, and memory usage when analyzing large datasets. This design emerged from studying the limitations of single-pass approaches and the inefficiencies of naive multi-pass systems.

## The Three-Pass Architecture

### Pass 1: Type Detection and Sampling

The first pass serves two critical purposes:

1. **Type inference through sampling**: We analyze a configurable sample (default: 10,000 rows) to detect the semantic data type. This is sufficient for reliable type detection while avoiding full table scans.

2. **Cardinality estimation**: We use HyperLogLog to estimate the number of distinct values. This determines whether to use exact counting (low cardinality) or approximate methods (high cardinality) in subsequent passes.

**Why sampling first?** Type detection doesn't require seeing every value. By sampling early, we can:
- Configure subsequent passes based on detected types
- Avoid expensive operations on inappropriate data types
- Fail fast if data quality issues are detected

### Pass 2: Basic Statistics Computation

The second pass computes core statistics using DataFusion's aggregation engine:

```sql
SELECT 
    COUNT(*) as row_count,
    COUNT(column) as non_null_count,
    COUNT(DISTINCT column) as distinct_count  -- Only if cardinality < threshold
FROM table
```

**Why a separate pass?** While we could combine this with Pass 1, separation provides:
- Clean separation of concerns
- Ability to use DataFusion's optimized aggregations
- Option to skip expensive operations based on Pass 1 results
- Better progress reporting and cancellation points

### Pass 3: Distribution Analysis

The third pass performs type-specific analysis:

- **Numeric columns**: Quantiles, histograms, outlier detection
- **String columns**: Pattern matching, length statistics
- **Temporal columns**: Date range analysis, format validation
- **Categorical columns**: Value frequency histograms

**Why type-specific processing?** Different data types require fundamentally different algorithms:
- Numeric quantiles use sorting-based algorithms
- String patterns use regex matching
- Categorical histograms need frequency counting

## Design Decisions

### Memory Management

We chose bounded memory algorithms wherever possible:

1. **HyperLogLog for cardinality**: O(1) memory instead of O(distinct values)
2. **Reservoir sampling for patterns**: Fixed-size sample regardless of data size
3. **Streaming quantiles**: Approximate algorithms for very large datasets
4. **Categorical limits**: Track only top-N values for high-cardinality columns

### Adaptive Behavior

The profiler adapts based on data characteristics:

```rust
if estimated_cardinality > threshold {
    // Use approximate algorithms
    use_hyperloglog = true;
    use_count_min_sketch = true;
} else {
    // Use exact counting
    use_hashset = true;
}
```

This ensures optimal performance across different data distributions.

### Parallelization Strategy

We parallelize at the column level, not the row level:

```rust
// Good: Process columns in parallel
let profiles = columns
    .par_iter()
    .map(|col| profile_column(col))
    .collect();

// Avoided: Row-level parallelism
// Would require complex synchronization
```

**Why column-level?** 
- Natural boundaries with no synchronization needed
- Each column's profile is independent
- Matches DataFusion's columnar execution model
- Better cache locality

## Performance Optimizations

### 1. Lazy Pattern Compilation

We use `lazy_static!` for regex patterns to compile them once:

```rust
lazy_static! {
    static ref EMAIL_PATTERN: Regex = Regex::new(r"...").unwrap();
    static ref DATE_PATTERN: Regex = Regex::new(r"...").unwrap();
}
```

This avoids recompilation costs across multiple profile operations.

### 2. Early Termination

The profiler can terminate early when confidence is reached:

```rust
if samples_processed > min_samples && confidence > threshold {
    return Ok(result);
}
```

### 3. Memory Pool Integration

We integrate with DataFusion's memory management:

```rust
let memory_pool = MemoryPool::new(
    MemoryPoolType::Fair,
    config.max_memory_bytes,
);
ctx.set_memory_pool(memory_pool);
```

This prevents out-of-memory errors on large datasets.

### 4. Pushdown Optimizations

We push filters and limits to the storage layer:

```rust
// Instead of loading all data
let df = ctx.table("large_table")?;
let sample = df.limit(0, Some(10000))?;

// We generate optimized SQL
let sql = "SELECT * FROM large_table LIMIT 10000";
```

## Trade-offs and Limitations

### Accuracy vs Performance

We chose approximate algorithms for large datasets:

- **Quantiles**: T-Digest algorithm (±1% error) vs exact sorting
- **Cardinality**: HyperLogLog (±2% error) vs exact counting
- **Patterns**: Sample-based detection vs full scan

These trade-offs are configurable through the builder API.

### Memory vs Completeness

For categorical data, we limit tracked values:

```rust
pub struct CategoricalHistogram {
    buckets: Vec<Bucket>,     // Limited to top 100
    total_count: u64,
    is_complete: bool,        // False if values were dropped
}
```

Users can adjust limits based on their memory constraints.

### Generality vs Optimization

We optimize for common cases while supporting edge cases:

1. **Fast path**: Numeric columns with no nulls
2. **Standard path**: Mixed types with some nulls  
3. **Slow path**: High-cardinality strings with patterns

## Comparison with Alternatives

### Deequ (Spark-based)

Deequ uses Spark's distributed computing model:
- **Pros**: Handles massive scale through distribution
- **Cons**: Requires Spark cluster, high latency for small datasets

Term's approach:
- **Pros**: No cluster required, low latency, zero-copy operations
- **Cons**: Single-machine memory limits

### Pandas Profiling

Pandas loads all data into memory:
- **Pros**: Simple API, rich visualizations
- **Cons**: Memory intensive, not suitable for large data

Term's approach:
- **Pros**: Streaming algorithms, configurable memory usage
- **Cons**: Less integrated visualization

### Great Expectations

Great Expectations focuses on expectations, not profiling:
- **Pros**: Rich expectation language, good for testing
- **Cons**: Profiling is secondary concern

Term's approach:
- **Pros**: Profiling-first design, generates expectations
- **Cons**: Less mature expectation ecosystem

## Future Directions

### Incremental Profiling

We're designing support for incremental updates:

```rust
let profile = profiler.profile_incremental(
    base_profile,
    new_data,
)?;
```

This will enable continuous profiling of streaming data.

### Distributed Profiling

Future versions may support distributed execution:

```rust
let profile = profiler
    .distributed()
    .profile_partitioned(table, partitions)?;
```

### Machine Learning Integration

We're exploring ML-based optimizations:
- Predict optimal sample sizes
- Detect anomalies in distributions
- Suggest validation rules automatically

## Conclusion

Term's three-pass profiling algorithm represents a pragmatic balance between competing concerns. By separating type detection, basic statistics, and distribution analysis into distinct passes, we achieve:

1. **Predictable performance**: Each pass has known complexity
2. **Configurable accuracy**: Users control the trade-offs
3. **Bounded memory**: Algorithms scale to large datasets
4. **Type safety**: Type-specific processing prevents errors

The key insight is that not all profiling operations require seeing all data. By intelligently sampling and using approximate algorithms where appropriate, we can provide accurate profiles at a fraction of the cost of naive approaches.

## Related Topics

- [Column Profiler API](../reference/profiler.md) - Technical reference
- [Profile Large Datasets](../how-to/profile-large-datasets.md) - Practical guide
- [Type Inference System](type-inference-system.md) - Type detection design
- [Memory Management](memory-management.md) - Memory optimization strategies