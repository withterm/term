# ColumnProfiler API Reference

<!-- 
This is a REFERENCE document following Diátaxis principles.
It provides complete technical information about the ColumnProfiler API.
-->

## Overview

The `ColumnProfiler` analyzes data columns to compute statistics, detect types, and understand data characteristics.

## ColumnProfiler

```rust
pub struct ColumnProfiler {
    sample_size: Option<u64>,
    cardinality_threshold: u64,
    max_memory_bytes: Option<u64>,
    enable_parallel: bool,
    memory_pool_config: Option<MemoryPoolConfig>,
    progress_callback: Option<Box<dyn Fn(f64) + Send + Sync>>,
}
```

### Constructor

```rust
pub fn new() -> Self
```

Creates a profiler with default settings:
- `sample_size`: None (full scan)
- `cardinality_threshold`: 1000
- `max_memory_bytes`: None (unlimited)
- `enable_parallel`: false
- `memory_pool_config`: None
- `progress_callback`: None

### Builder

```rust
pub fn builder() -> ColumnProfilerBuilder
```

Returns a builder for configuring the profiler.

### Methods

#### profile_column

```rust
pub async fn profile_column(
    &self,
    ctx: &SessionContext,
    table_name: &str,
    column_name: &str,
) -> Result<ColumnProfile>
```

**Parameters:**
- `ctx`: DataFusion session context with registered table
- `table_name`: Name of the table to profile
- `column_name`: Name of the column to profile

**Returns:** `ColumnProfile` containing all computed statistics

**Errors:**
- `AnalyzerError::TableNotFound` - Table doesn't exist
- `AnalyzerError::ColumnNotFound` - Column doesn't exist
- `AnalyzerError::QueryExecution` - Query failed

#### profile_column_with_options

```rust
pub async fn profile_column_with_options(
    &self,
    ctx: &SessionContext,
    table_name: &str,
    column_name: &str,
) -> ProfileOptions
```

Returns a fluent interface for configuring profile options.

#### profile_table

```rust
pub async fn profile_table(
    &self,
    ctx: &SessionContext,
    table_name: &str,
) -> Result<Vec<ColumnProfile>>
```

**Parameters:**
- `ctx`: DataFusion session context
- `table_name`: Name of the table to profile

**Returns:** Vector of `ColumnProfile`, one per column

#### profile_table_with_filter

```rust
pub async fn profile_table_with_filter(
    &self,
    ctx: &SessionContext,
    table_name: &str,
    filter: &str,
) -> Result<Vec<ColumnProfile>>
```

**Parameters:**
- `ctx`: DataFusion session context
- `table_name`: Name of the table
- `filter`: SQL WHERE clause (without WHERE keyword)

**Returns:** Vector of profiles for filtered data

## ColumnProfilerBuilder

```rust
pub struct ColumnProfilerBuilder {
    sample_size: Option<u64>,
    cardinality_threshold: u64,
    max_memory_bytes: Option<u64>,
    enable_parallel: bool,
    memory_pool_config: Option<MemoryPoolConfig>,
    progress_callback: Option<Box<dyn Fn(f64) + Send + Sync>>,
    // Advanced options
    skip_pattern_analysis: bool,
    skip_categorical_analysis: bool,
    skip_numeric_analysis: bool,
    enable_numeric_histogram: bool,
    max_categorical_values: usize,
    pattern_sample_size: usize,
    confidence_threshold: f64,
}
```

### Methods

#### sample_size

```rust
pub fn sample_size(mut self, size: u64) -> Self
```

Sets the maximum number of rows to analyze. Default: None (full scan).

#### cardinality_threshold

```rust
pub fn cardinality_threshold(mut self, threshold: u64) -> Self
```

Sets the threshold for switching between exact and approximate cardinality counting. Default: 1000.

#### max_memory_bytes

```rust
pub fn max_memory_bytes(mut self, bytes: u64) -> Self
```

Sets the maximum memory usage in bytes. Default: None (unlimited).

#### enable_parallel

```rust
pub fn enable_parallel(mut self, enabled: bool) -> Self
```

Enables parallel processing of multiple columns. Default: false.

#### memory_pool_config

```rust
pub fn memory_pool_config(mut self, config: MemoryPoolConfig) -> Self
```

Configures DataFusion memory pool settings.

#### with_progress_callback

```rust
pub fn with_progress_callback<F>(mut self, callback: F) -> Self
where
    F: Fn(f64) + Send + Sync + 'static
```

Sets a callback for progress updates (0.0 to 1.0).

#### skip_pattern_analysis

```rust
pub fn skip_pattern_analysis(mut self, skip: bool) -> Self
```

Skips regex pattern detection for string columns. Default: false.

#### skip_categorical_analysis

```rust
pub fn skip_categorical_analysis(mut self, skip: bool) -> Self
```

Skips categorical histogram generation. Default: false.

#### skip_numeric_analysis

```rust
pub fn skip_numeric_analysis(mut self, skip: bool) -> Self
```

Skips numeric distribution analysis. Default: false.

#### enable_numeric_histogram

```rust
pub fn enable_numeric_histogram(mut self, enabled: bool) -> Self
```

Enables histogram generation for numeric columns. Default: false.

#### max_categorical_values

```rust
pub fn max_categorical_values(mut self, max: usize) -> Self
```

Sets maximum unique values to track for categorical columns. Default: 100.

#### pattern_sample_size

```rust
pub fn pattern_sample_size(mut self, size: usize) -> Self
```

Sets sample size for pattern detection. Default: 1000.

#### confidence_threshold

```rust
pub fn confidence_threshold(mut self, threshold: f64) -> Self
```

Sets confidence threshold for type inference (0.0-1.0). Default: 0.9.

#### build

```rust
pub fn build(self) -> ColumnProfiler
```

Creates the configured profiler.

## ProfileOptions

Fluent interface for runtime profile configuration.

```rust
pub struct ProfileOptions<'a> {
    profiler: &'a ColumnProfiler,
    ctx: &'a SessionContext,
    table_name: String,
    column_name: String,
    sample_ratio: Option<f64>,
    timeout_seconds: Option<u64>,
    adaptive_sampling: bool,
    min_sample_size: Option<u64>,
    max_sample_size: Option<u64>,
}
```

### Methods

#### sample_ratio

```rust
pub fn sample_ratio(mut self, ratio: f64) -> Self
```

Sets sampling ratio (0.0-1.0). Overrides profiler's sample_size.

#### timeout_seconds

```rust
pub fn timeout_seconds(mut self, seconds: u64) -> Self
```

Sets maximum execution time.

#### adaptive_sampling

```rust
pub fn adaptive_sampling(mut self, enabled: bool) -> Self
```

Enables automatic sample size adjustment based on data characteristics.

#### min_sample_size

```rust
pub fn min_sample_size(mut self, size: u64) -> Self
```

Sets minimum sample size for adaptive sampling.

#### max_sample_size

```rust
pub fn max_sample_size(mut self, size: u64) -> Self
```

Sets maximum sample size for adaptive sampling.

## ColumnProfile

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnProfile {
    pub column_name: String,
    pub data_type: DataType,
    pub inferred_type: Option<InferredDataType>,
    pub basic_stats: BasicStatistics,
    pub numeric_distribution: Option<NumericDistribution>,
    pub categorical_histogram: Option<CategoricalHistogram>,
    pub patterns: Option<Vec<DetectedPattern>>,
    pub quality_score: f64,
    pub profiling_time: Duration,
}
```

### Fields

- `column_name`: Name of the profiled column
- `data_type`: DataFusion data type
- `inferred_type`: Semantic type from TypeInferenceEngine
- `basic_stats`: Core statistics (count, nulls, etc.)
- `numeric_distribution`: Statistics for numeric columns
- `categorical_histogram`: Value frequencies for categorical data
- `patterns`: Detected regex patterns
- `quality_score`: Overall data quality score (0.0-1.0)
- `profiling_time`: Time taken to profile

## BasicStatistics

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BasicStatistics {
    pub row_count: u64,
    pub null_count: u64,
    pub null_percentage: f64,
    pub distinct_count: u64,
    pub uniqueness: f64,
    pub min_length: Option<u32>,
    pub max_length: Option<u32>,
    pub avg_length: Option<f64>,
}
```

### Fields

- `row_count`: Total number of rows
- `null_count`: Number of null values
- `null_percentage`: Fraction of nulls (0.0-1.0)
- `distinct_count`: Number of unique values
- `uniqueness`: Fraction of unique values (0.0-1.0)
- `min_length`: Minimum string length (strings only)
- `max_length`: Maximum string length (strings only)
- `avg_length`: Average string length (strings only)

## NumericDistribution

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NumericDistribution {
    pub min: f64,
    pub max: f64,
    pub mean: Option<f64>,
    pub std_dev: Option<f64>,
    pub variance: Option<f64>,
    pub sum: Option<f64>,
    pub quantiles: HashMap<String, f64>,
    pub histogram: Option<Vec<HistogramBucket>>,
}
```

### Fields

- `min`: Minimum value
- `max`: Maximum value
- `mean`: Arithmetic mean
- `std_dev`: Standard deviation
- `variance`: Statistical variance
- `sum`: Sum of all values
- `quantiles`: Percentiles (P01, P05, P25, P50, P75, P95, P99)
- `histogram`: Equal-width histogram buckets

### Quantile Keys

Standard quantiles computed:
- `"P01"`: 1st percentile
- `"P05"`: 5th percentile
- `"P25"`: 25th percentile (Q1)
- `"P50"`: 50th percentile (median)
- `"P75"`: 75th percentile (Q3)
- `"P95"`: 95th percentile
- `"P99"`: 99th percentile

## CategoricalHistogram

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoricalHistogram {
    pub buckets: Vec<CategoricalBucket>,
    pub total_count: u64,
    pub is_complete: bool,
}
```

### Fields

- `buckets`: Value frequency buckets, sorted by count descending
- `total_count`: Total number of values
- `is_complete`: Whether all values are represented

## CategoricalBucket

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoricalBucket {
    pub value: String,
    pub count: u64,
    pub percentage: f64,
}
```

### Fields

- `value`: The categorical value
- `count`: Number of occurrences
- `percentage`: Percentage of total (0.0-100.0)

## DetectedPattern

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectedPattern {
    pub pattern_type: String,
    pub regex: String,
    pub match_percentage: f64,
    pub sample_matches: Vec<String>,
}
```

### Fields

- `pattern_type`: Pattern name (e.g., "email", "phone", "date")
- `regex`: Regular expression matching the pattern
- `match_percentage`: Percentage of values matching (0.0-100.0)
- `sample_matches`: Example matching values

## MemoryPoolConfig

```rust
#[derive(Debug, Clone)]
pub struct MemoryPoolConfig {
    pub size: Option<u64>,
    pub pool_type: MemoryPoolType,
    pub max_partitions: Option<usize>,
    pub enable_memory_manager: bool,
}
```

### Fields

- `size`: Memory pool size in bytes
- `pool_type`: Allocation strategy
- `max_partitions`: Maximum concurrent partitions
- `enable_memory_manager`: Enable memory monitoring

## MemoryPoolType

```rust
#[derive(Debug, Clone)]
pub enum MemoryPoolType {
    Greedy,    // Allocate aggressively
    Fair,      // Balance allocations
}
```

## Error Types

```rust
pub enum ProfilerError {
    TableNotFound(String),
    ColumnNotFound { table: String, column: String },
    QueryExecution(String),
    InvalidSampleRatio(f64),
    Timeout { elapsed: Duration, timeout: Duration },
    MemoryExceeded { used: u64, limit: u64 },
    InvalidConfiguration(String),
}
```

## Performance Characteristics

| Operation | Time Complexity | Memory Complexity |
|-----------|----------------|-------------------|
| Basic stats | O(n) | O(1) |
| Type inference | O(sample_size) | O(types) |
| Cardinality (exact) | O(n) | O(distinct) |
| Cardinality (approx) | O(n) | O(1) |
| Numeric distribution | O(n) | O(1) |
| Categorical histogram | O(n) | O(distinct) |
| Pattern detection | O(sample_size) | O(patterns) |

## Thread Safety

- `ColumnProfiler` is `Send + Sync`
- Progress callbacks must be `Send + Sync`
- All result types are `Send + Sync`

## See Also

- [`TypeInferenceEngine`](type-inference.md) - Type detection reference
- [`SuggestionEngine`](analyzers.md#constraint-suggestion-system) - Constraint suggestions
- [Profiling Algorithm](../explanation/profiling-algorithm.md) - Implementation details
- [Profile Large Datasets](../how-to/profile-large-datasets.md) - Performance guide