# Analyzers API Reference

<!-- 
This is a REFERENCE document following Diátaxis principles.
It provides complete technical information about all analyzers.
-->

## Overview

Analyzers compute metrics from data without performing validation. They implement the `Analyzer` trait and follow a two-phase computation model.

## Trait: Analyzer

```rust
#[async_trait]
pub trait Analyzer: Send + Sync + Debug {
    type State: AnalyzerState;
    type Metric: Into<MetricValue> + Send + Sync + Debug;
    
    async fn compute_state_from_data(&self, ctx: &SessionContext) -> AnalyzerResult<Self::State>;
    fn compute_metric_from_state(&self, state: &Self::State) -> AnalyzerResult<Self::Metric>;
    fn name(&self) -> &str;
    fn description(&self) -> &str { "" }
    fn metric_key(&self) -> String { self.name().to_string() }
    fn columns(&self) -> Vec<&str> { vec![] }
}
```

## Basic Analyzers

### SizeAnalyzer

Counts the total number of rows in the dataset.

```rust
pub struct SizeAnalyzer;
```

**Metric Key:** `"size"`  
**Metric Type:** `MetricValue::Long`  
**Module:** `term_guard::analyzers::basic::size`

**Example:**
```rust
let analyzer = SizeAnalyzer::new();
// Returns: MetricValue::Long(row_count)
```

### CompletenessAnalyzer

Measures the fraction of non-null values in a column.

```rust
pub struct CompletenessAnalyzer {
    column: String,
}
```

**Constructor:**
```rust
pub fn new(column: impl Into<String>) -> Self
```

**Metric Key:** `"completeness.{column}"`  
**Metric Type:** `MetricValue::Double` (0.0 to 1.0)  
**Module:** `term_guard::analyzers::basic::completeness`

**Example:**
```rust
let analyzer = CompletenessAnalyzer::new("user_id");
// Returns: MetricValue::Double(0.95) for 95% complete
```

### DistinctnessAnalyzer

Measures the fraction of unique values in a column.

```rust
pub struct DistinctnessAnalyzer {
    column: String,
}
```

**Constructor:**
```rust
pub fn new(column: impl Into<String>) -> Self
```

**Metric Key:** `"distinctness.{column}"`  
**Metric Type:** `MetricValue::Double` (0.0 to 1.0)  
**Module:** `term_guard::analyzers::basic::distinctness`

**Formula:** `distinct_count / total_count`

### MeanAnalyzer

Computes the arithmetic mean of a numeric column.

```rust
pub struct MeanAnalyzer {
    column: String,
}
```

**Constructor:**
```rust
pub fn new(column: impl Into<String>) -> Self
```

**Metric Key:** `"mean.{column}"`  
**Metric Type:** `MetricValue::Double`  
**Module:** `term_guard::analyzers::basic::mean`

**Requirements:** Column must be numeric (Int or Float types)

### MinAnalyzer

Finds the minimum value in a numeric column.

```rust
pub struct MinAnalyzer {
    column: String,
}
```

**Constructor:**
```rust
pub fn new(column: impl Into<String>) -> Self
```

**Metric Key:** `"min.{column}"`  
**Metric Type:** `MetricValue::Double`  
**Module:** `term_guard::analyzers::basic::min_max`

**Supports:** Int64 and Float64 columns

### MaxAnalyzer

Finds the maximum value in a numeric column.

```rust
pub struct MaxAnalyzer {
    column: String,
}
```

**Constructor:**
```rust
pub fn new(column: impl Into<String>) -> Self
```

**Metric Key:** `"max.{column}"`  
**Metric Type:** `MetricValue::Double`  
**Module:** `term_guard::analyzers::basic::min_max`

**Supports:** Int64 and Float64 columns

### SumAnalyzer

Computes the sum of values in a numeric column.

```rust
pub struct SumAnalyzer {
    column: String,
}
```

**Constructor:**
```rust
pub fn new(column: impl Into<String>) -> Self
```

**Metric Key:** `"sum.{column}"`  
**Metric Type:** `MetricValue::Double`  
**Module:** `term_guard::analyzers::basic::sum`

## Advanced Analyzers

### ApproxCountDistinctAnalyzer

Approximates the number of distinct values using HyperLogLog algorithm.

```rust
pub struct ApproxCountDistinctAnalyzer {
    column: String,
}
```

**Constructor:**
```rust
pub fn new(column: impl Into<String>) -> Self
```

**Metric Key:** `"approx_count_distinct.{column}"`  
**Metric Type:** `MetricValue::Long`  
**Module:** `term_guard::analyzers::advanced::approx_count_distinct`

**Accuracy:** ~2% standard error  
**Performance:** O(1) memory, much faster than exact count

### StandardDeviationAnalyzer

Computes statistical variance and standard deviation metrics.

```rust
pub struct StandardDeviationAnalyzer {
    column: String,
}
```

**Constructor:**
```rust
pub fn new(column: impl Into<String>) -> Self
```

**Metric Key:** `"standard_deviation"`  
**Metric Type:** `MetricValue::Distribution` containing:
- `count`: Number of values
- `mean`: Average value
- `stddev`: Population standard deviation
- `stddev_sample`: Sample standard deviation
- `variance`: Population variance
- `variance_sample`: Sample variance
- `cv`: Coefficient of variation

**Module:** `term_guard::analyzers::advanced::standard_deviation`

### DataTypeAnalyzer

Infers and analyzes the data types present in a column.

```rust
pub struct DataTypeAnalyzer {
    column: String,
}
```

**Constructor:**
```rust
pub fn new(column: impl Into<String>) -> Self
```

**Metric Key:** `"data_type"`  
**Metric Type:** `MetricValue::DataTypeDistribution`  
**Module:** `term_guard::analyzers::advanced::data_type`

**Detected Types:**
- Boolean
- Integer
- Double
- Date
- Timestamp
- String

### HistogramAnalyzer

Creates a histogram of value distributions.

```rust
pub struct HistogramAnalyzer {
    column: String,
    num_buckets: usize,
}
```

**Constructor:**
```rust
pub fn new(column: impl Into<String>, num_buckets: usize) -> Self
```

**Metric Key:** `"histogram"`  
**Metric Type:** `MetricValue::Histogram`  
**Module:** `term_guard::analyzers::advanced::histogram`

**Bucket Strategy:** Equal-width buckets between min and max values

### EntropyAnalyzer

Computes information entropy and Gini impurity.

```rust
pub struct EntropyAnalyzer {
    column: String,
    max_unique_values: usize,
}
```

**Constructor:**
```rust
pub fn new(column: impl Into<String>) -> Self
pub fn with_max_values(column: impl Into<String>, max_unique_values: usize) -> Self
```

**Metric Key:** `"entropy"`  
**Metric Type:** `MetricValue::Distribution` containing:
- `shannon_entropy`: Information entropy in bits
- `gini_impurity`: Gini impurity coefficient
- `unique_count`: Number of unique values

**Module:** `term_guard::analyzers::advanced::entropy`

**Default:** `max_unique_values = 1000`

### ComplianceAnalyzer

Evaluates custom SQL predicates for compliance checking.

```rust
pub struct ComplianceAnalyzer {
    predicate: String,
    name: Option<String>,
}
```

**Constructor:**
```rust
pub fn new(predicate: impl Into<String>) -> Self
pub fn with_name(predicate: impl Into<String>, name: impl Into<String>) -> Self
```

**Metric Key:** `"compliance"` or `"compliance.{name}"`  
**Metric Type:** `MetricValue::Double` (fraction of compliant rows)  
**Module:** `term_guard::analyzers::advanced::compliance`

**Security:** Validates predicates to prevent SQL injection

## State Types

Each analyzer has a corresponding state type that implements `AnalyzerState`:

```rust
pub trait AnalyzerState: Clone + Send + Sync + Debug + Serialize + DeserializeOwned {
    fn merge(states: Vec<Self>) -> AnalyzerResult<Self> where Self: Sized;
    fn is_empty(&self) -> bool { false }
}
```

### Common State Patterns

**Counter State:**
```rust
struct SizeState {
    count: u64,
}
```

**Statistical State:**
```rust
struct MeanState {
    sum: f64,
    count: u64,
}
```

**Distribution State:**
```rust
struct HistogramState {
    buckets: Vec<HistogramBucket>,
    total_count: u64,
}
```

## Metric Values

All analyzers produce metrics that convert to `MetricValue`:

```rust
pub enum MetricValue {
    Long(i64),
    Double(f64),
    Boolean(bool),
    String(String),
    Distribution(HashMap<String, MetricValue>),
    Histogram(Vec<HistogramBucket>),
    DataTypeDistribution(HashMap<String, u64>),
}
```

## Error Handling

Analyzers return `AnalyzerResult<T>` which may contain:

- `AnalyzerError::NoData` - No data available for analysis
- `AnalyzerError::InvalidData` - Data type mismatch
- `AnalyzerError::InvalidConfiguration` - Invalid analyzer parameters
- `AnalyzerError::QueryExecution` - DataFusion query failed

## Performance Characteristics

| Analyzer | Time Complexity | Memory Complexity | Supports Nulls |
|----------|----------------|-------------------|----------------|
| SizeAnalyzer | O(1) | O(1) | Yes |
| CompletenessAnalyzer | O(n) | O(1) | Yes |
| DistinctnessAnalyzer | O(n) | O(distinct) | Yes |
| MeanAnalyzer | O(n) | O(1) | Yes |
| Min/MaxAnalyzer | O(n) | O(1) | Yes |
| SumAnalyzer | O(n) | O(1) | Yes |
| ApproxCountDistinctAnalyzer | O(n) | O(1) | Yes |
| StandardDeviationAnalyzer | O(n) | O(1) | Yes |
| DataTypeAnalyzer | O(n) | O(types) | Yes |
| HistogramAnalyzer | O(n log n) | O(buckets) | Yes |
| EntropyAnalyzer | O(n) | O(unique) | Yes |
| ComplianceAnalyzer | O(n) | O(1) | Yes |

## Usage Requirements

1. **Table Name**: All analyzers expect data in a table named "data"
2. **DataFusion Context**: Requires a configured `SessionContext`
3. **Column Types**: Numeric analyzers require Int64 or Float64 columns
4. **Memory**: Sufficient memory for DataFusion query execution

## See Also

- [`AnalysisRunner`](analysis-runner.md) - Orchestrate multiple analyzers
- [`MetricValue`](metric-value.md) - Metric value types
- [Analyzer Architecture](../explanation/analyzer-architecture.md) - Design rationale