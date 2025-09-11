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

### KllSketchAnalyzer

Computes approximate quantiles using the KLL (Karnin-Lang-Liberty) sketch algorithm with O(k log n) memory complexity.

```rust
pub struct KllSketchAnalyzer {
    column: String,
    k: usize,
    quantiles: Vec<f64>,
}
```

**Constructor:**
```rust
pub fn new(column: impl Into<String>) -> Self
pub fn with_k(column: impl Into<String>, k: usize) -> Self
pub fn with_quantiles(column: impl Into<String>, quantiles: Vec<f64>) -> Self
```

**Metric Key:** `"kll_sketch"`  
**Metric Type:** `MetricValue::Distribution` containing:
- `min`: Minimum value
- `max`: Maximum value
- `count`: Number of values
- `quantile_{p}`: Value at percentile p (for each requested quantile)

**Module:** `term_guard::analyzers::advanced::kll_sketch`

**Default:** `k = 200`, `quantiles = [0.25, 0.5, 0.75, 0.9, 0.95, 0.99]`

**Memory:** O(k) where k controls accuracy/memory tradeoff

### CorrelationAnalyzer

Analyzes statistical relationships between two columns using various correlation methods.

```rust
pub struct CorrelationAnalyzer {
    column1: String,
    column2: String,
    method: CorrelationMethod,
}

pub enum CorrelationMethod {
    Pearson,
    Spearman,
    Covariance,
}
```

**Constructor:**
```rust
pub fn pearson(column1: impl Into<String>, column2: impl Into<String>) -> Self
pub fn spearman(column1: impl Into<String>, column2: impl Into<String>) -> Self
pub fn covariance(column1: impl Into<String>, column2: impl Into<String>) -> Self
```

**Metric Key:** `"correlation.{method}"`  
**Metric Type:** `MetricValue::Distribution` containing:
- `coefficient`: Correlation coefficient (-1 to 1 for Pearson/Spearman)
- `covariance`: Covariance value (for covariance method)
- `count`: Number of paired values
- `method`: The correlation method used

**Module:** `term_guard::analyzers::advanced::correlation`

**Methods:**
- **Pearson**: Linear correlation for continuous variables
- **Spearman**: Rank correlation for monotonic relationships
- **Covariance**: Measures joint variability

### MutualInformationAnalyzer

Measures statistical dependence between two columns using information theory.

```rust
pub struct MutualInformationAnalyzer {
    column1: String,
    column2: String,
    num_bins: Option<usize>,
}
```

**Constructor:**
```rust
pub fn new(column1: impl Into<String>, column2: impl Into<String>) -> Self
pub fn with_bins(column1: impl Into<String>, column2: impl Into<String>, num_bins: usize) -> Self
```

**Metric Key:** `"mutual_information"`  
**Metric Type:** `MetricValue::Distribution` containing:
- `mutual_information`: MI score in bits (0 = independent, higher = more dependent)
- `normalized_mi`: Normalized MI (0 to 1)
- `entropy_x`: Entropy of first column
- `entropy_y`: Entropy of second column
- `joint_entropy`: Joint entropy of both columns

**Module:** `term_guard::analyzers::advanced::mutual_information`

**Default:** `num_bins = 10` for continuous data discretization

**Use Cases:**
- Feature selection in machine learning
- Detecting non-linear relationships
- Information flow analysis

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
| KllSketchAnalyzer | O(n log k) | O(k) | Yes |
| CorrelationAnalyzer | O(n) | O(1) | Yes |
| MutualInformationAnalyzer | O(n) | O(bins²) | Yes |
| DataTypeAnalyzer | O(n) | O(types) | Yes |
| HistogramAnalyzer | O(n log n) | O(buckets) | Yes |
| EntropyAnalyzer | O(n) | O(unique) | Yes |
| ComplianceAnalyzer | O(n) | O(1) | Yes |

## Usage Requirements

1. **Table Name**: All analyzers expect data in a table named "data"
2. **DataFusion Context**: Requires a configured `SessionContext`
3. **Column Types**: Numeric analyzers require Int64 or Float64 columns
4. **Memory**: Sufficient memory for DataFusion query execution

## Constraint Suggestion System

### Overview

The suggestion system analyzes column profiles to recommend appropriate data quality constraints.

### SuggestionEngine

```rust
pub struct SuggestionEngine {
    rules: Vec<Box<dyn ConstraintSuggestionRule>>,
    confidence_threshold: f64,
    max_suggestions_per_column: usize,
}
```

**Methods:**
- `new()` - Create engine with default settings
- `add_rule(rule)` - Add a suggestion rule
- `confidence_threshold(threshold)` - Set minimum confidence (0.0-1.0)
- `max_suggestions_per_column(max)` - Limit suggestions per column
- `suggest_constraints(&profile)` - Generate suggestions for one column
- `suggest_constraints_batch(&profiles)` - Process multiple columns

### SuggestedConstraint

```rust
pub struct SuggestedConstraint {
    pub check_type: String,
    pub column: String,
    pub parameters: HashMap<String, ConstraintParameter>,
    pub confidence: f64,
    pub rationale: String,
    pub priority: SuggestionPriority,
}
```

### Suggestion Rules

#### CompletenessRule

Analyzes null percentage to suggest completeness constraints.

**Thresholds:**
- `>98%` complete → `is_complete`
- `90-98%` complete → `has_completeness` with threshold
- `<50%` complete → `monitor_completeness` (Critical priority)

**Configuration:**
```rust
CompletenessRule::new()
CompletenessRule::with_thresholds(high: f64, medium: f64)
```

#### UniquenessRule

Detects potential key columns based on cardinality.

**Thresholds:**
- `>95%` unique → `is_unique`
- `80-95%` unique → `has_uniqueness` with monitoring
- ID/key columns → `primary_key_candidate`

**Configuration:**
```rust
UniquenessRule::new()
UniquenessRule::with_thresholds(high: f64, medium: f64)
```

#### PatternRule

Identifies common data formats in string columns.

**Detects:**
- Email patterns → `matches_email_pattern`
- Date patterns → `matches_date_pattern`
- Phone patterns → `matches_phone_pattern`

**Configuration:**
```rust
PatternRule::new()
```

#### RangeRule

Suggests bounds for numeric columns.

**Suggestions:**
- Min/max values → `has_min`, `has_max`
- Non-negative data → `is_positive`
- Outlier detection → `has_no_outliers` (based on P99)

**Configuration:**
```rust
RangeRule::new()
```

#### DataTypeRule

Enforces type consistency.

**Cases:**
- Mixed types → `has_consistent_type` (Critical)
- Unknown type → `validate_data_type` (High)
- Consistent type → `has_data_type` with expected type

**Configuration:**
```rust
DataTypeRule::new()
```

#### CardinalityRule

Detects categorical columns and cardinality issues.

**Thresholds:**
- `≤10` distinct → `is_categorical`
- With histogram → `is_in_set` with valid values
- `≤50` distinct → `has_max_cardinality`
- `>80%` unique → `monitor_cardinality`

**Configuration:**
```rust
CardinalityRule::new()
CardinalityRule::with_thresholds(categorical: u64, low_cardinality: u64)
```

### Priority Levels

```rust
pub enum SuggestionPriority {
    Critical,  // Data quality issues likely to cause failures
    High,      // Important constraints for data integrity
    Medium,    // Useful constraints for monitoring
    Low,       // Optional constraints for completeness
}
```

### Example Usage

```rust
use term_guard::analyzers::{
    ColumnProfiler, SuggestionEngine,
    CompletenessRule, UniquenessRule, PatternRule
};

// Profile column
let profiler = ColumnProfiler::new();
let profile = profiler.profile_column(&ctx, "table", "column").await?;

// Configure engine
let engine = SuggestionEngine::new()
    .add_rule(Box::new(CompletenessRule::new()))
    .add_rule(Box::new(UniquenessRule::new()))
    .add_rule(Box::new(PatternRule::new()))
    .confidence_threshold(0.8);

// Get suggestions
let suggestions = engine.suggest_constraints(&profile);
```

## See Also

- [`AnalysisRunner`](analysis-runner.md) - Orchestrate multiple analyzers
- [`MetricValue`](metric-value.md) - Metric value types
- [Analyzer Architecture](../explanation/analyzer-architecture.md) - Design rationale
- [How to Use Constraint Suggestions](../how-to/use-constraint-suggestions.md) - Practical guide