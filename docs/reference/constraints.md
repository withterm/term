# Constraints Reference

## Overview

Constraints are the core validation components in Term that evaluate data quality conditions. Each constraint implements the `Constraint` trait and returns a `ConstraintResult` when evaluated.

## Synopsis

```rust
use term_guard::constraints::*;
use term_guard::core::Check;

// Basic constraint usage
let check = Check::builder("validation")
    .constraint(CompletenessConstraint::new("column", 1.0)?)
    .constraint(UniquenessConstraint::new("id")?)
    .build();
```

## Constraint Types

### Completeness Constraints

Validate data completeness (presence of non-null values).

#### `CompletenessConstraint::new`

```rust
pub fn new(column: &str, threshold: f64) -> Result<Self>
```

##### Parameters

- **`column`**: `&str` - Column name to validate
- **`threshold`**: `f64` - Minimum completeness ratio (0.0 to 1.0)

##### Returns

- `Result<CompletenessConstraint>` - Configured constraint or error

#### `MultiCompletenessConstraint::new`

```rust
pub fn new(columns: Vec<&str>, threshold: f64, strategy: CompletenessStrategy) -> Result<Self>
```

##### Parameters

- **`columns`**: `Vec<&str>` - Column names to validate
- **`threshold`**: `f64` - Minimum completeness ratio
- **`strategy`**: `CompletenessStrategy` - Evaluation strategy (All, Any, AtLeastOne)

#### Builder Methods

- **`.is_complete(column)`** - 100% completeness check
- **`.has_completeness(column, threshold)`** - Threshold-based completeness
- **`.are_complete(columns)`** - All columns must be complete
- **`.are_any_complete(columns)`** - At least one column must be complete

### Uniqueness Constraints

Validate data uniqueness and primary key constraints.

#### `UniquenessConstraint::new`

```rust
pub fn new(column: &str) -> Result<Self>
```

##### Parameters

- **`column`**: `&str` - Column name to check for uniqueness

#### `CompositeUniquenessConstraint::new`

```rust
pub fn new(columns: Vec<&str>) -> Result<Self>
```

##### Parameters

- **`columns`**: `Vec<&str>` - Column names for composite uniqueness check

#### `PrimaryKeyConstraint::new`

```rust
pub fn new(columns: Vec<&str>) -> Result<Self>
```

##### Parameters

- **`columns`**: `Vec<&str>` - Column names that form the primary key

#### Builder Methods

- **`.is_unique(column)`** - Single column uniqueness
- **`.are_unique(columns)`** - Composite uniqueness
- **`.is_primary_key(columns)`** - Primary key validation (unique + non-null)

### Distinctness Constraints

Validate distinctness ratios (distinct values / total values).

#### `DistinctnessConstraint::new`

```rust
pub fn new(columns: Vec<&str>, assertion: Assertion) -> Result<Self>
```

##### Parameters

- **`columns`**: `Vec<&str>` - Column names to analyze
- **`assertion`**: `Assertion` - Expected distinctness condition

##### Returns

- `Result<DistinctnessConstraint>` - Configured constraint or error

#### Builder Methods

- **`.has_distinctness(columns, assertion)`** - Distinctness ratio validation

### Cardinality Constraints

Validate the number of distinct values in columns.

#### `ApproxCountDistinctConstraint::new`

```rust
pub fn new(column: &str, assertion: Assertion) -> Result<Self>
```

##### Parameters

- **`column`**: `&str` - Column name to analyze
- **`assertion`**: `Assertion` - Expected cardinality condition

##### Performance

- **Algorithm**: HyperLogLog
- **Error Margin**: 2-3%
- **Time Complexity**: O(n) single pass
- **Space Complexity**: O(1) constant memory

#### Builder Methods

- **`.has_approx_count_distinct(column, assertion)`** - Approximate cardinality check

### Value Constraints

Validate value ranges and data types.

#### `ContainmentConstraint::new`

```rust
pub fn new(column: &str, allowed_values: Vec<String>) -> Result<Self>
```

##### Parameters

- **`column`**: `&str` - Column name to validate
- **`allowed_values`**: `Vec<String>` - Set of permitted values

#### `NonNegativeConstraint::new`

```rust
pub fn new(column: &str) -> Result<Self>
```

##### Parameters

- **`column`**: `&str` - Numeric column to validate

#### `DataTypeConstraint::new`

```rust
pub fn new(column: &str, data_type: DataType, threshold: f64) -> Result<Self>
```

##### Parameters

- **`column`**: `&str` - Column name to validate
- **`data_type`**: `DataType` - Expected data type
- **`threshold`**: `f64` - Minimum conformance ratio (0.0 to 1.0)

##### Supported DataTypes

- `DataType::Integer` - Whole numbers
- `DataType::Float` - Decimal numbers
- `DataType::Boolean` - Boolean values
- `DataType::Date` - ISO date format (YYYY-MM-DD)
- `DataType::Timestamp` - Date and time values
- `DataType::String` - Text values

#### Builder Methods

- **`.is_contained_in(column, values)`** - Value set validation
- **`.is_non_negative(column)`** - Non-negative numeric validation
- **`.has_data_type(column, data_type, threshold)`** - Data type conformance

### String Length Constraints

Validate string length requirements.

#### `MinLengthConstraint::new`

```rust
pub fn new(column: &str, min_length: usize) -> Result<Self>
```

##### Parameters

- **`column`**: `&str` - String column to validate
- **`min_length`**: `usize` - Minimum required length

#### `MaxLengthConstraint::new`

```rust
pub fn new(column: &str, max_length: usize) -> Result<Self>
```

##### Parameters

- **`column`**: `&str` - String column to validate
- **`max_length`**: `usize` - Maximum allowed length

##### Behavior

- NULL values are ignored in length validation
- Empty strings have length 0

#### Builder Methods

- **`.has_min_length(column, min_length)`** - Minimum length validation
- **`.has_max_length(column, max_length)`** - Maximum length validation

### Pattern Constraints

Validate data against regular expressions and common formats.

#### `PatternConstraint::new`

```rust
pub fn new(column: &str, regex: &str, threshold: f64) -> Result<Self>
```

##### Parameters

- **`column`**: `&str` - Column to validate
- **`regex`**: `&str` - Regular expression pattern
- **`threshold`**: `f64` - Minimum match ratio (0.0 to 1.0)

#### `UrlConstraint::new`

```rust
pub fn new(column: &str, threshold: f64) -> Result<Self>
```

#### `EmailConstraint::new`

```rust
pub fn new(column: &str, threshold: f64) -> Result<Self>
```

#### `CreditCardConstraint::new`

```rust
pub fn new(column: &str, threshold: f64, should_detect: bool) -> Result<Self>
```

##### Parameters

- **`should_detect`**: `bool` - Whether to expect credit card numbers (false for security checks)

#### `SsnConstraint::new`

```rust
pub fn new(column: &str, threshold: f64, should_detect: bool) -> Result<Self>
```

#### Builder Methods

- **`.has_pattern(column, regex, threshold)`** - Custom regex validation
- **`.contains_url(column, threshold)`** - URL format validation
- **`.contains_email(column, threshold)`** - Email format validation
- **`.contains_credit_card_number(column, threshold)`** - Credit card detection
- **`.contains_social_security_number(column, threshold)`** - SSN detection

### Schema Constraints

Validate dataset structure and schema properties.

#### `ColumnCountConstraint::new`

```rust
pub fn new(assertion: Assertion) -> Result<Self>
```

##### Parameters

- **`assertion`**: `Assertion` - Expected column count condition

#### Builder Methods

- **`.has_column_count(assertion)`** - Column count validation

### Statistical Constraints

Validate statistical properties of numeric data.

#### Basic Statistical Constraints

##### `SizeConstraint::new`

```rust
pub fn new(assertion: Assertion) -> Result<Self>
```

##### `MinConstraint::new`

```rust
pub fn new(column: &str, assertion: Assertion) -> Result<Self>
```

##### `MaxConstraint::new`

```rust
pub fn new(column: &str, assertion: Assertion) -> Result<Self>
```

##### `MeanConstraint::new`

```rust
pub fn new(column: &str, assertion: Assertion) -> Result<Self>
```

##### `SumConstraint::new`

```rust
pub fn new(column: &str, assertion: Assertion) -> Result<Self>
```

##### Parameters

- **`column`**: `&str` - Numeric column to analyze (not required for SizeConstraint)
- **`assertion`**: `Assertion` - Expected statistical condition

##### Builder Methods

- **`.has_size(assertion)`** - Row count validation
- **`.has_min(column, assertion)`** - Minimum value validation
- **`.has_max(column, assertion)`** - Maximum value validation
- **`.has_mean(column, assertion)`** - Mean value validation
- **`.has_sum(column, assertion)`** - Sum validation

#### Advanced Statistical Constraints

##### `StandardDeviationConstraint::new`

```rust
pub fn new(column: &str, assertion: Assertion) -> Result<Self>
```

##### `CorrelationConstraint::new`

```rust
pub fn new(column1: &str, column2: &str, assertion: Assertion) -> Result<Self>
```

##### `EntropyConstraint::new`

```rust
pub fn new(column: &str, assertion: Assertion) -> Result<Self>
```

##### `MutualInformationConstraint::new`

```rust
pub fn new(column1: &str, column2: &str, assertion: Assertion) -> Result<Self>
```

##### Builder Methods

- **`.has_standard_deviation(column, assertion)`** - Standard deviation validation
- **`.has_correlation(column1, column2, assertion)`** - Pearson correlation validation
- **`.has_entropy(column, assertion)`** - Shannon entropy validation
- **`.has_mutual_information(column1, column2, assertion)`** - Mutual information validation

#### Quantile Constraints

Validate quantile values (percentiles) in numeric columns.

##### `ApproxQuantileConstraint::new`

```rust
pub fn new(column: &str, quantile: f64, assertion: Assertion) -> Result<Self>
```

##### `ExactQuantileConstraint::new`

```rust
pub fn new(column: &str, quantile: f64, assertion: Assertion) -> Result<Self>
```

##### Parameters

- **`column`**: `&str` - Numeric column to analyze
- **`quantile`**: `f64` - Quantile value (0.0 to 1.0, e.g., 0.5 for median)
- **`assertion`**: `Assertion` - Expected quantile condition

##### Performance

- **ApproxQuantile**: Fast, ~2% error margin using sketching algorithms
- **ExactQuantile**: Precise, requires full data sort

##### Builder Methods

- **`.has_approx_quantile(column, quantile, assertion)`** - Approximate quantile validation
- **`.has_exact_quantile(column, quantile, assertion)`** - Exact quantile validation

### Custom SQL Constraints

Validate data using custom SQL expressions and business rules.

#### `SqlConstraint::new`

```rust
pub fn new(sql_expression: &str, message: Option<String>) -> Result<Self>
```

##### Parameters

- **`sql_expression`**: `&str` - SQL WHERE clause expression
- **`message`**: `Option<String>` - Custom failure message

##### Security

- SQL expressions are validated to prevent injection
- Only WHERE clause conditions are supported
- No DDL/DML operations allowed

##### Builder Methods

- **`.satisfies(expression, message)`** - Custom SQL validation

### Consistency Constraints

Validate data consistency across time or datasets.

#### `ConsistencyConstraint::new`

```rust
pub fn new(columns: Vec<&str>, assertion: Assertion) -> Result<Self>
```

##### Parameters

- **`columns`**: `Vec<&str>` - Column names to check for consistency
- **`assertion`**: `Assertion` - Expected consistency condition

##### Builder Methods

- **`.has_consistency(columns, assertion)`** - Cross-run consistency validation

## Assertion Types

Assertions define expected conditions for constraint validation.

### `Assertion` Enum

```rust
pub enum Assertion {
    Equals(f64),
    GreaterThan(f64),
    GreaterThanOrEqual(f64),
    LessThan(f64),
    LessThanOrEqual(f64),
    Between(f64, f64),
}
```

#### Variants

- **`Equals(value)`**: Exact numeric match
- **`GreaterThan(value)`**: Strictly greater than comparison
- **`GreaterThanOrEqual(value)`**: Greater than or equal comparison
- **`LessThan(value)`**: Strictly less than comparison
- **`LessThanOrEqual(value)`**: Less than or equal comparison
- **`Between(min, max)`**: Inclusive range comparison

## Behavior

### Constraint Evaluation

All constraints implement the `Constraint` trait:

```rust
#[async_trait]
pub trait Constraint: Send + Sync {
    async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult>;
}
```

### Result Types

```rust
pub struct ConstraintResult {
    pub status: ConstraintStatus,
    pub metric: Option<f64>,
    pub message: String,
}

pub enum ConstraintStatus {
    Success,
    Failure,
    Error,
}
```

### Error Handling

- Constraints return `ConstraintStatus::Error` for execution failures
- Constraints return `ConstraintStatus::Failure` for validation failures
- SQL injection attempts result in immediate error responses

## Performance Characteristics

### Query Optimization

- Multiple constraints on the same table can be combined into single queries
- Statistical constraints benefit from column-oriented execution
- Pattern matching uses compiled regular expressions with caching

### Memory Usage

- Constraints use streaming evaluation where possible
- Large datasets are processed in configurable batches
- Approximate algorithms (HyperLogLog, sketching) use constant memory

### Thread Safety

- All constraints are `Send + Sync`
- Can be executed concurrently across multiple datasets
- Internal state is immutable after construction

## Examples

### Basic Usage

```rust
use term_guard::constraints::*;
use term_guard::core::Check;

// Single constraint
let check = Check::builder("validation")
    .constraint(CompletenessConstraint::new("email", 0.95)?)
    .build();
```

### Advanced Usage

```rust
// Multiple constraint types
let check = Check::builder("comprehensive")
    .constraint(CompletenessConstraint::new("id", 1.0)?)
    .constraint(UniquenessConstraint::new("id")?)
    .constraint(EmailConstraint::new("email", 0.99)?)
    .constraint(StatisticalConstraint::min("age", Assertion::GreaterThanOrEqual(0.0))?)
    .build();
```

## See Also

- [`Check`] - Container for constraint validation
- [`ValidationSuite`] - Collection of checks
- [How to Build Validation Suites](../how-to/comprehensive-examples.md)
- [Understanding Data Quality Concepts](../explanation/data-quality-principles.md)