# Term Constraints Documentation

This document provides a comprehensive overview of all constraints available in Term for data validation.

## Available Constraints

### Completeness Constraints

#### `is_complete(column)`
Verifies that a column has no null values (100% completeness).

```rust
Check::builder("completeness")
    .is_complete("customer_id")
    .build()
```

#### `has_completeness(column, threshold)`
Checks that a column meets a minimum completeness threshold.

```rust
Check::builder("completeness")
    .has_completeness("email", 0.95)  // 95% complete
    .build()
```

#### `are_complete(columns)`
Ensures all specified columns have no null values.

```rust
Check::builder("completeness")
    .are_complete(vec!["first_name", "last_name"])
    .build()
```

#### `are_any_complete(columns)`
Verifies that at least one of the specified columns is complete.

```rust
Check::builder("completeness")
    .are_any_complete(vec!["phone", "mobile"])
    .build()
```

### Uniqueness Constraints

#### `is_unique(column)`
Ensures all values in a column are unique.

```rust
Check::builder("uniqueness")
    .is_unique("user_id")
    .build()
```

#### `are_unique(columns)`
Verifies that the combination of specified columns is unique (composite key).

```rust
Check::builder("uniqueness")
    .are_unique(vec!["order_id", "line_item"])
    .build()
```

#### `is_primary_key(columns)`
Checks that columns form a valid primary key (unique and non-null).

```rust
Check::builder("keys")
    .is_primary_key(vec!["id"])
    .build()
```

### Distinctness Constraints

#### `has_distinctness(columns, assertion)`
Validates the distinctness ratio of values in columns. Distinctness is the ratio of distinct values to total values.

```rust
Check::builder("data_quality")
    // Low distinctness for categorical columns
    .has_distinctness(vec!["status"], Assertion::LessThan(0.1))
    // High distinctness for ID columns  
    .has_distinctness(vec!["user_id"], Assertion::GreaterThan(0.95))
    // Multi-column distinctness
    .has_distinctness(vec!["city", "state"], Assertion::Between(0.7, 0.9))
    .build()
```

Note: Distinctness is different from uniqueness. Uniqueness checks if all values are unique (no duplicates), while distinctness measures the ratio of distinct values to total values.

### Cardinality Constraints

#### `has_approx_count_distinct(column, assertion)`
Validates the approximate count of distinct values in a column using HyperLogLog algorithm. This is much faster than exact COUNT(DISTINCT) for large datasets while maintaining accuracy within 2-3% error margin.

```rust
Check::builder("cardinality_validation")
    // High cardinality check (e.g., user IDs)
    .has_approx_count_distinct("user_id", Assertion::GreaterThan(1000000.0))
    // Low cardinality check (e.g., country codes)
    .has_approx_count_distinct("country_code", Assertion::LessThan(200.0))
    // Moderate cardinality
    .has_approx_count_distinct("product_category", Assertion::Between(100.0, 1000.0))
    .build()
```

Use this constraint when:
- You need to validate cardinality on large datasets
- Exact count is not required (2-3% error margin is acceptable)
- Performance is a priority over precision

### Value Constraints

#### `is_contained_in(column, allowed_values)`
Ensures all values in a column are within a specified set.

```rust
Check::builder("values")
    .is_contained_in("status", vec!["active", "pending", "closed"])
    .build()
```

#### `is_non_negative(column)`
Verifies that all numeric values in a column are non-negative.

```rust
Check::builder("values")
    .is_non_negative("quantity")
    .build()
```

#### `has_data_type(column, data_type, threshold)`
Validates that a minimum percentage of values conform to a specific data type.

```rust
Check::builder("data_types")
    .has_data_type("age", DataType::Integer, 0.95)  // 95% integers
    .has_data_type("price", DataType::Float, 0.99)  // 99% numeric
    .build()
```

Supported data types:
- `DataType::Integer` - Whole numbers (positive/negative)
- `DataType::Float` - Decimal numbers (including scientific notation)
- `DataType::Boolean` - true/false values (various formats)
- `DataType::Date` - ISO date format (YYYY-MM-DD)
- `DataType::Timestamp` - Date and time values
- `DataType::String` - Any text value

### String Length Constraints

#### `has_min_length(column, min_length)`
Verifies that all string values meet a minimum length requirement (NULL values are ignored).

```rust
Check::builder("string_validation")
    .has_min_length("password", 8)
    .build()
```

#### `has_max_length(column, max_length)`
Ensures that all string values do not exceed a maximum length (NULL values are ignored).

```rust
Check::builder("string_validation")
    .has_max_length("username", 20)
    .build()
```

### Pattern Constraints

#### `has_pattern(column, regex, threshold)`
Checks that values match a regular expression pattern.

```rust
Check::builder("patterns")
    .has_pattern("email", r"^[^@]+@[^@]+\.[^@]+$", 0.99)
    .build()
```

#### `contains_url(column, threshold)`
Validates that column values contain valid URLs.

```rust
Check::builder("patterns")
    .contains_url("website", 0.95)
    .build()
```

#### `contains_email(column, threshold)`
Checks for valid email addresses in a column.

```rust
Check::builder("patterns")
    .contains_email("contact_email", 0.98)
    .build()
```

#### `contains_credit_card_number(column, threshold)`
Detects credit card numbers (useful for data security).

```rust
Check::builder("security")
    .contains_credit_card_number("description", 0.0)  // Should be 0%
    .build()
```

#### `contains_social_security_number(column, threshold)`
Detects social security numbers.

```rust
Check::builder("security")
    .contains_social_security_number("notes", 0.0)
    .build()
```

### Schema Constraints

#### `has_column_count(assertion)`
Validates the number of columns in a dataset.

```rust
Check::builder("schema_validation")
    .has_column_count(Assertion::Equals(15.0))  // Exactly 15 columns
    .has_column_count(Assertion::GreaterThanOrEqual(10.0))  // At least 10 columns
    .build()
```

### Statistical Constraints

#### Basic Statistics

##### `has_size(assertion)`
Validates the number of rows in the dataset.

```rust
Check::builder("statistics")
    .has_size(Assertion::GreaterThan(1000.0))
    .build()
```

##### `has_min(column, assertion)`
Checks the minimum value in a column.

```rust
Check::builder("statistics")
    .has_min("age", Assertion::GreaterThanOrEqual(0.0))
    .build()
```

##### `has_max(column, assertion)`
Validates the maximum value in a column.

```rust
Check::builder("statistics")
    .has_max("age", Assertion::LessThanOrEqual(150.0))
    .build()
```

##### `has_mean(column, assertion)`
Checks the average value in a column.

```rust
Check::builder("statistics")
    .has_mean("price", Assertion::Between(10.0, 100.0))
    .build()
```

##### `has_sum(column, assertion)`
Validates the sum of values in a column.

```rust
Check::builder("statistics")
    .has_sum("quantity", Assertion::Equals(50000.0))
    .build()
```

#### Advanced Statistics

##### `has_standard_deviation(column, assertion)`
Checks the standard deviation of values.

```rust
Check::builder("statistics")
    .has_standard_deviation("response_time", Assertion::LessThan(50.0))
    .build()
```

##### `has_correlation(column1, column2, assertion)`
Validates the correlation between two columns.

```rust
Check::builder("statistics")
    .has_correlation("temperature", "sales", Assertion::GreaterThan(0.7))
    .build()
```

##### `has_entropy(column, assertion)`
Checks the entropy (randomness) of values.

```rust
Check::builder("statistics")
    .has_entropy("user_id", Assertion::GreaterThan(0.8))
    .build()
```

##### `has_mutual_information(column1, column2, assertion)`
Validates mutual information between columns.

```rust
Check::builder("statistics")
    .has_mutual_information("category", "price", Assertion::GreaterThan(0.5))
    .build()
```

#### Quantile Constraints

##### `has_approx_quantile(column, quantile, assertion)`
Checks approximate quantile values using HyperLogLog algorithm. Fast but with small error margin.

```rust
Check::builder("performance")
    .has_approx_quantile("load_time", 0.95, Assertion::LessThan(2.0))
    .build()
```

##### `has_exact_quantile(column, quantile, assertion)`
Checks exact quantile values with no approximation. Slower but precise, ideal for small-medium datasets.

```rust
Check::builder("sla_validation")
    // Exact median salary check
    .has_exact_quantile("salary", 0.5, Assertion::Between(50000.0, 70000.0))
    // Exact 99th percentile for SLA compliance
    .has_exact_quantile("response_time_ms", 0.99, Assertion::LessThan(1000.0))
    .build()
```

### Custom SQL Constraints

#### `satisfies(sql_expression, message)`
Validates data using custom SQL expressions.

```rust
Check::builder("business_rules")
    .satisfies(
        "order_date <= ship_date", 
        Some("Orders cannot ship before order date")
    )
    .satisfies(
        "discount_amount <= total_amount * 0.5",
        Some("Discount cannot exceed 50% of total")
    )
    .build()
```

### Consistency Constraints

#### `has_consistency(columns, assertion)`
Checks consistency across multiple runs or datasets.

```rust
Check::builder("consistency")
    .has_consistency(vec!["total_revenue"], Assertion::Between(0.95, 1.05))
    .build()
```

## Assertion Types

Assertions are used to specify the expected values or ranges for statistical constraints:

- `Equals(value)` - Exact match
- `GreaterThan(value)` - Strictly greater than
- `GreaterThanOrEqual(value)` - Greater than or equal to
- `LessThan(value)` - Strictly less than
- `LessThanOrEqual(value)` - Less than or equal to
- `Between(min, max)` - Within range (inclusive)

## Combining Constraints

Multiple constraints can be combined in a single check:

```rust
let check = Check::builder("comprehensive_validation")
    // Completeness
    .is_complete("id")
    .has_completeness("email", 0.95)
    
    // Uniqueness
    .is_unique("id")
    
    // Patterns
    .has_pattern("email", r"^[^@]+@[^@]+\.[^@]+$", 0.99)
    
    // Statistics
    .has_min("age", Assertion::GreaterThanOrEqual(18.0))
    .has_max("age", Assertion::LessThanOrEqual(120.0))
    
    // Custom rules
    .satisfies("created_at <= updated_at", Some("Update time must be after creation"))
    
    .build();
```

## Performance Considerations

1. **Use the Query Optimizer**: When running multiple constraints, enable the optimizer for better performance.

2. **Group Related Constraints**: Constraints on the same table should be grouped in the same check when possible.

3. **Order Matters**: Place more selective constraints first to fail fast on data quality issues.

4. **Cache Statistics**: For frequently validated datasets, consider caching basic statistics.

## Error Handling

Each constraint returns a `ConstraintResult` with:
- `status`: Success, Failure, or Error
- `metric`: The computed metric value (if applicable)
- `message`: Detailed information about failures

## Extending Constraints

To create custom constraints:

1. Implement the `Constraint` trait
2. Define the `evaluate()` method
3. Return appropriate `ConstraintResult`

Example:
```rust
#[async_trait]
impl Constraint for CustomConstraint {
    async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult> {
        // Your validation logic here
    }
}
```