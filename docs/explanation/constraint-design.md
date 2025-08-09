# Understanding Constraint Design

## Overview

This document explains the design principles behind Term's constraint system, exploring why constraints are structured the way they are and how they achieve both performance and flexibility.

## The Constraint Philosophy

Term's constraint system is built around a core principle: **data quality validation should be composable, performant, and predictable**. This philosophy drives every design decision in the constraint architecture.

### Why Constraints Matter

Data quality issues cascade through systems, causing:
- Incorrect business decisions based on flawed analytics
- System failures when downstream processes can't handle invalid data
- Security vulnerabilities from unvalidated input
- Compliance violations with data regulations

Term's constraints act as a firewall against these issues, catching problems at the source.

## Design Principles

### 1. Composability Through Traits

Every constraint implements the same `Constraint` trait, making them composable building blocks:

```rust
#[async_trait]
pub trait Constraint: Send + Sync {
    async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult>;
}
```

**Why this matters:** You can combine any constraints together without worrying about compatibility. A completeness check works the same way as a complex statistical analysis in the validation framework.

### 2. Lazy Evaluation and Query Optimization

Constraints don't immediately execute when created. Instead, they build up a description of the validation work to be done, which Term's query optimizer can then combine and optimize.

**The benefit:** Multiple constraints on the same table can be merged into a single SQL query, reducing I/O and improving performance dramatically.

### 3. Threshold-Based Validation

Rather than requiring 100% perfection, Term uses thresholds (0.0 to 1.0) for most validations:

```rust
// 95% of emails must be valid - allows for some data quality issues
FormatConstraint::email("email", 0.95)
```

**Why thresholds:** Real-world data is messy. Requiring 100% compliance often results in false positives that make validation systems unusable. Thresholds let you set realistic expectations.

### 4. Result-Oriented Error Reporting

Constraints return structured results, not just pass/fail:

```rust
pub struct ConstraintResult {
    pub status: ConstraintStatus,  // Success/Failure/Error
    pub metric: Option<f64>,       // Actual measured value
    pub message: String,           // Human-readable explanation
}
```

**The advantage:** You get actionable feedback. Instead of "validation failed," you see "email validation: 87% valid (expected 95%), found 156 invalid addresses."

## Constraint Categories and Their Trade-offs

### Completeness Constraints

**Purpose:** Detect missing data (null values)  
**Trade-off:** Fast to compute vs. limited scope (only detects nulls, not empty strings)  
**When to use:** Essential fields that must be present

### Uniqueness Constraints

**Purpose:** Detect duplicate values  
**Trade-off:** Critical for data integrity vs. expensive on large datasets  
**Optimization:** Uses hash-based algorithms and early termination for performance

### Format Constraints

**Purpose:** Validate data structure (emails, phone numbers, etc.)  
**Trade-off:** Catches many common issues vs. regex complexity can impact performance  
**Design choice:** Pre-compiled regex patterns with caching for speed

### Statistical Constraints

**Purpose:** Validate numeric properties (min, max, mean, etc.)  
**Trade-off:** Rich validation capabilities vs. requires numeric data  
**Innovation:** Supports both exact and approximate algorithms based on performance needs

## Why SQL-Based Validation

Term generates SQL queries for constraint evaluation rather than loading data into memory. This design choice has several implications:

### Advantages

1. **Leverages database optimizations:** Query planners, indexes, and columnar storage
2. **Handles large datasets:** No memory limitations from dataset size
3. **Consistent performance:** Predictable execution patterns
4. **Familiar semantics:** SQL behavior is well-understood

### Limitations

1. **Database dependency:** Requires SQL-compatible data sources
2. **Complex aggregations:** Some operations are easier in procedural code
3. **Custom logic:** Business rules may not map cleanly to SQL

## The Async Design

Every constraint evaluation is async, even when the underlying operation could be synchronous.

### Why Async Everywhere?

1. **Future compatibility:** Database connections, file I/O, and network operations benefit from async
2. **Consistent API:** No mixing of sync/async constraint types
3. **Composability:** Async constraints can be combined with async data sources seamlessly
4. **Performance:** Allows concurrent evaluation of independent constraints

### The Cost

- Slightly more complex API with `await` everywhere
- Runtime dependency on async executor (Tokio)
- Learning curve for developers unfamiliar with async Rust

The Term team believes this trade-off is worthwhile for the performance and scalability benefits.

## Error Handling Philosophy

Term distinguishes between three types of constraint outcomes:

### Success
The constraint passed validation. The data meets quality expectations.

### Failure
The constraint detected a data quality issue. This is expected behavior - the point of validation is to catch these issues.

### Error
Something went wrong during validation itself (network failure, SQL syntax error, etc.). This indicates a problem with the validation process, not the data.

**Why this distinction matters:** Your application can handle failures differently from errors. Failures might trigger data cleaning processes, while errors might indicate infrastructure problems.

## Performance Optimization Strategies

### Query Batching

Term can combine multiple constraints into single SQL queries:

```sql
-- Instead of multiple queries:
SELECT COUNT(*) FROM data WHERE email IS NOT NULL;
SELECT COUNT(DISTINCT email) FROM data;
SELECT AVG(age) FROM data;

-- Term generates:
SELECT 
  COUNT(*) as total_rows,
  COUNT(email) as email_count,
  COUNT(DISTINCT email) as unique_emails,
  AVG(age) as avg_age
FROM data;
```

### Early Termination

For uniqueness checks, Term can stop as soon as a duplicate is found rather than scanning the entire dataset.

### Approximate Algorithms

For large datasets, Term offers approximate constraints (using HyperLogLog, sketching) that trade small accuracy losses for major performance gains.

## Extensibility

The constraint system is designed for extension:

### Custom Constraints

Implement the `Constraint` trait for domain-specific validation:

```rust
pub struct BusinessHoursConstraint {
    timestamp_column: String,
    timezone: String,
}

#[async_trait]
impl Constraint for BusinessHoursConstraint {
    async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult> {
        // Custom validation logic
    }
}
```

### Builder Pattern Integration

Custom constraints integrate with the builder API:

```rust
impl CheckBuilder {
    pub fn validates_business_hours(self, column: &str, timezone: &str) -> Self {
        self.constraint(BusinessHoursConstraint::new(column, timezone))
    }
}
```

## Future Evolution

The constraint system is designed to evolve:

### Planned Enhancements

1. **Machine learning integration:** Anomaly detection constraints
2. **Streaming validation:** Real-time constraint evaluation
3. **Cross-table constraints:** Referential integrity and consistency checks
4. **Adaptive thresholds:** Self-tuning validation parameters

### Backward Compatibility

Term maintains API compatibility through:
- Trait-based design allows adding methods without breaking existing code
- Builder patterns hide implementation complexity
- Semantic versioning for breaking changes

## Conclusion

Term's constraint design balances several competing concerns:

- **Performance vs. Flexibility:** SQL-based evaluation with extensible trait system
- **Accuracy vs. Speed:** Threshold-based validation with approximate algorithms available
- **Simplicity vs. Power:** Builder API hides complexity while exposing full functionality
- **Safety vs. Usability:** Strong typing with ergonomic async API

These design choices make Term suitable for both quick data quality checks and production-scale validation pipelines. The constraint system serves as the foundation for reliable, performant data validation in modern applications.

## See Also

- [Architecture Overview](./architecture.md) - How constraints fit into Term's overall design
- [Analyzer Architecture](./analyzer-architecture.md) - The two-phase computation model
- [How to Write Custom Constraints](../how-to/write-custom-constraints.md) - Extending the constraint system
- [Constraint Reference](../reference/constraints.md) - Complete API documentation