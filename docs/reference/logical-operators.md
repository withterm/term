# Logical Operators and Unified Constraints

This document describes the core infrastructure for logical operators and unified constraints in Term, which enables flexible multi-column constraint evaluation.

## Overview

The logical operators infrastructure provides:

- **Logical Operators**: Combine boolean results from multiple column evaluations
- **Column Specifications**: Handle both single and multiple column constraints uniformly  
- **Constraint Options**: Builder pattern for configuring constraints
- **Unified Constraints**: Base traits for implementing consolidated constraint types

## Logical Operators

Term supports five logical operators for combining constraint results across multiple columns:

```rust
use term_guard::core::LogicalOperator;

// All columns must satisfy the constraint (AND logic)
let all = LogicalOperator::All;

// At least one column must satisfy (OR logic)
let any = LogicalOperator::Any;

// Exactly N columns must satisfy
let exactly_two = LogicalOperator::Exactly(2);

// At least N columns must satisfy
let at_least_three = LogicalOperator::AtLeast(3);

// At most N columns must satisfy
let at_most_one = LogicalOperator::AtMost(1);
```

### Evaluation Examples

```rust
let results = vec![true, false, true, false];

LogicalOperator::All.evaluate(&results);         // false (not all true)
LogicalOperator::Any.evaluate(&results);         // true  (at least one true)
LogicalOperator::Exactly(2).evaluate(&results);  // true  (exactly 2 true)
LogicalOperator::AtLeast(2).evaluate(&results);  // true  (2 >= 2)
LogicalOperator::AtMost(2).evaluate(&results);   // true  (2 <= 2)
```

### Edge Cases

- **Empty results**: 
  - `All` returns `true` (vacuous truth)
  - `Any` returns `false` (no elements satisfy)
  - `Exactly(0)` returns `true`
  - `AtLeast(0)` returns `true`
  - `AtMost(n)` returns `true` for any n

## Column Specifications

The `ColumnSpec` enum provides a unified way to handle both single and multiple columns:

```rust
use term_guard::core::ColumnSpec;

// Single column
let single = ColumnSpec::Single("user_id".to_string());

// Multiple columns
let multiple = ColumnSpec::Multiple(vec!["email".to_string(), "phone".to_string()]);

// Automatic conversions
let from_str = ColumnSpec::from("user_id");                    // Single
let from_vec_single = ColumnSpec::from(vec!["user_id"]);       // Single
let from_vec_multi = ColumnSpec::from(vec!["email", "phone"]); // Multiple
let from_array = ColumnSpec::from(["col1", "col2", "col3"]);   // Multiple
```

### Methods

```rust
// Get all columns as a vector
spec.as_vec();        // Vec<&str>

// Check properties
spec.len();           // Number of columns
spec.is_empty();      // True if no columns
spec.is_single();     // True if single column
spec.is_multiple();   // True if multiple columns

// Convert to multiple (always returns Vec<String>)
spec.to_multiple();
```

## Constraint Options

The `ConstraintOptions` struct provides a builder pattern for configuring constraints:

```rust
use term_guard::core::ConstraintOptions;

let options = ConstraintOptions::new()
    .with_operator(LogicalOperator::Any)
    .with_threshold(0.95)
    .with_flag("case_sensitive", true)
    .with_option("pattern", "[A-Z]+");

// Access options with defaults
let operator = options.operator_or(LogicalOperator::All);  // Returns Any
let threshold = options.threshold_or(1.0);                  // Returns 0.95
let case_sensitive = options.flag("case_sensitive");        // Returns true
let pattern = options.option("pattern");                    // Returns Some("[A-Z]+")
```

## Unified Constraint Trait

The `UnifiedConstraint` trait provides the foundation for implementing constraints that work with multiple columns:

```rust
use term_guard::core::{UnifiedConstraint, ColumnSpec, LogicalOperator};

#[async_trait]
pub trait UnifiedConstraint: Constraint {
    /// Returns the column specification
    fn column_spec(&self) -> &ColumnSpec;
    
    /// Returns the logical operator (optional)
    fn logical_operator(&self) -> Option<LogicalOperator>;
    
    /// Evaluates a single column
    async fn evaluate_column(
        &self, 
        ctx: &SessionContext, 
        column: &str
    ) -> Result<ConstraintResult>;
    
    /// Default implementation handles multi-column logic
    async fn evaluate_unified(&self, ctx: &SessionContext) -> Result<ConstraintResult> {
        // Automatically handles single vs multiple columns
        // Applies logical operator to combine results
    }
}
```

## Example: Implementing a Unified Constraint

Here's a complete example of implementing a unified constraint:

```rust
use async_trait::async_trait;
use term_guard::core::*;

#[derive(Debug, Clone)]
struct PositiveValueConstraint {
    columns: ColumnSpec,
    operator: LogicalOperator,
    strict: bool,  // > 0 if true, >= 0 if false
}

impl PositiveValueConstraint {
    pub fn new(columns: impl Into<ColumnSpec>, options: ConstraintOptions) -> Self {
        Self {
            columns: columns.into(),
            operator: options.operator_or(LogicalOperator::All),
            strict: options.flag("strict"),
        }
    }
}

#[async_trait]
impl UnifiedConstraint for PositiveValueConstraint {
    fn column_spec(&self) -> &ColumnSpec {
        &self.columns
    }
    
    fn logical_operator(&self) -> Option<LogicalOperator> {
        Some(self.operator)
    }
    
    async fn evaluate_column(
        &self,
        ctx: &SessionContext,
        column: &str,
    ) -> Result<ConstraintResult> {
        // Evaluate single column
        let comparison = if self.strict { "> 0" } else { ">= 0" };
        let sql = format!(
            "SELECT COUNT(CASE WHEN {} {} THEN 1 END) as positive_count,
                    COUNT(*) as total_count
             FROM data WHERE {} IS NOT NULL",
            column, comparison, column
        );
        
        // Execute query and return result...
    }
}

// Usage
let constraint = PositiveValueConstraint::new(
    vec!["revenue", "profit", "growth"],
    ConstraintOptions::new()
        .with_operator(LogicalOperator::AtLeast(2))
        .with_flag("strict", true)
);
```

## Migration Path

When migrating existing constraints to use the unified infrastructure:

1. Change single column constraints to use `ColumnSpec::Single`
2. Consolidate similar constraints into one implementation
3. Add logical operator support where appropriate
4. Use `ConstraintOptions` for configuration

### Before
```rust
// Separate constraints for different cases
let c1 = CompletenessConstraint::new("email", 0.95);
let c2 = MultipleCompletenessConstraint::all_complete(vec!["a", "b"]);
let c3 = MultipleCompletenessConstraint::any_complete(vec!["x", "y"]);
```

### After
```rust
// Single unified constraint with options
let c1 = UnifiedCompletenessConstraint::new(
    "email",
    ConstraintOptions::new().with_threshold(0.95)
);

let c2 = UnifiedCompletenessConstraint::new(
    vec!["a", "b"],
    ConstraintOptions::new().with_operator(LogicalOperator::All)
);

let c3 = UnifiedCompletenessConstraint::new(
    vec!["x", "y"],
    ConstraintOptions::new().with_operator(LogicalOperator::Any)
);
```

## Performance Considerations

- Single column constraints bypass multi-column logic for efficiency
- Results are evaluated lazily - stops early when possible
- Column evaluation can be parallelized in future versions
- Metrics are averaged across columns by default

## Next Steps

This infrastructure enables the consolidation of constraint families:

1. **Completeness**: `is_complete`, `has_completeness`, `are_complete`, `are_any_complete`
2. **Statistical**: `MinConstraint`, `MaxConstraint`, `MeanConstraint`, etc.
3. **String Length**: `MinLengthConstraint`, `MaxLengthConstraint`
4. **Pattern/Format**: `PatternConstraint`, `EmailConstraint`, `UrlConstraint`
5. **Uniqueness**: Various uniqueness and distinctness constraints

Each family can be unified into a single, flexible implementation using this infrastructure.