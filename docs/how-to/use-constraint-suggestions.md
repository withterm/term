# How to Use Constraint Suggestions

This guide shows you how to use Term's constraint suggestion system to automatically discover appropriate data quality checks for your datasets.

## When to Use This

Use constraint suggestions when:
- You're working with a new dataset and don't know what to validate
- You want to discover data quality patterns automatically
- You need to establish baseline quality metrics
- You're migrating from manual data checks to automated validation

## Prerequisites

- Term installed with the analyzer features
- A dataset loaded into a SessionContext
- Basic understanding of Term's validation concepts

## Basic Usage

### Step 1: Profile Your Data

First, use the ColumnProfiler to analyze your column:

```rust
use term_guard::analyzers::ColumnProfiler;
use datafusion::prelude::*;

// Assume you have a SessionContext with data loaded
let ctx = SessionContext::new();
ctx.register_csv("sales", "sales_data.csv", CsvReadOptions::new()).await?;

// Create a profiler and analyze a column
let profiler = ColumnProfiler::new();
let profile = profiler.profile_column(&ctx, "sales", "customer_email").await?;
```

### Step 2: Configure the Suggestion Engine

Create a SuggestionEngine with the rules you want to apply:

```rust
use term_guard::analyzers::{
    SuggestionEngine, CompletenessRule, UniquenessRule, 
    PatternRule, RangeRule, DataTypeRule, CardinalityRule
};

let engine = SuggestionEngine::new()
    .add_rule(Box::new(CompletenessRule::new()))
    .add_rule(Box::new(UniquenessRule::new()))
    .add_rule(Box::new(PatternRule::new()))
    .add_rule(Box::new(RangeRule::new()))
    .add_rule(Box::new(DataTypeRule::new()))
    .add_rule(Box::new(CardinalityRule::new()))
    .confidence_threshold(0.7); // Only show suggestions with 70%+ confidence
```

### Step 3: Get Suggestions

Run the engine on your profile to get constraint suggestions:

```rust
let suggestions = engine.suggest_constraints(&profile);

for suggestion in suggestions {
    println!("Suggested Check: {}", suggestion.check_type);
    println!("  Confidence: {:.0}%", suggestion.confidence * 100.0);
    println!("  Rationale: {}", suggestion.rationale);
    println!("  Priority: {:?}", suggestion.priority);
    
    // Access parameters if needed
    for (param_name, param_value) in &suggestion.parameters {
        println!("  Parameter {}: {:?}", param_name, param_value);
    }
}
```

## Advanced Configuration

### Custom Rule Thresholds

Some rules support custom thresholds:

```rust
// Customize when to suggest completeness constraints
let completeness_rule = CompletenessRule::with_thresholds(
    0.99,  // Suggest is_complete for >99% completeness
    0.95   // Suggest has_completeness for >95% completeness
);

// Customize uniqueness detection
let uniqueness_rule = UniquenessRule::with_thresholds(
    0.98,  // Suggest is_unique for >98% unique values
    0.90   // Suggest monitoring for >90% uniqueness
);

// Customize cardinality thresholds
let cardinality_rule = CardinalityRule::with_thresholds(
    100,   // Categorical threshold
    20     // Low cardinality threshold
);
```

### Filtering Suggestions

Control the number and quality of suggestions:

```rust
let engine = SuggestionEngine::new()
    .confidence_threshold(0.8)        // Higher confidence requirement
    .max_suggestions_per_column(5);   // Limit suggestions per column
```

### Batch Processing

Analyze multiple columns at once:

```rust
// Profile multiple columns
let profiles = vec![
    profiler.profile_column(&ctx, "sales", "customer_id").await?,
    profiler.profile_column(&ctx, "sales", "product_code").await?,
    profiler.profile_column(&ctx, "sales", "amount").await?,
];

// Get suggestions for all columns
let all_suggestions = engine.suggest_constraints_batch(&profiles);

for (column_name, suggestions) in all_suggestions {
    println!("\nColumn: {}", column_name);
    for suggestion in suggestions {
        println!("  - {} (confidence: {:.0}%)", 
                 suggestion.check_type, 
                 suggestion.confidence * 100.0);
    }
}
```

## Converting Suggestions to Checks

Once you have suggestions, convert them into actual validation checks:

```rust
use term_guard::prelude::*;

// Based on suggestions, create validation checks
let mut check_builder = Check::builder("Suggested validations");

for suggestion in suggestions {
    match suggestion.check_type.as_str() {
        "is_complete" => {
            check_builder = check_builder.is_complete(&suggestion.column);
        },
        "is_unique" => {
            check_builder = check_builder.is_unique(&suggestion.column);
        },
        "has_completeness" => {
            if let Some(ConstraintParameter::Float(threshold)) = 
                suggestion.parameters.get("threshold") {
                check_builder = check_builder
                    .has_completeness(&suggestion.column, *threshold);
            }
        },
        "matches_email_pattern" => {
            check_builder = check_builder
                .has_pattern(&suggestion.column, r"^[^@]+@[^@]+\.[^@]+$", 0.95);
        },
        // Add more mappings as needed
        _ => {}
    }
}

let check = check_builder.build();
```

## Rule Types and Their Suggestions

### CompletenessRule
- `is_complete`: Column has >98% non-null values
- `has_completeness`: Column has 90-98% non-null values
- `monitor_completeness`: Column has <50% non-null values (critical)

### UniquenessRule
- `is_unique`: Column has >95% unique values
- `has_uniqueness`: Column has 80-95% unique values
- `primary_key_candidate`: Column name contains "id" or "key" with high uniqueness

### PatternRule
- `matches_email_pattern`: Values match email format
- `matches_date_pattern`: Values match date format
- `matches_phone_pattern`: Values match phone number format

### RangeRule
- `has_min`: Suggests minimum value constraint
- `has_max`: Suggests maximum value constraint
- `is_positive`: All values are non-negative
- `has_no_outliers`: Based on P99 quantile

### DataTypeRule
- `has_consistent_type`: Column has consistent data type
- `has_data_type`: Validates specific data type
- `validate_data_type`: Type validation needed (unknown type)

### CardinalityRule
- `is_categorical`: Low cardinality (<10 distinct values)
- `is_in_set`: Specific valid values from histogram
- `has_max_cardinality`: Medium cardinality monitoring
- `monitor_cardinality`: High cardinality warning

## Best Practices

1. **Start with profiling**: Always profile your data before running suggestions
2. **Review confidence scores**: Higher confidence means more reliable suggestions
3. **Consider priority levels**: Critical > High > Medium > Low
4. **Validate suggestions**: Review suggestions before implementing them
5. **Iterate**: Run suggestions periodically as your data evolves

## Troubleshooting

### No suggestions generated
- Check if your confidence threshold is too high
- Ensure the column has enough data for meaningful analysis
- Verify the profiler completed all passes successfully

### Too many suggestions
- Increase the confidence threshold
- Reduce max_suggestions_per_column
- Focus on specific rule types relevant to your use case

### Performance considerations
- Profile columns in parallel for better performance
- Use sampling for very large datasets
- Cache profiles if analyzing the same data repeatedly

## See Also

- [Column Profiler Reference](../reference/analyzers.md#column-profiler)
- [Analyzer Architecture](../explanation/analyzer-architecture.md)
- [Writing Custom Constraints](./write-custom-constraints.md)