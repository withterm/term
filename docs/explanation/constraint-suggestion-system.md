# Understanding Term's Constraint Suggestion System

<!-- 
This is an EXPLANATION document following Diátaxis principles.
It explains the concepts behind automatic constraint generation.
-->

## The Challenge of Data Quality Rules

Writing comprehensive data quality rules is tedious and error-prone. Teams often:

1. **Under-specify constraints**: Missing important checks that could prevent issues
2. **Over-specify constraints**: Creating brittle rules that break with valid data evolution  
3. **Use inconsistent patterns**: Different developers write similar rules differently
4. **Miss edge cases**: Not considering nulls, outliers, or rare values

Term's constraint suggestion system addresses these challenges by automatically analyzing data profiles and recommending appropriate validation rules.

## Design Philosophy

### Evidence-Based Suggestions

Every suggestion is grounded in observed data characteristics:

```rust
// We don't suggest is_complete unless data shows it
if null_percentage < 0.02 {  // Less than 2% nulls
    suggest("is_complete", confidence: 0.98)
}
```

This prevents suggesting constraints that would immediately fail.

### Confidence Scoring

Each suggestion includes a confidence score based on:

1. **Sample size**: More data = higher confidence
2. **Consistency**: Uniform patterns = higher confidence  
3. **Edge cases**: Presence of outliers = lower confidence

```rust
confidence = base_confidence 
    * sample_size_factor 
    * consistency_factor
    * edge_case_penalty
```

### Priority Levels

Suggestions are prioritized to help teams focus:

- **Critical**: Data quality issues that will likely cause failures
- **High**: Important constraints for data integrity
- **Medium**: Useful constraints for monitoring
- **Low**: Optional constraints for completeness

## The Suggestion Pipeline

### 1. Profile Analysis

The system analyzes column profiles looking for patterns:

```rust
struct ProfileAnalysis {
    has_nulls: bool,
    null_pattern: NullPattern,      // Random, systematic, increasing
    cardinality_type: CardinalityType, // Unique, low, medium, high
    distribution_shape: Shape,       // Normal, skewed, bimodal
    anomalies: Vec<Anomaly>,
}
```

### 2. Rule Matching

Each `ConstraintSuggestionRule` examines the profile:

```rust
trait ConstraintSuggestionRule {
    fn examine(&self, profile: &ColumnProfile) -> Vec<Suggestion>;
}
```

Rules are specialized for different aspects:
- **CompletenessRule**: Null patterns
- **UniquenessRule**: Cardinality patterns
- **RangeRule**: Numeric boundaries
- **PatternRule**: String formats
- **DataTypeRule**: Type consistency

### 3. Conflict Resolution

Multiple rules might suggest conflicting constraints:

```rust
// Rule 1: is_complete (no nulls allowed)
// Rule 2: has_completeness >= 0.98 (some nulls ok)

// Resolution: Choose the more permissive constraint
// that still maintains data quality
```

## Intelligent Pattern Detection

### Null Patterns

The system distinguishes between different null patterns:

1. **Random nulls**: Suggests `has_completeness` with threshold
2. **Systematic nulls**: Suggests investigating data pipeline
3. **Increasing nulls**: Suggests `monitor_completeness` (critical)

### Cardinality Analysis

Cardinality reveals the nature of the column:

```rust
match cardinality_ratio {
    r if r > 0.95 => suggest_unique_constraint(),
    r if r > 0.80 => suggest_uniqueness_monitoring(),
    r if distinct < 10 => suggest_categorical_constraint(),
    _ => suggest_cardinality_monitoring(),
}
```

### Distribution Shapes

For numeric columns, distribution shapes inform constraints:

- **Normal distribution**: Suggest mean/stddev monitoring
- **Skewed distribution**: Suggest percentile-based rules
- **Bimodal distribution**: Suggest investigating data segments

## Domain-Specific Intelligence

### ID Column Detection

The system recognizes common ID patterns:

```rust
fn is_likely_id_column(name: &str, profile: &ColumnProfile) -> bool {
    let name_indicates_id = name.ends_with("_id") 
        || name.ends_with("_key")
        || name == "id";
    
    let data_indicates_id = profile.basic_stats.uniqueness > 0.95
        && profile.data_type.is_integer_or_string();
    
    name_indicates_id && data_indicates_id
}
```

### Business Rule Patterns

Common business patterns are recognized:

```rust
// Percentage columns
if name.contains("percentage") || name.ends_with("_pct") {
    if all_values_between(0, 100) {
        suggest("is_between", min: 0, max: 100)
    }
}

// Status columns  
if name.contains("status") && distinct_count < 10 {
    suggest("is_in_set", values: observed_values)
}
```

## Adaptive Suggestions

### Data Evolution Awareness

Suggestions consider potential data evolution:

```rust
// If current max is 95 but increasing trend observed
if trend.is_increasing() {
    buffer = calculate_growth_buffer(trend);
    suggest("has_max", value: current_max + buffer)
} else {
    suggest("has_max", value: current_max * 1.1) // 10% buffer
}
```

### Seasonal Patterns

For time-series data, seasonal patterns affect suggestions:

```rust
if detect_seasonality(&time_series) {
    // Don't suggest tight bounds that break seasonally
    use_percentile_based_bounds()
} else {
    use_absolute_bounds()
}
```

## The Psychology of Suggestions

### Progressive Disclosure

Suggestions are ordered to avoid overwhelming users:

1. Start with critical data quality issues
2. Show high-confidence suggestions next
3. Present monitoring suggestions last

### Actionable Rationale

Each suggestion includes a clear rationale:

```rust
Suggestion {
    check_type: "is_complete",
    rationale: "Column has 0 nulls in 1M rows sampled. 
               Enforcing completeness prevents future null introduction.",
    evidence: "null_count: 0, sample_size: 1000000",
}
```

## Anti-Patterns We Avoid

### Over-Fitting to Current Data

We don't suggest constraints that exactly match current data:

```rust
// Bad: Suggests exact current bounds
suggest("is_between", min: 23.7, max: 94.3)

// Good: Rounds to reasonable boundaries  
suggest("is_between", min: 20, max: 100)
```

### Brittle Type Constraints

We avoid overly specific type constraints:

```rust
// Bad: Requires exact format
suggest("matches_pattern", pattern: "^\\d{4}-\\d{2}-\\d{2}$")

// Good: Allows format variations
suggest("is_date", formats: ["YYYY-MM-DD", "MM/DD/YYYY"])
```

### Redundant Suggestions

We detect and eliminate redundant constraints:

```rust
// If suggesting is_complete, don't also suggest:
// - has_completeness >= 1.0
// - has_no_nulls
// These are equivalent
```

## Integration with Validation Workflow

### From Suggestion to Implementation

```rust
// 1. Generate suggestions
let suggestions = engine.suggest_constraints(&profile);

// 2. Review and customize
let approved = suggestions
    .filter(|s| s.confidence > 0.9)
    .map(|s| customize_threshold(s));

// 3. Generate validation code
let checks = generate_checks(approved);

// 4. Create validation suite
let suite = ValidationSuite::builder()
    .add_checks(checks)
    .build();
```

### Continuous Improvement

Suggestions can be refined based on validation results:

```rust
// Track which suggestions were accepted/rejected
// Adjust confidence scoring based on feedback
// Learn domain-specific patterns
```

## Future Directions

### Machine Learning Enhancement

We're exploring ML to improve suggestions:

1. **Pattern learning**: Discover domain-specific patterns
2. **Threshold optimization**: Learn optimal bounds from historical data
3. **Anomaly prediction**: Predict future data quality issues

### Cross-Column Intelligence

Future versions will consider column relationships:

```rust
// If order_date and ship_date columns exist
suggest_relationship_constraint(
    "ship_date >= order_date"
)
```

### Semantic Understanding

Integration with business glossaries for better suggestions:

```rust
// If column semantics indicate currency
if semantic_type == "currency" {
    suggest_decimal_precision(2)
    suggest_non_negative()
}
```

## Conclusion

Term's constraint suggestion system transforms the tedious task of writing data quality rules into a guided, evidence-based process. By analyzing actual data characteristics and applying intelligent heuristics, it helps teams create comprehensive validation suites that are neither too strict nor too lenient.

The key insight is that good constraints emerge from understanding data patterns, not from guessing. By providing smart defaults while allowing customization, we enable teams to achieve robust data quality with minimal effort.

## Related Topics

- [Constraint Suggestions Reference](../reference/analyzers.md#constraint-suggestion-system) - API details
- [How to Use Suggestions](../how-to/use-constraint-suggestions.md) - Practical guide
- [Column Profiler](../reference/profiler.md) - Profile generation
- [Validation Patterns](validation-patterns.md) - Best practices