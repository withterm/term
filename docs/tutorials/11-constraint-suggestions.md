# Tutorial: Automatic Constraint Generation

## Introduction

In this tutorial, you'll learn how to use Term's constraint suggestion system to automatically generate data quality checks based on your data's characteristics. This feature analyzes column profiles and recommends appropriate validation rules with confidence scores.

## What You'll Learn

- How constraint suggestion works
- How to profile data for suggestion generation
- How to customize suggestion rules
- How to evaluate and apply suggestions
- How to build adaptive validation pipelines

## Prerequisites

Before starting, you should:
- Understand Term's validation framework
- Have completed the [Column Profiling tutorial](./04-column-profiling.md)
- Have sample data representing "good" quality

## Step 1: Understanding Constraint Suggestions

The suggestion system analyzes data profiles and applies heuristic rules to recommend constraints:

```rust
use term_guard::analyzers::{ColumnProfiler, SuggestionEngine};
use term_guard::analyzers::suggestions::*;

// Suggestion workflow:
// 1. Profile your data to understand its characteristics
// 2. Apply rule-based heuristics to generate suggestions
// 3. Review suggestions with confidence scores
// 4. Convert high-confidence suggestions to validation checks
```

## Step 2: Your First Constraint Suggestions

Let's generate suggestions for a customer dataset:

```rust
use term_guard::prelude::*;
use term_guard::analyzers::{ColumnProfiler, SuggestionEngine};
use term_guard::analyzers::suggestions::*;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    // Load your "good" reference data
    let ctx = SessionContext::new();
    ctx.register_csv(
        "customers",
        "data/customers_clean.csv",  // Use clean, validated data
        CsvReadOptions::default()
    ).await?;
    
    // Profile the email column
    let profiler = ColumnProfiler::builder()
        .cardinality_threshold(100)
        .build();
    
    let email_profile = profiler.profile_column(&ctx, "customers", "email").await?;
    
    // Generate suggestions
    let engine = SuggestionEngine::new()
        .add_rule(Box::new(CompletenessRule::new()))
        .add_rule(Box::new(UniquenessRule::new()))
        .add_rule(Box::new(PatternRule::new()))
        .add_rule(Box::new(DataTypeRule::new()));
    
    let suggestions = engine.suggest_constraints(&email_profile);
    
    // Review suggestions
    println!("Suggested constraints for 'email' column:");
    println!("{:=^60}", "");
    
    for suggestion in suggestions {
        println!("\n📌 {}", suggestion.check_type);
        println!("   Confidence: {:.0}%", suggestion.confidence * 100.0);
        println!("   Rationale: {}", suggestion.rationale);
        println!("   Priority: {:?}", suggestion.priority);
        
        if suggestion.confidence > 0.9 {
            println!("   ✅ High confidence - recommended for implementation");
        }
    }
    
    Ok(())
}
```

## Step 3: Built-in Suggestion Rules

Term provides several built-in rules for common patterns:

```rust
async fn explore_suggestion_rules(ctx: &SessionContext) -> Result<()> {
    let profiler = ColumnProfiler::builder().build();
    
    // Profile multiple columns
    let profiles = vec![
        profiler.profile_column(ctx, "orders", "order_id").await?,
        profiler.profile_column(ctx, "orders", "customer_email").await?,
        profiler.profile_column(ctx, "orders", "amount").await?,
        profiler.profile_column(ctx, "orders", "status").await?,
    ];
    
    // Different rules for different patterns
    let rules: Vec<Box<dyn ConstraintSuggestionRule>> = vec![
        // Completeness Rule: Suggests is_complete or has_completeness
        Box::new(CompletenessRule::new()
            .with_threshold(0.01)),  // Suggest if <1% nulls
        
        // Uniqueness Rule: Suggests is_unique or has_uniqueness
        Box::new(UniquenessRule::new()
            .with_threshold(0.95)),  // Suggest if >95% unique
        
        // Pattern Rule: Detects and suggests format validators
        Box::new(PatternRule::new()
            .with_min_samples(100)
            .with_confidence_threshold(0.8)),
        
        // Data Type Rule: Suggests type constraints
        Box::new(DataTypeRule::new()),
        
        // Range Rule: Suggests min/max/between constraints
        Box::new(RangeRule::new()
            .with_outlier_detection(true)),
        
        // Cardinality Rule: Suggests is_contained_in for categoricals
        Box::new(CardinalityRule::new()
            .with_max_categories(20)),
        
        // Statistical Rule: Suggests mean/stddev constraints
        Box::new(StatisticalRule::new()
            .with_stability_threshold(0.1)),  // 10% variation tolerance
    ];
    
    // Apply all rules to each profile
    for profile in profiles {
        println!("\n📊 Suggestions for column: {}", profile.column_name);
        
        for rule in &rules {
            let suggestions = rule.apply(&profile);
            
            for suggestion in suggestions {
                println!("  • {} ({}): {}", 
                         suggestion.check_type,
                         rule.name(),
                         suggestion.rationale);
            }
        }
    }
    
    Ok(())
}
```

## Step 4: Custom Suggestion Rules

Create domain-specific suggestion rules:

```rust
use term_guard::analyzers::suggestions::{
    ConstraintSuggestionRule, SuggestedConstraint, ConstraintParameter
};

// Custom rule for business identifiers
struct BusinessIdRule {
    id_patterns: HashMap<String, String>,
}

impl BusinessIdRule {
    fn new() -> Self {
        let mut patterns = HashMap::new();
        patterns.insert("customer_id".to_string(), r"^CUS-\d{6}$".to_string());
        patterns.insert("order_id".to_string(), r"^ORD-\d{8}$".to_string());
        patterns.insert("product_sku".to_string(), r"^[A-Z]{3}-\d{4}$".to_string());
        
        Self { id_patterns }
    }
}

impl ConstraintSuggestionRule for BusinessIdRule {
    fn apply(&self, profile: &ColumnProfile) -> Vec<SuggestedConstraint> {
        let mut suggestions = Vec::new();
        
        // Check if column name matches known ID patterns
        for (id_type, pattern) in &self.id_patterns {
            if profile.column_name.contains(id_type) {
                // Sample values to verify pattern
                let matching = profile.basic_stats.sample_values.iter()
                    .filter(|v| regex::Regex::new(pattern).unwrap().is_match(v))
                    .count();
                
                let confidence = matching as f64 / profile.basic_stats.sample_values.len() as f64;
                
                if confidence > 0.9 {
                    suggestions.push(SuggestedConstraint {
                        check_type: "validates_regex".to_string(),
                        column: profile.column_name.clone(),
                        parameters: HashMap::from([
                            ("pattern".to_string(), ConstraintParameter::String(pattern.clone())),
                            ("threshold".to_string(), ConstraintParameter::Float(0.99)),
                        ]),
                        confidence,
                        rationale: format!("Column follows {} pattern", id_type),
                        priority: SuggestionPriority::High,
                    });
                    
                    // Also suggest uniqueness for IDs
                    suggestions.push(SuggestedConstraint {
                        check_type: "is_unique".to_string(),
                        column: profile.column_name.clone(),
                        parameters: HashMap::new(),
                        confidence: 0.95,
                        rationale: "Business IDs should be unique".to_string(),
                        priority: SuggestionPriority::Critical,
                    });
                }
            }
        }
        
        suggestions
    }
    
    fn name(&self) -> &str {
        "BusinessIdRule"
    }
}

// Use the custom rule
async fn apply_custom_rules(ctx: &SessionContext) -> Result<()> {
    let profiler = ColumnProfiler::builder().build();
    let profile = profiler.profile_column(ctx, "orders", "order_id").await?;
    
    let engine = SuggestionEngine::new()
        .add_rule(Box::new(BusinessIdRule::new()))
        .add_rule(Box::new(CompletenessRule::new()));
    
    let suggestions = engine.suggest_constraints(&profile);
    
    Ok(())
}
```

## Step 5: Converting Suggestions to Validation Checks

Transform high-confidence suggestions into actual validation checks:

```rust
async fn suggestions_to_validation(ctx: &SessionContext) -> Result<()> {
    // Profile all columns in the dataset
    let profiler = ColumnProfiler::builder().build();
    let table_profile = profiler.profile_table(ctx, "sales").await?;
    
    // Generate suggestions for each column
    let engine = SuggestionEngine::default();  // Uses all default rules
    
    let mut all_suggestions = Vec::new();
    for column_profile in table_profile.column_profiles {
        let suggestions = engine.suggest_constraints(&column_profile);
        all_suggestions.extend(suggestions);
    }
    
    // Build validation suite from suggestions
    let mut suite_builder = ValidationSuite::builder("auto_generated_checks");
    let mut check_builder = Check::builder("suggested_validations");
    
    for suggestion in all_suggestions {
        // Only use high-confidence suggestions
        if suggestion.confidence < 0.85 {
            continue;
        }
        
        // Convert suggestion to check based on type
        check_builder = match suggestion.check_type.as_str() {
            "is_complete" => {
                check_builder.is_complete(&suggestion.column)
            },
            "is_unique" => {
                check_builder.is_unique(&suggestion.column)
            },
            "has_completeness" => {
                if let Some(ConstraintParameter::Float(threshold)) = 
                    suggestion.parameters.get("threshold") {
                    check_builder.has_completeness(&suggestion.column, *threshold)
                } else {
                    check_builder
                }
            },
            "validates_email" => {
                check_builder.validates_email(&suggestion.column, 0.99)
            },
            "validates_regex" => {
                if let Some(ConstraintParameter::String(pattern)) = 
                    suggestion.parameters.get("pattern") {
                    check_builder.validates_regex(&suggestion.column, pattern, 0.95)
                } else {
                    check_builder
                }
            },
            "has_min" => {
                if let Some(ConstraintParameter::Float(min)) = 
                    suggestion.parameters.get("min") {
                    check_builder.has_min(&suggestion.column, Assertion::GreaterThan(*min))
                } else {
                    check_builder
                }
            },
            "is_contained_in" => {
                if let Some(ConstraintParameter::String(values)) = 
                    suggestion.parameters.get("allowed_values") {
                    let values: Vec<String> = serde_json::from_str(values)?;
                    check_builder.is_contained_in(&suggestion.column, values)
                } else {
                    check_builder
                }
            },
            _ => {
                println!("⚠️ Unknown suggestion type: {}", suggestion.check_type);
                check_builder
            }
        };
        
        println!("✅ Added check: {} for {}", 
                 suggestion.check_type, suggestion.column);
    }
    
    // Build and run the validation suite
    let suite = suite_builder
        .add_check(check_builder.build())
        .build();
    
    let results = suite.run(ctx).await?;
    
    println!("\n📋 Validation Results:");
    println!("Status: {:?}", results.status());
    
    Ok(())
}
```

## Step 6: Train-Test Split for Suggestion Validation

Validate suggestions using a train-test split approach:

```rust
async fn validate_suggestions_with_split(ctx: &SessionContext) -> Result<()> {
    // Split data into training and test sets
    let total_rows = get_row_count(ctx, "customers").await?;
    let train_size = (total_rows as f64 * 0.8) as usize;
    
    // Create training context (80% of data)
    let train_ctx = SessionContext::new();
    train_ctx.sql(&format!(
        "CREATE VIEW customers_train AS 
         SELECT * FROM customers 
         LIMIT {}", train_size
    )).await?;
    
    // Create test context (20% of data)
    let test_ctx = SessionContext::new();
    test_ctx.sql(&format!(
        "CREATE VIEW customers_test AS 
         SELECT * FROM customers 
         OFFSET {} LIMIT {}", 
        train_size, total_rows - train_size
    )).await?;
    
    // Generate suggestions on training data
    let profiler = ColumnProfiler::builder().build();
    let train_profile = profiler.profile_table(&train_ctx, "customers_train").await?;
    
    let engine = SuggestionEngine::default()
        .with_confidence_adjustment(0.1);  // Be more conservative
    
    let suggestions = engine.suggest_constraints_for_table(&train_profile);
    
    // Test suggestions on test data
    println!("🧪 Validating suggestions on test set:");
    
    let mut valid_suggestions = Vec::new();
    
    for suggestion in suggestions {
        // Convert suggestion to check
        let check = suggestion_to_check(&suggestion)?;
        
        // Run check on test data
        let suite = ValidationSuite::builder("validation_test")
            .add_check(check)
            .build();
        
        let result = suite.run(&test_ctx).await?;
        
        if result.is_success() {
            println!("  ✅ Valid: {}", suggestion.check_type);
            valid_suggestions.push(suggestion);
        } else {
            println!("  ❌ Invalid: {} (failed on test data)", suggestion.check_type);
        }
    }
    
    println!("\nValidation Summary:");
    println!("  Total suggestions: {}", suggestions.len());
    println!("  Valid suggestions: {}", valid_suggestions.len());
    println!("  Success rate: {:.1}%", 
             valid_suggestions.len() as f64 / suggestions.len() as f64 * 100.0);
    
    Ok(())
}
```

## Step 7: Adaptive Constraint Generation

Build a system that adapts suggestions based on validation history:

```rust
struct AdaptiveSuggestionSystem {
    repository: Arc<dyn MetricsRepository>,
    learning_rate: f64,
    confidence_history: HashMap<String, Vec<f64>>,
}

impl AdaptiveSuggestionSystem {
    async fn generate_adaptive_suggestions(&mut self, ctx: &SessionContext) -> Result<Vec<SuggestedConstraint>> {
        // Profile current data
        let profiler = ColumnProfiler::builder().build();
        let profile = profiler.profile_table(ctx, "data").await?;
        
        // Get historical validation results
        let history = self.repository.load().await
            .after(Utc::now() - Duration::days(30))
            .with_tag("type", "validation_result")
            .execute().await?;
        
        // Analyze which constraints have been stable
        let stability_scores = self.calculate_stability_scores(&history);
        
        // Generate base suggestions
        let engine = SuggestionEngine::default();
        let mut suggestions = engine.suggest_constraints_for_table(&profile);
        
        // Adjust confidence based on historical stability
        for suggestion in &mut suggestions {
            let key = format!("{}:{}", suggestion.column, suggestion.check_type);
            
            if let Some(stability) = stability_scores.get(&key) {
                // Boost confidence for stable constraints
                suggestion.confidence *= 1.0 + (stability * self.learning_rate);
                suggestion.confidence = suggestion.confidence.min(1.0);
                
                // Update rationale
                suggestion.rationale.push_str(&format!(
                    " (Historical stability: {:.0}%)", 
                    stability * 100.0
                ));
            }
            
            // Track confidence history
            self.confidence_history
                .entry(key)
                .or_insert_with(Vec::new)
                .push(suggestion.confidence);
        }
        
        // Filter based on adaptive thresholds
        let adaptive_threshold = self.calculate_adaptive_threshold();
        suggestions.retain(|s| s.confidence > adaptive_threshold);
        
        Ok(suggestions)
    }
    
    fn calculate_stability_scores(&self, history: &[(ResultKey, AnalyzerContext)]) -> HashMap<String, f64> {
        let mut scores = HashMap::new();
        let mut success_counts = HashMap::new();
        let mut total_counts = HashMap::new();
        
        for (_, context) in history {
            for (constraint_key, result) in context.validation_results() {
                *total_counts.entry(constraint_key.clone()).or_insert(0) += 1;
                if result.is_success() {
                    *success_counts.entry(constraint_key.clone()).or_insert(0) += 1;
                }
            }
        }
        
        for (key, total) in total_counts {
            let success = success_counts.get(&key).unwrap_or(&0);
            scores.insert(key, *success as f64 / total as f64);
        }
        
        scores
    }
    
    fn calculate_adaptive_threshold(&self) -> f64 {
        // Adjust threshold based on historical performance
        let all_confidences: Vec<f64> = self.confidence_history
            .values()
            .flatten()
            .copied()
            .collect();
        
        if all_confidences.is_empty() {
            return 0.85;  // Default threshold
        }
        
        // Use 75th percentile as adaptive threshold
        let mut sorted = all_confidences.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let idx = (sorted.len() as f64 * 0.75) as usize;
        
        sorted[idx.min(sorted.len() - 1)]
    }
}
```

## Step 8: Exercise - Complete Suggestion Pipeline

Build an end-to-end constraint suggestion and validation pipeline:

```rust
async fn complete_suggestion_pipeline() -> Result<()> {
    // Step 1: Load and profile reference data
    let ctx = SessionContext::new();
    load_reference_data(&ctx).await?;
    
    let profiler = ColumnProfiler::builder()
        .with_kll_profiling(true)  // Enable advanced profiling
        .cardinality_threshold(100)
        .sample_size(10000)
        .build();
    
    let table_profile = profiler.profile_table(&ctx, "reference_data").await?;
    
    // Step 2: Generate comprehensive suggestions
    let engine = SuggestionEngine::new()
        // Add all standard rules
        .add_rule(Box::new(CompletenessRule::new()))
        .add_rule(Box::new(UniquenessRule::new()))
        .add_rule(Box::new(PatternRule::new()))
        .add_rule(Box::new(DataTypeRule::new()))
        .add_rule(Box::new(RangeRule::new()))
        .add_rule(Box::new(CardinalityRule::new()))
        .add_rule(Box::new(StatisticalRule::new()))
        // Add custom business rules
        .add_rule(Box::new(BusinessIdRule::new()))
        .add_rule(Box::new(DateConsistencyRule::new()))
        .add_rule(Box::new(ReferentialIntegrityRule::new()));
    
    let all_suggestions = engine.suggest_constraints_for_table(&table_profile);
    
    // Step 3: Prioritize and filter suggestions
    let prioritized = prioritize_suggestions(all_suggestions);
    
    // Step 4: Generate validation code
    let validation_code = generate_validation_code(&prioritized)?;
    
    // Step 5: Save to file
    std::fs::write(
        "generated_validations.rs",
        validation_code
    )?;
    
    println!("✅ Generated {} validation checks", prioritized.len());
    println!("📄 Saved to generated_validations.rs");
    
    // Step 6: Create documentation
    generate_validation_documentation(&prioritized)?;
    
    Ok(())
}

fn prioritize_suggestions(suggestions: Vec<SuggestedConstraint>) -> Vec<SuggestedConstraint> {
    let mut prioritized = suggestions;
    
    // Sort by priority and confidence
    prioritized.sort_by(|a, b| {
        match (&a.priority, &b.priority) {
            (SuggestionPriority::Critical, SuggestionPriority::Critical) => {
                b.confidence.partial_cmp(&a.confidence).unwrap()
            },
            (SuggestionPriority::Critical, _) => std::cmp::Ordering::Less,
            (_, SuggestionPriority::Critical) => std::cmp::Ordering::Greater,
            _ => b.confidence.partial_cmp(&a.confidence).unwrap(),
        }
    });
    
    // Take top suggestions based on confidence thresholds
    prioritized.into_iter()
        .filter(|s| match s.priority {
            SuggestionPriority::Critical => s.confidence > 0.8,
            SuggestionPriority::High => s.confidence > 0.85,
            SuggestionPriority::Medium => s.confidence > 0.9,
            SuggestionPriority::Low => s.confidence > 0.95,
        })
        .collect()
}

fn generate_validation_code(suggestions: &[SuggestedConstraint]) -> Result<String> {
    let mut code = String::from(
        "// Auto-generated validation checks\n\
         use term_guard::prelude::*;\n\n\
         pub fn create_validation_suite() -> ValidationSuite {\n\
             ValidationSuite::builder(\"auto_generated\")\n"
    );
    
    // Group suggestions by column
    let mut by_column: HashMap<String, Vec<&SuggestedConstraint>> = HashMap::new();
    for suggestion in suggestions {
        by_column.entry(suggestion.column.clone())
            .or_insert_with(Vec::new)
            .push(suggestion);
    }
    
    // Generate checks for each column
    for (column, column_suggestions) in by_column {
        code.push_str(&format!(
            "        .add_check(\n\
             Check::builder(\"{}_checks\")\n",
            column
        ));
        
        for suggestion in column_suggestions {
            code.push_str(&format!(
                "                // {}\n\
                 {}\n",
                suggestion.rationale,
                suggestion_to_code_line(suggestion)
            ));
        }
        
        code.push_str("                .build()\n        )\n");
    }
    
    code.push_str("        .build()\n}\n");
    
    Ok(code)
}
```

## Summary

You've learned how to:
- ✅ Profile data for suggestion generation
- ✅ Use built-in suggestion rules
- ✅ Create custom suggestion rules
- ✅ Convert suggestions to validation checks
- ✅ Validate suggestions with train-test splits
- ✅ Build adaptive suggestion systems

## Next Steps

- Explore [Incremental Analysis](./12-incremental-analysis.md)
- Read about [Suggestion System Architecture](../explanation/constraint-suggestion-system.md)
- Learn [How to Use Constraint Suggestions](../how-to/use-constraint-suggestions.md)

## Troubleshooting

**Q: Suggestions don't match my data patterns?**
A: Create custom rules that understand your domain-specific patterns.

**Q: Too many low-confidence suggestions?**
A: Increase confidence thresholds or use more representative reference data.

**Q: Suggestions fail on new data?**
A: Use train-test validation and adaptive learning to improve robustness.

## Exercises

1. Create custom rules for your business domain
2. Build a UI for reviewing and approving suggestions
3. Implement suggestion versioning and change tracking
4. Create a feedback loop to improve suggestions over time