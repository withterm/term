# How to Migrate from Deequ to Term

> **Type**: How-To Guide (Task-oriented)
> **Audience**: Teams migrating from Deequ
> **Goal**: Successfully migrate Deequ validation pipelines to Term

## Goal

Migrate your existing Deequ data quality checks to Term while maintaining the same validation logic and thresholds.

## Prerequisites

Before you begin, ensure you have:
- [ ] Existing Deequ validation code to migrate
- [ ] Term v0.0.1 or later in your Rust project
- [ ] Understanding of your current Deequ checks
- [ ] Access to the same datasets used in Deequ

## Quick Migration Reference

| Deequ | Term | Notes |
|-------|------|-------|
| `VerificationSuite` | `ValidationSuite` | Similar builder pattern |
| `Check` | `Check` | Direct equivalent |
| `.hasSize()` | `.has_size()` | Rust naming convention |
| `.isComplete()` | `.is_complete()` | Same parameters |
| `.isUnique()` | `.is_unique()` | Same behavior |
| `.hasMin()` | `.has_min()` | Same threshold logic |
| `.hasMax()` | `.has_max()` | Same threshold logic |
| `.hasPattern()` | `.has_pattern()` | Regex syntax identical |
| `AnalysisRunner` | `AnalyzerRunner` | Similar API |
| `.onData()` | `.run(&ctx, "table")` | Different execution model |

## Step-by-Step Migration

### Step 1: Map Your Deequ Checks to Term

First, identify all your Deequ checks and their Term equivalents:

**Deequ (Scala):**
```scala
val verificationResult = VerificationSuite()
  .onData(df)
  .addCheck(
    Check(CheckLevel.Error, "Review Check")
      .hasSize(_ >= 100000)
      .isComplete("review_id")
      .isUnique("review_id")
      .isComplete("star_rating")
      .isContainedIn("star_rating", Array("1", "2", "3", "4", "5"))
  )
  .run()
```

**Term (Rust):**
```rust
let suite = ValidationSuite::builder("review_check")
    .add_check(
        Check::new("review_validations")
            .has_size(Comparison::GreaterThanOrEqual, 100000)
            .is_complete("review_id")
            .is_unique("review_id")
            .is_complete("star_rating")
            .is_contained_in("star_rating", vec!["1", "2", "3", "4", "5"])
    )
    .build();

let results = ValidationRunner::new().run(&suite, &ctx).await?;
```

### Step 2: Migrate Analyzers

Convert Deequ analyzers to Term's analyzer framework:

**Deequ (Scala):**
```scala
val analysisResult = AnalysisRunner
  .onData(df)
  .addAnalyzer(Size())
  .addAnalyzer(Completeness("review_id"))
  .addAnalyzer(Distinctness("review_id"))
  .addAnalyzer(Mean("star_rating"))
  .addAnalyzer(StandardDeviation("star_rating"))
  .addAnalyzer(Quantile("helpful_votes", 0.5))
  .run()
```

**Term (Rust):**
```rust
let runner = AnalyzerRunner::new()
    .add_analyzer(Size::new())
    .add_analyzer(Completeness::new("review_id"))
    .add_analyzer(Distinctness::new("review_id"))
    .add_analyzer(Mean::new("star_rating"))
    .add_analyzer(StandardDeviation::new("star_rating"))
    .add_analyzer(Quantile::new("helpful_votes", 0.5));

let metrics = runner.run(&ctx, "reviews").await?;
```

### Step 3: Handle Data Sources

Migrate from Spark DataFrames to Term's data sources:

**Deequ (with Spark):**
```scala
val df = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("data/reviews.csv")
```

**Term:**
```rust
use term::sources::{DataSource, CsvSource};
use datafusion::prelude::SessionContext;

let ctx = SessionContext::new();

let source = CsvSource::builder()
    .path("data/reviews.csv")
    .has_header(true)
    .infer_schema(true)
    .build()?;

source.register(&ctx, "reviews").await?;
```

### Step 4: Migrate Constraint Suggestions

Convert Deequ's constraint suggestion to Term:

**Deequ:**
```scala
val suggestionResult = ConstraintSuggestionRunner()
  .onData(df)
  .addConstraintRule(CompleteIfCompleteRule())
  .addConstraintRule(UniqueIfApproximatelyUniqueRule())
  .run()
```

**Term:**
```rust
use term::suggestions::{SuggestionRunner, Rules};

let suggestions = SuggestionRunner::new()
    .add_rule(Rules::CompleteIfComplete)
    .add_rule(Rules::UniqueIfApproximatelyUnique)
    .run(&ctx, "data")
    .await?;

for suggestion in suggestions {
    println!("Suggested: {}", suggestion.to_check());
}
```

## Complete Migration Example

Here's a complete example migrating a Deequ verification suite:

**Original Deequ Code:**
```scala
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel}

val verificationResult = VerificationSuite()
  .onData(customerDf)
  .addCheck(
    Check(CheckLevel.Error, "Customer Data Quality")
      .hasSize(_ >= 10000)
      .isComplete("customer_id")
      .isUnique("customer_id")
      .isComplete("email")
      .hasPattern("email", """.+@.+\..+""")
      .isNonNegative("age")
      .hasMin("age", _ >= 18)
      .hasMax("age", _ <= 120)
  )
  .addCheck(
    Check(CheckLevel.Warning, "Optional Fields")
      .hasCompleteness("phone", _ >= 0.8)
      .hasDistinctness("country", _ <= 200)
  )
  .run()

if (verificationResult.status == CheckStatus.Success) {
  println("All checks passed!")
} else {
  val failedChecks = verificationResult.checkResults
    .filter(_.status != CheckStatus.Success)
  failedChecks.foreach(println)
}
```

**Migrated Term Code:**
```rust
use term::core::{
    ValidationSuite, Check, CheckLevel, 
    ValidationRunner, ConstraintStatus, Comparison
};
use term::sources::{DataSource, ParquetSource};
use datafusion::prelude::SessionContext;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup data source
    let ctx = SessionContext::new();
    
    let source = ParquetSource::builder()
        .path("data/customers.parquet")
        .build()?;
    
    source.register(&ctx, "customers").await?;
    
    // Create validation suite (equivalent to VerificationSuite)
    let suite = ValidationSuite::builder("customer_validation")
        .add_check(
            Check::new("customer_data_quality")
                .level(CheckLevel::Error)
                .has_size(Comparison::GreaterThanOrEqual, 10000)
                .is_complete("customer_id")
                .is_unique("customer_id")
                .is_complete("email")
                .has_pattern("email", r".+@.+\..+")
                .is_non_negative("age")
                .has_min("age", 18.0)
                .has_max("age", 120.0)
        )
        .add_check(
            Check::new("optional_fields")
                .level(CheckLevel::Warning)
                .has_completeness("phone", 0.8)
                .has_distinctness("country", Comparison::LessThanOrEqual, 200)
        )
        .build();
    
    // Run validation
    let runner = ValidationRunner::new();
    let results = runner.run(&suite, &ctx).await?;
    
    // Process results
    if results.all_checks_passed() {
        println!("All checks passed!");
    } else {
        let failed_checks: Vec<_> = results
            .check_results()
            .iter()
            .filter(|r| r.status() != &ConstraintStatus::Success)
            .collect();
        
        for check in failed_checks {
            println!("Failed: {} - {}", 
                check.check_name(),
                check.message().unwrap_or("No details")
            );
        }
    }
    
    Ok(())
}
```

## Migrating Custom Checks

If you have custom Deequ checks, here's how to migrate them:

**Deequ Custom Check:**
```scala
class CustomCheck extends Check {
  def hasCustomCondition(column: String): CheckWithLastConstraintFilterable = {
    addConstraint(customConstraint(column))
  }
}
```

**Term Custom Constraint:**
```rust
use term::core::{Constraint, ConstraintResult};
use async_trait::async_trait;

#[derive(Debug, Clone)]
struct CustomConstraint {
    column: String,
}

#[async_trait]
impl Constraint for CustomConstraint {
    async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult> {
        // Your custom validation logic
        let sql = format!("SELECT ... FROM data WHERE {}", self.column);
        let df = ctx.sql(&sql).await?;
        // Process results
        Ok(ConstraintResult::success())
    }
    
    fn name(&self) -> &str {
        "custom_constraint"
    }
}

// Add to check
let check = Check::new("custom")
    .add_constraint(Box::new(CustomConstraint { 
        column: "my_column".to_string() 
    }));
```

## Handling Differences

### Execution Model

**Deequ**: Synchronous Spark operations
```scala
val result = verificationSuite.run()  // Blocking
```

**Term**: Async Rust operations
```rust
let result = runner.run(&suite, &ctx).await?;  // Non-blocking
```

### Data Processing

**Deequ**: Spark RDD/DataFrame operations
**Term**: DataFusion Arrow-based operations

Both are distributed and efficient, but Term doesn't require a Spark cluster.

### Configuration

**Deequ**: Spark configuration
```scala
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

**Term**: DataFusion configuration
```rust
let config = SessionConfig::new()
    .with_batch_size(8192)
    .with_target_partitions(4);
let ctx = SessionContext::with_config(config);
```

## Verification

After migration, verify that:

1. All checks produce the same pass/fail results
2. Metrics match within acceptable tolerance
3. Performance meets requirements

```rust
// Verification test
#[cfg(test)]
mod migration_tests {
    #[tokio::test]
    async fn test_migrated_checks_match_deequ() {
        // Run Term validation
        let term_results = run_term_validation().await.unwrap();
        
        // Compare with known Deequ results
        assert_eq!(term_results.check_count(), 15);
        assert_eq!(term_results.failed_count(), 2);
        
        // Verify specific metrics
        let completeness = term_results
            .get_metric("email", "completeness")
            .unwrap();
        assert!((completeness - 0.987).abs() < 0.001);
    }
}
```

## Troubleshooting

### Problem: Different results between Deequ and Term
**Solution:** Check for:
- Data type differences (Spark vs Arrow types)
- Null handling differences
- Regex engine differences (Java vs Rust)

### Problem: Performance regression after migration
**Solution:**
- Tune DataFusion settings: `with_target_partitions()`
- Use partitioned data sources
- Enable query optimization: `with_optimizer_rules()`

### Problem: Missing Deequ feature in Term
**Solution:**
- Check Term's roadmap for planned features
- Implement as custom constraint
- Contact Term community for guidance

## Related Guides

- [How to Create Custom Constraints](custom-constraints.md)
- [How to Optimize Validation Performance](optimize-performance.md)
- [Reference: Complete API Mapping](../reference/deequ-mapping.md)
- [Explanation: Why Term over Deequ](../explanation/term-vs-deequ.md)