# How to Use Analyzers for Data Profiling

> **Type**: How-To Guide (Task-oriented)
> **Audience**: Practitioners using Term
> **Goal**: Profile datasets using Term's analyzer framework

## Goal

Profile your dataset to understand data distributions, detect patterns, and gather statistics using Term's analyzer framework.

## Prerequisites

Before you begin, ensure you have:
- [ ] Term v0.0.1 or later installed
- [ ] A dataset registered with DataFusion SessionContext
- [ ] Understanding of basic Term concepts (see [Getting Started](../tutorials/getting-started.md))

## Quick Solution

```rust
use term::analyzers::{AnalyzerRunner, StandardAnalyzers};
use datafusion::prelude::SessionContext;

// Profile all columns with standard analyzers
let runner = AnalyzerRunner::new();
let metrics = runner
    .add_analyzers(StandardAnalyzers::all())
    .run(&ctx, "data")
    .await?;

// Access profiling results
for (column, stats) in metrics.column_statistics() {
    println!("{}: mean={:.2}, stddev={:.2}", 
        column, stats.mean, stats.stddev);
}
```

## Step-by-Step Guide

### Step 1: Set Up Your Data Source

Register your data with a SessionContext:

```rust
use datafusion::prelude::SessionContext;
use term::sources::{DataSource, ParquetSource};

let ctx = SessionContext::new();

let source = ParquetSource::builder()
    .path("data/transactions.parquet")
    .build()?;

source.register(&ctx, "transactions").await?;
```

### Step 2: Configure Analyzers

Choose which analyzers to run based on your profiling needs:

```rust
use term::analyzers::{
    AnalyzerRunner,
    Completeness,
    Distinctness,
    Mean,
    StandardDeviation,
    Quantile
};

let runner = AnalyzerRunner::new()
    // Basic statistics
    .add_analyzer(Completeness::new("customer_id"))
    .add_analyzer(Distinctness::new("customer_id"))
    
    // Numeric analysis
    .add_analyzer(Mean::new("amount"))
    .add_analyzer(StandardDeviation::new("amount"))
    .add_analyzer(Quantile::new("amount", 0.5))  // median
    .add_analyzer(Quantile::new("amount", 0.95)) // 95th percentile
    
    // Pattern detection
    .add_analyzer(PatternMatch::new("email", r"^[^@]+@[^@]+\.[^@]+$"));
```

### Step 3: Run Analysis and Collect Metrics

Execute the analyzers and gather results:

```rust
let metrics = runner.run(&ctx, "transactions").await?;

// Get overall statistics
let total_rows = metrics.row_count();
println!("Analyzed {} rows", total_rows);

// Access specific metric
if let Some(completeness) = metrics.get_metric("customer_id", "completeness") {
    println!("Customer ID completeness: {:.2%}", completeness);
}

// Iterate through all metrics
for (analyzer_name, result) in metrics.all_metrics() {
    match result {
        MetricValue::Numeric(value) => {
            println!("{}: {:.4}", analyzer_name, value);
        }
        MetricValue::Distribution(dist) => {
            println!("{}: {} unique values", analyzer_name, dist.len());
        }
    }
}
```

## Complete Example

Here's a complete profiling pipeline:

```rust
use term::analyzers::{AnalyzerRunner, StandardAnalyzers, MetricValue};
use term::sources::{DataSource, CsvSource};
use datafusion::prelude::SessionContext;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize context and data
    let ctx = SessionContext::new();
    
    let source = CsvSource::builder()
        .path("data/sales.csv")
        .has_header(true)
        .infer_schema(true)
        .build()?;
    
    source.register(&ctx, "sales").await?;
    
    // Configure comprehensive profiling
    let runner = AnalyzerRunner::new()
        // Completeness for all columns
        .add_analyzer(Completeness::all_columns())
        // Statistics for numeric columns
        .add_analyzer(Mean::numeric_columns())
        .add_analyzer(StandardDeviation::numeric_columns())
        .add_analyzer(Min::numeric_columns())
        .add_analyzer(Max::numeric_columns())
        // Uniqueness analysis
        .add_analyzer(Distinctness::new("transaction_id"))
        .add_analyzer(Uniqueness::new("transaction_id"))
        // Distribution analysis
        .add_analyzer(Histogram::new("amount", 10)) // 10 bins
        .add_analyzer(FrequencyDistribution::new("category"));
    
    // Run analysis
    let metrics = runner.run(&ctx, "sales").await?;
    
    // Generate profile report
    println!("=== Data Profile Report ===");
    println!("Total Rows: {}", metrics.row_count());
    println!("\nColumn Statistics:");
    
    for (column, stats) in metrics.column_statistics() {
        println!("\n{}:", column);
        println!("  Completeness: {:.2%}", stats.completeness);
        println!("  Distinct: {}", stats.distinct_count);
        
        if let Some(mean) = stats.mean {
            println!("  Mean: {:.2}", mean);
            println!("  Std Dev: {:.2}", stats.stddev.unwrap_or(0.0));
            println!("  Min: {:.2}", stats.min.unwrap_or(0.0));
            println!("  Max: {:.2}", stats.max.unwrap_or(0.0));
        }
    }
    
    // Export metrics for further analysis
    let json_metrics = metrics.to_json()?;
    std::fs::write("profile_results.json", json_metrics)?;
    
    Ok(())
}
```

## Variations

### Incremental Profiling

For large datasets, profile incrementally:

```rust
use term::analyzers::{IncrementalAnalyzer, IncrementalState};

let mut state = IncrementalState::new();

// Process in batches
for batch_path in batch_paths {
    let source = ParquetSource::builder()
        .path(batch_path)
        .build()?;
    
    source.register(&ctx, "batch").await?;
    
    let analyzer = IncrementalAnalyzer::new()
        .add_analyzer(Mean::new("value"))
        .with_state(&mut state);
    
    analyzer.run(&ctx, "batch").await?;
}

// Get final metrics
let final_metrics = state.finalize();
```

### Custom Analyzers

Create domain-specific analyzers:

```rust
use term::analyzers::{Analyzer, AnalyzerResult};

struct EmailDomainAnalyzer {
    column: String,
}

#[async_trait]
impl Analyzer for EmailDomainAnalyzer {
    async fn analyze(&self, ctx: &SessionContext) -> Result<AnalyzerResult> {
        let sql = format!(
            "SELECT 
                SUBSTRING({} FROM '@(.+)$') as domain,
                COUNT(*) as count 
             FROM data 
             GROUP BY domain",
            self.column
        );
        
        let df = ctx.sql(&sql).await?;
        // Process results into metrics
        
        Ok(AnalyzerResult::Distribution(domain_counts))
    }
}
```

### Conditional Analysis

Profile based on conditions:

```rust
let runner = AnalyzerRunner::new()
    .add_analyzer(
        Mean::new("order_value")
            .where_clause("status = 'completed'")
    )
    .add_analyzer(
        Completeness::new("email")
            .where_clause("user_type = 'premium'")
    );
```

## Verification

To verify profiling worked correctly:

1. Check that metrics were generated for all specified columns
2. Validate that row counts match your expectations
3. Compare sample statistics with known values if available

```rust
// Verification checks
assert!(metrics.row_count() > 0, "No rows analyzed");
assert!(metrics.has_metric("amount", "mean"), "Missing expected metric");

let completeness = metrics.get_metric("id", "completeness").unwrap();
assert!(completeness > 0.99, "Unexpected low completeness for ID column");
```

## Troubleshooting

### Problem: "Column not found" error
**Solution:** Verify column names match exactly (case-sensitive). Use `source.schema()` to list available columns.

### Problem: Analyzer runs slowly on large dataset
**Solution:** 
- Use sampling: `runner.with_sample_size(10000)`
- Run analyzers in parallel: `runner.parallel(true)`
- Consider incremental analysis for very large datasets

### Problem: Memory issues with distribution analyzers
**Solution:** 
- Limit distinct value tracking: `Distinctness::new("col").with_max_distinct(1000)`
- Use approximate algorithms: `ApproximateDistinctness::new("col")`

## Performance Considerations

- **Sampling**: For large datasets, use `runner.with_sample_size(n)` for faster approximate results
- **Column Selection**: Only analyze necessary columns to reduce computation
- **Caching**: Reuse SessionContext and computed metrics when analyzing multiple times
- **Parallelism**: Enable parallel execution with `runner.parallel(true)`

## Related Guides

- [How to Create Custom Analyzers](custom-analyzers.md)
- [How to Export Profiling Results](export-metrics.md)
- [Tutorial: Understanding Data Profiling](../tutorials/data-profiling.md)
- [Reference: Analyzer API](../reference/analyzers.md)