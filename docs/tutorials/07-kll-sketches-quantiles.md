# Tutorial: Approximate Quantiles with KLL Sketches

## Introduction

In this tutorial, you'll learn how to use Term's KLL (Karnin-Lang-Liberty) sketch implementation to efficiently compute approximate quantiles on large datasets. By the end, you'll understand how to analyze data distributions without loading entire datasets into memory.

## What You'll Learn

- What KLL sketches are and why they're useful
- How to compute approximate quantiles
- How to tune accuracy vs memory trade-offs
- How to analyze results from KLL sketches

## Prerequisites

Before starting, you should:
- Have Term installed (`cargo add term-guard`)
- Understand basic Term validation concepts (see [Getting Started](./01-getting-started.md))
- Have a sample dataset ready (we'll use TPC-H data)

## Step 1: Understanding the Problem

Traditional exact quantile computation requires sorting all data, which becomes impractical for large datasets:

```rust
// This won't scale to billions of rows!
let mut values: Vec<f64> = load_all_data().await?;
values.sort_by(|a, b| a.partial_cmp(b).unwrap());
let median = values[values.len() / 2];
```

KLL sketches solve this by maintaining a compact summary that provides approximate quantiles with guaranteed error bounds.

## Step 2: Your First KLL Sketch

Let's start by analyzing order amounts in a sales dataset:

```rust
use term_guard::prelude::*;
use term_guard::analyzers::advanced::KLLSketchAnalyzer;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    // Create a session and load your data
    let ctx = SessionContext::new();
    ctx.register_csv(
        "orders",
        "data/orders.csv",
        CsvReadOptions::default()
    ).await?;

    // Create a KLL sketch analyzer with default settings
    let analyzer = KLLSketchAnalyzer::builder()
        .column("order_amount")
        .build();

    // Run the analysis
    let result = analyzer.analyze(&ctx).await?;
    
    // Extract the sketch from results
    if let MetricValue::KLLSketch(sketch) = result.metric_value() {
        // Query for median (50th percentile)
        let median = sketch.quantile(0.5)?;
        println!("Median order amount: ${:.2}", median);
        
        // Query for other percentiles
        let p95 = sketch.quantile(0.95)?;
        println!("95th percentile: ${:.2}", p95);
    }

    Ok(())
}
```

## Step 3: Understanding Accuracy

KLL sketches provide approximate results. Let's explore the accuracy guarantees:

```rust
// Create sketch with specific accuracy
let analyzer = KLLSketchAnalyzer::builder()
    .column("order_amount")
    .sketch_size(200)  // Default: ~1.65% normalized rank error
    .build();

// For comparison, let's also compute exact quantiles on a sample
let sample_analyzer = ApproxQuantileAnalyzer::builder()
    .column("order_amount")
    .quantile(0.5)
    .build();

// Run both analyzers
let kll_result = analyzer.analyze(&ctx).await?;
let exact_result = sample_analyzer.analyze(&ctx).await?;

println!("KLL median: {}", kll_result.get_quantile(0.5)?);
println!("Exact median (sample): {}", exact_result.value());
```

## Step 4: Tuning Memory vs Accuracy

The sketch size parameter controls the trade-off between memory usage and accuracy:

```rust
// High accuracy, more memory
let high_accuracy = KLLSketchAnalyzer::builder()
    .column("revenue")
    .sketch_size(400)  // ~0.8% error, uses more memory
    .build();

// Lower accuracy, less memory
let low_memory = KLLSketchAnalyzer::builder()
    .column("revenue")
    .sketch_size(100)  // ~3.3% error, uses less memory
    .build();

// Measure performance
use std::time::Instant;

let start = Instant::now();
let _ = high_accuracy.analyze(&ctx).await?;
println!("High accuracy took: {:?}", start.elapsed());

let start = Instant::now();
let _ = low_memory.analyze(&ctx).await?;
println!("Low memory took: {:?}", start.elapsed());
```

## Step 5: Analyzing Distributions

KLL sketches are perfect for understanding data distributions:

```rust
async fn analyze_distribution(ctx: &SessionContext) -> Result<()> {
    let analyzer = KLLSketchAnalyzer::builder()
        .column("response_time_ms")
        .build();
    
    let result = analyzer.analyze(ctx).await?;
    
    if let MetricValue::KLLSketch(sketch) = result.metric_value() {
        // Compute multiple quantiles at once
        let quantiles = vec![0.25, 0.5, 0.75, 0.90, 0.95, 0.99];
        
        println!("Response Time Distribution:");
        println!("{'='*40}");
        
        for q in quantiles {
            let value = sketch.quantile(q)?;
            let percentile = (q * 100.0) as u32;
            println!("P{:2}: {:6.2} ms", percentile, value);
        }
        
        // Identify outliers (values above P99)
        let p99 = sketch.quantile(0.99)?;
        println!("\nValues above P99 ({:.2} ms) are potential outliers", p99);
    }
    
    Ok(())
}
```

## Step 6: Incremental Updates

KLL sketches support incremental updates for streaming data:

```rust
use term_guard::analyzers::incremental::IncrementalAnalysisRunner;

async fn track_quantiles_over_time(ctx: &SessionContext) -> Result<()> {
    let mut runner = IncrementalAnalysisRunner::new()
        .add_analyzer(Box::new(
            KLLSketchAnalyzer::builder()
                .column("latency")
                .build()
        ));
    
    // Process data for different time periods
    for date in ["2024-01-01", "2024-01-02", "2024-01-03"] {
        // Load partition for specific date
        ctx.register_csv(
            "requests",
            &format!("data/requests_{}.csv", date),
            CsvReadOptions::default()
        ).await?;
        
        // Analyze this partition
        let results = runner.analyze_partition(ctx, date).await?;
        
        // The sketch automatically merges with previous data
        if let Some(sketch) = results.get_kll_sketch("latency") {
            println!("Cumulative P50 after {}: {:.2} ms", 
                     date, sketch.quantile(0.5)?);
        }
    }
    
    Ok(())
}
```

## Step 7: Practical Exercise

Now let's put it all together with a real-world scenario:

```rust
/// Analyze customer purchase patterns to identify VIP customers
async fn identify_vip_customers(ctx: &SessionContext) -> Result<()> {
    // Analyze spending distribution
    let spend_analyzer = KLLSketchAnalyzer::builder()
        .column("total_spent")
        .sketch_size(200)
        .build();
    
    let result = spend_analyzer.analyze(ctx).await?;
    
    if let MetricValue::KLLSketch(sketch) = result.metric_value() {
        // Define VIP threshold as top 5% of spenders
        let vip_threshold = sketch.quantile(0.95)?;
        
        println!("VIP Customer Analysis:");
        println!("Minimum spend for VIP status: ${:.2}", vip_threshold);
        
        // Show the distribution
        println!("\nSpending Distribution:");
        println!("Bottom 25%: < ${:.2}", sketch.quantile(0.25)?);
        println!("Middle 50%: ${:.2} - ${:.2}", 
                 sketch.quantile(0.25)?, sketch.quantile(0.75)?);
        println!("Top 25%: > ${:.2}", sketch.quantile(0.75)?);
        println!("Top 5% (VIP): > ${:.2}", vip_threshold);
        
        // Now create a validation check based on this
        let suite = ValidationSuite::builder("vip_monitoring")
            .add_check(
                Check::builder("detect_vip_customers")
                    .has_approx_quantile(
                        "total_spent",
                        0.95,
                        Assertion::GreaterThan(vip_threshold * 0.9)
                    )
                    .build()
            )
            .build();
        
        // This check will alert if VIP spending patterns change significantly
        let validation = suite.run(ctx).await?;
        println!("\nVIP spending pattern check: {:?}", validation.status());
    }
    
    Ok(())
}
```

## Summary

You've learned how to:
- ✅ Create and configure KLL sketch analyzers
- ✅ Query sketches for approximate quantiles
- ✅ Tune the accuracy vs memory trade-off
- ✅ Analyze data distributions efficiently
- ✅ Use sketches with incremental analysis

## Next Steps

- Learn about [Using KLL Sketches in Production](../how-to/use-kll-sketches.md) for detailed configuration
- Explore [Anomaly Detection](./08-anomaly-detection.md) using quantile-based thresholds
- Read the [KLL Algorithm Explanation](../explanation/kll-algorithm.md) to understand the theory

## Troubleshooting

**Q: How do I know if my sketch size is appropriate?**
A: Monitor the actual vs approximate values on a sample. If the error exceeds your tolerance, increase sketch_size.

**Q: Can I merge sketches from different columns?**
A: No, sketches are column-specific. Create separate sketches for each column.

**Q: What happens with null values?**
A: Null values are ignored by the sketch. Use a completeness check if nulls are important.

## Exercises

1. Analyze response times in your application logs using KLL sketches
2. Compare sketch accuracy with different sketch_size values
3. Build an alerting system based on percentile thresholds
4. Implement a dashboard showing distribution changes over time