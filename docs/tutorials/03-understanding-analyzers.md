# Tutorial: Understanding Data Quality Analyzers

<!-- 
This is a TUTORIAL following Diátaxis principles.
It teaches through hands-on experience with analyzers.
-->

## What You'll Learn

In this tutorial, you will:
- Create and run your first data quality analyzer
- Understand how analyzers compute metrics from data
- Combine multiple analyzers using the AnalysisRunner
- Interpret analysis results and handle errors

## What You'll Need

Before starting this tutorial, make sure you have:
- Rust 1.70 or later installed
- Term added to your project dependencies
- Completed the "Getting Started" tutorial

**Time to complete:** ~30 minutes

## Getting Started

Data quality analysis is about measuring characteristics of your data. Instead of just checking if data is "good" or "bad", analyzers compute specific metrics that help you understand your data better.

Let's explore this by analyzing a simple dataset of product sales.

## Step 1: Your First Analyzer

First, let's measure the simplest metric: how many rows are in our dataset.

```rust
use term_guard::prelude::*;
use term_guard::analyzers::basic::SizeAnalyzer;

#[tokio::main]
async fn main() -> Result<()> {
    // Create a session with sample data
    let ctx = SessionContext::new();
    
    // Create sample sales data
    let df = ctx.read_csv("sales.csv", CsvReadOptions::new()).await?;
    ctx.register_table("data", df)?;
    
    // Create and run a size analyzer
    let analyzer = SizeAnalyzer::new();
    let state = analyzer.compute_state_from_data(&ctx).await?;
    let metric = analyzer.compute_metric_from_state(&state)?;
    
    println!("Dataset size: {:?}", metric);
    Ok(())
}
```

Run this code with a sample CSV file:
```csv
product_id,quantity,price
A001,5,10.99
A002,3,25.50
A001,2,10.99
```

You should see:
```
Dataset size: Long(3)
```

> 💡 **What's happening here?** The SizeAnalyzer counts rows in your data. It stores this as a `MetricValue::Long(3)`, meaning 3 rows.

## Step 2: Analyzing Column Quality

Now let's check if any values are missing in our quantity column:

```rust
use term_guard::analyzers::basic::CompletenessAnalyzer;

// ... previous code ...

// Check completeness of the quantity column
let completeness = CompletenessAnalyzer::new("quantity");
let state = completeness.compute_state_from_data(&ctx).await?;
let metric = completeness.compute_metric_from_state(&state)?;

println!("Quantity completeness: {:?}", metric);
```

### Try It Yourself

Before moving on, add a row with a missing quantity to your CSV:
```csv
product_id,quantity,price
A001,5,10.99
A002,,25.50  # Missing quantity!
A001,2,10.99
```

What happens to the completeness metric?

<details>
<summary>💡 Hint</summary>

The completeness should drop from 1.0 (100%) to 0.67 (67%), as only 2 out of 3 values are present.

</details>

## Step 3: Running Multiple Analyzers

Analyzing one metric at a time is tedious. Let's use the AnalysisRunner to run multiple analyzers together:

```rust
use term_guard::analyzers::{AnalysisRunner, basic::*};

// Create a runner with multiple analyzers
let runner = AnalysisRunner::new()
    .add(SizeAnalyzer::new())
    .add(CompletenessAnalyzer::new("quantity"))
    .add(CompletenessAnalyzer::new("price"))
    .add(MeanAnalyzer::new("quantity"))
    .add(MaxAnalyzer::new("price"));

// Run all analyzers
let context = runner.run(&ctx).await?;

// Access results
println!("All metrics: {:#?}", context.all_metrics());
```

> ⚠️ **Common Mistake:** Each analyzer needs the table to be named "data". Always register your DataFrame with this name when using analyzers.

## Step 4: Handling Analysis Progress

For large datasets, you might want to track progress. Let's add a progress callback:

```rust
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

let completed = Arc::new(AtomicUsize::new(0));
let total = 5; // We're running 5 analyzers

let runner = AnalysisRunner::new()
    .add(SizeAnalyzer::new())
    .add(CompletenessAnalyzer::new("quantity"))
    .add(CompletenessAnalyzer::new("price"))
    .add(MeanAnalyzer::new("quantity"))
    .add(MaxAnalyzer::new("price"))
    .on_progress(move |progress| {
        let percent = (progress * 100.0) as u32;
        println!("Analysis progress: {}%", percent);
    });

let context = runner.run(&ctx).await?;
```

## Step 5: Understanding Metric Keys

Notice how each metric has a unique key in the results:

```rust
// Size analyzer produces: "size"
if let Some(size) = context.get_metric("size") {
    println!("Total rows: {:?}", size);
}

// Column analyzers include the column name
if let Some(comp) = context.get_metric("completeness.quantity") {
    println!("Quantity completeness: {:?}", comp);
}

// Different columns get different keys
if let Some(mean) = context.get_metric("mean.quantity") {
    println!("Average quantity: {:?}", mean);
}
```

## What You've Learned

Congratulations! You've learned how to:

✅ Create and run individual data quality analyzers  
✅ Use the AnalysisRunner to orchestrate multiple analyzers efficiently  
✅ Track analysis progress with callbacks  
✅ Access metrics using their unique keys

### Key Takeaways

- **Analyzers**: Compute specific metrics about your data (size, completeness, statistics)
- **AnalysisRunner**: Orchestrates multiple analyzers with a builder pattern API
- **Metric Keys**: Each metric has a unique identifier like "analyzer.column"

## Next Steps

Now that you understand analyzers, you're ready to:

1. **Continue Learning**: Try the [Column Profiling](04-column-profiling.md) tutorial
2. **Apply Your Knowledge**: See [How to Analyze Large Datasets](../how-to/analyze-large-datasets.md)
3. **Go Deeper**: Read [Why Analyzers Use Two-Phase Computation](../explanation/analyzer-architecture.md)

## Exercises

1. **Exercise 1**: Add a `DistinctnessAnalyzer` to measure unique product IDs
2. **Exercise 2**: Create an analyzer that handles errors gracefully using `continue_on_error()`
3. **Challenge**: Build a runner that analyzes all numeric columns automatically

---