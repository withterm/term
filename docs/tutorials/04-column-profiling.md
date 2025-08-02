# Tutorial: Column Profiling with Term

This tutorial teaches you how to profile your data columns to understand their characteristics and quality. You'll learn to use Term's ColumnProfiler to analyze data types, distributions, and statistics.

## What You'll Learn

- How column profiling helps with data quality
- Setting up and using the ColumnProfiler
- Understanding profiler results
- Using type inference to detect data types
- Generating constraint suggestions from profiles

## Prerequisites

- Basic Rust knowledge
- Term library installed
- Completed Tutorial 01 (Getting Started)

## Step 1: Understanding Column Profiling

Column profiling analyzes your data to discover:
- Data types and formats
- Statistical distributions
- Null patterns
- Cardinality and uniqueness
- Value patterns and anomalies

Let's start with a simple example:

```rust
use term_guard::analyzers::{ColumnProfiler, ColumnProfile};
use term_guard::test_fixtures::create_test_context;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a test context with sample data
    let ctx = create_test_context().await?;
    
    // Create a profiler with default settings
    let profiler = ColumnProfiler::new();
    
    // Profile a single column
    let profile = profiler
        .profile_column(&ctx, "test_table", "user_id")
        .await?;
    
    println!("Column: {}", profile.column_name);
    println!("Data Type: {:?}", profile.data_type);
    println!("Row Count: {}", profile.basic_stats.row_count);
    println!("Null Count: {}", profile.basic_stats.null_count);
    
    Ok(())
}
```

## Step 2: Configuring the Profiler

The profiler can be customized for different scenarios:

```rust
use term_guard::analyzers::ColumnProfilerBuilder;

// Configure for large datasets
let profiler = ColumnProfiler::builder()
    .sample_size(10_000)              // Sample first 10k rows
    .cardinality_threshold(1000)      // Switch algorithms at 1k unique values
    .max_memory_bytes(1024 * 1024 * 512) // 512MB memory limit
    .enable_parallel(true)            // Use parallel processing
    .build();

// Configure for detailed analysis
let detailed_profiler = ColumnProfiler::builder()
    .sample_size(100_000)             // Larger sample
    .enable_numeric_histogram(true)   // Include histograms
    .confidence_threshold(0.95)       // High confidence requirements
    .build();
```

## Step 3: Type Inference in Action

Let's see how type inference works with mixed data:

```rust
use term_guard::analyzers::{TypeInferenceEngine, InferredDataType};

// Create a type inference engine
let engine = TypeInferenceEngine::builder()
    .sample_size(1000)
    .confidence_threshold(0.9)
    .build();

// Sample data that looks numeric but might have issues
let samples = vec![
    "123",
    "456.78",
    "1,234",  // Formatted number
    "N/A",    // Missing value
    "789",
];

// Infer the data type
let result = engine.infer_type(&samples)?;

match result.data_type {
    InferredDataType::Integer { nullable } => {
        println!("Detected as integer (nullable: {})", nullable);
    }
    InferredDataType::Decimal { precision, scale, nullable } => {
        println!("Detected as decimal ({},{}) nullable: {}", 
                 precision, scale, nullable);
    }
    InferredDataType::Mixed { types } => {
        println!("Mixed types detected: {:?}", types);
    }
    _ => println!("Other type: {:?}", result.data_type),
}

println!("Confidence: {:.2}%", result.confidence * 100.0);
```

## Step 4: Understanding Profile Results

Let's profile different types of columns and interpret the results:

```rust
// Profile a numeric column with the lineitem table
let ctx = create_tpc_h_context().await?;
let profiler = ColumnProfiler::new();

let quantity_profile = profiler
    .profile_column(&ctx, "lineitem", "l_quantity")
    .await?;

// Numeric columns have distribution information
if let Some(dist) = &quantity_profile.numeric_distribution {
    println!("Mean: {:?}", dist.mean);
    println!("Std Dev: {:?}", dist.std_dev);
    println!("P50 (Median): {:?}", dist.quantiles.get("P50"));
    println!("P95: {:?}", dist.quantiles.get("P95"));
}

// Profile a categorical column
let status_profile = profiler
    .profile_column(&ctx, "orders", "o_orderstatus")
    .await?;

// Categorical columns have histograms
if let Some(hist) = &status_profile.categorical_histogram {
    println!("Unique values: {}", hist.buckets.len());
    for bucket in &hist.buckets[..3] {  // Top 3 values
        println!("  {}: {} occurrences", bucket.value, bucket.count);
    }
}
```

## Step 5: Generating Constraint Suggestions

Use profiles to automatically suggest data quality constraints:

```rust
use term_guard::analyzers::{SuggestionEngine, CompletenessRule, UniquenessRule};

// Create a suggestion engine
let engine = SuggestionEngine::new()
    .add_rule(Box::new(CompletenessRule::new()))
    .add_rule(Box::new(UniquenessRule::new()));

// Get suggestions based on the profile
let suggestions = engine.suggest_constraints(&quantity_profile);

for suggestion in suggestions {
    println!("Suggested: {} on {}", 
             suggestion.check_type, 
             suggestion.column);
    println!("  Confidence: {:.2}", suggestion.confidence);
    println!("  Reason: {}", suggestion.rationale);
}
```

## Step 6: Profiling Multiple Columns

Profile an entire table efficiently:

```rust
// Sequential profiling
let columns = vec!["l_orderkey", "l_quantity", "l_extendedprice"];
let mut profiles = Vec::new();

for column in columns {
    let profile = profiler.profile_column(&ctx, "lineitem", column).await?;
    profiles.push(profile);
}

// Parallel profiling (when enabled)
let parallel_profiler = ColumnProfiler::builder()
    .enable_parallel(true)
    .build();

// Profile all columns at once
let all_profiles = parallel_profiler
    .profile_table(&ctx, "lineitem")
    .await?;

println!("Profiled {} columns", all_profiles.len());
```

## Exercise: Profile Your Own Data

Try profiling a CSV file:

```rust
use term_guard::sources::CsvSource;

// Load your data
let source = CsvSource::new(vec!["data/sales.csv"]);
let ctx = create_validation_context_from_source(source).await?;

// Profile interesting columns
let profiler = ColumnProfiler::builder()
    .sample_size(5000)
    .build();

let profile = profiler.profile_column(&ctx, "sales", "revenue").await?;

// Explore the results
// - What data type was detected?
// - What's the null percentage?
// - What's the value distribution?
```

## Next Steps

- Tutorial 05: Building validation suites from profiles
- How-To: Optimize profiling for large datasets
- Reference: Complete profiler API documentation
- Explanation: Three-pass profiling algorithm

## Summary

You've learned to:
- ✓ Set up and configure the ColumnProfiler
- ✓ Use type inference to detect data types
- ✓ Interpret profiling results for different column types
- ✓ Generate constraint suggestions from profiles
- ✓ Profile multiple columns efficiently

Column profiling is the foundation for data quality validation. In the next tutorial, you'll learn how to convert these profiles into validation suites.