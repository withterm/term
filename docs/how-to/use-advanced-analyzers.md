# How to Use Advanced Analyzers

This guide shows you how to use Term's advanced analyzers for sophisticated data quality analysis, including approximate quantiles, correlation analysis, and information theory metrics.

## Prerequisites

- Term v0.2.0 or later
- Understanding of [basic analyzers](use-analyzers.md)
- Numerical or categorical data for analysis

## KLL Sketch for Approximate Quantiles

The KLL (Karnin-Lang-Liberty) sketch provides memory-efficient approximate quantile computation for large datasets.

### Basic Usage

```rust
use term_guard::analyzers::advanced::KllSketchAnalyzer;
use term_guard::analyzers::runner::AnalysisRunner;

// Create analyzer with default settings
let kll_analyzer = KllSketchAnalyzer::new("response_time");

// Run analysis
let runner = AnalysisRunner::new();
let results = runner
    .add_analyzer(Box::new(kll_analyzer))
    .run(&ctx)
    .await?;

// Access quantile results
if let MetricValue::Distribution(dist) = &results["kll_sketch"] {
    println!("Median (p50): {}", dist["quantile_0.5"]);
    println!("95th percentile: {}", dist["quantile_0.95"]);
    println!("99th percentile: {}", dist["quantile_0.99"]);
}
```

### Custom Quantiles and Accuracy

```rust
// Specify custom quantiles
let kll_analyzer = KllSketchAnalyzer::with_quantiles(
    "latency",
    vec![0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999],
);

// Adjust accuracy/memory tradeoff (higher k = more accurate)
let kll_analyzer = KllSketchAnalyzer::new("price")
    .with_k(500); // Default is 200
```

### Use Cases

```rust
// Performance monitoring
let response_analyzer = KllSketchAnalyzer::with_quantiles(
    "api_response_ms",
    vec![0.5, 0.9, 0.95, 0.99], // SLA percentiles
);

// Price distribution analysis  
let price_analyzer = KllSketchAnalyzer::with_quantiles(
    "product_price",
    vec![0.25, 0.5, 0.75], // Quartiles for pricing strategy
);

// Data quality monitoring
let size_analyzer = KllSketchAnalyzer::new("file_size_bytes");
```

## Correlation Analysis

Analyze relationships between columns using various correlation methods.

### Pearson Correlation

For linear relationships between continuous variables:

```rust
use term_guard::analyzers::advanced::CorrelationAnalyzer;

// Analyze linear correlation
let correlation = CorrelationAnalyzer::pearson(
    "temperature",
    "ice_cream_sales"
);

let results = runner
    .add_analyzer(Box::new(correlation))
    .run(&ctx)
    .await?;

if let MetricValue::Distribution(dist) = &results["correlation.pearson"] {
    let coefficient = dist["coefficient"].as_f64();
    
    match coefficient {
        c if c > 0.7 => println!("Strong positive correlation: {}", c),
        c if c > 0.3 => println!("Moderate positive correlation: {}", c),
        c if c > -0.3 => println!("Weak/no correlation: {}", c),
        c if c > -0.7 => println!("Moderate negative correlation: {}", c),
        c => println!("Strong negative correlation: {}", c),
    }
}
```

### Spearman Rank Correlation

For monotonic relationships (doesn't assume linearity):

```rust
// Detect monotonic relationships
let rank_correlation = CorrelationAnalyzer::spearman(
    "customer_age",
    "purchase_frequency"
);

// Useful for ordinal data
let satisfaction_correlation = CorrelationAnalyzer::spearman(
    "service_rating",  // 1-5 scale
    "renewal_likelihood" // 1-10 scale
);
```

### Covariance

Measure how variables change together:

```rust
let covariance = CorrelationAnalyzer::covariance(
    "marketing_spend",
    "revenue"
);

// Interpret with context of scale
if let MetricValue::Distribution(dist) = &results["correlation.covariance"] {
    let cov = dist["covariance"].as_f64();
    // Positive: variables tend to increase together
    // Negative: one increases as other decreases
    // Magnitude depends on variable scales
}
```

### Multiple Correlation Analysis

```rust
// Analyze multiple pairs
let correlations = vec![
    ("age", "income"),
    ("education_years", "income"),
    ("experience_years", "income"),
];

for (col1, col2) in correlations {
    let analyzer = CorrelationAnalyzer::pearson(col1, col2);
    runner.add_analyzer(Box::new(analyzer));
}

let results = runner.run(&ctx).await?;

// Find strongest predictor
let mut best_predictor = ("", 0.0);
for (col1, col2) in correlations {
    let key = format!("correlation.pearson.{}.{}", col1, col2);
    if let Some(MetricValue::Distribution(dist)) = results.get(&key) {
        let coef = dist["coefficient"].as_f64().abs();
        if coef > best_predictor.1 {
            best_predictor = (col1, coef);
        }
    }
}
```

## Mutual Information Analysis

Detect both linear and non-linear dependencies between variables.

### Basic Usage

```rust
use term_guard::analyzers::advanced::MutualInformationAnalyzer;

// Detect any statistical dependence
let mi_analyzer = MutualInformationAnalyzer::new(
    "feature_1",
    "target_variable"
);

let results = runner
    .add_analyzer(Box::new(mi_analyzer))
    .run(&ctx)
    .await?;

if let MetricValue::Distribution(dist) = &results["mutual_information"] {
    let mi = dist["mutual_information"].as_f64();
    let normalized_mi = dist["normalized_mi"].as_f64();
    
    println!("Mutual Information: {} bits", mi);
    println!("Normalized MI: {:.2}%", normalized_mi * 100.0);
    
    // Interpretation
    if normalized_mi > 0.5 {
        println!("Strong dependency detected");
    } else if normalized_mi > 0.2 {
        println!("Moderate dependency detected");
    } else {
        println!("Weak or no dependency");
    }
}
```

### Discretization for Continuous Data

```rust
// Specify number of bins for continuous data
let mi_analyzer = MutualInformationAnalyzer::with_bins(
    "continuous_feature",
    "continuous_target",
    20  // Use 20 bins for discretization
);

// Adaptive binning based on data range
let data_range = get_data_range(&ctx, "feature").await?;
let num_bins = match data_range {
    r if r < 10.0 => 5,
    r if r < 100.0 => 10,
    r if r < 1000.0 => 20,
    _ => 50,
};

let mi_analyzer = MutualInformationAnalyzer::with_bins(
    "feature", 
    "target",
    num_bins
);
```

### Feature Selection

```rust
async fn select_best_features(
    ctx: &SessionContext,
    features: Vec<&str>,
    target: &str,
    top_k: usize,
) -> Result<Vec<(String, f64)>> {
    let mut runner = AnalysisRunner::new();
    
    // Add MI analyzer for each feature
    for feature in &features {
        let analyzer = MutualInformationAnalyzer::new(*feature, target);
        runner.add_analyzer(Box::new(analyzer));
    }
    
    let results = runner.run(ctx).await?;
    
    // Collect and sort by MI score
    let mut feature_scores: Vec<(String, f64)> = features
        .iter()
        .filter_map(|feature| {
            let key = format!("mutual_information.{}.{}", feature, target);
            results.get(&key).and_then(|metric| {
                if let MetricValue::Distribution(dist) = metric {
                    dist.get("normalized_mi")
                        .and_then(|v| v.as_f64())
                        .map(|score| (feature.to_string(), score))
                }
            })
        })
        .collect();
    
    feature_scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    feature_scores.truncate(top_k);
    
    Ok(feature_scores)
}

// Usage
let best_features = select_best_features(
    &ctx,
    vec!["age", "income", "education", "location", "occupation"],
    "purchase_likelihood",
    3
).await?;

println!("Top 3 predictive features:");
for (feature, score) in best_features {
    println!("  {} (MI: {:.3})", feature, score);
}
```

## Combining Advanced Analyzers

### Comprehensive Statistical Analysis

```rust
use term_guard::analyzers::advanced::{
    KllSketchAnalyzer,
    CorrelationAnalyzer,
    MutualInformationAnalyzer,
    StandardDeviationAnalyzer,
    EntropyAnalyzer,
};

async fn comprehensive_analysis(
    ctx: &SessionContext,
    numeric_col: &str,
    categorical_col: &str,
) -> Result<AnalysisReport> {
    let runner = AnalysisRunner::new()
        // Distribution analysis
        .add_analyzer(Box::new(KllSketchAnalyzer::new(numeric_col)))
        .add_analyzer(Box::new(StandardDeviationAnalyzer::new(numeric_col)))
        
        // Relationship analysis
        .add_analyzer(Box::new(
            CorrelationAnalyzer::pearson(numeric_col, "target")
        ))
        .add_analyzer(Box::new(
            MutualInformationAnalyzer::new(categorical_col, "target")
        ))
        
        // Information content
        .add_analyzer(Box::new(EntropyAnalyzer::new(categorical_col)));
    
    let results = runner.run(ctx).await?;
    
    Ok(AnalysisReport::from_metrics(results))
}
```

### Anomaly Detection Pipeline

```rust
async fn detect_distribution_anomalies(
    ctx: &SessionContext,
    column: &str,
    baseline_ctx: &SessionContext,
) -> Result<Vec<Anomaly>> {
    // Compute baseline distribution
    let baseline_kll = KllSketchAnalyzer::with_quantiles(
        column,
        vec![0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99],
    );
    
    let baseline_results = AnalysisRunner::new()
        .add_analyzer(Box::new(baseline_kll))
        .run(baseline_ctx)
        .await?;
    
    // Compute current distribution
    let current_results = AnalysisRunner::new()
        .add_analyzer(Box::new(KllSketchAnalyzer::new(column)))
        .run(ctx)
        .await?;
    
    // Compare distributions
    detect_quantile_shifts(baseline_results, current_results)
}
```

## Performance Considerations

### Memory Usage

```rust
// KLL Sketch memory usage
let memory_bytes = k * 8; // Approximately k double values

// Mutual Information memory usage  
let memory_bytes = num_bins * num_bins * 8; // Joint probability matrix

// Correlation memory usage
let memory_bytes = 5 * 8; // Only stores aggregates
```

### Optimization Tips

```rust
// Batch multiple analyzers for single data pass
let runner = AnalysisRunner::new()
    .add_analyzer(Box::new(kll1))
    .add_analyzer(Box::new(kll2))
    .add_analyzer(Box::new(correlation))
    .with_parallelism(4); // Process analyzers in parallel

// Cache results for repeated queries
let results = runner
    .with_cache(Duration::minutes(5))
    .run(&ctx)
    .await?;
```

## Error Handling

```rust
use term_guard::analyzers::errors::AnalyzerError;

match runner.run(&ctx).await {
    Ok(results) => process_results(results),
    Err(AnalyzerError::InvalidData(msg)) => {
        eprintln!("Data type mismatch: {}", msg);
        // Handle numeric analyzer on string column
    },
    Err(AnalyzerError::NoData) => {
        eprintln!("No data available for analysis");
        // Handle empty dataset
    },
    Err(e) => return Err(e.into()),
}
```

## Validation Examples

### Validate Model Predictions

```rust
// Check if model predictions correlate with actuals
let validation = CorrelationAnalyzer::pearson(
    "predicted_value",
    "actual_value"
);

let results = runner.add_analyzer(Box::new(validation)).run(&ctx).await?;

if let Some(MetricValue::Distribution(dist)) = results.get("correlation.pearson") {
    let r_squared = dist["coefficient"].as_f64().powi(2);
    println!("Model R²: {:.3}", r_squared);
    
    if r_squared < 0.6 {
        return Err("Model performance below threshold".into());
    }
}
```

### Data Quality Checks

```rust
// Detect unexpected relationships
let unexpected_correlation = CorrelationAnalyzer::pearson(
    "customer_id",  // Should be independent
    "purchase_amount"
);

let results = runner.add_analyzer(Box::new(unexpected_correlation)).run(&ctx).await?;

if let Some(MetricValue::Distribution(dist)) = results.get("correlation.pearson") {
    if dist["coefficient"].as_f64().abs() > 0.3 {
        warn!("Unexpected correlation detected - possible data leak");
    }
}
```

## Next Steps

- Learn about [incremental analysis](use-incremental-analysis.md) for large datasets
- Explore [anomaly detection](detect-anomalies.md) using advanced metrics
- See [performance optimization](optimize-performance.md) for large-scale analysis