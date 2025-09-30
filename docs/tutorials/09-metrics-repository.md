# Tutorial: Tracking Metrics Over Time with Repository

## Introduction

In this tutorial, you'll learn how to use Term's metrics repository system to store, track, and analyze data quality metrics over time. This enables trend analysis, anomaly detection, and historical comparisons essential for production data quality monitoring.

## What You'll Learn

- How to set up a metrics repository
- How to store validation results with versioning
- How to query historical metrics
- How to build time-series quality dashboards
- How to detect quality degradation trends

## Prerequisites

Before starting, you should:
- Understand Term's basic validation concepts
- Have completed the Getting Started tutorial
- Have a dataset that changes over time

## Step 1: Understanding the Repository System

The metrics repository provides persistent storage for validation results with:
- **Timestamping**: Every metric is versioned with a timestamp
- **Tagging**: Organize metrics with key-value tags
- **Querying**: Retrieve metrics by time range, tags, or analyzer type
- **Persistence**: Store metrics in memory, filesystem, or database

```rust
use term_guard::repository::{MetricsRepository, ResultKey, InMemoryRepository};
use chrono::Utc;

// Create a repository
let repository = InMemoryRepository::new();

// Create a result key with timestamp and tags
let key = ResultKey::new()
    .with_timestamp(Utc::now())
    .with_tag("environment", "production")
    .with_tag("dataset", "orders")
    .with_tag("version", "v1.2.3");
```

## Step 2: Your First Metric Storage

Let's run a validation and store the results:

```rust
use term_guard::prelude::*;
use term_guard::repository::{InMemoryRepository, ResultKey};
use datafusion::prelude::*;
use chrono::Utc;

#[tokio::main]
async fn main() -> Result<()> {
    // Set up repository
    let repository = InMemoryRepository::new();
    
    // Load data
    let ctx = SessionContext::new();
    ctx.register_csv(
        "sales",
        "data/sales_2024_01_15.csv",
        CsvReadOptions::default()
    ).await?;
    
    // Create validation suite
    let suite = ValidationSuite::builder("daily_sales_validation")
        .with_repository(repository.clone())  // Attach repository
        .add_check(
            Check::builder("sales_metrics")
                .has_size(Assertion::GreaterThan(1000))
                .has_completeness("order_id", 1.0)
                .has_mean("amount", Assertion::Between(50.0, 200.0))
                .build()
        )
        .build();
    
    // Run validation and auto-save to repository
    let result_key = ResultKey::new()
        .with_timestamp(Utc::now())
        .with_tag("date", "2024-01-15")
        .with_tag("pipeline", "daily_etl");
    
    let results = suite.run_and_save(&ctx, result_key).await?;
    
    println!("Validation completed: {:?}", results.status());
    println!("Metrics saved to repository");
    
    Ok(())
}
```

## Step 3: Querying Historical Metrics

Retrieve and analyze stored metrics:

```rust
use term_guard::repository::MetricsQuery;
use chrono::{Duration, Utc};

async fn analyze_historical_metrics(repository: &InMemoryRepository) -> Result<()> {
    // Query metrics from the last 7 days
    let one_week_ago = Utc::now() - Duration::days(7);
    
    let metrics = repository.load().await
        .after(one_week_ago)
        .with_tag("pipeline", "daily_etl")
        .execute().await?;
    
    // Analyze trends
    println!("Metrics from last 7 days:");
    for (key, context) in metrics {
        println!("  Date: {}", key.tags.get("date").unwrap_or(&"unknown".to_string()));
        
        // Extract specific metrics
        if let Some(size) = context.get_metric("row_count") {
            println!("    Row count: {}", size);
        }
        if let Some(completeness) = context.get_metric("order_id_completeness") {
            println!("    Order ID completeness: {:.2}%", completeness * 100.0);
        }
    }
    
    Ok(())
}
```

## Step 4: Building a Time-Series Dashboard

Create a dashboard showing quality metrics over time:

```rust
use plotters::prelude::*;

async fn create_quality_dashboard(repository: &InMemoryRepository) -> Result<()> {
    // Query 30 days of metrics
    let thirty_days_ago = Utc::now() - Duration::days(30);
    
    let metrics = repository.load().await
        .after(thirty_days_ago)
        .with_tag("pipeline", "daily_etl")
        .sort_by_timestamp()
        .execute().await?;
    
    // Extract time series data
    let mut dates = Vec::new();
    let mut completeness_values = Vec::new();
    let mut row_counts = Vec::new();
    
    for (key, context) in metrics {
        dates.push(key.timestamp);
        
        if let Some(completeness) = context.get_metric("order_id_completeness") {
            completeness_values.push(completeness * 100.0);
        }
        
        if let Some(count) = context.get_metric("row_count") {
            row_counts.push(count as f64);
        }
    }
    
    // Generate dashboard HTML
    generate_dashboard_html(&dates, &completeness_values, &row_counts)?;
    
    // Print summary statistics
    println!("\n📊 30-Day Quality Summary");
    println!("{'='*40}");
    println!("Average completeness: {:.2}%", 
             completeness_values.iter().sum::<f64>() / completeness_values.len() as f64);
    println!("Average row count: {:.0}", 
             row_counts.iter().sum::<f64>() / row_counts.len() as f64);
    println!("Completeness std dev: {:.2}%", 
             calculate_std_dev(&completeness_values));
    
    Ok(())
}

fn calculate_std_dev(values: &[f64]) -> f64 {
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values.iter()
        .map(|v| (v - mean).powi(2))
        .sum::<f64>() / values.len() as f64;
    variance.sqrt()
}
```

## Step 5: Detecting Quality Degradation

Use repository data to detect trends and degradation:

```rust
async fn detect_quality_degradation(repository: &InMemoryRepository) -> Result<()> {
    // Get metrics from last 14 days
    let two_weeks_ago = Utc::now() - Duration::days(14);
    
    let metrics = repository.load().await
        .after(two_weeks_ago)
        .for_analyzers(vec!["completeness", "row_count"])
        .execute().await?;
    
    // Split into two weeks for comparison
    let mut week1_completeness = Vec::new();
    let mut week2_completeness = Vec::new();
    let one_week_ago = Utc::now() - Duration::days(7);
    
    for (key, context) in metrics {
        if let Some(completeness) = context.get_metric("completeness") {
            if key.timestamp < one_week_ago {
                week1_completeness.push(completeness);
            } else {
                week2_completeness.push(completeness);
            }
        }
    }
    
    // Calculate averages
    let week1_avg = week1_completeness.iter().sum::<f64>() / week1_completeness.len() as f64;
    let week2_avg = week2_completeness.iter().sum::<f64>() / week2_completeness.len() as f64;
    
    // Check for degradation
    let degradation_pct = ((week1_avg - week2_avg) / week1_avg) * 100.0;
    
    if degradation_pct > 5.0 {
        println!("⚠️  ALERT: Data quality degradation detected!");
        println!("   Week 1 average: {:.2}%", week1_avg * 100.0);
        println!("   Week 2 average: {:.2}%", week2_avg * 100.0);
        println!("   Degradation: {:.1}%", degradation_pct);
        
        // Send alert
        send_quality_alert(degradation_pct).await?;
    } else {
        println!("✅ Data quality stable");
    }
    
    Ok(())
}
```

## Step 6: Filesystem Repository for Persistence

Store metrics permanently on disk:

```rust
use term_guard::repository::FileSystemRepository;
use std::path::PathBuf;

async fn setup_persistent_repository() -> Result<FileSystemRepository> {
    // Create repository with filesystem backend
    let repository = FileSystemRepository::new(
        PathBuf::from("/data/term/metrics"),
        FileSystemOptions {
            format: SerializationFormat::Json,
            compression: Some(CompressionType::Gzip),
            retention_days: Some(90),  // Auto-delete after 90 days
        }
    )?;
    
    // Run daily validation
    run_daily_validation(&repository).await?;
    
    // Repository persists across application restarts
    Ok(repository)
}

async fn run_daily_validation(repository: &FileSystemRepository) -> Result<()> {
    let ctx = load_todays_data().await?;
    
    // Create validation suite with analyzers
    let suite = ValidationSuite::builder("production_validation")
        .with_repository(repository.clone())
        .add_analyzers(vec![
            Box::new(SizeAnalyzer::new()),
            Box::new(CompletenessAnalyzer::new("order_id")),
            Box::new(MeanAnalyzer::new("amount")),
            Box::new(StandardDeviationAnalyzer::new("amount")),
            Box::new(ApproxQuantileAnalyzer::new("amount", 0.95)),
        ])
        .add_check(
            Check::builder("data_quality_checks")
                .has_size(Assertion::GreaterThan(1000))
                .has_completeness("order_id", 1.0)
                .build()
        )
        .build();
    
    // Save with daily timestamp
    let key = ResultKey::new()
        .with_timestamp(Utc::now())
        .with_tag("date", Utc::now().format("%Y-%m-%d").to_string())
        .with_tag("environment", "production");
    
    let results = suite.run_and_save(&ctx, key).await?;
    
    Ok(())
}
```

## Step 7: Comparing Metrics Across Environments

Compare quality between different environments or datasets:

```rust
async fn compare_environments(repository: &InMemoryRepository) -> Result<()> {
    // Query production metrics
    let prod_metrics = repository.load().await
        .with_tag("environment", "production")
        .last_n_results(7)
        .execute().await?;
    
    // Query staging metrics
    let staging_metrics = repository.load().await
        .with_tag("environment", "staging")
        .last_n_results(7)
        .execute().await?;
    
    // Compare key metrics
    println!("\n📊 Environment Comparison (Last 7 Days)");
    println!("{'='*50}");
    
    let prod_stats = calculate_statistics(prod_metrics);
    let staging_stats = calculate_statistics(staging_metrics);
    
    println!("Metric              Production    Staging      Diff");
    println!("{'-'*50}");
    
    for metric_name in ["completeness", "row_count", "mean_amount"] {
        let prod_val = prod_stats.get(metric_name).unwrap_or(&0.0);
        let staging_val = staging_stats.get(metric_name).unwrap_or(&0.0);
        let diff = ((prod_val - staging_val) / prod_val * 100.0).abs();
        
        println!("{:18} {:10.2} {:10.2} {:6.1}%",
                 metric_name, prod_val, staging_val, diff);
        
        // Alert on significant differences
        if diff > 10.0 {
            println!("  ⚠️  Significant difference detected!");
        }
    }
    
    Ok(())
}
```

## Step 8: Advanced Repository Queries

Leverage advanced query capabilities:

```rust
async fn advanced_repository_queries(repository: &InMemoryRepository) -> Result<()> {
    // Complex query with multiple filters
    let results = repository.load().await
        .after(Utc::now() - Duration::days(30))
        .before(Utc::now())
        .with_tag("environment", "production")
        .with_tag_in("region", vec!["us-east", "us-west"])
        .for_analyzers(vec!["completeness", "uniqueness"])
        .having_metric("completeness", |v| v < 0.95)  // Find quality issues
        .sort_by_metric("completeness", SortOrder::Ascending)
        .limit(10)
        .execute().await?;
    
    println!("\nTop 10 Quality Issues (Last 30 Days):");
    for (key, context) in results {
        println!("  {} - Region: {} - Completeness: {:.2}%",
                 key.timestamp.format("%Y-%m-%d"),
                 key.tags.get("region").unwrap_or(&"unknown".to_string()),
                 context.get_metric("completeness").unwrap_or(0.0) * 100.0);
    }
    
    // Aggregate metrics by tag
    let by_region = repository.load().await
        .after(Utc::now() - Duration::days(7))
        .group_by_tag("region")
        .aggregate_metrics(vec![
            ("completeness", AggregateFunction::Average),
            ("row_count", AggregateFunction::Sum),
        ])
        .execute().await?;
    
    println!("\nMetrics by Region (Last 7 Days):");
    for (region, metrics) in by_region {
        println!("  {}: Avg Completeness: {:.2}%, Total Rows: {}",
                 region,
                 metrics.get("completeness_avg").unwrap_or(&0.0) * 100.0,
                 metrics.get("row_count_sum").unwrap_or(&0.0) as i64);
    }
    
    Ok(())
}
```

## Step 9: Exercise - Building a Quality Monitoring System

Let's build a complete monitoring system:

```rust
struct QualityMonitoringSystem {
    repository: Arc<dyn MetricsRepository>,
    alert_thresholds: HashMap<String, f64>,
}

impl QualityMonitoringSystem {
    async fn run_daily_monitoring(&self) -> Result<()> {
        // 1. Run validation
        let results = self.run_validation().await?;
        
        // 2. Store metrics
        self.store_metrics(results).await?;
        
        // 3. Analyze trends
        let trends = self.analyze_trends().await?;
        
        // 4. Check for anomalies
        let anomalies = self.detect_anomalies().await?;
        
        // 5. Generate report
        self.generate_daily_report(trends, anomalies).await?;
        
        // 6. Send alerts if needed
        if !anomalies.is_empty() {
            self.send_alerts(anomalies).await?;
        }
        
        Ok(())
    }
    
    async fn analyze_trends(&self) -> Result<TrendAnalysis> {
        let two_weeks = self.repository.load().await
            .after(Utc::now() - Duration::days(14))
            .execute().await?;
        
        // Calculate moving averages
        let mut moving_avg = HashMap::new();
        for window_size in [3, 7, 14] {
            let avg = calculate_moving_average(&two_weeks, window_size);
            moving_avg.insert(format!("{}d_avg", window_size), avg);
        }
        
        // Detect trends using linear regression
        let trend_direction = detect_trend(&two_weeks);
        
        Ok(TrendAnalysis {
            moving_averages: moving_avg,
            trend: trend_direction,
            forecast: forecast_next_value(&two_weeks),
        })
    }
    
    async fn detect_anomalies(&self) -> Result<Vec<Anomaly>> {
        // Use statistical methods to detect anomalies
        let metrics = self.repository.load().await
            .last_n_results(30)
            .execute().await?;
        
        let mut anomalies = Vec::new();
        
        for (metric_name, threshold) in &self.alert_thresholds {
            if let Some(anomaly) = detect_metric_anomaly(&metrics, metric_name, *threshold) {
                anomalies.push(anomaly);
            }
        }
        
        Ok(anomalies)
    }
}

// Use the monitoring system
#[tokio::main]
async fn main() -> Result<()> {
    let monitoring = QualityMonitoringSystem {
        repository: Arc::new(FileSystemRepository::new("/data/metrics")?),
        alert_thresholds: HashMap::from([
            ("completeness".to_string(), 0.95),
            ("uniqueness".to_string(), 0.99),
            ("row_count_change".to_string(), 0.20),
        ]),
    };
    
    // Run daily
    monitoring.run_daily_monitoring().await?;
    
    Ok(())
}
```

## Summary

You've learned how to:
- ✅ Set up and configure metrics repositories
- ✅ Store validation results with versioning and tags
- ✅ Query historical metrics with complex filters
- ✅ Build time-series quality dashboards
- ✅ Detect quality degradation and anomalies
- ✅ Compare metrics across environments
- ✅ Implement comprehensive monitoring systems

## Next Steps

- Learn about [Anomaly Detection](./10-anomaly-detection.md) with historical baselines
- Explore [Incremental Analysis](./11-incremental-analysis.md) for efficient processing
- Read about [Repository Architecture](../explanation/repository-architecture.md)

## Troubleshooting

**Q: How much storage does the repository need?**
A: JSON format uses ~1-2KB per metric set. With compression, 1 million metric sets ≈ 100-200MB.

**Q: Can I use a database for the repository?**
A: Yes, implement the `MetricsRepository` trait for your database.

**Q: How do I handle repository migrations?**
A: Version your ResultKey tags and implement migration logic when querying.

## Exercises

1. Implement a custom repository backend using PostgreSQL
2. Build a real-time dashboard with WebSocket updates
3. Create an alerting system with Slack/email notifications
4. Implement metric forecasting using time-series analysis