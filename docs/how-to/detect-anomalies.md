# How to Detect Anomalies in Data Quality Metrics

This guide shows you how to set up anomaly detection to monitor your data quality metrics over time and catch issues before they escalate.

## Prerequisites

- Term project with validation checks configured
- Historical metric data (or willingness to collect it)
- Understanding of your expected data patterns

## Basic Setup

### 1. Choose a Metrics Repository

For testing and development, use the in-memory repository:

```rust
use term_guard::analyzers::InMemoryMetricsRepository;

let repository = InMemoryMetricsRepository::new();
```

For production, implement the `MetricsRepository` trait for your storage backend:

```rust
use term_guard::analyzers::{MetricsRepository, MetricDataPoint};

struct PostgresMetricsRepository { /* ... */ }

#[async_trait]
impl MetricsRepository for PostgresMetricsRepository {
    async fn store_metric(
        &self,
        metric_name: &str,
        value: MetricValue,
        timestamp: DateTime<Utc>,
    ) -> AnalyzerResult<()> {
        // Store in PostgreSQL
    }
    
    async fn get_metric_history(
        &self,
        metric_name: &str,
        since: Option<DateTime<Utc>>,
        until: Option<DateTime<Utc>>,
        limit: Option<usize>,
    ) -> AnalyzerResult<Vec<MetricDataPoint>> {
        // Retrieve from PostgreSQL
    }
}
```

### 2. Configure Detection Strategies

Choose appropriate strategies based on your metric types:

```rust
use term_guard::analyzers::{
    AnomalyDetectionRunner, RelativeRateOfChangeDetector,
    AbsoluteChangeDetector, ZScoreDetector
};

let detector = AnomalyDetectionRunner::builder()
    .repository(Box::new(repository))
    // Detect 10% relative changes in completeness
    .add_detector("completeness.*", Box::new(
        RelativeRateOfChangeDetector::new(0.1)
    ))
    // Detect absolute changes > 1000 in row counts
    .add_detector("size", Box::new(
        AbsoluteChangeDetector::new(1000.0)
    ))
    // Detect statistical outliers in numeric metrics
    .add_detector("mean_*", Box::new(
        ZScoreDetector::new(3.0)
            .with_min_history_size(20)
    ))
    .build()?;
```

### 3. Run Detection

```rust
// Run your regular validation
let runner = AnalysisRunner::new();
let metrics = runner.run(&ctx).await?;

// Detect anomalies
let anomalies = detector.detect_anomalies(&metrics).await?;

// Process results
for anomaly in anomalies {
    eprintln!(
        "Anomaly in {}: {} (confidence: {:.2})",
        anomaly.metric_name,
        anomaly.description,
        anomaly.confidence
    );
}
```

## Advanced Configuration

### Custom Detection Strategy

Implement the `AnomalyDetector` trait for custom logic:

```rust
use term_guard::analyzers::{AnomalyDetector, Anomaly};

struct SeasonalDetector {
    seasonal_factor: f64,
}

#[async_trait]
impl AnomalyDetector for SeasonalDetector {
    async fn detect(
        &self,
        metric_name: &str,
        current_value: &MetricValue,
        history: &[MetricDataPoint],
    ) -> AnalyzerResult<Option<Anomaly>> {
        // Compare against same day last week/month
        // Account for seasonal patterns
    }
    
    fn name(&self) -> &str {
        "Seasonal"
    }
    
    fn description(&self) -> &str {
        "Detects anomalies accounting for seasonal patterns"
    }
}
```

### Confidence Thresholds

Filter anomalies by confidence level:

```rust
use term_guard::analyzers::AnomalyDetectionConfig;

let config = AnomalyDetectionConfig {
    min_confidence: 0.8,  // Only report high-confidence anomalies
    store_current_metrics: true,
    default_history_window: Duration::days(30),
};

let detector = AnomalyDetectionRunner::builder()
    .repository(Box::new(repository))
    .config(config)
    // ... add detectors
    .build()?;
```

### Pattern Matching

Use wildcards to apply detectors to groups of metrics:

```rust
detector
    // All completeness metrics
    .add_detector("completeness.*", Box::new(detector1))
    // All metrics ending with "_count"
    .add_detector("*_count", Box::new(detector2))
    // Specific metric
    .add_detector("revenue_mean", Box::new(detector3))
    // All metrics (fallback)
    .add_detector("*", Box::new(detector4))
```

## Integration Examples

### With CI/CD Pipeline

```rust
async fn validate_and_check_anomalies(
    ctx: &SessionContext,
) -> Result<(), Box<dyn Error>> {
    // Initialize detector with persistent storage
    let detector = create_detector()?;
    
    // Run validation
    let runner = AnalysisRunner::new();
    let metrics = runner.run(ctx).await?;
    
    // Check for anomalies
    let anomalies = detector.detect_anomalies(&metrics).await?;
    
    if !anomalies.is_empty() {
        // Fail the build if critical anomalies detected
        let critical = anomalies.iter()
            .filter(|a| a.confidence > 0.9)
            .count();
        
        if critical > 0 {
            eprintln!("Critical anomalies detected!");
            for anomaly in anomalies {
                eprintln!("- {}", anomaly.description);
            }
            std::process::exit(1);
        }
    }
    
    Ok(())
}
```

### With Alerting System

```rust
async fn monitor_with_alerts(
    detector: &AnomalyDetectionRunner,
    metrics: &AnalyzerContext,
) -> Result<()> {
    let anomalies = detector.detect_anomalies(metrics).await?;
    
    for anomaly in anomalies {
        match anomaly.confidence {
            c if c >= 0.9 => {
                // Page on-call for critical anomalies
                alert_pagerduty(&anomaly).await?;
            }
            c if c >= 0.7 => {
                // Send Slack notification for warnings
                notify_slack(&anomaly).await?;
            }
            _ => {
                // Log low-confidence anomalies
                info!("Low confidence anomaly: {:?}", anomaly);
            }
        }
    }
    
    Ok(())
}
```

## Best Practices

1. **Build History First**: Collect at least 2 weeks of data before enabling anomaly detection
2. **Tune Thresholds**: Start with conservative thresholds and adjust based on false positive rates
3. **Use Multiple Strategies**: Different metrics benefit from different detection approaches
4. **Monitor Detector Performance**: Track false positives and adjust configurations
5. **Consider Business Context**: Some variations (holidays, releases) are expected

## Troubleshooting

### Too Many False Positives

- Increase thresholds (e.g., 0.2 instead of 0.1 for rate of change)
- Require more history (increase `min_history_size`)
- Use confidence filtering
- Consider seasonal patterns

### Missing Real Issues

- Decrease thresholds carefully
- Use multiple detection strategies
- Check if metrics are too aggregated
- Ensure sufficient historical data

### Performance Issues

- Limit history window size
- Batch metric retrieval
- Consider sampling for high-frequency metrics
- Use caching for repository queries