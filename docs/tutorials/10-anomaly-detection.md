# Tutorial: Detecting Data Anomalies

## Introduction

In this tutorial, you'll learn how to use Term's anomaly detection system to automatically identify unusual patterns in your data quality metrics. You'll understand how to establish baselines, configure detection strategies, and respond to anomalies in production.

## What You'll Learn

- How anomaly detection works in Term
- How to establish baseline metrics
- How to configure different detection strategies
- How to integrate anomaly detection into data pipelines
- How to tune sensitivity and reduce false positives

## Prerequisites

Before starting, you should:
- Have completed the [Metrics Repository tutorial](./09-metrics-repository.md)
- Understand Term's analyzer framework
- Have historical data for baseline establishment

## Step 1: Understanding Anomaly Detection

Term detects anomalies by comparing current metrics against historical baselines using various strategies:

```rust
use term_guard::analyzers::anomaly::{
    AnomalyDetectionRunner, 
    RelativeRateOfChangeDetector,
    AnomalyDetectionConfig
};

// Anomaly detection flow:
// 1. Establish baseline from historical metrics
// 2. Run current analysis
// 3. Apply detection strategy
// 4. Report anomalies if thresholds exceeded
```

## Step 2: Your First Anomaly Detection

Let's detect anomalies in data completeness:

```rust
use term_guard::prelude::*;
use term_guard::analyzers::anomaly::*;
use term_guard::repository::InMemoryRepository;
use datafusion::prelude::*;
use chrono::{Duration, Utc};

#[tokio::main]
async fn main() -> Result<()> {
    // Set up repository with historical data
    let repository = InMemoryRepository::new();
    
    // Load and analyze historical data (establishing baseline)
    for day in 1..=7 {
        let ctx = SessionContext::new();
        ctx.register_csv(
            "orders",
            &format!("data/orders_2024_01_{:02}.csv", day),
            CsvReadOptions::default()
        ).await?;
        
        // Run analysis and store metrics
        let analyzer = CompletenessAnalyzer::new("customer_id");
        let result = analyzer.analyze(&ctx).await?;
        
        let key = ResultKey::new()
            .with_timestamp(Utc::now() - Duration::days(8 - day))
            .with_tag("dataset", "orders");
        
        repository.save(key, result).await?;
    }
    
    // Now analyze today's data
    let ctx = SessionContext::new();
    ctx.register_csv(
        "orders",
        "data/orders_2024_01_08.csv",
        CsvReadOptions::default()
    ).await?;
    
    // Configure anomaly detection
    let detector = RelativeRateOfChangeDetector::new(0.1)  // 10% threshold
        .with_min_history_size(5);  // Need at least 5 data points
    
    // Run anomaly detection
    let mut runner = AnomalyDetectionRunner::new(repository.clone())
        .add_detector("customer_id_completeness", Box::new(detector));
    
    let anomalies = runner.detect(&ctx).await?;
    
    // Check for anomalies
    if anomalies.is_empty() {
        println!("✅ No anomalies detected");
    } else {
        for anomaly in anomalies {
            println!("⚠️ Anomaly detected!");
            println!("  Metric: {}", anomaly.metric_name);
            println!("  Current value: {:.2}", anomaly.current_value);
            println!("  Expected range: {:.2} - {:.2}", 
                     anomaly.lower_bound, anomaly.upper_bound);
            println!("  Severity: {:?}", anomaly.severity);
        }
    }
    
    Ok(())
}
```

## Step 3: Detection Strategies

Term provides multiple detection strategies for different scenarios:

```rust
use term_guard::analyzers::anomaly::*;

async fn configure_detection_strategies() -> Result<()> {
    let repository = setup_repository().await?;
    
    // 1. Relative Rate of Change - for percentage-based thresholds
    let relative_detector = RelativeRateOfChangeDetector::new(0.15)  // 15% change
        .with_min_history_size(7)
        .with_lookback_window(14);  // Use last 14 days
    
    // 2. Absolute Change - for fixed thresholds
    let absolute_detector = AbsoluteChangeDetector::new(1000.0)  // 1000 row change
        .with_direction(Direction::Both);  // Detect increases and decreases
    
    // 3. Z-Score - for statistical outliers
    let zscore_detector = ZScoreDetector::new(3.0)  // 3 standard deviations
        .with_min_samples(20)
        .with_use_robust_statistics(true);  // Use median/MAD instead of mean/std
    
    // 4. Custom detector for business rules
    let custom_detector = CustomAnomalyDetector::new(
        |history: &[f64], current: f64| {
            // Custom logic: Alert if current is lowest value ever
            if let Some(min) = history.iter().min_by(|a, b| a.partial_cmp(b).unwrap()) {
                current < *min
            } else {
                false
            }
        }
    );
    
    // Combine multiple detectors
    let mut runner = AnomalyDetectionRunner::new(repository)
        .add_detector("completeness.*", Box::new(relative_detector))
        .add_detector("row_count", Box::new(absolute_detector))
        .add_detector("mean_amount", Box::new(zscore_detector))
        .add_detector("min_amount", Box::new(custom_detector));
    
    Ok(())
}
```

## Step 4: Establishing Baselines

Create robust baselines for accurate detection:

```rust
async fn establish_baselines(repository: &InMemoryRepository) -> Result<()> {
    // Option 1: Rolling window baseline
    let rolling_baseline = BaselineConfig::RollingWindow {
        window_size: Duration::days(30),
        min_data_points: 20,
        exclude_weekends: true,
        exclude_holidays: vec!["2024-01-01", "2024-07-04"],
    };
    
    // Option 2: Seasonal baseline
    let seasonal_baseline = BaselineConfig::Seasonal {
        season_length: Duration::days(7),  // Weekly pattern
        num_seasons: 4,  // Use 4 weeks of history
        same_day_of_week: true,  // Compare Mondays to Mondays
    };
    
    // Option 3: Training period baseline
    let training_baseline = BaselineConfig::TrainingPeriod {
        start: Utc::now() - Duration::days(90),
        end: Utc::now() - Duration::days(30),
        exclude_outliers: true,
        outlier_threshold: 3.0,  // Z-score threshold
    };
    
    // Apply baseline configuration
    let detector = RelativeRateOfChangeDetector::new(0.1)
        .with_baseline_config(seasonal_baseline);
    
    // Run detection with seasonal awareness
    let mut runner = AnomalyDetectionRunner::new(repository.clone())
        .with_baseline_config(seasonal_baseline)
        .add_detector("order_volume", Box::new(detector));
    
    Ok(())
}
```

## Step 5: Configuring Sensitivity

Tune detection sensitivity to balance between catching issues and avoiding false positives:

```rust
async fn tune_detection_sensitivity(ctx: &SessionContext) -> Result<()> {
    // Start with conservative thresholds
    let mut sensitivity_config = SensitivityConfig {
        base_threshold: 0.2,  // 20% change
        confidence_multiplier: 1.0,
        min_confidence: 0.8,
    };
    
    // Test different sensitivity levels
    for sensitivity in [0.1, 0.15, 0.2, 0.25, 0.3] {
        let detector = RelativeRateOfChangeDetector::new(sensitivity);
        
        // Run detection
        let anomalies = run_detection_with_detector(&ctx, detector).await?;
        
        println!("Sensitivity {}: {} anomalies detected", 
                 sensitivity, anomalies.len());
        
        // Analyze false positive rate
        let false_positives = calculate_false_positive_rate(&anomalies).await?;
        println!("  False positive rate: {:.2}%", false_positives * 100.0);
        
        // Find optimal sensitivity
        if false_positives < 0.05 && anomalies.len() > 0 {
            println!("  ✓ Optimal sensitivity found: {}", sensitivity);
            sensitivity_config.base_threshold = sensitivity;
            break;
        }
    }
    
    Ok(())
}
```

## Step 6: Multi-Metric Anomaly Detection

Detect anomalies across multiple related metrics:

```rust
async fn detect_multi_metric_anomalies(ctx: &SessionContext) -> Result<()> {
    // Configure correlated metric detection
    let correlation_config = CorrelatedAnomalyConfig {
        metrics: vec![
            "order_count".to_string(),
            "total_revenue".to_string(),
            "average_order_value".to_string(),
        ],
        correlation_threshold: 0.7,  // Metrics with >0.7 correlation
        joint_probability_threshold: 0.95,  // 95% confidence
    };
    
    // Create multi-metric detector
    let multi_detector = MultiMetricAnomalyDetector::new()
        .with_correlation_config(correlation_config)
        .add_rule(
            "revenue_consistency",
            |metrics: &HashMap<String, f64>| {
                // Check if revenue = orders * avg_value (within 5% tolerance)
                let expected = metrics.get("order_count").unwrap_or(&0.0) 
                             * metrics.get("average_order_value").unwrap_or(&0.0);
                let actual = metrics.get("total_revenue").unwrap_or(&0.0);
                
                (actual - expected).abs() / expected > 0.05
            }
        );
    
    // Run detection
    let results = multi_detector.detect(ctx).await?;
    
    for anomaly in results {
        if anomaly.is_multi_metric {
            println!("🔍 Multi-metric anomaly detected!");
            println!("  Affected metrics: {:?}", anomaly.affected_metrics);
            println!("  Correlation impact: {:.2}", anomaly.correlation_score);
        }
    }
    
    Ok(())
}
```

## Step 7: Real-Time Anomaly Detection

Implement streaming anomaly detection:

```rust
use tokio::time::{interval, Duration as TokioDuration};

async fn real_time_anomaly_detection() -> Result<()> {
    let repository = InMemoryRepository::new();
    
    // Configure real-time detection
    let config = RealTimeAnomalyConfig {
        check_interval: TokioDuration::from_secs(60),  // Check every minute
        metrics_to_monitor: vec![
            "api_response_time_p99",
            "error_rate",
            "throughput",
        ],
        alert_cooldown: TokioDuration::from_secs(300),  // 5 min between alerts
    };
    
    // Create detectors with different strategies
    let mut detector_chain = AnomalyDetectorChain::new()
        .add_detector(
            "response_time",
            Box::new(ZScoreDetector::new(2.5)),  // Sensitive for latency
            AlertPriority::High
        )
        .add_detector(
            "error_rate",
            Box::new(AbsoluteChangeDetector::new(0.05)),  // 5% error rate change
            AlertPriority::Critical
        )
        .add_detector(
            "throughput",
            Box::new(RelativeRateOfChangeDetector::new(0.3)),  // 30% throughput change
            AlertPriority::Medium
        );
    
    // Start monitoring loop
    let mut ticker = interval(config.check_interval);
    let mut last_alert_time = HashMap::new();
    
    loop {
        ticker.tick().await;
        
        // Get latest metrics
        let ctx = get_latest_data_context().await?;
        
        // Run anomaly detection
        let anomalies = detector_chain.detect(&ctx, &repository).await?;
        
        // Process anomalies
        for anomaly in anomalies {
            // Check cooldown
            let should_alert = should_send_alert(
                &anomaly,
                &last_alert_time,
                config.alert_cooldown
            );
            
            if should_alert {
                send_alert(anomaly.clone()).await?;
                last_alert_time.insert(
                    anomaly.metric_name.clone(),
                    Utc::now()
                );
            }
        }
        
        // Update repository with latest metrics
        update_repository(&repository, &ctx).await?;
    }
}
```

## Step 8: Anomaly Response Automation

Automate responses to detected anomalies:

```rust
async fn setup_anomaly_response_automation() -> Result<()> {
    // Define response rules
    let response_rules = ResponseRuleSet::new()
        .add_rule(
            "data_quality_degradation",
            ResponseRule {
                condition: |anomaly: &Anomaly| {
                    anomaly.metric_name.contains("completeness") 
                    && anomaly.severity == Severity::Critical
                },
                actions: vec![
                    Action::PauseDataPipeline,
                    Action::NotifyOnCall,
                    Action::CreateIncident,
                    Action::TriggerDataValidation,
                ],
                auto_remediate: true,
            }
        )
        .add_rule(
            "performance_degradation",
            ResponseRule {
                condition: |anomaly: &Anomaly| {
                    anomaly.metric_name.contains("latency")
                    && anomaly.current_value > anomaly.upper_bound * 2.0
                },
                actions: vec![
                    Action::ScaleResources,
                    Action::EnableCircuitBreaker,
                    Action::NotifyTeam,
                ],
                auto_remediate: false,  // Require manual approval
            }
        );
    
    // Create response handler
    let response_handler = AnomalyResponseHandler::new()
        .with_rules(response_rules)
        .with_notification_channel(NotificationChannel::Slack {
            webhook_url: "https://hooks.slack.com/...",
            channel: "#data-quality-alerts",
        })
        .with_incident_manager(IncidentManager::PagerDuty {
            api_key: "...",
            service_id: "data-platform",
        });
    
    // Process anomalies
    let anomalies = detect_anomalies().await?;
    
    for anomaly in anomalies {
        let response = response_handler.handle(anomaly).await?;
        
        println!("Anomaly handled:");
        println!("  Actions taken: {:?}", response.actions_taken);
        println!("  Remediation status: {:?}", response.remediation_status);
        
        if response.requires_manual_intervention {
            println!("  ⚠️ Manual intervention required!");
        }
    }
    
    Ok(())
}
```

## Step 9: Exercise - Production Monitoring System

Build a complete production anomaly detection system:

```rust
struct ProductionAnomalyMonitor {
    repository: Arc<dyn MetricsRepository>,
    detector_registry: HashMap<String, Box<dyn AnomalyDetector>>,
    response_handler: AnomalyResponseHandler,
    config: MonitorConfig,
}

impl ProductionAnomalyMonitor {
    async fn run(&mut self) -> Result<()> {
        loop {
            // 1. Collect current metrics
            let metrics = self.collect_metrics().await?;
            
            // 2. Run anomaly detection
            let anomalies = self.detect_anomalies(metrics).await?;
            
            // 3. Filter and prioritize
            let prioritized = self.prioritize_anomalies(anomalies);
            
            // 4. Handle each anomaly
            for anomaly in prioritized {
                self.handle_anomaly(anomaly).await?;
            }
            
            // 5. Update dashboards
            self.update_dashboards().await?;
            
            // 6. Sleep until next check
            tokio::time::sleep(self.config.check_interval).await;
        }
    }
    
    async fn detect_anomalies(&self, metrics: MetricSet) -> Result<Vec<Anomaly>> {
        let mut all_anomalies = Vec::new();
        
        // Get historical baseline
        let baseline = self.repository.load().await
            .after(Utc::now() - Duration::days(30))
            .execute().await?;
        
        // Run each detector
        for (metric_pattern, detector) in &self.detector_registry {
            let matching_metrics = metrics.filter(metric_pattern);
            
            for (metric_name, value) in matching_metrics {
                if let Some(anomaly) = detector.detect(&baseline, metric_name, value).await? {
                    all_anomalies.push(anomaly);
                }
            }
        }
        
        // Check for correlated anomalies
        let correlated = self.find_correlated_anomalies(&all_anomalies);
        all_anomalies.extend(correlated);
        
        Ok(all_anomalies)
    }
    
    async fn handle_anomaly(&self, anomaly: Anomaly) -> Result<()> {
        // Log anomaly
        tracing::warn!(
            metric = %anomaly.metric_name,
            value = %anomaly.current_value,
            severity = ?anomaly.severity,
            "Anomaly detected"
        );
        
        // Execute response
        let response = self.response_handler.handle(anomaly.clone()).await?;
        
        // Track in repository
        self.repository.save(
            ResultKey::new()
                .with_timestamp(Utc::now())
                .with_tag("type", "anomaly")
                .with_tag("metric", &anomaly.metric_name),
            AnomalyContext::from(anomaly)
        ).await?;
        
        Ok(())
    }
}
```

## Summary

You've learned how to:
- ✅ Set up anomaly detection with various strategies
- ✅ Establish baselines from historical data
- ✅ Configure detection sensitivity
- ✅ Detect multi-metric anomalies
- ✅ Implement real-time monitoring
- ✅ Automate anomaly responses

## Next Steps

- Explore [Incremental Analysis](./11-incremental-analysis.md) for efficient processing
- Learn about [Constraint Suggestions](./12-constraint-suggestions.md)
- Read the [Anomaly Detection Reference](../reference/anomaly-detection.md)

## Troubleshooting

**Q: Too many false positives?**
A: Increase thresholds, use longer baseline windows, or switch to robust statistics (median/MAD).

**Q: Missing real anomalies?**
A: Decrease thresholds, use multiple detection strategies, or check for seasonal patterns.

**Q: Slow detection performance?**
A: Use incremental metrics computation, cache baselines, or reduce monitoring frequency.

## Exercises

1. Implement custom anomaly detector for your business metrics
2. Build a dashboard showing anomaly trends
3. Create automated remediation for common anomalies
4. Implement anomaly detection for categorical data