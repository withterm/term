//! Example demonstrating the anomaly detection strategy pattern.
//!
//! This example shows how to use the RelativeRateOfChangeStrategy to detect
//! anomalies in metric data based on percentage changes between consecutive values.
//!
//! Run with:
//! ```bash
//! cargo run --example anomaly_detection_strategy
//! ```

use chrono::{Duration, Utc};
use term_guard::analyzers::anomaly::{
    AnomalyDetectionStrategy, MetricPoint, RelativeRateOfChangeStrategy,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Anomaly Detection Strategy Example ===\n");

    // Create a strategy with symmetric thresholds (10% max change)
    let symmetric_strategy = RelativeRateOfChangeStrategy::new(0.1)?;

    // Create historical data
    let now = Utc::now();
    let history = vec![
        MetricPoint::with_timestamp(100.0, now - Duration::days(3)),
        MetricPoint::with_timestamp(105.0, now - Duration::days(2)),
        MetricPoint::with_timestamp(103.0, now - Duration::days(1)),
    ];

    println!("Historical data:");
    for point in &history {
        println!(
            "  {} - value: {}",
            point.timestamp.format("%Y-%m-%d"),
            point.value
        );
    }
    println!();

    // Test normal change (within threshold)
    println!("Testing normal change (5% increase):");
    let current = MetricPoint::with_timestamp(108.15, now);
    let result = symmetric_strategy.detect(&history, current.clone()).await?;
    println!(
        "  Value: {} -> Result: {}",
        current.value,
        if result.is_anomaly {
            "ANOMALY"
        } else {
            "NORMAL"
        }
    );
    println!("  Explanation: {}", result.explanation);
    if let Some(range) = result.expected_range {
        println!("  Expected range: {:.2} - {:.2}", range.0, range.1);
    }
    println!();

    // Test anomalous increase
    println!("Testing anomalous increase (20% increase):");
    let current = MetricPoint::with_timestamp(123.6, now);
    let result = symmetric_strategy.detect(&history, current.clone()).await?;
    println!(
        "  Value: {} -> Result: {}",
        current.value,
        if result.is_anomaly {
            "ANOMALY"
        } else {
            "NORMAL"
        }
    );
    println!("  Confidence: {:.2}", result.confidence);
    println!("  Explanation: {}", result.explanation);
    if let Some(range) = result.expected_range {
        println!("  Expected range: {:.2} - {:.2}", range.0, range.1);
    }
    println!();

    // Create asymmetric strategy (allow more increase than decrease)
    println!("=== Asymmetric Thresholds Example ===\n");
    let asymmetric_strategy = RelativeRateOfChangeStrategy::with_asymmetric_thresholds(
        Some(0.2),  // Allow 20% increase
        Some(0.05), // Allow only 5% decrease
    )?;

    println!("Testing with asymmetric thresholds (max +20%, max -5%):");

    // Test 15% increase (should be OK)
    let current = MetricPoint::with_timestamp(118.45, now);
    let result = asymmetric_strategy.detect(&history, current).await?;
    println!(
        "  15% increase -> {}",
        if result.is_anomaly {
            "ANOMALY"
        } else {
            "NORMAL"
        }
    );

    // Test 7% decrease (should be anomaly)
    let current = MetricPoint::with_timestamp(95.79, now);
    let result = asymmetric_strategy.detect(&history, current).await?;
    println!(
        "  7% decrease -> {}",
        if result.is_anomaly {
            "ANOMALY"
        } else {
            "NORMAL"
        }
    );
    println!();

    // Edge case: zero baseline
    println!("=== Edge Case: Zero Baseline ===\n");
    let zero_history = vec![MetricPoint::with_timestamp(0.0, now - Duration::hours(1))];

    let current = MetricPoint::with_timestamp(10.0, now);
    let result = symmetric_strategy.detect(&zero_history, current).await?;
    println!("Change from 0 to 10:");
    println!(
        "  Result: {}",
        if result.is_anomaly {
            "ANOMALY"
        } else {
            "NORMAL"
        }
    );
    println!("  Explanation: {}", result.explanation);
    for (key, value) in &result.details {
        println!("  {key}: {value}");
    }
    println!();

    // Insufficient history
    println!("=== Insufficient History Example ===\n");
    let strategy_needs_history = RelativeRateOfChangeStrategy::new(0.1)?.with_min_history(5);

    let short_history = vec![
        MetricPoint::with_timestamp(100.0, now - Duration::hours(2)),
        MetricPoint::with_timestamp(105.0, now - Duration::hours(1)),
    ];

    let current = MetricPoint::new(110.0);
    let result = strategy_needs_history
        .detect(&short_history, current)
        .await?;
    println!("With only 2 history points (requires 5):");
    println!("  Result: {}", result.explanation);

    Ok(())
}
