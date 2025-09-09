//! Strategy pattern implementation for anomaly detection.
//!
//! This module provides an alternative strategy-based approach to anomaly detection,
//! complementing the existing detector trait. The strategy pattern allows for more
//! flexible configuration and easier composition of detection algorithms.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, instrument};

use crate::analyzers::{AnalyzerError, AnalyzerResult};

/// Epsilon value for floating point comparisons to handle precision issues.
const EPSILON: f64 = 1e-10;

/// Represents a single metric data point in time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricPoint {
    /// The metric value.
    pub value: f64,

    /// When this value was recorded.
    pub timestamp: DateTime<Utc>,

    /// Optional metadata about the data point.
    pub metadata: HashMap<String, String>,
}

impl MetricPoint {
    /// Creates a new metric point with the given value and current timestamp.
    pub fn new(value: f64) -> Self {
        Self {
            value,
            timestamp: Utc::now(),
            metadata: HashMap::new(),
        }
    }

    /// Creates a new metric point with a specific timestamp.
    pub fn with_timestamp(value: f64, timestamp: DateTime<Utc>) -> Self {
        Self {
            value,
            timestamp,
            metadata: HashMap::new(),
        }
    }

    /// Adds metadata to this metric point.
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }
}

/// Result of an anomaly detection strategy execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnomalyResult {
    /// Whether an anomaly was detected.
    pub is_anomaly: bool,

    /// Confidence score of the detection (0.0 to 1.0).
    pub confidence: f64,

    /// Expected value range if applicable.
    pub expected_range: Option<(f64, f64)>,

    /// The actual value that was checked.
    pub actual_value: f64,

    /// Human-readable explanation of the result.
    pub explanation: String,

    /// Additional details about the detection.
    pub details: HashMap<String, String>,
}

impl AnomalyResult {
    /// Creates a result indicating no anomaly was detected.
    pub fn no_anomaly(actual_value: f64, explanation: String) -> Self {
        Self {
            is_anomaly: false,
            confidence: 0.0,
            expected_range: None,
            actual_value,
            explanation,
            details: HashMap::new(),
        }
    }

    /// Creates a result indicating an anomaly was detected.
    pub fn anomaly_detected(actual_value: f64, confidence: f64, explanation: String) -> Self {
        Self {
            is_anomaly: true,
            confidence,
            expected_range: None,
            actual_value,
            explanation,
            details: HashMap::new(),
        }
    }

    /// Creates a result indicating insufficient history for detection.
    pub fn insufficient_history() -> Self {
        Self {
            is_anomaly: false,
            confidence: 0.0,
            expected_range: None,
            actual_value: 0.0,
            explanation: "Insufficient historical data for anomaly detection".to_string(),
            details: HashMap::new(),
        }
    }

    /// Sets the expected range for this result.
    pub fn with_expected_range(mut self, min: f64, max: f64) -> Self {
        self.expected_range = Some((min, max));
        self
    }

    /// Adds a detail to the result.
    pub fn with_detail(mut self, key: String, value: String) -> Self {
        self.details.insert(key, value);
        self
    }
}

/// Trait for implementing anomaly detection strategies.
#[async_trait]
pub trait AnomalyDetectionStrategy: Send + Sync {
    /// Detects anomalies based on historical data and current value.
    ///
    /// # Arguments
    /// * `history` - Historical metric values (sorted by timestamp, oldest first)
    /// * `current` - The current metric value to check
    ///
    /// # Returns
    /// An `AnomalyResult` indicating whether an anomaly was detected
    async fn detect(
        &self,
        history: &[MetricPoint],
        current: MetricPoint,
    ) -> AnalyzerResult<AnomalyResult>;

    /// Returns the name of this strategy.
    fn name(&self) -> &str;

    /// Returns a description of this strategy.
    fn description(&self) -> &str;
}

/// Detects anomalies based on relative rate of change between consecutive values.
#[derive(Debug, Clone)]
pub struct RelativeRateOfChangeStrategy {
    /// Maximum allowed rate of increase (e.g., 0.1 for 10% increase).
    /// None means no limit on increases.
    pub max_rate_increase: Option<f64>,

    /// Maximum allowed rate of decrease (e.g., 0.1 for 10% decrease).
    /// None means no limit on decreases.
    pub max_rate_decrease: Option<f64>,

    /// Minimum number of history points required for detection.
    pub min_history_points: usize,
}

impl RelativeRateOfChangeStrategy {
    /// Creates a new strategy with symmetric thresholds.
    ///
    /// # Arguments
    /// * `max_rate` - Maximum allowed rate of change in either direction (must be finite and non-negative)
    ///
    /// # Errors
    /// Returns an error if max_rate is not finite or is negative.
    pub fn new(max_rate: f64) -> AnalyzerResult<Self> {
        Self::validate_rate(max_rate, "max_rate")?;
        Ok(Self {
            max_rate_increase: Some(max_rate),
            max_rate_decrease: Some(max_rate),
            min_history_points: 1,
        })
    }

    /// Creates a new strategy with asymmetric thresholds.
    ///
    /// # Arguments
    /// * `max_increase` - Maximum allowed rate of increase (None for no limit)
    /// * `max_decrease` - Maximum allowed rate of decrease (None for no limit)
    ///
    /// # Errors
    /// Returns an error if any provided rate is not finite or is negative.
    pub fn with_asymmetric_thresholds(
        max_increase: Option<f64>,
        max_decrease: Option<f64>,
    ) -> AnalyzerResult<Self> {
        if let Some(rate) = max_increase {
            Self::validate_rate(rate, "max_increase")?;
        }
        if let Some(rate) = max_decrease {
            Self::validate_rate(rate, "max_decrease")?;
        }
        Ok(Self {
            max_rate_increase: max_increase,
            max_rate_decrease: max_decrease,
            min_history_points: 1,
        })
    }

    /// Sets the minimum history size required.
    pub fn with_min_history(mut self, min_points: usize) -> Self {
        self.min_history_points = min_points;
        self
    }

    /// Validates that a rate threshold is valid (finite and non-negative).
    fn validate_rate(rate: f64, name: &str) -> AnalyzerResult<()> {
        if !rate.is_finite() || rate < 0.0 {
            return Err(AnalyzerError::Custom(format!(
                "{name} must be finite and non-negative, got: {rate}"
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl AnomalyDetectionStrategy for RelativeRateOfChangeStrategy {
    #[instrument(skip(self, history))]
    async fn detect(
        &self,
        history: &[MetricPoint],
        current: MetricPoint,
    ) -> AnalyzerResult<AnomalyResult> {
        // Note: This implementation assumes history is sorted by timestamp in ascending order.
        // If history might be unsorted, consider validating or sorting before processing.
        // Check if we have enough history
        if history.len() < self.min_history_points {
            debug!(
                history_size = history.len(),
                required = self.min_history_points,
                "Insufficient history for rate of change detection"
            );
            return Ok(AnomalyResult::insufficient_history());
        }

        // Get the most recent historical value
        let previous = history
            .last()
            .ok_or_else(|| AnalyzerError::Custom("No previous value in history".to_string()))?;

        // Handle edge case where previous value is near zero (using epsilon for floating point comparison)
        if previous.value.abs() < EPSILON {
            // Special handling for near-zero baseline
            if current.value.abs() < EPSILON {
                // No change from zero to zero
                return Ok(AnomalyResult::no_anomaly(
                    current.value,
                    "No change detected (both values are near zero)".to_string(),
                ));
            }

            // Infinite rate of change from near-zero baseline
            let explanation = format!(
                "Cannot calculate rate of change from near-zero baseline (previous: {}, current: {})",
                previous.value, current.value
            );

            // Treat as anomaly if we have thresholds set
            if self.max_rate_increase.is_some() || self.max_rate_decrease.is_some() {
                return Ok(AnomalyResult::anomaly_detected(
                    current.value,
                    1.0, // High confidence since it's an infinite change
                    explanation,
                )
                .with_detail("previous_value".to_string(), previous.value.to_string())
                .with_detail("rate_of_change".to_string(), "infinite".to_string()));
            } else {
                return Ok(AnomalyResult::no_anomaly(current.value, explanation));
            }
        }

        // Calculate rate of change (safe from division by zero due to epsilon check above)
        let rate_of_change = (current.value - previous.value) / previous.value.abs();

        debug!(
            previous = previous.value,
            current = current.value,
            rate_of_change = rate_of_change,
            "Calculated rate of change"
        );

        // Check against thresholds
        let mut is_anomaly = false;
        let mut confidence = 0.0;
        let mut explanation = String::new();

        if rate_of_change > 0.0 {
            // Increase detected
            if let Some(max_increase) = self.max_rate_increase {
                if rate_of_change > max_increase {
                    is_anomaly = true;
                    confidence = (rate_of_change / max_increase).min(1.0);
                    explanation = format!(
                        "Rate of increase ({:.1}%) exceeds threshold ({:.1}%)",
                        rate_of_change * 100.0,
                        max_increase * 100.0
                    );
                }
            }
        } else if rate_of_change < 0.0 {
            // Decrease detected
            let abs_rate = rate_of_change.abs();
            if let Some(max_decrease) = self.max_rate_decrease {
                if abs_rate > max_decrease {
                    is_anomaly = true;
                    confidence = (abs_rate / max_decrease).min(1.0);
                    explanation = format!(
                        "Rate of decrease ({:.1}%) exceeds threshold ({:.1}%)",
                        abs_rate * 100.0,
                        max_decrease * 100.0
                    );
                }
            }
        }

        // Create result
        let mut result = if is_anomaly {
            AnomalyResult::anomaly_detected(current.value, confidence, explanation)
        } else {
            let explanation = format!(
                "Rate of change ({:.1}%) is within acceptable limits",
                rate_of_change * 100.0
            );
            AnomalyResult::no_anomaly(current.value, explanation)
        };

        // Add details
        result = result
            .with_detail("previous_value".to_string(), previous.value.to_string())
            .with_detail("rate_of_change".to_string(), format!("{rate_of_change:.4}"))
            .with_detail(
                "rate_of_change_percentage".to_string(),
                format!("{:.1}%", rate_of_change * 100.0),
            );

        // Add expected range if thresholds are defined
        if let (Some(max_inc), Some(max_dec)) = (self.max_rate_increase, self.max_rate_decrease) {
            let min_expected = previous.value * (1.0 - max_dec);
            let max_expected = previous.value * (1.0 + max_inc);
            result = result.with_expected_range(min_expected, max_expected);
        }

        Ok(result)
    }

    fn name(&self) -> &str {
        "RelativeRateOfChange"
    }

    fn description(&self) -> &str {
        "Detects anomalies based on relative rate of change between consecutive values"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[tokio::test]
    async fn test_relative_rate_of_change_normal() {
        let strategy = RelativeRateOfChangeStrategy::new(0.1).unwrap(); // 10% threshold

        let now = Utc::now();
        let history = vec![
            MetricPoint::with_timestamp(100.0, now - Duration::hours(2)),
            MetricPoint::with_timestamp(105.0, now - Duration::hours(1)),
        ];

        // 5% increase - should be normal
        let current = MetricPoint::with_timestamp(110.25, now);
        let result = strategy.detect(&history, current).await.unwrap();

        assert!(!result.is_anomaly);
        assert_eq!(result.confidence, 0.0);
        assert!(result.explanation.contains("within acceptable limits"));
    }

    #[tokio::test]
    async fn test_relative_rate_of_change_anomaly_increase() {
        let strategy = RelativeRateOfChangeStrategy::new(0.1).unwrap(); // 10% threshold

        let now = Utc::now();
        let history = vec![MetricPoint::with_timestamp(100.0, now - Duration::hours(1))];

        // 20% increase - should be anomaly
        let current = MetricPoint::with_timestamp(120.0, now);
        let result = strategy.detect(&history, current).await.unwrap();

        assert!(result.is_anomaly);
        assert!(result.confidence > 0.9); // Should be close to 1.0 (20% / 10% = 2.0, capped at 1.0)
        assert!(result.explanation.contains("exceeds threshold"));

        // Check expected range with tolerance for floating point precision
        if let Some((min, max)) = result.expected_range {
            assert!((min - 90.0).abs() < 0.001);
            assert!((max - 110.0).abs() < 0.001);
        } else {
            panic!("Expected range should be Some");
        }
    }

    #[tokio::test]
    async fn test_relative_rate_of_change_anomaly_decrease() {
        let strategy = RelativeRateOfChangeStrategy::new(0.1).unwrap(); // 10% threshold

        let now = Utc::now();
        let history = vec![MetricPoint::with_timestamp(100.0, now - Duration::hours(1))];

        // 15% decrease - should be anomaly
        let current = MetricPoint::with_timestamp(85.0, now);
        let result = strategy.detect(&history, current).await.unwrap();

        assert!(result.is_anomaly);
        assert!(result.confidence > 0.9); // Should be 1.0 (15% / 10% = 1.5, capped at 1.0)
        assert!(result.explanation.contains("decrease"));
        assert!(result.explanation.contains("exceeds threshold"));
    }

    #[tokio::test]
    async fn test_asymmetric_thresholds() {
        // Allow 20% increase but only 5% decrease
        let strategy = RelativeRateOfChangeStrategy::with_asymmetric_thresholds(
            Some(0.2),  // 20% increase allowed
            Some(0.05), // 5% decrease allowed
        )
        .unwrap();

        let now = Utc::now();
        let history = vec![MetricPoint::with_timestamp(100.0, now - Duration::hours(1))];

        // Test 15% increase - should be normal
        let current = MetricPoint::with_timestamp(115.0, now);
        let result = strategy.detect(&history, current).await.unwrap();
        assert!(!result.is_anomaly);

        // Test 7% decrease - should be anomaly
        let current = MetricPoint::with_timestamp(93.0, now);
        let result = strategy.detect(&history, current).await.unwrap();
        assert!(result.is_anomaly);
        assert!(result.explanation.contains("decrease"));
    }

    #[tokio::test]
    async fn test_zero_baseline_edge_case() {
        let strategy = RelativeRateOfChangeStrategy::new(0.1).unwrap();

        let now = Utc::now();
        let history = vec![MetricPoint::with_timestamp(0.0, now - Duration::hours(1))];

        // Change from zero
        let current = MetricPoint::with_timestamp(10.0, now);
        let result = strategy.detect(&history, current).await.unwrap();

        assert!(result.is_anomaly);
        assert_eq!(result.confidence, 1.0);
        assert!(result.explanation.contains("zero baseline"));
        assert!(result
            .details
            .get("rate_of_change")
            .unwrap()
            .contains("infinite"));
    }

    #[tokio::test]
    async fn test_insufficient_history() {
        let strategy = RelativeRateOfChangeStrategy::new(0.1)
            .unwrap()
            .with_min_history(3);

        let history = vec![MetricPoint::new(100.0), MetricPoint::new(105.0)];

        let current = MetricPoint::new(110.0);
        let result = strategy.detect(&history, current).await.unwrap();

        assert!(!result.is_anomaly);
        assert!(result.explanation.contains("Insufficient"));
    }

    #[tokio::test]
    async fn test_no_limit_on_increases() {
        let strategy = RelativeRateOfChangeStrategy::with_asymmetric_thresholds(
            None,      // No limit on increases
            Some(0.1), // 10% decrease limit
        )
        .unwrap();

        let now = Utc::now();
        let history = vec![MetricPoint::with_timestamp(100.0, now - Duration::hours(1))];

        // Test 500% increase - should be normal (no limit)
        let current = MetricPoint::with_timestamp(600.0, now);
        let result = strategy.detect(&history, current).await.unwrap();
        assert!(!result.is_anomaly);

        // Test 15% decrease - should be anomaly
        let current = MetricPoint::with_timestamp(85.0, now);
        let result = strategy.detect(&history, current).await.unwrap();
        assert!(result.is_anomaly);
    }

    #[tokio::test]
    async fn test_invalid_rate_validation() {
        // Test NaN rate
        let result = RelativeRateOfChangeStrategy::new(f64::NAN);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must be finite"));

        // Test infinite rate
        let result = RelativeRateOfChangeStrategy::new(f64::INFINITY);
        assert!(result.is_err());

        // Test negative rate
        let result = RelativeRateOfChangeStrategy::new(-0.1);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("non-negative"));

        // Test asymmetric with invalid rates
        let result =
            RelativeRateOfChangeStrategy::with_asymmetric_thresholds(Some(f64::NAN), Some(0.1));
        assert!(result.is_err());

        let result =
            RelativeRateOfChangeStrategy::with_asymmetric_thresholds(Some(0.1), Some(-0.05));
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_near_zero_baseline() {
        let strategy = RelativeRateOfChangeStrategy::new(0.1).unwrap();

        let now = Utc::now();
        // Very small value, but not exactly zero
        let history = vec![MetricPoint::with_timestamp(1e-11, now - Duration::hours(1))];

        let current = MetricPoint::with_timestamp(10.0, now);
        let result = strategy.detect(&history, current).await.unwrap();

        // Should be treated as infinite change from near-zero
        assert!(result.is_anomaly);
        assert_eq!(result.confidence, 1.0);
        assert!(result.explanation.contains("near-zero baseline"));
        assert!(result
            .details
            .get("rate_of_change")
            .unwrap()
            .contains("infinite"));
    }

    #[tokio::test]
    async fn test_very_small_changes() {
        let strategy = RelativeRateOfChangeStrategy::new(0.1).unwrap();

        let now = Utc::now();
        let history = vec![MetricPoint::with_timestamp(1e-6, now - Duration::hours(1))];

        // 5% increase on very small value
        let current = MetricPoint::with_timestamp(1.05e-6, now);
        let result = strategy.detect(&history, current).await.unwrap();

        assert!(!result.is_anomaly);
        assert!(result.explanation.contains("within acceptable limits"));
    }
}
