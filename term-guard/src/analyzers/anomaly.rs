//! Anomaly detection framework for data quality monitoring.
//!
//! This module provides infrastructure for detecting anomalies in metrics over time
//! using various statistical methods. It's designed to work with the analyzer framework
//! to monitor data quality metrics and alert when significant deviations occur.
//!
//! ## Architecture
//!
//! The anomaly detection system consists of:
//! - `AnomalyDetector`: Core trait for anomaly detection strategies
//! - `MetricsRepository`: Storage abstraction for historical metrics
//! - Detection strategies: RelativeRateOfChange, AbsoluteChange, Z-score
//! - `AnomalyDetectionRunner`: Orchestrates detection across metrics
//!
//! ## Example
//!
//! ```rust,ignore
//! use term_guard::analyzers::anomaly::{
//!     AnomalyDetectionRunner, InMemoryMetricsRepository,
//!     RelativeRateOfChangeDetector, ZScoreDetector
//! };
//! use term_guard::analyzers::AnalysisRunner;
//! use datafusion::prelude::*;
//!
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! // Create metrics repository
//! let repository = InMemoryMetricsRepository::new();
//!
//! // Create detection runner with strategies
//! let detector = AnomalyDetectionRunner::builder()
//!     .repository(Box::new(repository))
//!     .add_detector("completeness.*", Box::new(RelativeRateOfChangeDetector::new(0.1)))
//!     .add_detector("size", Box::new(ZScoreDetector::new(3.0)))
//!     .build();
//!
//! // Run analysis
//! let ctx = SessionContext::new();
//! let runner = AnalysisRunner::new();
//! let metrics = runner.run(&ctx).await?;
//!
//! // Detect anomalies
//! let anomalies = detector.detect_anomalies(&metrics).await?;
//! for anomaly in anomalies {
//!     println!("Anomaly detected in {}: {} (confidence: {:.2})",
//!              anomaly.metric_name, anomaly.description, anomaly.confidence);
//! }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, instrument, warn};

use crate::analyzers::{AnalyzerContext, AnalyzerError, AnalyzerResult, MetricValue};

/// Represents a detected anomaly in a metric.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Anomaly {
    /// The name of the metric where the anomaly was detected.
    pub metric_name: String,

    /// The current value of the metric.
    pub current_value: MetricValue,

    /// The expected value or range.
    pub expected_value: Option<MetricValue>,

    /// The detection strategy that identified this anomaly.
    pub detection_strategy: String,

    /// Confidence score of the anomaly (0.0 to 1.0).
    pub confidence: f64,

    /// Human-readable description of the anomaly.
    pub description: String,

    /// Timestamp when the anomaly was detected.
    pub detected_at: DateTime<Utc>,

    /// Additional context or metadata.
    pub metadata: HashMap<String, String>,
}

impl Anomaly {
    /// Creates a new anomaly with the given parameters.
    pub fn new(
        metric_name: String,
        current_value: MetricValue,
        detection_strategy: String,
        confidence: f64,
        description: String,
    ) -> Self {
        Self {
            metric_name,
            current_value,
            expected_value: None,
            detection_strategy,
            confidence,
            description,
            detected_at: Utc::now(),
            metadata: HashMap::new(),
        }
    }

    /// Sets the expected value for this anomaly.
    pub fn with_expected_value(mut self, value: MetricValue) -> Self {
        self.expected_value = Some(value);
        self
    }

    /// Adds metadata to this anomaly.
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }
}

/// Historical data point for a metric.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricDataPoint {
    /// The metric value.
    pub value: MetricValue,

    /// When this value was recorded.
    pub timestamp: DateTime<Utc>,

    /// Optional metadata about the data point.
    pub metadata: HashMap<String, String>,
}

/// Trait for anomaly detection strategies.
#[async_trait]
pub trait AnomalyDetector: Send + Sync {
    /// Detects anomalies in the given metric.
    ///
    /// # Arguments
    /// * `metric_name` - Name of the metric being analyzed
    /// * `current_value` - The current value to check
    /// * `history` - Historical values for comparison
    ///
    /// # Returns
    /// An optional anomaly if one is detected
    async fn detect(
        &self,
        metric_name: &str,
        current_value: &MetricValue,
        history: &[MetricDataPoint],
    ) -> AnalyzerResult<Option<Anomaly>>;

    /// Returns the name of this detection strategy.
    fn name(&self) -> &str;

    /// Returns a description of this detection strategy.
    fn description(&self) -> &str;
}

/// Trait for storing and retrieving metric history.
#[async_trait]
pub trait MetricsRepository: Send + Sync {
    /// Stores a metric value.
    async fn store_metric(
        &self,
        metric_name: &str,
        value: MetricValue,
        timestamp: DateTime<Utc>,
    ) -> AnalyzerResult<()>;

    /// Retrieves historical values for a metric.
    ///
    /// # Arguments
    /// * `metric_name` - Name of the metric
    /// * `since` - Optional start time for the history
    /// * `until` - Optional end time for the history
    /// * `limit` - Maximum number of data points to return
    async fn get_metric_history(
        &self,
        metric_name: &str,
        since: Option<DateTime<Utc>>,
        until: Option<DateTime<Utc>>,
        limit: Option<usize>,
    ) -> AnalyzerResult<Vec<MetricDataPoint>>;

    /// Stores metrics from an analyzer context.
    async fn store_context(&self, context: &AnalyzerContext) -> AnalyzerResult<()> {
        let timestamp = Utc::now();
        for (metric_name, value) in context.all_metrics() {
            self.store_metric(metric_name, value.clone(), timestamp)
                .await?;
        }
        Ok(())
    }
}

/// Configuration for in-memory metrics repository.
#[derive(Debug, Clone)]
pub struct InMemoryMetricsConfig {
    /// Maximum number of data points per metric (default: 10,000).
    pub max_points_per_metric: usize,
    /// Maximum total number of metrics (default: 1,000).
    pub max_metrics: usize,
    /// Maximum age of data points in seconds (default: 30 days).
    pub max_age_seconds: i64,
}

impl Default for InMemoryMetricsConfig {
    fn default() -> Self {
        Self {
            max_points_per_metric: 10_000,
            max_metrics: 1_000,
            max_age_seconds: 30 * 24 * 60 * 60, // 30 days
        }
    }
}

/// In-memory implementation of MetricsRepository for testing and development.
///
/// **Security Note**: This implementation includes memory limits to prevent OOM attacks
/// when used in production scenarios. Configure limits appropriately for your use case.
#[derive(Clone)]
pub struct InMemoryMetricsRepository {
    data: Arc<tokio::sync::RwLock<HashMap<String, Vec<MetricDataPoint>>>>,
    config: InMemoryMetricsConfig,
}

impl InMemoryMetricsRepository {
    /// Creates a new in-memory metrics repository with default limits.
    pub fn new() -> Self {
        Self::with_config(InMemoryMetricsConfig::default())
    }

    /// Creates a new in-memory metrics repository with custom configuration.
    pub fn with_config(config: InMemoryMetricsConfig) -> Self {
        Self {
            data: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Returns the current memory usage statistics.
    pub async fn memory_stats(&self) -> MemoryStats {
        let data = self.data.read().await;
        let mut total_points = 0;
        let mut oldest_timestamp = None;
        let mut newest_timestamp = None;

        for history in data.values() {
            total_points += history.len();
            for point in history {
                match oldest_timestamp {
                    None => oldest_timestamp = Some(point.timestamp),
                    Some(oldest) if point.timestamp < oldest => {
                        oldest_timestamp = Some(point.timestamp)
                    }
                    _ => {}
                }
                match newest_timestamp {
                    None => newest_timestamp = Some(point.timestamp),
                    Some(newest) if point.timestamp > newest => {
                        newest_timestamp = Some(point.timestamp)
                    }
                    _ => {}
                }
            }
        }

        MemoryStats {
            total_metrics: data.len(),
            total_data_points: total_points,
            oldest_data_point: oldest_timestamp,
            newest_data_point: newest_timestamp,
            estimated_memory_bytes: Self::estimate_memory_usage(&data),
        }
    }

    /// Estimates memory usage in bytes.
    fn estimate_memory_usage(data: &HashMap<String, Vec<MetricDataPoint>>) -> usize {
        let mut size = std::mem::size_of::<HashMap<String, Vec<MetricDataPoint>>>();

        for (key, values) in data {
            size += std::mem::size_of::<String>() + key.len();
            size += std::mem::size_of::<Vec<MetricDataPoint>>();
            size += values.len() * std::mem::size_of::<MetricDataPoint>();

            // Estimate metadata size
            for point in values {
                for (k, v) in &point.metadata {
                    size += std::mem::size_of::<String>() * 2 + k.len() + v.len();
                }
            }
        }

        size
    }

    /// Performs cleanup of old data points based on configuration limits.
    async fn cleanup_if_needed(&self) {
        let mut data = self.data.write().await;

        // Remove metrics if we exceed the limit
        if data.len() > self.config.max_metrics {
            warn!(
                current_metrics = data.len(),
                max_metrics = self.config.max_metrics,
                "Metrics limit exceeded, removing oldest metrics"
            );

            // Keep the most recently updated metrics
            let mut metrics_by_latest: Vec<_> = data
                .iter()
                .map(|(name, points)| {
                    let latest = points.iter().map(|p| p.timestamp).max().unwrap_or_default();
                    (name.clone(), latest)
                })
                .collect();

            metrics_by_latest.sort_by(|a, b| b.1.cmp(&a.1)); // Sort by latest timestamp desc

            // Remove oldest metrics
            let to_remove = metrics_by_latest.len() - self.config.max_metrics;
            for (metric_name, _) in metrics_by_latest.iter().skip(self.config.max_metrics) {
                data.remove(metric_name);
            }

            info!(
                removed_metrics = to_remove,
                remaining_metrics = data.len(),
                "Cleaned up old metrics"
            );
        }

        // Clean up old data points and enforce per-metric limits
        let cutoff_time = Utc::now() - Duration::seconds(self.config.max_age_seconds);
        let mut total_points_removed = 0;

        for (metric_name, points) in data.iter_mut() {
            let original_len = points.len();

            // Remove points older than cutoff
            points.retain(|p| p.timestamp >= cutoff_time);

            // Limit points per metric (keep most recent)
            if points.len() > self.config.max_points_per_metric {
                points.sort_by_key(|p| p.timestamp);
                let to_keep = points.len() - self.config.max_points_per_metric;
                points.drain(0..to_keep);
            }

            let removed = original_len - points.len();
            if removed > 0 {
                total_points_removed += removed;
                debug!(
                    metric = metric_name,
                    removed_points = removed,
                    remaining_points = points.len(),
                    "Cleaned up old data points"
                );
            }
        }

        if total_points_removed > 0 {
            info!(
                total_removed = total_points_removed,
                "Completed data point cleanup"
            );
        }
    }
}

/// Memory usage statistics for the in-memory repository.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryStats {
    /// Total number of metrics stored.
    pub total_metrics: usize,
    /// Total number of data points across all metrics.
    pub total_data_points: usize,
    /// Timestamp of the oldest data point.
    pub oldest_data_point: Option<DateTime<Utc>>,
    /// Timestamp of the newest data point.
    pub newest_data_point: Option<DateTime<Utc>>,
    /// Estimated memory usage in bytes.
    pub estimated_memory_bytes: usize,
}

impl Default for InMemoryMetricsRepository {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MetricsRepository for InMemoryMetricsRepository {
    async fn store_metric(
        &self,
        metric_name: &str,
        value: MetricValue,
        timestamp: DateTime<Utc>,
    ) -> AnalyzerResult<()> {
        // Check limits before storing
        {
            let data = self.data.read().await;
            if data.len() >= self.config.max_metrics && !data.contains_key(metric_name) {
                return Err(AnalyzerError::Custom(format!(
                    "Maximum metrics limit ({}) exceeded",
                    self.config.max_metrics
                )));
            }

            if let Some(existing) = data.get(metric_name) {
                if existing.len() >= self.config.max_points_per_metric {
                    // Will be handled by cleanup, but log a warning
                    warn!(
                        metric = metric_name,
                        current_points = existing.len(),
                        max_points = self.config.max_points_per_metric,
                        "Metric approaching memory limit"
                    );
                }
            }
        }

        // Store the new data point
        {
            let mut data = self.data.write().await;
            let entry = data.entry(metric_name.to_string()).or_insert_with(Vec::new);
            entry.push(MetricDataPoint {
                value,
                timestamp,
                metadata: HashMap::new(),
            });
            // Keep entries sorted by timestamp
            entry.sort_by_key(|dp| dp.timestamp);
        }

        // Perform cleanup if needed (async to avoid holding lock too long)
        self.cleanup_if_needed().await;

        Ok(())
    }

    async fn get_metric_history(
        &self,
        metric_name: &str,
        since: Option<DateTime<Utc>>,
        until: Option<DateTime<Utc>>,
        limit: Option<usize>,
    ) -> AnalyzerResult<Vec<MetricDataPoint>> {
        let data = self.data.read().await;

        if let Some(history) = data.get(metric_name) {
            let mut filtered: Vec<_> = history
                .iter()
                .filter(|dp| {
                    let after_since = since.map_or(true, |s| dp.timestamp >= s);
                    let before_until = until.map_or(true, |u| dp.timestamp <= u);
                    after_since && before_until
                })
                .cloned()
                .collect();

            // Apply limit if specified
            if let Some(limit) = limit {
                filtered.truncate(limit);
            }

            Ok(filtered)
        } else {
            Ok(Vec::new())
        }
    }
}

/// Detects anomalies based on relative rate of change.
pub struct RelativeRateOfChangeDetector {
    /// Maximum allowed rate of change (e.g., 0.1 for 10%).
    pub max_rate_of_change: f64,

    /// Minimum history size required for detection.
    pub min_history_size: usize,
}

impl RelativeRateOfChangeDetector {
    /// Creates a new relative rate of change detector.
    ///
    /// # Arguments
    /// * `max_rate_of_change` - Maximum allowed relative change (e.g., 0.1 for 10%)
    pub fn new(max_rate_of_change: f64) -> Self {
        Self {
            max_rate_of_change,
            min_history_size: 2,
        }
    }

    /// Sets the minimum history size required.
    pub fn with_min_history_size(mut self, size: usize) -> Self {
        self.min_history_size = size;
        self
    }
}

#[async_trait]
impl AnomalyDetector for RelativeRateOfChangeDetector {
    async fn detect(
        &self,
        metric_name: &str,
        current_value: &MetricValue,
        history: &[MetricDataPoint],
    ) -> AnalyzerResult<Option<Anomaly>> {
        if history.len() < self.min_history_size {
            debug!(
                metric = metric_name,
                history_size = history.len(),
                required = self.min_history_size,
                "Insufficient history for rate of change detection"
            );
            return Ok(None);
        }

        // Get the most recent historical value
        let previous = history.last().unwrap();

        debug!(
            metric = metric_name,
            current = ?current_value,
            previous = ?previous.value,
            "Comparing values for rate of change"
        );

        // Calculate rate of change for numeric metrics
        match (current_value, &previous.value) {
            (MetricValue::Long(current), MetricValue::Long(previous)) => {
                if *previous == 0 {
                    return Ok(None); // Can't calculate rate of change from zero
                }

                let rate_of_change = ((*current - *previous) as f64).abs() / (*previous as f64);

                debug!(
                    metric = metric_name,
                    rate_of_change = rate_of_change,
                    threshold = self.max_rate_of_change,
                    "Calculated rate of change"
                );

                if rate_of_change > self.max_rate_of_change {
                    let anomaly = Anomaly::new(
                        metric_name.to_string(),
                        current_value.clone(),
                        self.name().to_string(),
                        rate_of_change / self.max_rate_of_change, // Confidence based on severity
                        format!(
                            "Relative change of {:.1}% exceeds threshold of {:.1}%",
                            rate_of_change * 100.0,
                            self.max_rate_of_change * 100.0
                        ),
                    )
                    .with_expected_value(MetricValue::Long(*previous))
                    .with_metadata("rate_of_change".to_string(), format!("{rate_of_change:.4}"));

                    return Ok(Some(anomaly));
                }
            }
            (MetricValue::Double(current), MetricValue::Double(previous)) => {
                if *previous == 0.0 {
                    return Ok(None); // Can't calculate rate of change from zero
                }

                let rate_of_change = ((current - previous).abs()) / previous.abs();

                if rate_of_change > self.max_rate_of_change {
                    let anomaly = Anomaly::new(
                        metric_name.to_string(),
                        current_value.clone(),
                        self.name().to_string(),
                        rate_of_change / self.max_rate_of_change, // Confidence based on severity
                        format!(
                            "Relative change of {:.1}% exceeds threshold of {:.1}%",
                            rate_of_change * 100.0,
                            self.max_rate_of_change * 100.0
                        ),
                    )
                    .with_expected_value(MetricValue::Double(*previous))
                    .with_metadata("rate_of_change".to_string(), format!("{rate_of_change:.4}"));

                    return Ok(Some(anomaly));
                }
            }
            _ => {
                // Non-numeric metrics or type mismatch
                return Ok(None);
            }
        }

        Ok(None)
    }

    fn name(&self) -> &str {
        "RelativeRateOfChange"
    }

    fn description(&self) -> &str {
        "Detects anomalies when the relative rate of change exceeds a threshold"
    }
}

/// Detects anomalies based on absolute change thresholds.
pub struct AbsoluteChangeDetector {
    /// Maximum allowed absolute change.
    pub max_absolute_change: f64,

    /// Minimum history size required for detection.
    pub min_history_size: usize,
}

impl AbsoluteChangeDetector {
    /// Creates a new absolute change detector.
    ///
    /// # Arguments
    /// * `max_absolute_change` - Maximum allowed absolute change
    pub fn new(max_absolute_change: f64) -> Self {
        Self {
            max_absolute_change,
            min_history_size: 1,
        }
    }

    /// Sets the minimum history size required.
    pub fn with_min_history_size(mut self, size: usize) -> Self {
        self.min_history_size = size;
        self
    }
}

#[async_trait]
impl AnomalyDetector for AbsoluteChangeDetector {
    async fn detect(
        &self,
        metric_name: &str,
        current_value: &MetricValue,
        history: &[MetricDataPoint],
    ) -> AnalyzerResult<Option<Anomaly>> {
        if history.len() < self.min_history_size {
            return Ok(None);
        }

        let previous = history.last().unwrap();

        match (current_value, &previous.value) {
            (MetricValue::Long(current), MetricValue::Long(previous)) => {
                let change = (*current - *previous).abs() as f64;

                if change > self.max_absolute_change {
                    let anomaly = Anomaly::new(
                        metric_name.to_string(),
                        current_value.clone(),
                        self.name().to_string(),
                        change / self.max_absolute_change,
                        format!(
                            "Absolute change of {change} exceeds threshold of {}",
                            self.max_absolute_change
                        ),
                    )
                    .with_expected_value(MetricValue::Long(*previous))
                    .with_metadata("absolute_change".to_string(), format!("{change}"));

                    return Ok(Some(anomaly));
                }
            }
            (MetricValue::Double(current), MetricValue::Double(previous)) => {
                let change = (current - previous).abs();

                if change > self.max_absolute_change {
                    let anomaly = Anomaly::new(
                        metric_name.to_string(),
                        current_value.clone(),
                        self.name().to_string(),
                        change / self.max_absolute_change,
                        format!(
                            "Absolute change of {change:.4} exceeds threshold of {:.4}",
                            self.max_absolute_change
                        ),
                    )
                    .with_expected_value(MetricValue::Double(*previous))
                    .with_metadata("absolute_change".to_string(), format!("{change:.4}"));

                    return Ok(Some(anomaly));
                }
            }
            _ => return Ok(None),
        }

        Ok(None)
    }

    fn name(&self) -> &str {
        "AbsoluteChange"
    }

    fn description(&self) -> &str {
        "Detects anomalies when the absolute change exceeds a threshold"
    }
}

/// Detects anomalies using Z-score (standard deviations from mean).
pub struct ZScoreDetector {
    /// Z-score threshold for anomaly detection (e.g., 3.0 for 3 standard deviations).
    pub z_score_threshold: f64,

    /// Minimum history size required for meaningful statistics.
    pub min_history_size: usize,
}

impl ZScoreDetector {
    /// Creates a new Z-score detector.
    ///
    /// # Arguments
    /// * `z_score_threshold` - Number of standard deviations for anomaly threshold
    pub fn new(z_score_threshold: f64) -> Self {
        Self {
            z_score_threshold,
            min_history_size: 10,
        }
    }

    /// Sets the minimum history size required.
    pub fn with_min_history_size(mut self, size: usize) -> Self {
        self.min_history_size = size;
        self
    }
}

#[async_trait]
impl AnomalyDetector for ZScoreDetector {
    async fn detect(
        &self,
        metric_name: &str,
        current_value: &MetricValue,
        history: &[MetricDataPoint],
    ) -> AnalyzerResult<Option<Anomaly>> {
        if history.len() < self.min_history_size {
            return Ok(None);
        }

        // Extract numeric values from history
        let numeric_values: Vec<f64> = history
            .iter()
            .filter_map(|dp| match &dp.value {
                MetricValue::Long(v) => Some(*v as f64),
                MetricValue::Double(v) => Some(*v),
                _ => None,
            })
            .collect();

        if numeric_values.len() < self.min_history_size {
            return Ok(None);
        }

        // Calculate mean and standard deviation
        let mean = numeric_values.iter().sum::<f64>() / numeric_values.len() as f64;
        let variance = numeric_values
            .iter()
            .map(|v| (v - mean).powi(2))
            .sum::<f64>()
            / numeric_values.len() as f64;
        let std_dev = variance.sqrt();

        // Can't calculate Z-score if standard deviation is zero
        if std_dev == 0.0 {
            return Ok(None);
        }

        // Calculate Z-score for current value
        let current_numeric = match current_value {
            MetricValue::Long(v) => *v as f64,
            MetricValue::Double(v) => *v,
            _ => return Ok(None),
        };

        let z_score = (current_numeric - mean).abs() / std_dev;

        if z_score > self.z_score_threshold {
            let anomaly = Anomaly::new(
                metric_name.to_string(),
                current_value.clone(),
                self.name().to_string(),
                (z_score / self.z_score_threshold).min(1.0),
                format!(
                    "Value is {z_score:.1} standard deviations from mean (threshold: {:.1})",
                    self.z_score_threshold
                ),
            )
            .with_expected_value(MetricValue::Double(mean))
            .with_metadata("z_score".to_string(), format!("{z_score:.2}"))
            .with_metadata("mean".to_string(), format!("{mean:.4}"))
            .with_metadata("std_dev".to_string(), format!("{std_dev:.4}"));

            return Ok(Some(anomaly));
        }

        Ok(None)
    }

    fn name(&self) -> &str {
        "ZScore"
    }

    fn description(&self) -> &str {
        "Detects anomalies using statistical Z-score analysis"
    }
}

/// Configuration for anomaly detection.
#[derive(Debug, Clone)]
pub struct AnomalyDetectionConfig {
    /// Minimum confidence threshold for reporting anomalies.
    pub min_confidence: f64,

    /// Whether to store current metrics in the repository.
    pub store_current_metrics: bool,

    /// Default time window for historical data retrieval.
    pub default_history_window: Duration,
}

impl Default for AnomalyDetectionConfig {
    fn default() -> Self {
        Self {
            min_confidence: 0.7,
            store_current_metrics: true,
            default_history_window: Duration::days(30),
        }
    }
}

/// Orchestrates anomaly detection across metrics.
pub struct AnomalyDetectionRunner {
    repository: Box<dyn MetricsRepository>,
    detectors: Vec<(String, Box<dyn AnomalyDetector>)>,
    config: AnomalyDetectionConfig,
}

impl AnomalyDetectionRunner {
    /// Creates a new builder for the anomaly detection runner.
    pub fn builder() -> AnomalyDetectionRunnerBuilder {
        AnomalyDetectionRunnerBuilder::default()
    }

    /// Detects anomalies in the given metrics.
    #[instrument(skip(self, context))]
    pub async fn detect_anomalies(
        &self,
        context: &AnalyzerContext,
    ) -> AnalyzerResult<Vec<Anomaly>> {
        let mut anomalies = Vec::new();

        // Remember the current time BEFORE storing metrics
        // Subtract 1ms to ensure current metrics stored after this time are excluded
        let detection_time = Utc::now() - chrono::Duration::milliseconds(1);

        // Store current metrics if configured
        if self.config.store_current_metrics {
            self.repository.store_context(context).await?;
        }

        // Check each metric against configured detectors
        for (metric_name, metric_value) in context.all_metrics() {
            for (pattern, detector) in &self.detectors {
                // Check if metric name matches pattern
                if self.matches_pattern(metric_name, pattern) {
                    // Get historical data, excluding the current timestamp
                    let since = Utc::now() - self.config.default_history_window;
                    let history = self
                        .repository
                        .get_metric_history(metric_name, Some(since), Some(detection_time), None)
                        .await?;

                    debug!(
                        metric = metric_name,
                        history_size = history.len(),
                        current_value = ?metric_value,
                        "Running anomaly detection"
                    );

                    // Run detection
                    match detector.detect(metric_name, metric_value, &history).await {
                        Ok(Some(anomaly)) => {
                            if anomaly.confidence >= self.config.min_confidence {
                                info!(
                                    metric = metric_name,
                                    strategy = anomaly.detection_strategy,
                                    confidence = anomaly.confidence,
                                    "Anomaly detected"
                                );
                                anomalies.push(anomaly);
                            }
                        }
                        Ok(None) => {
                            // No anomaly detected
                        }
                        Err(e) => {
                            warn!(
                                metric = metric_name,
                                detector = detector.name(),
                                error = %e,
                                "Error during anomaly detection"
                            );
                        }
                    }
                }
            }
        }

        Ok(anomalies)
    }

    /// Checks if a metric name matches a pattern.
    fn matches_pattern(&self, metric_name: &str, pattern: &str) -> bool {
        if pattern == "*" {
            return true;
        }

        if let Some(prefix) = pattern.strip_suffix('*') {
            return metric_name.starts_with(prefix);
        }

        metric_name == pattern
    }
}

/// Builder for AnomalyDetectionRunner.
#[derive(Default)]
pub struct AnomalyDetectionRunnerBuilder {
    repository: Option<Box<dyn MetricsRepository>>,
    detectors: Vec<(String, Box<dyn AnomalyDetector>)>,
    config: AnomalyDetectionConfig,
}

impl AnomalyDetectionRunnerBuilder {
    /// Sets the metrics repository.
    pub fn repository(mut self, repository: Box<dyn MetricsRepository>) -> Self {
        self.repository = Some(repository);
        self
    }

    /// Adds a detector for metrics matching the given pattern.
    ///
    /// # Arguments
    /// * `pattern` - Metric name pattern (supports * wildcard at end)
    /// * `detector` - The anomaly detector to use
    pub fn add_detector(mut self, pattern: &str, detector: Box<dyn AnomalyDetector>) -> Self {
        self.detectors.push((pattern.to_string(), detector));
        self
    }

    /// Sets the configuration.
    pub fn config(mut self, config: AnomalyDetectionConfig) -> Self {
        self.config = config;
        self
    }

    /// Builds the AnomalyDetectionRunner.
    pub fn build(self) -> AnalyzerResult<AnomalyDetectionRunner> {
        let repository = self
            .repository
            .ok_or_else(|| AnalyzerError::Custom("Metrics repository is required".to_string()))?;

        Ok(AnomalyDetectionRunner {
            repository,
            detectors: self.detectors,
            config: self.config,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_relative_rate_of_change_detector() {
        let detector = RelativeRateOfChangeDetector::new(0.1).with_min_history_size(1); // 10% threshold

        // Create history
        let history = vec![MetricDataPoint {
            value: MetricValue::Long(100),
            timestamp: Utc::now() - Duration::hours(1),
            metadata: HashMap::new(),
        }];

        // Test normal change (5%)
        let current = MetricValue::Long(105);
        let result = detector
            .detect("test_metric", &current, &history)
            .await
            .unwrap();
        assert!(result.is_none());

        // Test anomalous change (20%)
        let current = MetricValue::Long(120);
        let result = detector
            .detect("test_metric", &current, &history)
            .await
            .unwrap();
        assert!(result.is_some());
        let anomaly = result.unwrap();
        assert_eq!(anomaly.detection_strategy, "RelativeRateOfChange");
        assert!(anomaly.confidence > 1.0); // Should be ~2.0 (20% / 10%)
    }

    #[tokio::test]
    async fn test_z_score_detector() {
        let detector = ZScoreDetector::new(2.0); // 2 standard deviations

        // Create history with normal distribution around 100
        let mut history = Vec::new();
        for i in 0..20 {
            history.push(MetricDataPoint {
                value: MetricValue::Long(95 + (i % 10)),
                timestamp: Utc::now() - Duration::hours(i),
                metadata: HashMap::new(),
            });
        }

        // Test value within normal range
        let current = MetricValue::Long(102);
        let result = detector
            .detect("test_metric", &current, &history)
            .await
            .unwrap();
        assert!(result.is_none());

        // Test outlier value
        let current = MetricValue::Long(150);
        let result = detector
            .detect("test_metric", &current, &history)
            .await
            .unwrap();
        assert!(result.is_some());
        let anomaly = result.unwrap();
        assert_eq!(anomaly.detection_strategy, "ZScore");
    }

    #[tokio::test]
    async fn test_in_memory_repository_memory_limits() {
        // Test with small limits
        let config = InMemoryMetricsConfig {
            max_metrics: 2,
            max_points_per_metric: 3,
            max_age_seconds: 60,
        };
        let repo = InMemoryMetricsRepository::with_config(config);

        let now = Utc::now();

        // Add metrics up to the limit
        repo.store_metric("metric1", MetricValue::Long(100), now)
            .await
            .unwrap();
        repo.store_metric("metric2", MetricValue::Long(200), now)
            .await
            .unwrap();

        // Third metric should fail
        let result = repo
            .store_metric("metric3", MetricValue::Long(300), now)
            .await;
        assert!(result.is_err());

        // Check memory stats
        let stats = repo.memory_stats().await;
        assert_eq!(stats.total_metrics, 2);
        assert_eq!(stats.total_data_points, 2);

        // Add more points to existing metrics (should work)
        repo.store_metric(
            "metric1",
            MetricValue::Long(101),
            now + Duration::seconds(1),
        )
        .await
        .unwrap();
        repo.store_metric(
            "metric1",
            MetricValue::Long(102),
            now + Duration::seconds(2),
        )
        .await
        .unwrap();
        repo.store_metric(
            "metric1",
            MetricValue::Long(103),
            now + Duration::seconds(3),
        )
        .await
        .unwrap();

        // Check that cleanup has happened (should only keep 3 points per metric)
        let history = repo
            .get_metric_history("metric1", None, None, None)
            .await
            .unwrap();
        assert!(history.len() <= 3);

        let final_stats = repo.memory_stats().await;
        assert!(final_stats.estimated_memory_bytes > 0);
    }

    #[tokio::test]
    async fn test_in_memory_repository() {
        let repo = InMemoryMetricsRepository::new();

        // Store some metrics
        let now = Utc::now();
        repo.store_metric("metric1", MetricValue::Long(100), now)
            .await
            .unwrap();
        repo.store_metric("metric1", MetricValue::Long(110), now + Duration::hours(1))
            .await
            .unwrap();
        repo.store_metric("metric2", MetricValue::Double(0.95), now)
            .await
            .unwrap();

        // Retrieve history
        let history = repo
            .get_metric_history("metric1", None, None, None)
            .await
            .unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].value, MetricValue::Long(100));
        assert_eq!(history[1].value, MetricValue::Long(110));

        // Test filtering by time
        let history = repo
            .get_metric_history("metric1", Some(now + Duration::minutes(30)), None, None)
            .await
            .unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].value, MetricValue::Long(110));
    }
}
