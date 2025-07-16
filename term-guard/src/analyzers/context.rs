//! Context for storing analyzer computation results.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::errors::AnalyzerError;
use super::types::MetricValue;

/// Context that stores the results of analyzer computations.
///
/// The AnalyzerContext provides a centralized storage for metrics computed
/// by different analyzers, allowing for efficient access and serialization
/// of results.
///
/// # Example
///
/// ```rust,ignore
/// use term_guard::analyzers::{AnalyzerContext, MetricValue};
///
/// let mut context = AnalyzerContext::new();
///
/// // Store metrics from different analyzers
/// context.store_metric("size", MetricValue::Long(1000));
/// context.store_metric("completeness.user_id", MetricValue::Double(0.98));
///
/// // Retrieve metrics
/// if let Some(size) = context.get_metric("size") {
///     println!("Dataset size: {}", size);
/// }
///
/// // Get all metrics for a specific analyzer
/// let completeness_metrics = context.get_analyzer_metrics("completeness");
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyzerContext {
    /// Stored metrics indexed by analyzer name and metric key.
    metrics: HashMap<String, MetricValue>,

    /// Metadata about the analysis run.
    metadata: AnalysisMetadata,

    /// Errors that occurred during analysis.
    errors: Vec<AnalysisError>,
}

impl AnalyzerContext {
    /// Creates a new empty analyzer context.
    pub fn new() -> Self {
        Self {
            metrics: HashMap::new(),
            metadata: AnalysisMetadata::new(),
            errors: Vec::new(),
        }
    }

    /// Creates a new context with the given dataset name.
    pub fn with_dataset(dataset_name: impl Into<String>) -> Self {
        Self {
            metrics: HashMap::new(),
            metadata: AnalysisMetadata::with_dataset(dataset_name),
            errors: Vec::new(),
        }
    }

    /// Stores a metric value with the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The metric key, typically in format "analyzer_name.metric_name"
    /// * `value` - The metric value to store
    pub fn store_metric(&mut self, key: impl Into<String>, value: MetricValue) {
        self.metrics.insert(key.into(), value);
    }

    /// Stores a metric with a composite key built from analyzer and metric names.
    ///
    /// # Arguments
    ///
    /// * `analyzer_name` - The name of the analyzer
    /// * `metric_name` - The name of the specific metric
    /// * `value` - The metric value to store
    pub fn store_analyzer_metric(
        &mut self,
        analyzer_name: &str,
        metric_name: &str,
        value: MetricValue,
    ) {
        let key = format!("{analyzer_name}.{metric_name}");
        self.store_metric(key, value);
    }

    /// Retrieves a metric value by key.
    pub fn get_metric(&self, key: &str) -> Option<&MetricValue> {
        self.metrics.get(key)
    }

    /// Retrieves all metrics for a specific analyzer.
    ///
    /// Returns metrics whose keys start with the analyzer name followed by a dot.
    pub fn get_analyzer_metrics(&self, analyzer_name: &str) -> HashMap<String, &MetricValue> {
        let prefix = format!("{analyzer_name}.");
        self.metrics
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(k, v)| {
                let metric_name = k.strip_prefix(&prefix).unwrap_or(k);
                (metric_name.to_string(), v)
            })
            .collect()
    }

    /// Returns all stored metrics.
    pub fn all_metrics(&self) -> &HashMap<String, MetricValue> {
        &self.metrics
    }

    /// Records an error that occurred during analysis.
    pub fn record_error(&mut self, analyzer_name: impl Into<String>, error: AnalyzerError) {
        self.errors.push(AnalysisError {
            analyzer_name: analyzer_name.into(),
            error: error.to_string(),
        });
    }

    /// Returns all recorded errors.
    pub fn errors(&self) -> &[AnalysisError] {
        &self.errors
    }

    /// Checks if any errors occurred during analysis.
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// Returns the analysis metadata.
    pub fn metadata(&self) -> &AnalysisMetadata {
        &self.metadata
    }

    /// Returns a mutable reference to the analysis metadata.
    pub fn metadata_mut(&mut self) -> &mut AnalysisMetadata {
        &mut self.metadata
    }

    /// Merges another context into this one.
    ///
    /// Metrics from the other context will overwrite existing metrics with the same key.
    pub fn merge(&mut self, other: AnalyzerContext) {
        self.metrics.extend(other.metrics);
        self.errors.extend(other.errors);
        self.metadata.merge(other.metadata);
    }

    /// Creates a summary of the analysis results.
    pub fn summary(&self) -> AnalysisSummary {
        AnalysisSummary {
            total_metrics: self.metrics.len(),
            total_errors: self.errors.len(),
            analyzer_count: self.count_analyzers(),
            dataset_name: self.metadata.dataset_name.clone(),
        }
    }

    /// Counts the number of unique analyzers that contributed metrics.
    fn count_analyzers(&self) -> usize {
        let mut analyzers = std::collections::HashSet::new();
        for key in self.metrics.keys() {
            if let Some(analyzer_name) = key.split('.').next() {
                analyzers.insert(analyzer_name);
            }
        }
        analyzers.len()
    }
}

impl Default for AnalyzerContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Metadata about an analysis run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalysisMetadata {
    /// Name of the dataset being analyzed.
    pub dataset_name: Option<String>,

    /// Timestamp when the analysis started.
    pub start_time: Option<chrono::DateTime<chrono::Utc>>,

    /// Timestamp when the analysis completed.
    pub end_time: Option<chrono::DateTime<chrono::Utc>>,

    /// Additional custom metadata.
    pub custom: HashMap<String, String>,
}

impl AnalysisMetadata {
    /// Creates new empty metadata.
    pub fn new() -> Self {
        Self {
            dataset_name: None,
            start_time: None,
            end_time: None,
            custom: HashMap::new(),
        }
    }

    /// Creates metadata with a dataset name.
    pub fn with_dataset(name: impl Into<String>) -> Self {
        Self {
            dataset_name: Some(name.into()),
            start_time: None,
            end_time: None,
            custom: HashMap::new(),
        }
    }

    /// Records the start time of the analysis.
    pub fn record_start(&mut self) {
        self.start_time = Some(chrono::Utc::now());
    }

    /// Records the end time of the analysis.
    pub fn record_end(&mut self) {
        self.end_time = Some(chrono::Utc::now());
    }

    /// Returns the duration of the analysis if both start and end times are recorded.
    pub fn duration(&self) -> Option<chrono::Duration> {
        match (self.start_time, self.end_time) {
            (Some(start), Some(end)) => Some(end - start),
            _ => None,
        }
    }

    /// Adds custom metadata.
    pub fn add_custom(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.custom.insert(key.into(), value.into());
    }

    /// Merges another metadata instance into this one.
    fn merge(&mut self, other: AnalysisMetadata) {
        if self.dataset_name.is_none() {
            self.dataset_name = other.dataset_name;
        }
        if self.start_time.is_none() {
            self.start_time = other.start_time;
        }
        if self.end_time.is_none() {
            self.end_time = other.end_time;
        }
        self.custom.extend(other.custom);
    }
}

impl Default for AnalysisMetadata {
    fn default() -> Self {
        Self::new()
    }
}

/// Error information from analysis.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalysisError {
    /// Name of the analyzer that produced the error.
    pub analyzer_name: String,

    /// Error message.
    pub error: String,
}

/// Summary of analysis results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalysisSummary {
    /// Total number of metrics computed.
    pub total_metrics: usize,

    /// Total number of errors encountered.
    pub total_errors: usize,

    /// Number of unique analyzers that ran.
    pub analyzer_count: usize,

    /// Name of the analyzed dataset.
    pub dataset_name: Option<String>,
}
