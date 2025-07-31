//! Types for analyzer metrics and values.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// Represents different types of metric values that analyzers can produce.
///
/// This enum covers all common metric types needed for data quality analysis,
/// from simple scalars to complex distributions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum MetricValue {
    /// A floating-point metric value (e.g., mean, percentage).
    Double(f64),

    /// An integer metric value (e.g., count, size).
    Long(i64),

    /// A histogram distribution of values.
    Histogram(MetricDistribution),

    /// A vector of values (e.g., for multi-dimensional metrics).
    Vector(Vec<f64>),

    /// A string metric value (e.g., mode, most frequent value).
    String(String),

    /// A boolean metric value (e.g., presence/absence).
    Boolean(bool),

    /// A map of string keys to metric values (e.g., grouped metrics).
    Map(HashMap<String, MetricValue>),
}

impl MetricValue {
    /// Checks if the metric value is numeric (Double or Long).
    pub fn is_numeric(&self) -> bool {
        matches!(self, MetricValue::Double(_) | MetricValue::Long(_))
    }

    /// Attempts to get the numeric value as f64.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            MetricValue::Double(v) => Some(*v),
            MetricValue::Long(v) => Some(*v as f64),
            _ => None,
        }
    }

    /// Attempts to get the value as i64.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            MetricValue::Long(v) => Some(*v),
            MetricValue::Double(v) => {
                if v.fract() == 0.0 {
                    Some(*v as i64)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Returns a human-readable string representation of the metric value.
    pub fn to_string_pretty(&self) -> String {
        match self {
            MetricValue::Double(v) => {
                if v.fract() == 0.0 {
                    format!("{v:.0}")
                } else {
                    format!("{v:.4}")
                }
            }
            MetricValue::Long(v) => v.to_string(),
            MetricValue::String(s) => s.clone(),
            MetricValue::Boolean(b) => b.to_string(),
            MetricValue::Histogram(h) => format!("Histogram({} buckets)", h.buckets.len()),
            MetricValue::Vector(v) => format!("Vector({} elements)", v.len()),
            MetricValue::Map(m) => format!("Map({} entries)", m.len()),
        }
    }
}

impl fmt::Display for MetricValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string_pretty())
    }
}

/// Represents a histogram distribution of values.
///
/// Used for metrics that capture value distributions rather than single values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricDistribution {
    /// The histogram buckets with their boundaries and counts.
    pub buckets: Vec<HistogramBucket>,

    /// Total count of values in the distribution.
    pub total_count: u64,

    /// Minimum value in the distribution.
    pub min: Option<f64>,

    /// Maximum value in the distribution.
    pub max: Option<f64>,

    /// Mean value of the distribution.
    pub mean: Option<f64>,

    /// Standard deviation of the distribution.
    pub std_dev: Option<f64>,
}

impl MetricDistribution {
    /// Creates a new empty distribution.
    pub fn new() -> Self {
        Self {
            buckets: Vec::new(),
            total_count: 0,
            min: None,
            max: None,
            mean: None,
            std_dev: None,
        }
    }

    /// Creates a distribution from a set of buckets.
    pub fn from_buckets(buckets: Vec<HistogramBucket>) -> Self {
        let total_count = buckets.iter().map(|b| b.count).sum();
        Self {
            buckets,
            total_count,
            min: None,
            max: None,
            mean: None,
            std_dev: None,
        }
    }

    /// Adds statistical summary information to the distribution.
    pub fn with_stats(mut self, min: f64, max: f64, mean: f64, std_dev: f64) -> Self {
        self.min = Some(min);
        self.max = Some(max);
        self.mean = Some(mean);
        self.std_dev = Some(std_dev);
        self
    }
}

impl Default for MetricDistribution {
    fn default() -> Self {
        Self::new()
    }
}

/// Represents a single bucket in a histogram.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistogramBucket {
    /// Lower bound of the bucket (inclusive).
    pub lower_bound: f64,

    /// Upper bound of the bucket (exclusive).
    pub upper_bound: f64,

    /// Count of values in this bucket.
    pub count: u64,
}

impl HistogramBucket {
    /// Creates a new histogram bucket.
    pub fn new(lower_bound: f64, upper_bound: f64, count: u64) -> Self {
        Self {
            lower_bound,
            upper_bound,
            count,
        }
    }

    /// Returns the width of the bucket.
    pub fn width(&self) -> f64 {
        self.upper_bound - self.lower_bound
    }

    /// Returns the midpoint of the bucket.
    pub fn midpoint(&self) -> f64 {
        (self.lower_bound + self.upper_bound) / 2.0
    }
}

/// Type alias for analyzer-specific metric types.
///
/// This allows analyzers to define their own metric types while ensuring
/// they can be converted to the standard MetricValue enum.
pub trait AnalyzerMetric: Into<MetricValue> + Send + Sync + fmt::Debug {}

/// Blanket implementation for MetricValue itself.
impl AnalyzerMetric for MetricValue {}

/// Implementation for f64 values.
impl From<f64> for MetricValue {
    fn from(value: f64) -> Self {
        MetricValue::Double(value)
    }
}

/// Implementation for i64 values.
impl From<i64> for MetricValue {
    fn from(value: i64) -> Self {
        MetricValue::Long(value)
    }
}

/// Implementation for bool values.
impl From<bool> for MetricValue {
    fn from(value: bool) -> Self {
        MetricValue::Boolean(value)
    }
}

/// Implementation for String values.
impl From<String> for MetricValue {
    fn from(value: String) -> Self {
        MetricValue::String(value)
    }
}

/// Implementation for &str values.
impl From<&str> for MetricValue {
    fn from(value: &str) -> Self {
        MetricValue::String(value.to_string())
    }
}
