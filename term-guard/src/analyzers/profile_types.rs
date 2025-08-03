//! Temporary types for profile data structures used by the suggestion system.
//! These will be replaced by the actual profiler module in a future PR.

use serde::{Deserialize, Serialize};

/// Detected data type for a column
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DetectedDataType {
    Integer,
    Double,
    String,
    Boolean,
    Date,
    Timestamp,
    Mixed,
    Unknown,
}

/// Basic statistics for a column
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BasicStatistics {
    pub count: i64,
    pub null_count: i64,
    pub distinct_count: Option<i64>,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub mean: Option<f64>,
    pub std_dev: Option<f64>,
}

/// Represents a single bucket in a categorical histogram
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CategoricalBucket {
    pub value: String,
    pub count: i64,
    pub percentage: f64,
}

/// Histogram for categorical columns
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CategoricalHistogram {
    pub buckets: Vec<CategoricalBucket>,
    pub total_count: i64,
}

/// Distribution information for numeric columns
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NumericDistribution {
    pub min: f64,
    pub max: f64,
    pub quantiles: std::collections::HashMap<String, f64>,
    pub histogram_buckets: Option<Vec<(f64, f64, i64)>>,
}

/// Complete profile of a column
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnProfile {
    pub column_name: String,
    pub data_type: DetectedDataType,
    pub row_count: u64,
    pub null_count: u64,
    pub null_percentage: f64,
    pub distinct_count: u64,
    pub distinct_percentage: f64,
    pub unique_count: Option<u64>,
    pub basic_stats: Option<BasicStatistics>,
    pub categorical_histogram: Option<CategoricalHistogram>,
    pub numeric_distribution: Option<NumericDistribution>,
    pub sample_values: Vec<String>,
    pub pattern_matches: std::collections::HashMap<String, f64>,
    pub passes_executed: Vec<u8>,
    pub profiling_time_ms: u64,
}
