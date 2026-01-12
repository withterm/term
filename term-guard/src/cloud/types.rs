use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::security::SecureString;

/// Configuration for connecting to Term Cloud.
#[derive(Debug, Clone)]
pub struct CloudConfig {
    api_key: SecureString,
    endpoint: String,
    timeout: Duration,
    max_retries: u32,
    buffer_size: usize,
    batch_size: usize,
    flush_interval: Duration,
    offline_cache_path: Option<PathBuf>,
}

impl CloudConfig {
    /// Create a new CloudConfig with the given API key.
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: SecureString::new(api_key.into()),
            endpoint: "https://api.term.dev".to_string(),
            timeout: Duration::from_secs(30),
            max_retries: 3,
            buffer_size: 1000,
            batch_size: 100,
            flush_interval: Duration::from_secs(5),
            offline_cache_path: None,
        }
    }

    /// Set a custom API endpoint.
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = endpoint.into();
        self
    }

    /// Set the HTTP request timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the maximum number of retry attempts.
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Set the in-memory buffer size (number of metrics).
    pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    /// Set the batch size for uploads.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the flush interval for background uploads.
    pub fn with_flush_interval(mut self, interval: Duration) -> Self {
        self.flush_interval = interval;
        self
    }

    /// Set a custom path for offline cache storage.
    pub fn with_offline_cache_path(mut self, path: impl AsRef<Path>) -> Self {
        self.offline_cache_path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Get the API key.
    ///
    /// # Security
    /// Returns a reference to the secure string. Use `expose()` to access
    /// the underlying value. Avoid storing or logging the exposed value.
    pub fn api_key(&self) -> &SecureString {
        &self.api_key
    }

    /// Get the API endpoint.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Get the HTTP request timeout.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Get the maximum number of retry attempts.
    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }

    /// Get the in-memory buffer size.
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// Get the batch size for uploads.
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// Get the flush interval.
    pub fn flush_interval(&self) -> Duration {
        self.flush_interval
    }

    /// Get the offline cache path.
    pub fn offline_cache_path(&self) -> Option<&Path> {
        self.offline_cache_path.as_deref()
    }
}

/// A metric ready for transmission to Term Cloud.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudMetric {
    pub result_key: CloudResultKey,
    pub metrics: HashMap<String, CloudMetricValue>,
    pub metadata: CloudMetadata,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validation_result: Option<CloudValidationResult>,
}

/// Key identifying a set of metrics.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CloudResultKey {
    pub dataset_date: i64,
    pub tags: HashMap<String, String>,
}

/// A metric value in wire format.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum CloudMetricValue {
    #[serde(rename = "double")]
    Double(f64),
    #[serde(rename = "long")]
    Long(i64),
    #[serde(rename = "string")]
    String(String),
    #[serde(rename = "boolean")]
    Boolean(bool),
    #[serde(rename = "histogram")]
    Histogram(CloudHistogram),
}

/// Histogram data in wire format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudHistogram {
    pub buckets: Vec<CloudHistogramBucket>,
    pub total_count: u64,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub mean: Option<f64>,
    pub std_dev: Option<f64>,
}

/// A single histogram bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudHistogramBucket {
    pub lower_bound: f64,
    pub upper_bound: f64,
    pub count: u64,
}

/// Metadata about the metrics collection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dataset_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_time: Option<String>,
    pub term_version: String,
    #[serde(default)]
    pub custom: HashMap<String, String>,
}

/// Validation result summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudValidationResult {
    pub status: String,
    pub total_checks: usize,
    pub passed_checks: usize,
    pub failed_checks: usize,
    pub issues: Vec<CloudValidationIssue>,
}

/// A single validation issue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudValidationIssue {
    pub check_name: String,
    pub constraint_name: String,
    pub level: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metric: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cloud_config_default() {
        let config = CloudConfig::new("test-api-key");

        assert_eq!(config.api_key().expose(), "test-api-key");
        assert_eq!(config.endpoint(), "https://api.term.dev");
        assert_eq!(config.timeout(), Duration::from_secs(30));
        assert_eq!(config.max_retries(), 3);
        assert_eq!(config.buffer_size(), 1000);
    }

    #[test]
    fn test_cloud_config_builder() {
        let config = CloudConfig::new("key")
            .with_endpoint("https://custom.endpoint")
            .with_timeout(Duration::from_secs(60))
            .with_max_retries(5)
            .with_buffer_size(5000);

        assert_eq!(config.endpoint(), "https://custom.endpoint");
        assert_eq!(config.timeout(), Duration::from_secs(60));
        assert_eq!(config.max_retries(), 5);
        assert_eq!(config.buffer_size(), 5000);
    }

    #[test]
    fn test_api_key_not_leaked_in_debug() {
        let secret_key = "super-secret-api-key-12345";
        let config = CloudConfig::new(secret_key);

        let debug_output = format!("{:?}", config);

        assert!(
            !debug_output.contains(secret_key),
            "API key should not appear in debug output"
        );
        assert!(
            debug_output.contains("SecureString(***)"),
            "Debug output should show masked SecureString"
        );
    }

    #[test]
    fn test_offline_cache_path_with_pathbuf() {
        let config = CloudConfig::new("key").with_offline_cache_path("/tmp/cache");

        assert_eq!(config.offline_cache_path(), Some(Path::new("/tmp/cache")));
    }

    #[test]
    fn test_cloud_metric_serialization() {
        let metric = CloudMetric {
            result_key: CloudResultKey {
                dataset_date: 1704931200000,
                tags: vec![("env".to_string(), "prod".to_string())]
                    .into_iter()
                    .collect(),
            },
            metrics: vec![("completeness.id".to_string(), CloudMetricValue::Double(1.0))]
                .into_iter()
                .collect(),
            metadata: CloudMetadata {
                dataset_name: Some("orders".to_string()),
                start_time: Some("2024-01-10T12:00:00Z".to_string()),
                end_time: Some("2024-01-10T12:05:00Z".to_string()),
                term_version: "0.0.2".to_string(),
                custom: Default::default(),
            },
            validation_result: None,
        };

        let json = serde_json::to_string(&metric).unwrap();
        assert!(json.contains("completeness.id"));
        assert!(json.contains("1704931200000"));
    }
}
