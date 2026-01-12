use std::path::{Path, PathBuf};
use std::time::Duration;

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
}
