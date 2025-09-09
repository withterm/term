//! Metrics repository framework for persisting and querying analyzer results.
//!
//! This module provides a flexible framework for storing and retrieving metrics
//! across different storage backends (e.g., filesystem, S3, databases).

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::analyzers::context::AnalyzerContext;
use crate::error::{Result, TermError};

pub mod datafusion_executor;
pub mod in_memory;
pub mod query;
pub mod result_key;

pub use datafusion_executor::{DataFusionQueryExecutor, DataFusionQueryExecutorExt};
pub use in_memory::InMemoryRepository;
pub use query::{MetricsQuery, SortOrder};
pub use result_key::ResultKey;

/// Trait for implementing metrics storage backends.
///
/// This trait defines the interface that all metrics repositories must implement,
/// allowing for flexible storage options while maintaining a consistent API.
///
/// # Example
///
/// ```rust,ignore
/// use term_guard::repository::{MetricsRepository, ResultKey};
/// use term_guard::analyzers::AnalyzerContext;
///
/// #[derive(Clone)]
/// struct InMemoryRepository {
///     metrics: Arc<RwLock<HashMap<ResultKey, AnalyzerContext>>>,
/// }
///
/// #[async_trait]
/// impl MetricsRepository for InMemoryRepository {
///     async fn save(&self, key: ResultKey, metrics: AnalyzerContext) -> Result<()> {
///         let mut store = self.metrics.write().await;
///         store.insert(key, metrics);
///         Ok(())
///     }
///
///     async fn load(&self) -> MetricsQuery {
///         MetricsQuery::new(Arc::new(self.clone()))
///     }
///
///     async fn delete(&self, key: ResultKey) -> Result<()> {
///         let mut store = self.metrics.write().await;
///         store.remove(&key);
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait MetricsRepository: Send + Sync {
    /// Saves metrics with the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The result key containing timestamp and tags
    /// * `metrics` - The analyzer context containing computed metrics
    ///
    /// # Errors
    ///
    /// Returns an error if the save operation fails (e.g., I/O error, permission issue).
    async fn save(&self, key: ResultKey, metrics: AnalyzerContext) -> Result<()>;

    /// Creates a query builder for retrieving metrics.
    ///
    /// The returned `MetricsQuery` can be used to filter and retrieve metrics
    /// based on various criteria such as time range, tags, and analyzer types.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let query = repository.load().await
    ///     .after(start_timestamp)
    ///     .before(end_timestamp)
    ///     .with_tag("environment", "production");
    ///
    /// let results = query.execute().await?;
    /// ```
    async fn load(&self) -> MetricsQuery;

    /// Deletes metrics with the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The result key identifying the metrics to delete
    ///
    /// # Errors
    ///
    /// Returns an error if the delete operation fails or if the key doesn't exist.
    async fn delete(&self, key: ResultKey) -> Result<()>;

    /// Lists all available keys in the repository.
    ///
    /// This method is optional and may not be implemented by all backends.
    /// It's primarily useful for debugging and management operations.
    ///
    /// # Errors
    ///
    /// Returns an error if the listing operation fails.
    async fn list_keys(&self) -> Result<Vec<ResultKey>> {
        Err(TermError::NotSupported(
            "list_keys not implemented for this repository".to_string(),
        ))
    }

    /// Loads a specific metric by key.
    ///
    /// # Arguments
    ///
    /// * `key` - The result key to load
    ///
    /// # Returns
    ///
    /// Returns the AnalyzerContext if found, None otherwise.
    async fn get(&self, _key: &ResultKey) -> Result<Option<AnalyzerContext>> {
        // Default implementation returns None
        // Repositories should override this for actual data retrieval
        Ok(None)
    }

    /// Checks if a key exists in the repository.
    ///
    /// # Arguments
    ///
    /// * `key` - The result key to check
    ///
    /// # Returns
    ///
    /// Returns `true` if the key exists, `false` otherwise.
    async fn exists(&self, key: &ResultKey) -> Result<bool> {
        // Default implementation using list_keys
        let keys = self.list_keys().await?;
        Ok(keys.iter().any(|k| k == key))
    }

    /// Returns metadata about the repository.
    ///
    /// This can include information such as the backend type, configuration,
    /// and storage statistics.
    async fn metadata(&self) -> Result<RepositoryMetadata> {
        Ok(RepositoryMetadata::default())
    }
}

/// Metadata about a metrics repository.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RepositoryMetadata {
    /// The type of repository backend (e.g., "filesystem", "s3", "memory").
    pub backend_type: Option<String>,

    /// Total number of stored metrics.
    pub total_metrics: Option<usize>,

    /// Total storage size in bytes.
    pub storage_size_bytes: Option<u64>,

    /// Repository-specific configuration.
    pub config: HashMap<String, String>,

    /// Last modification timestamp.
    pub last_modified: Option<chrono::DateTime<chrono::Utc>>,
}

impl RepositoryMetadata {
    /// Creates a new repository metadata instance.
    pub fn new(backend_type: impl Into<String>) -> Self {
        Self {
            backend_type: Some(backend_type.into()),
            ..Default::default()
        }
    }

    /// Sets the total number of metrics.
    pub fn with_total_metrics(mut self, count: usize) -> Self {
        self.total_metrics = Some(count);
        self
    }

    /// Sets the storage size in bytes.
    pub fn with_storage_size(mut self, size: u64) -> Self {
        self.storage_size_bytes = Some(size);
        self
    }

    /// Adds a configuration parameter.
    pub fn with_config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.insert(key.into(), value.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_repository_metadata_builder() {
        let metadata = RepositoryMetadata::new("filesystem")
            .with_total_metrics(100)
            .with_storage_size(1024 * 1024)
            .with_config("path", "/var/metrics");

        assert_eq!(metadata.backend_type, Some("filesystem".to_string()));
        assert_eq!(metadata.total_metrics, Some(100));
        assert_eq!(metadata.storage_size_bytes, Some(1024 * 1024));
        assert_eq!(
            metadata.config.get("path"),
            Some(&"/var/metrics".to_string())
        );
    }
}
