//! TermCloudRepository - Main repository implementation for Term Cloud.
//!
//! This module provides the primary interface for persisting metrics to Term Cloud,
//! implementing the MetricsRepository trait with support for:
//! - Asynchronous background uploads via UploadWorker
//! - Offline operation with automatic sync via OfflineCache
//! - Local buffering via MetricsBuffer
//!
//! # Example
//!
//! ```rust,ignore
//! use term_guard::cloud::{CloudConfig, TermCloudRepository};
//! use term_guard::repository::ResultKey;
//! use term_guard::analyzers::AnalyzerContext;
//!
//! let config = CloudConfig::new("your-api-key");
//! let repository = TermCloudRepository::new(config)?;
//!
//! // Save metrics
//! let key = ResultKey::now().with_tag("env", "production");
//! let context = AnalyzerContext::new();
//! repository.save(key, context).await?;
//!
//! // Graceful shutdown
//! repository.shutdown().await?;
//! ```

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use directories::ProjectDirs;
use tokio::sync::{watch, RwLock};
use tracing::{debug, error, info, instrument, warn};

use crate::analyzers::context::AnalyzerContext;
use crate::analyzers::types::MetricValue;
use crate::cloud::{
    BufferEntry, CloudConfig, CloudError, CloudMetadata, CloudMetric, CloudMetricValue,
    CloudResult, CloudResultKey, MetricsBuffer, OfflineCache, TermCloudClient, UploadWorker,
    WorkerStats,
};
use crate::error::{Result, TermError};
use crate::repository::{MetricsQuery, MetricsRepository, RepositoryMetadata, ResultKey};

/// Main repository implementation for persisting metrics to Term Cloud.
///
/// TermCloudRepository provides a complete solution for metrics persistence with:
/// - Local buffering for high-throughput scenarios
/// - Background upload worker for asynchronous transmission
/// - Offline cache for resilience against network failures
/// - Automatic sync when connectivity is restored
///
/// # Architecture
///
/// ```text
/// ┌─────────────────┐
/// │   Application   │
/// └────────┬────────┘
///          │ save()
///          ▼
/// ┌─────────────────┐
/// │  MetricsBuffer  │ (in-memory)
/// └────────┬────────┘
///          │
///          ▼
/// ┌─────────────────┐     ┌─────────────────┐
/// │  UploadWorker   │────▶│  TermCloudClient │
/// └────────┬────────┘     └────────┬────────┘
///          │                       │
///          │ (on failure)          │
///          ▼                       ▼
/// ┌─────────────────┐     ┌─────────────────┐
/// │  OfflineCache   │     │   Term Cloud    │
/// │    (SQLite)     │     │      API        │
/// └─────────────────┘     └─────────────────┘
/// ```
pub struct TermCloudRepository {
    config: Arc<CloudConfig>,
    client: TermCloudClient,
    buffer: MetricsBuffer,
    cache: Option<OfflineCache>,
    shutdown_tx: watch::Sender<bool>,
    worker_handle: Option<RwLock<Option<tokio::task::JoinHandle<WorkerStats>>>>,
}

impl TermCloudRepository {
    /// Creates a new TermCloudRepository and starts the background upload worker.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for connecting to Term Cloud
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client or upload worker cannot be created.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use term_guard::cloud::{CloudConfig, TermCloudRepository};
    ///
    /// let config = CloudConfig::new("your-api-key")
    ///     .with_buffer_size(5000)
    ///     .with_batch_size(100);
    ///
    /// let repository = TermCloudRepository::new(config)?;
    /// ```
    #[instrument(skip(config), fields(endpoint = %config.endpoint()))]
    pub fn new(config: CloudConfig) -> CloudResult<Self> {
        let config = Arc::new(config);
        let client = TermCloudClient::new((*config).clone())?;
        let buffer = MetricsBuffer::new(config.buffer_size());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let worker = UploadWorker::new((*config).clone(), buffer.clone(), shutdown_rx)?;
        let worker_handle = tokio::spawn(async move { worker.run().await });

        info!("TermCloudRepository initialized with background worker");

        Ok(Self {
            config,
            client,
            buffer,
            cache: None,
            shutdown_tx,
            worker_handle: Some(RwLock::new(Some(worker_handle))),
        })
    }

    /// Sets up the offline cache for persisting metrics during network failures.
    ///
    /// If no path is provided in the config, uses the default platform-specific
    /// cache directory (e.g., `~/.cache/term/metrics.db` on Linux).
    ///
    /// # Arguments
    ///
    /// * `path` - Optional custom path for the cache database
    ///
    /// # Errors
    ///
    /// Returns an error if the cache database cannot be created or opened.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mut repository = TermCloudRepository::new(config)?;
    ///
    /// // Use default cache location
    /// repository.setup_cache(None)?;
    ///
    /// // Or specify a custom path
    /// repository.setup_cache(Some("/var/cache/myapp/metrics.db"))?;
    /// ```
    #[instrument(skip(self, path))]
    pub fn setup_cache(&mut self, path: Option<&Path>) -> CloudResult<()> {
        let cache_path = if let Some(p) = path {
            p.to_path_buf()
        } else if let Some(p) = self.config.offline_cache_path() {
            p.to_path_buf()
        } else {
            Self::default_cache_path()?
        };

        if let Some(parent) = cache_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| CloudError::CacheError {
                message: format!("Failed to create cache directory: {e}"),
            })?;
        }

        let cache = OfflineCache::new(&cache_path)?;
        info!(path = %cache_path.display(), "Offline cache initialized");
        self.cache = Some(cache);
        Ok(())
    }

    /// Returns the default platform-specific cache path.
    fn default_cache_path() -> CloudResult<std::path::PathBuf> {
        ProjectDirs::from("dev", "term", "term-guard")
            .map(|dirs| dirs.cache_dir().join("metrics.db"))
            .ok_or_else(|| CloudError::Configuration {
                message: "Could not determine cache directory".to_string(),
            })
    }

    /// Returns the number of metrics currently pending in the buffer.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let pending = repository.pending_count().await;
    /// println!("Pending metrics: {}", pending);
    /// ```
    pub async fn pending_count(&self) -> usize {
        self.buffer.len().await
    }

    /// Forces an immediate flush of all buffered metrics.
    ///
    /// This method drains the buffer and attempts to upload all metrics directly,
    /// bypassing the background worker. Failed uploads are saved to the offline
    /// cache if available.
    ///
    /// # Errors
    ///
    /// Returns an error if the upload fails and no cache is available.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Force upload before application shutdown
    /// repository.flush().await?;
    /// ```
    #[instrument(skip(self))]
    pub async fn flush(&self) -> CloudResult<()> {
        let entries = self.buffer.clear().await;
        if entries.is_empty() {
            return Ok(());
        }

        info!(count = entries.len(), "Flushing metrics");
        self.upload_entries(entries).await
    }

    /// Performs a graceful shutdown of the repository.
    ///
    /// This method:
    /// 1. Signals the background worker to stop
    /// 2. Waits for the worker to finish processing
    /// 3. Saves any remaining buffered metrics to the offline cache
    ///
    /// # Errors
    ///
    /// Returns an error if remaining metrics cannot be saved.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Graceful shutdown
    /// let stats = repository.shutdown().await?;
    /// println!("Uploaded {} metrics during operation", stats.metrics_uploaded);
    /// ```
    #[instrument(skip(self))]
    pub async fn shutdown(&self) -> CloudResult<Option<WorkerStats>> {
        info!("Initiating graceful shutdown");

        self.shutdown_tx.send(true).map_err(|e| CloudError::Configuration {
            message: format!("Failed to send shutdown signal: {e}"),
        })?;

        let stats = if let Some(ref handle_lock) = self.worker_handle {
            let mut guard = handle_lock.write().await;
            if let Some(handle) = guard.take() {
                match handle.await {
                    Ok(stats) => {
                        info!(
                            uploaded = stats.metrics_uploaded,
                            failed = stats.metrics_failed,
                            "Worker shutdown complete"
                        );
                        Some(stats)
                    }
                    Err(e) => {
                        error!("Worker task failed: {}", e);
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        };

        let remaining = self.buffer.clear().await;
        if !remaining.is_empty() {
            warn!(count = remaining.len(), "Saving remaining metrics to cache");
            self.save_to_cache(&remaining)?;
        }

        Ok(stats)
    }

    /// Checks connectivity to Term Cloud.
    ///
    /// # Errors
    ///
    /// Returns an error if the health check fails.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// match repository.health_check().await {
    ///     Ok(response) => println!("Connected to Term Cloud v{}", response.version),
    ///     Err(e) => eprintln!("Connection failed: {}", e),
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn health_check(&self) -> CloudResult<crate::cloud::HealthResponse> {
        self.client.health_check().await
    }

    /// Synchronizes offline cached metrics to Term Cloud.
    ///
    /// Loads all cached metrics and attempts to upload them. Successfully
    /// uploaded metrics are removed from the cache.
    ///
    /// # Returns
    ///
    /// Returns the number of metrics successfully synchronized.
    ///
    /// # Errors
    ///
    /// Returns an error if no cache is configured or if cache operations fail.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Check if we have cached metrics and sync them
    /// let synced = repository.sync_offline_cache().await?;
    /// println!("Synced {} cached metrics", synced);
    /// ```
    #[instrument(skip(self))]
    pub async fn sync_offline_cache(&self) -> CloudResult<usize> {
        let cache = self.cache.as_ref().ok_or_else(|| CloudError::Configuration {
            message: "Offline cache not configured".to_string(),
        })?;

        let entries = cache.load_all()?;
        if entries.is_empty() {
            debug!("No cached metrics to sync");
            return Ok(0);
        }

        info!(count = entries.len(), "Syncing cached metrics");

        let mut synced = 0;
        let mut synced_ids = Vec::new();

        for cache_entry in entries {
            let metrics = vec![cache_entry.entry.metric.clone()];
            match self.client.ingest(&metrics).await {
                Ok(response) => {
                    synced += response.accepted;
                    synced_ids.push(cache_entry.id);
                }
                Err(e) if e.is_retryable() => {
                    warn!("Retryable error during sync, will try again later: {}", e);
                    break;
                }
                Err(e) => {
                    error!("Non-retryable error during sync: {}", e);
                    synced_ids.push(cache_entry.id);
                }
            }
        }

        if !synced_ids.is_empty() {
            cache.delete_ids(&synced_ids)?;
        }

        info!(synced = synced, "Cache sync complete");
        Ok(synced)
    }

    /// Converts a ResultKey and AnalyzerContext to a CloudMetric.
    fn to_cloud_metric(key: &ResultKey, context: &AnalyzerContext) -> CloudMetric {
        let mut cloud_metrics = HashMap::new();

        for (metric_key, value) in context.all_metrics() {
            let cloud_value = match value {
                MetricValue::Double(v) => CloudMetricValue::Double(*v),
                MetricValue::Long(v) => CloudMetricValue::Long(*v),
                MetricValue::String(v) => CloudMetricValue::String(v.clone()),
                MetricValue::Boolean(v) => CloudMetricValue::Boolean(*v),
                MetricValue::Histogram(h) => CloudMetricValue::Histogram(
                    crate::cloud::CloudHistogram {
                        buckets: h
                            .buckets
                            .iter()
                            .map(|b| crate::cloud::CloudHistogramBucket {
                                lower_bound: b.lower_bound,
                                upper_bound: b.upper_bound,
                                count: b.count,
                            })
                            .collect(),
                        total_count: h.total_count,
                        min: h.min,
                        max: h.max,
                        mean: h.mean,
                        std_dev: h.std_dev,
                    },
                ),
                MetricValue::Vector(_) | MetricValue::Map(_) => {
                    continue;
                }
            };
            cloud_metrics.insert(metric_key.clone(), cloud_value);
        }

        let metadata = context.metadata();
        CloudMetric {
            result_key: CloudResultKey {
                dataset_date: key.timestamp,
                tags: key.tags.clone(),
            },
            metrics: cloud_metrics,
            metadata: CloudMetadata {
                dataset_name: metadata.dataset_name.clone(),
                start_time: metadata.start_time.map(|t| t.to_rfc3339()),
                end_time: metadata.end_time.map(|t| t.to_rfc3339()),
                term_version: env!("CARGO_PKG_VERSION").to_string(),
                custom: metadata.custom.clone(),
            },
            validation_result: None,
        }
    }

    /// Uploads entries directly to Term Cloud.
    async fn upload_entries(&self, entries: Vec<BufferEntry>) -> CloudResult<()> {
        let metrics: Vec<CloudMetric> = entries.iter().map(|e| e.metric.clone()).collect();

        match self.client.ingest(&metrics).await {
            Ok(response) => {
                debug!(
                    accepted = response.accepted,
                    rejected = response.rejected,
                    "Direct upload complete"
                );
                Ok(())
            }
            Err(e) => {
                warn!("Direct upload failed: {}, saving to cache", e);
                self.save_to_cache(&entries)?;
                Ok(())
            }
        }
    }

    /// Saves entries to the offline cache.
    fn save_to_cache(&self, entries: &[BufferEntry]) -> CloudResult<()> {
        if let Some(ref cache) = self.cache {
            for entry in entries {
                cache.save(&entry.metric, entry.retry_count)?;
            }
            Ok(())
        } else {
            Err(CloudError::CacheError {
                message: "Offline cache not configured, metrics will be lost".to_string(),
            })
        }
    }

    /// Returns a reference to the underlying client.
    pub fn client(&self) -> &TermCloudClient {
        &self.client
    }

    /// Returns a reference to the configuration.
    pub fn config(&self) -> &CloudConfig {
        &self.config
    }
}

#[async_trait]
impl MetricsRepository for TermCloudRepository {
    /// Saves metrics to the buffer for asynchronous upload.
    ///
    /// Metrics are buffered locally and uploaded by the background worker.
    /// If the buffer is full, returns a BufferOverflow error.
    #[instrument(skip(self, metrics), fields(key.timestamp = %key.timestamp, repository_type = "term_cloud"))]
    async fn save(&self, key: ResultKey, metrics: AnalyzerContext) -> Result<()> {
        if let Err(validation_error) = key.validate_tags() {
            return Err(TermError::repository_validation(
                "tags",
                validation_error,
                key.to_string(),
            ));
        }

        let cloud_metric = Self::to_cloud_metric(&key, &metrics);

        self.buffer.push(cloud_metric).await.map_err(|e| {
            TermError::repository("term_cloud", "save", e.to_string())
        })?;

        debug!("Metric queued for upload");
        Ok(())
    }

    /// Creates a query builder for retrieving metrics from Term Cloud.
    ///
    /// Note: Query execution requires network access to Term Cloud.
    #[instrument(skip(self))]
    async fn load(&self) -> MetricsQuery {
        MetricsQuery::new(Arc::new(TermCloudQueryAdapter {
            client: self.client.clone(),
        }))
    }

    /// Deletes metrics by key from Term Cloud.
    #[instrument(skip(self), fields(key.timestamp = %key.timestamp, repository_type = "term_cloud"))]
    async fn delete(&self, key: ResultKey) -> Result<()> {
        let cloud_key = CloudResultKey {
            dataset_date: key.timestamp,
            tags: key.tags.clone(),
        };

        self.client
            .delete(&cloud_key)
            .await
            .map_err(|e| TermError::repository("term_cloud", "delete", e.to_string()))
    }

    /// Returns metadata about the repository.
    #[instrument(skip(self))]
    async fn metadata(&self) -> Result<RepositoryMetadata> {
        let pending = self.buffer.len().await;
        let cached = self.cache.as_ref().map(|c| c.count().unwrap_or(0)).unwrap_or(0);

        Ok(RepositoryMetadata::new("term_cloud")
            .with_config("endpoint", self.config.endpoint())
            .with_config("pending_metrics", pending.to_string())
            .with_config("cached_metrics", cached.to_string()))
    }
}

/// Adapter for executing queries via TermCloudClient.
struct TermCloudQueryAdapter {
    client: TermCloudClient,
}

#[async_trait]
impl MetricsRepository for TermCloudQueryAdapter {
    async fn save(&self, _key: ResultKey, _metrics: AnalyzerContext) -> Result<()> {
        Err(TermError::NotSupported(
            "save not supported on query adapter".to_string(),
        ))
    }

    async fn load(&self) -> MetricsQuery {
        MetricsQuery::new(Arc::new(Self {
            client: self.client.clone(),
        }))
    }

    async fn delete(&self, _key: ResultKey) -> Result<()> {
        Err(TermError::NotSupported(
            "delete not supported on query adapter".to_string(),
        ))
    }

    async fn list_keys(&self) -> Result<Vec<ResultKey>> {
        let query = crate::cloud::MetricsQuery::default();
        let response = self
            .client
            .query(query)
            .await
            .map_err(|e| TermError::repository("term_cloud", "list_keys", e.to_string()))?;

        Ok(response
            .results
            .into_iter()
            .map(|m| {
                ResultKey::new(m.result_key.dataset_date).with_tags(m.result_key.tags)
            })
            .collect())
    }

    async fn get(&self, key: &ResultKey) -> Result<Option<AnalyzerContext>> {
        let query = crate::cloud::MetricsQuery {
            after: Some(key.timestamp),
            before: Some(key.timestamp + 1),
            tags: key.tags.clone(),
            limit: Some(1),
            ..Default::default()
        };

        let response = self
            .client
            .query(query)
            .await
            .map_err(|e| TermError::repository("term_cloud", "get", e.to_string()))?;

        Ok(response.results.into_iter().next().map(|m| {
            let mut context = AnalyzerContext::new();
            for (metric_key, value) in m.metrics {
                let metric_value = match value {
                    CloudMetricValue::Double(v) => MetricValue::Double(v),
                    CloudMetricValue::Long(v) => MetricValue::Long(v),
                    CloudMetricValue::String(v) => MetricValue::String(v),
                    CloudMetricValue::Boolean(v) => MetricValue::Boolean(v),
                    CloudMetricValue::Histogram(_) => continue,
                };
                context.store_metric(metric_key, metric_value);
            }
            context
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn make_test_config() -> CloudConfig {
        CloudConfig::new("test-api-key")
            .with_endpoint("http://localhost:1")
            .with_buffer_size(100)
            .with_flush_interval(Duration::from_millis(50))
    }

    #[tokio::test]
    async fn test_repository_creation() {
        let config = make_test_config();
        let result = TermCloudRepository::new(config);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_repository_save_queues_metric() {
        let config = make_test_config();
        let repository = TermCloudRepository::new(config).unwrap();

        let key = ResultKey::new(1704931200000).with_tag("env", "test");
        let context = AnalyzerContext::new();

        let result = repository.save(key, context).await;
        assert!(result.is_ok());

        assert_eq!(repository.pending_count().await, 1);

        let _ = repository.shutdown().await;
    }

    #[tokio::test]
    async fn test_repository_save_validates_tags() {
        let config = make_test_config();
        let repository = TermCloudRepository::new(config).unwrap();

        let key = ResultKey::new(1704931200000).with_tag("", "invalid");
        let context = AnalyzerContext::new();

        let result = repository.save(key, context).await;
        assert!(result.is_err());

        let _ = repository.shutdown().await;
    }

    #[tokio::test]
    async fn test_repository_pending_count() {
        let config = make_test_config();
        let repository = TermCloudRepository::new(config).unwrap();

        assert_eq!(repository.pending_count().await, 0);

        for i in 0..5 {
            let key = ResultKey::new(1704931200000 + i).with_tag("index", i.to_string());
            let context = AnalyzerContext::new();
            repository.save(key, context).await.unwrap();
        }

        assert_eq!(repository.pending_count().await, 5);

        let _ = repository.shutdown().await;
    }

    #[tokio::test]
    async fn test_repository_shutdown_returns_stats() {
        let config = make_test_config();
        let repository = TermCloudRepository::new(config).unwrap();

        let result = repository.shutdown().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_repository_metadata() {
        let config = make_test_config();
        let repository = TermCloudRepository::new(config).unwrap();

        let metadata = repository.metadata().await.unwrap();
        assert_eq!(metadata.backend_type, Some("term_cloud".to_string()));
        assert!(metadata.config.contains_key("endpoint"));
        assert!(metadata.config.contains_key("pending_metrics"));

        let _ = repository.shutdown().await;
    }

    #[tokio::test]
    async fn test_to_cloud_metric_conversion() {
        let key = ResultKey::new(1704931200000)
            .with_tag("env", "prod")
            .with_tag("region", "us-east-1");

        let mut context = AnalyzerContext::with_dataset("test_dataset");
        context.store_metric("completeness.col1", MetricValue::Double(0.98));
        context.store_metric("size", MetricValue::Long(1000));
        context.store_metric("is_valid", MetricValue::Boolean(true));

        let cloud_metric = TermCloudRepository::to_cloud_metric(&key, &context);

        assert_eq!(cloud_metric.result_key.dataset_date, 1704931200000);
        assert_eq!(cloud_metric.result_key.tags.get("env"), Some(&"prod".to_string()));
        assert_eq!(cloud_metric.metadata.dataset_name, Some("test_dataset".to_string()));
        assert!(cloud_metric.metrics.contains_key("completeness.col1"));
        assert!(cloud_metric.metrics.contains_key("size"));
        assert!(cloud_metric.metrics.contains_key("is_valid"));
    }

    #[tokio::test]
    async fn test_repository_cache_setup() {
        let config = make_test_config();
        let mut repository = TermCloudRepository::new(config).unwrap();

        let temp_dir = tempfile::tempdir().unwrap();
        let cache_path = temp_dir.path().join("test_cache.db");

        let result = repository.setup_cache(Some(&cache_path));
        assert!(result.is_ok());

        let _ = repository.shutdown().await;
    }

    #[tokio::test]
    async fn test_repository_flush() {
        let config = make_test_config();
        let mut repository = TermCloudRepository::new(config).unwrap();

        let temp_dir = tempfile::tempdir().unwrap();
        let cache_path = temp_dir.path().join("flush_test.db");
        repository.setup_cache(Some(&cache_path)).unwrap();

        let key = ResultKey::new(1704931200000).with_tag("env", "test");
        let context = AnalyzerContext::new();
        repository.save(key, context).await.unwrap();

        assert_eq!(repository.pending_count().await, 1);

        let result = repository.flush().await;
        assert!(result.is_ok());

        assert_eq!(repository.pending_count().await, 0);

        let _ = repository.shutdown().await;
    }

    #[tokio::test]
    async fn test_repository_sync_without_cache() {
        let config = make_test_config();
        let repository = TermCloudRepository::new(config).unwrap();

        let result = repository.sync_offline_cache().await;
        assert!(result.is_err());

        let _ = repository.shutdown().await;
    }

    #[tokio::test]
    async fn test_repository_sync_empty_cache() {
        let config = make_test_config();
        let mut repository = TermCloudRepository::new(config).unwrap();

        let temp_dir = tempfile::tempdir().unwrap();
        let cache_path = temp_dir.path().join("sync_test.db");
        repository.setup_cache(Some(&cache_path)).unwrap();

        let result = repository.sync_offline_cache().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);

        let _ = repository.shutdown().await;
    }

    #[tokio::test]
    async fn test_default_cache_path() {
        let result = TermCloudRepository::default_cache_path();
        assert!(result.is_ok());
        let path = result.unwrap();
        assert!(path.to_string_lossy().contains("term"));
    }
}
