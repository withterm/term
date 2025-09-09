//! In-memory implementation of MetricsRepository for testing and development.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::instrument;

use crate::analyzers::context::AnalyzerContext;
use crate::error::{Result, TermError};

use super::{MetricsQuery, MetricsRepository, RepositoryMetadata, ResultKey};

/// In-memory implementation of the MetricsRepository trait.
///
/// This implementation stores all metrics in memory and is useful for:
/// - Testing and development
/// - Small-scale applications
/// - Caching layers
///
/// # Example
///
/// ```rust,ignore
/// use term_guard::repository::{InMemoryRepository, ResultKey};
/// use term_guard::analyzers::AnalyzerContext;
///
/// let repository = InMemoryRepository::new();
///
/// // Save metrics
/// let key = ResultKey::now().with_tag("env", "test");
/// let context = AnalyzerContext::new();
/// repository.save(key, context).await?;
///
/// // Query metrics
/// let results = repository.load().await
///     .with_tag("env", "test")
///     .execute()
///     .await?;
/// ```
#[derive(Clone)]
pub struct InMemoryRepository {
    /// Storage for metrics, keyed by ResultKey.
    storage: Arc<RwLock<HashMap<ResultKey, AnalyzerContext>>>,

    /// Repository metadata.
    metadata: Arc<RwLock<RepositoryMetadata>>,
}

impl InMemoryRepository {
    /// Creates a new empty in-memory repository.
    pub fn new() -> Self {
        let mut metadata = RepositoryMetadata::new("in_memory");
        metadata.total_metrics = Some(0);
        Self {
            storage: Arc::new(RwLock::new(HashMap::new())),
            metadata: Arc::new(RwLock::new(metadata)),
        }
    }

    /// Creates a new repository with pre-populated data.
    ///
    /// # Arguments
    ///
    /// * `data` - Initial metrics to populate the repository with
    pub fn with_data(data: HashMap<ResultKey, AnalyzerContext>) -> Self {
        let repo = Self::new();
        let storage = repo.storage.clone();

        tokio::spawn(async move {
            let mut store = storage.write().await;
            store.extend(data);
        });

        repo
    }

    /// Returns the number of stored metrics.
    pub async fn size(&self) -> usize {
        self.storage.read().await.len()
    }

    /// Clears all stored metrics.
    pub async fn clear(&mut self) {
        self.storage.write().await.clear();
        self.update_metadata().await;
    }

    /// Updates repository metadata after changes.
    async fn update_metadata(&self) {
        let store = self.storage.read().await;
        let mut metadata = self.metadata.write().await;

        metadata.total_metrics = Some(store.len());
        metadata.last_modified = Some(chrono::Utc::now());

        // Calculate approximate storage size
        let size_bytes: usize = store
            .iter()
            .map(|(k, v)| {
                // Rough estimation of memory usage
                std::mem::size_of_val(k)
                    + std::mem::size_of_val(v)
                    + k.tags
                        .iter()
                        .map(|(key, val)| key.len() + val.len())
                        .sum::<usize>()
            })
            .sum();

        metadata.storage_size_bytes = Some(size_bytes as u64);
    }
}

impl Default for InMemoryRepository {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MetricsRepository for InMemoryRepository {
    #[instrument(skip(self, metrics), fields(key.timestamp = %key.timestamp, repository_type = "in_memory"))]
    async fn save(&self, key: ResultKey, metrics: AnalyzerContext) -> Result<()> {
        // Validate the key before saving
        if let Err(validation_error) = key.validate_tags() {
            return Err(TermError::repository_validation(
                "tags",
                validation_error,
                key.to_string(),
            ));
        }

        // Check for potential key collisions using normalized keys
        let normalized_key = key.to_normalized_storage_key();
        let store = self.storage.read().await;

        // Check if a different key with the same normalized representation exists
        for existing_key in store.keys() {
            if existing_key != &key && existing_key.to_normalized_storage_key() == normalized_key {
                return Err(TermError::repository_key_collision(
                    key.to_string(),
                    format!("Key collision detected with existing key: {existing_key}"),
                ));
            }
        }

        drop(store);

        let mut store = self.storage.write().await;
        store.insert(key, metrics);
        drop(store);

        self.update_metadata().await;
        Ok(())
    }

    #[instrument(skip(self))]
    async fn load(&self) -> MetricsQuery {
        // Create a special query that has access to the storage
        MetricsQuery::new(Arc::new(self.clone()))
    }

    #[instrument(skip(self), fields(key.timestamp = %key.timestamp, repository_type = "in_memory"))]
    async fn delete(&self, key: ResultKey) -> Result<()> {
        let mut store = self.storage.write().await;

        if store.remove(&key).is_none() {
            return Err(TermError::repository(
                "in_memory",
                "delete",
                format!("Key not found: {key}"),
            ));
        }

        drop(store);
        self.update_metadata().await;
        Ok(())
    }

    #[instrument(skip(self))]
    async fn list_keys(&self) -> Result<Vec<ResultKey>> {
        let store = self.storage.read().await;
        Ok(store.keys().cloned().collect())
    }

    #[instrument(skip(self), fields(key.timestamp = %key.timestamp, repository_type = "in_memory"))]
    async fn get(&self, key: &ResultKey) -> Result<Option<AnalyzerContext>> {
        let store = self.storage.read().await;
        Ok(store.get(key).cloned())
    }

    #[instrument(skip(self), fields(key.timestamp = %key.timestamp, repository_type = "in_memory"))]
    async fn exists(&self, key: &ResultKey) -> Result<bool> {
        let store = self.storage.read().await;
        Ok(store.contains_key(key))
    }

    #[instrument(skip(self))]
    async fn metadata(&self) -> Result<RepositoryMetadata> {
        Ok(self.metadata.read().await.clone())
    }
}

/// Override the load method to return actual stored contexts
impl InMemoryRepository {
    /// Loads a query that will return actual stored contexts.
    pub async fn load_with_data(&self) -> InMemoryMetricsQuery {
        InMemoryMetricsQuery::new(self.clone())
    }

    /// Determines if the dataset is large enough to benefit from DataFusion.
    ///
    /// DataFusion has overhead for small datasets, so we only use it when
    /// the performance benefits outweigh the setup costs.
    pub async fn should_use_datafusion(&self) -> bool {
        const DATAFUSION_THRESHOLD: usize = 1000; // Threshold based on benchmarks
        self.size().await >= DATAFUSION_THRESHOLD
    }
}

/// A custom query implementation for in-memory repository that returns actual data.
pub struct InMemoryMetricsQuery {
    repository: InMemoryRepository,
    before: Option<i64>,
    after: Option<i64>,
    tags: HashMap<String, String>,
    analyzers: Option<Vec<String>>,
    limit: Option<usize>,
    offset: Option<usize>,
    sort_order: super::query::SortOrder,
}

impl InMemoryMetricsQuery {
    pub fn new(repository: InMemoryRepository) -> Self {
        Self {
            repository,
            before: None,
            after: None,
            tags: HashMap::new(),
            analyzers: None,
            limit: None,
            offset: None,
            sort_order: super::query::SortOrder::Descending,
        }
    }

    pub fn before(mut self, timestamp: i64) -> Self {
        self.before = Some(timestamp);
        self
    }

    pub fn after(mut self, timestamp: i64) -> Self {
        self.after = Some(timestamp);
        self
    }

    pub fn between(mut self, start: i64, end: i64) -> Self {
        self.after = Some(start);
        self.before = Some(end);
        self
    }

    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    pub fn with_tags<I, K, V>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        for (k, v) in tags {
            self.tags.insert(k.into(), v.into());
        }
        self
    }

    pub fn for_analyzers<I, S>(mut self, analyzers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.analyzers = Some(analyzers.into_iter().map(|s| s.into()).collect());
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    pub fn sort(mut self, order: super::query::SortOrder) -> Self {
        self.sort_order = order;
        self
    }

    #[instrument(skip(self), fields(
        query.filters.time_range = format_args!("{:?}-{:?}", self.after, self.before),
        query.filters.tag_count = self.tags.len(),
        query.limit = self.limit,
        query.offset = self.offset
    ))]
    pub async fn execute(self) -> Result<Vec<(ResultKey, AnalyzerContext)>> {
        self.repository
            .execute_query_optimized(
                self.before,
                self.after,
                &self.tags,
                &self.analyzers,
                self.limit,
                self.offset,
                self.sort_order == super::query::SortOrder::Ascending,
            )
            .await
    }

    #[instrument(skip(self))]
    pub async fn count(self) -> Result<usize> {
        let results = self.execute().await?;
        Ok(results.len())
    }

    #[instrument(skip(self))]
    pub async fn exists(self) -> Result<bool> {
        let limited = self.limit(1);
        let results = limited.execute().await?;
        Ok(!results.is_empty())
    }
}

/// Custom query executor for in-memory repository with optimized filtering.
impl InMemoryRepository {
    /// Executes a query with in-memory optimizations.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip(self, tags, analyzers), fields(
        repository_type = "in_memory",
        time_range.before = before,
        time_range.after = after,
        limit = limit,
        offset = offset,
        ascending = ascending
    ))]
    pub async fn execute_query_optimized(
        &self,
        before: Option<i64>,
        after: Option<i64>,
        tags: &HashMap<String, String>,
        analyzers: &Option<Vec<String>>,
        limit: Option<usize>,
        offset: Option<usize>,
        ascending: bool,
    ) -> Result<Vec<(ResultKey, AnalyzerContext)>> {
        let store = self.storage.read().await;

        // Filter and collect results
        let mut results: Vec<(ResultKey, AnalyzerContext)> = store
            .iter()
            .filter(|(key, _)| {
                // Time range filter
                if let Some(before) = before {
                    if key.timestamp >= before {
                        return false;
                    }
                }
                if let Some(after) = after {
                    if key.timestamp < after {
                        return false;
                    }
                }

                // Tag filter
                if !key.matches_tags(tags) {
                    return false;
                }

                true
            })
            .filter(|(_, context)| {
                // Analyzer filter
                if let Some(ref analyzers) = analyzers {
                    analyzers
                        .iter()
                        .any(|analyzer| !context.get_analyzer_metrics(analyzer).is_empty())
                } else {
                    true
                }
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // Sort results
        if ascending {
            results.sort_by_key(|(key, _)| key.timestamp);
        } else {
            results.sort_by_key(|(key, _)| -key.timestamp);
        }

        // Apply pagination
        let start = offset.unwrap_or(0);
        let end = if let Some(limit) = limit {
            (start + limit).min(results.len())
        } else {
            results.len()
        };

        Ok(results[start..end].to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyzers::types::MetricValue;
    use crate::repository::MetricsRepository;

    #[tokio::test]
    async fn test_in_memory_repository_basic_operations() {
        let repo = InMemoryRepository::new();

        // Test save
        let key1 = ResultKey::new(1000).with_tag("env", "test");
        let mut context1 = AnalyzerContext::new();
        context1.store_metric("size", MetricValue::Long(100));

        repo.save(key1.clone(), context1.clone()).await.unwrap();

        // Test exists
        assert!(repo.exists(&key1).await.unwrap());

        // Test list_keys
        let keys = repo.list_keys().await.unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], key1);

        // Test size
        assert_eq!(repo.size().await, 1);

        // Test delete
        repo.delete(key1.clone()).await.unwrap();
        assert!(!repo.exists(&key1).await.unwrap());
        assert_eq!(repo.size().await, 0);
    }

    #[tokio::test]
    async fn test_in_memory_repository_metadata() {
        let repo = InMemoryRepository::new();

        let metadata = repo.metadata().await.unwrap();
        assert_eq!(metadata.backend_type, Some("in_memory".to_string()));
        assert_eq!(metadata.total_metrics, Some(0));

        // Add some data
        let key = ResultKey::now().with_tag("test", "value");
        let context = AnalyzerContext::new();
        repo.save(key, context).await.unwrap();

        let metadata = repo.metadata().await.unwrap();
        assert_eq!(metadata.total_metrics, Some(1));
        assert!(metadata.last_modified.is_some());
        assert!(metadata.storage_size_bytes.is_some());
    }

    #[tokio::test]
    async fn test_in_memory_repository_query() {
        let repo = InMemoryRepository::new();

        // Add test data
        for i in 0..5 {
            let key = ResultKey::new(i * 1000)
                .with_tag("env", if i % 2 == 0 { "prod" } else { "staging" })
                .with_tag("version", format!("v{i}"));

            let mut context = AnalyzerContext::new();
            context.store_metric("size", MetricValue::Long(i * 100));

            repo.save(key, context).await.unwrap();
        }

        // Test query with tag filter
        let results = repo
            .load()
            .await
            .with_tag("env", "prod")
            .execute()
            .await
            .unwrap();

        assert_eq!(results.len(), 3); // 0, 2, 4 are prod

        // Test query with time range
        let results = repo
            .load()
            .await
            .after(1000)
            .before(4000)
            .execute()
            .await
            .unwrap();

        assert_eq!(results.len(), 3); // 1000, 2000, 3000

        // Test query with pagination
        let results = repo
            .load()
            .await
            .limit(2)
            .offset(1)
            .execute()
            .await
            .unwrap();

        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_in_memory_repository_clear() {
        let mut repo = InMemoryRepository::new();

        // Add data
        for i in 0..3 {
            let key = ResultKey::new(i * 1000);
            let context = AnalyzerContext::new();
            repo.save(key, context).await.unwrap();
        }

        assert_eq!(repo.size().await, 3);

        // Clear repository
        repo.clear().await;
        assert_eq!(repo.size().await, 0);

        let keys = repo.list_keys().await.unwrap();
        assert!(keys.is_empty());
    }
}
