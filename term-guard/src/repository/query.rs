//! Query builder for filtering and retrieving metrics from repositories.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::instrument;

use crate::analyzers::context::AnalyzerContext;
use crate::error::{Result, TermError};

use super::{MetricsRepository, ResultKey};

/// Builder for constructing queries against a metrics repository.
///
/// `MetricsQuery` provides a fluent API for filtering metrics by various criteria
/// including time ranges, tags, and analyzer types. Queries are constructed
/// incrementally and executed asynchronously.
///
/// # Example
///
/// ```rust,ignore
/// use term_guard::repository::MetricsQuery;
///
/// let results = repository.load().await
///     .after(start_timestamp)
///     .before(end_timestamp)
///     .with_tag("environment", "production")
///     .for_analyzers(vec!["completeness", "size"])
///     .execute()
///     .await?;
///
/// for (key, context) in results {
///     println!("Metrics at {}: {:?}", key.timestamp, context.all_metrics());
/// }
/// ```
pub struct MetricsQuery {
    /// The repository to query against.
    repository: Arc<dyn MetricsRepository>,

    /// Filter for metrics before this timestamp (exclusive).
    before: Option<i64>,

    /// Filter for metrics after this timestamp (inclusive).
    after: Option<i64>,

    /// Filter for metrics with matching tags.
    tags: HashMap<String, String>,

    /// Filter for specific analyzer types.
    analyzers: Option<Vec<String>>,

    /// Maximum number of results to return.
    limit: Option<usize>,

    /// Offset for pagination.
    offset: Option<usize>,

    /// Sort order for results.
    sort_order: SortOrder,
}

/// Sort order for query results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    /// Sort by timestamp ascending (oldest first).
    Ascending,
    /// Sort by timestamp descending (newest first).
    Descending,
}

impl MetricsQuery {
    /// Creates a new query for the given repository.
    ///
    /// # Arguments
    ///
    /// * `repository` - The repository to query against
    pub fn new(repository: Arc<dyn MetricsRepository>) -> Self {
        Self {
            repository,
            before: None,
            after: None,
            tags: HashMap::new(),
            analyzers: None,
            limit: None,
            offset: None,
            sort_order: SortOrder::Descending,
        }
    }

    /// Filters results to metrics before the specified timestamp.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - Unix timestamp in milliseconds (exclusive)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let end_time = chrono::Utc::now().timestamp_millis();
    /// let query = repository.load().await.before(end_time);
    /// ```
    pub fn before(mut self, timestamp: i64) -> Self {
        self.before = Some(timestamp);
        self
    }

    /// Filters results to metrics after the specified timestamp.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - Unix timestamp in milliseconds (inclusive)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let start_time = (chrono::Utc::now() - chrono::Duration::days(7)).timestamp_millis();
    /// let query = repository.load().await.after(start_time);
    /// ```
    pub fn after(mut self, timestamp: i64) -> Self {
        self.after = Some(timestamp);
        self
    }

    /// Filters results to metrics within a time range.
    ///
    /// # Arguments
    ///
    /// * `start` - Start timestamp in milliseconds (inclusive)
    /// * `end` - End timestamp in milliseconds (exclusive)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let query = repository.load().await.between(start_time, end_time);
    /// ```
    pub fn between(mut self, start: i64, end: i64) -> Self {
        self.after = Some(start);
        self.before = Some(end);
        self
    }

    /// Filters results to metrics with a specific tag.
    ///
    /// # Arguments
    ///
    /// * `key` - The tag key
    /// * `value` - The tag value
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let query = repository.load().await
    ///     .with_tag("environment", "production")
    ///     .with_tag("region", "us-west-2");
    /// ```
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    /// Filters results to metrics with multiple tags.
    ///
    /// # Arguments
    ///
    /// * `tags` - Iterator of (key, value) pairs
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let tags = vec![
    ///     ("environment", "production"),
    ///     ("dataset", "users"),
    /// ];
    /// let query = repository.load().await.with_tags(tags);
    /// ```
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

    /// Filters results to specific analyzer types.
    ///
    /// Only metrics from the specified analyzers will be included in the results.
    ///
    /// # Arguments
    ///
    /// * `analyzers` - List of analyzer names
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let query = repository.load().await
    ///     .for_analyzers(vec!["completeness", "size", "mean"]);
    /// ```
    pub fn for_analyzers<I, S>(mut self, analyzers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.analyzers = Some(analyzers.into_iter().map(|s| s.into()).collect());
        self
    }

    /// Limits the number of results returned.
    ///
    /// # Arguments
    ///
    /// * `limit` - Maximum number of results
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let query = repository.load().await.limit(100);
    /// ```
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Validates query parameters for correctness.
    ///
    /// # Returns
    ///
    /// Returns an error if the query parameters are invalid.
    pub fn validate(&self) -> Result<()> {
        // Validate time range
        if let (Some(after), Some(before)) = (self.after, self.before) {
            if after >= before {
                return Err(TermError::invalid_repository_query(
                    "Invalid time range: 'after' timestamp must be less than 'before' timestamp",
                    format!("after: {after}, before: {before}"),
                ));
            }
        }

        // Validate limit
        if let Some(limit) = self.limit {
            if limit == 0 {
                return Err(TermError::invalid_repository_query(
                    "Limit must be greater than 0",
                    format!("limit: {limit}"),
                ));
            }
            if limit > 1_000_000 {
                return Err(TermError::invalid_repository_query(
                    "Limit too large (max: 1,000,000)",
                    format!("limit: {limit}"),
                ));
            }
        }

        // Validate tag keys and values
        for (key, value) in &self.tags {
            if key.is_empty() {
                return Err(TermError::invalid_repository_query(
                    "Tag key cannot be empty",
                    format!("tag: '{key}' = '{value}'"),
                ));
            }
            if key.len() > 256 {
                return Err(TermError::invalid_repository_query(
                    "Tag key too long (max: 256 characters)",
                    format!("tag: '{key}' ({} chars)", key.len()),
                ));
            }
            if value.len() > 1024 {
                return Err(TermError::invalid_repository_query(
                    "Tag value too long (max: 1024 characters)",
                    format!("tag: '{key}' = '{value}' ({} chars)", value.len()),
                ));
            }
        }

        // Validate analyzer names
        if let Some(ref analyzers) = self.analyzers {
            if analyzers.is_empty() {
                return Err(TermError::invalid_repository_query(
                    "Analyzer list cannot be empty (use None instead)",
                    "analyzers: []".to_string(),
                ));
            }
            for analyzer in analyzers {
                if analyzer.is_empty() {
                    return Err(TermError::invalid_repository_query(
                        "Analyzer name cannot be empty",
                        format!("analyzers: {analyzers:?}"),
                    ));
                }
            }
        }

        Ok(())
    }

    /// Sets the offset for pagination.
    ///
    /// # Arguments
    ///
    /// * `offset` - Number of results to skip
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Get results 100-200
    /// let query = repository.load().await.offset(100).limit(100);
    /// ```
    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Sets the sort order for results.
    ///
    /// # Arguments
    ///
    /// * `order` - The sort order to use
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use term_guard::repository::query::SortOrder;
    ///
    /// let query = repository.load().await.sort(SortOrder::Ascending);
    /// ```
    pub fn sort(mut self, order: SortOrder) -> Self {
        self.sort_order = order;
        self
    }

    /// Executes the query and returns the results.
    ///
    /// # Returns
    ///
    /// A vector of (ResultKey, AnalyzerContext) pairs matching the query criteria,
    /// sorted by timestamp according to the specified sort order.
    ///
    /// # Errors
    ///
    /// Returns an error if the query execution fails (e.g., I/O error, invalid query).
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let results = repository.load().await
    ///     .after(start_time)
    ///     .with_tag("environment", "production")
    ///     .execute()
    ///     .await?;
    ///
    /// for (key, context) in results {
    ///     println!("Timestamp: {}", key.timestamp);
    ///     println!("Metrics: {:?}", context.all_metrics());
    /// }
    /// ```
    #[instrument(skip(self), fields(
        query.filters.time_range = format_args!("{:?}-{:?}", self.after, self.before),
        query.filters.tag_count = self.tags.len(),
        query.limit = self.limit,
        query.offset = self.offset
    ))]
    pub async fn execute(self) -> Result<Vec<(ResultKey, AnalyzerContext)>> {
        // Validate query parameters before execution
        self.validate()?;

        // This is a default implementation that can be overridden by specific repositories
        // for more efficient querying. For now, we'll load all keys and filter in memory.

        let all_keys = self.repository.list_keys().await?;

        let mut filtered_results = Vec::new();

        for key in all_keys {
            // Apply time filters
            if let Some(before) = self.before {
                if key.timestamp >= before {
                    continue;
                }
            }

            if let Some(after) = self.after {
                if key.timestamp < after {
                    continue;
                }
            }

            // Apply tag filters
            if !key.matches_tags(&self.tags) {
                continue;
            }

            // Load the context for this key
            // Try to get the actual context from the repository
            let context = match self.repository.get(&key).await {
                Ok(Some(ctx)) => ctx,
                _ => AnalyzerContext::new(),
            };

            // Apply analyzer filter if specified
            if let Some(ref analyzers) = self.analyzers {
                // Check if any of the requested analyzers have metrics
                let has_analyzer = analyzers
                    .iter()
                    .any(|analyzer| !context.get_analyzer_metrics(analyzer).is_empty());

                if !has_analyzer && !context.all_metrics().is_empty() {
                    continue;
                }
            }

            filtered_results.push((key, context));
        }

        // Sort results
        match self.sort_order {
            SortOrder::Ascending => {
                filtered_results.sort_by_key(|(key, _)| key.timestamp);
            }
            SortOrder::Descending => {
                filtered_results.sort_by_key(|(key, _)| -key.timestamp);
            }
        }

        // Apply pagination
        if let Some(offset) = self.offset {
            filtered_results = filtered_results.into_iter().skip(offset).collect();
        }

        if let Some(limit) = self.limit {
            filtered_results.truncate(limit);
        }

        Ok(filtered_results)
    }

    /// Returns a count of metrics matching the query criteria without loading them.
    ///
    /// This is more efficient than executing the query and counting results
    /// when only the count is needed.
    ///
    /// # Errors
    ///
    /// Returns an error if the count operation fails.
    #[instrument(skip(self), fields(
        query.filters.time_range = format_args!("{:?}-{:?}", self.after, self.before),
        query.filters.tag_count = self.tags.len()
    ))]
    pub async fn count(self) -> Result<usize> {
        // Default implementation executes the query and counts results
        // Specific repositories can override this for efficiency
        let results = self.execute().await?;
        Ok(results.len())
    }

    /// Checks if any metrics match the query criteria.
    ///
    /// # Returns
    ///
    /// Returns `true` if at least one metric matches, `false` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if the check operation fails.
    #[instrument(skip(self), fields(
        query.filters.time_range = format_args!("{:?}-{:?}", self.after, self.before),
        query.filters.tag_count = self.tags.len()
    ))]
    pub async fn exists(self) -> Result<bool> {
        let limited = self.limit(1);
        let results = limited.execute().await?;
        Ok(!results.is_empty())
    }

    /// Accessor methods for DataFusion integration
    pub fn get_before(&self) -> Option<i64> {
        self.before
    }

    pub fn get_after(&self) -> Option<i64> {
        self.after
    }

    pub fn get_tags(&self) -> &HashMap<String, String> {
        &self.tags
    }

    pub fn get_analyzers(&self) -> &Option<Vec<String>> {
        &self.analyzers
    }

    pub fn get_limit(&self) -> Option<usize> {
        self.limit
    }

    pub fn get_offset(&self) -> Option<usize> {
        self.offset
    }

    pub fn get_sort_order(&self) -> SortOrder {
        self.sort_order
    }

    pub fn is_ascending(&self) -> bool {
        self.sort_order == SortOrder::Ascending
    }
}

/// Extension trait for repositories to provide custom query execution.
#[async_trait]
pub trait QueryExecutor: MetricsRepository {
    /// Executes a query with repository-specific optimizations.
    ///
    /// Repositories can implement this method to provide more efficient
    /// query execution than the default in-memory filtering.
    #[instrument(skip(self, query))]
    async fn execute_query(
        &self,
        query: MetricsQuery,
    ) -> Result<Vec<(ResultKey, AnalyzerContext)>> {
        // Default implementation delegates to the query's execute method
        query.execute().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::MetricsRepository;

    // Mock repository for testing
    struct MockRepository;

    #[async_trait]
    impl MetricsRepository for MockRepository {
        async fn save(&self, _key: ResultKey, _metrics: AnalyzerContext) -> Result<()> {
            Ok(())
        }

        async fn load(&self) -> MetricsQuery {
            MetricsQuery::new(Arc::new(MockRepository))
        }

        async fn delete(&self, _key: ResultKey) -> Result<()> {
            Ok(())
        }

        async fn list_keys(&self) -> Result<Vec<ResultKey>> {
            Ok(vec![
                ResultKey::new(1000).with_tag("env", "prod"),
                ResultKey::new(2000).with_tag("env", "staging"),
                ResultKey::new(3000)
                    .with_tag("env", "prod")
                    .with_tag("version", "1.0"),
                ResultKey::new(4000)
                    .with_tag("env", "prod")
                    .with_tag("version", "2.0"),
            ])
        }
    }

    #[tokio::test]
    async fn test_query_time_filters() {
        let repo = Arc::new(MockRepository);
        let query = MetricsQuery::new(repo.clone()).after(1500).before(3500);

        let results = query.execute().await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0.timestamp, 3000);
        assert_eq!(results[1].0.timestamp, 2000);
    }

    #[tokio::test]
    async fn test_query_tag_filters() {
        let repo = Arc::new(MockRepository);
        let query = MetricsQuery::new(repo.clone()).with_tag("env", "prod");

        let results = query.execute().await.unwrap();
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn test_query_multiple_tags() {
        let repo = Arc::new(MockRepository);
        let query = MetricsQuery::new(repo.clone())
            .with_tag("env", "prod")
            .with_tag("version", "1.0");

        let results = query.execute().await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0.timestamp, 3000);
    }

    #[tokio::test]
    async fn test_query_sort_order() {
        let repo = Arc::new(MockRepository);

        // Test descending (default)
        let query = MetricsQuery::new(repo.clone());
        let results = query.execute().await.unwrap();
        assert_eq!(results[0].0.timestamp, 4000);
        assert_eq!(results[3].0.timestamp, 1000);

        // Test ascending
        let query = MetricsQuery::new(repo.clone()).sort(SortOrder::Ascending);
        let results = query.execute().await.unwrap();
        assert_eq!(results[0].0.timestamp, 1000);
        assert_eq!(results[3].0.timestamp, 4000);
    }

    #[tokio::test]
    async fn test_query_pagination() {
        let repo = Arc::new(MockRepository);
        let query = MetricsQuery::new(repo.clone())
            .sort(SortOrder::Ascending)
            .offset(1)
            .limit(2);

        let results = query.execute().await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0.timestamp, 2000);
        assert_eq!(results[1].0.timestamp, 3000);
    }

    #[tokio::test]
    async fn test_query_exists() {
        let repo = Arc::new(MockRepository);

        let exists = MetricsQuery::new(repo.clone())
            .with_tag("env", "prod")
            .exists()
            .await
            .unwrap();
        assert!(exists);

        let not_exists = MetricsQuery::new(repo.clone())
            .with_tag("env", "nonexistent")
            .exists()
            .await
            .unwrap();
        assert!(!not_exists);
    }
}
