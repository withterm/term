//! Multi-source validation engine for cross-table validation in Term.
//!
//! This module provides the core infrastructure for validating relationships, referential integrity,
//! and business rules spanning multiple datasets. It addresses the critical market gap where 68% of
//! data quality issues involve relationships between tables.
//!
//! # Architecture
//!
//! The `MultiSourceValidator` coordinates:
//! - Registration of heterogeneous data sources
//! - Query optimization across joined tables
//! - Caching of intermediate results
//! - Performance monitoring and telemetry
//!
//! # Examples
//!
//! ```rust
//! use term_guard::core::{MultiSourceValidator, ValidationSuite, Check};
//! use term_guard::sources::{CsvSource, ParquetSource};
//! use datafusion::prelude::*;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut validator = MultiSourceValidator::new();
//!
//! // Register multiple data sources
//! validator.add_source("orders", CsvSource::new("orders.csv")?).await?;
//! validator.add_source("customers", ParquetSource::new("customers.parquet")?).await?;
//!
//! // Create validation suite with cross-table constraints
//! let suite = ValidationSuite::builder("cross_table_validation")
//!     .check(
//!         Check::builder("referential_integrity")
//!             .foreign_key("orders.customer_id", "customers.id")
//!             .build()
//!     )
//!     .build();
//!
//! // Run validation
//! let results = validator.run_suite(&suite).await?;
//! # Ok(())
//! # }
//! ```

use crate::core::{ValidationResult, ValidationSuite};
use crate::error::{Result, TermError};
use crate::sources::DataSource;
use crate::telemetry::TermTelemetry;
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info, instrument, span, Level};

/// Multi-source validation engine for cross-table data validation.
///
/// This engine manages multiple data sources and provides efficient validation
/// across heterogeneous datasets with optimized query execution and caching.
pub struct MultiSourceValidator {
    /// DataFusion session context for query execution
    ctx: SessionContext,
    /// Registered data sources by name
    sources: HashMap<String, Arc<dyn DataSource>>,
    /// Query result cache for performance optimization
    query_cache: HashMap<String, CachedResult>,
    /// Optional telemetry for observability
    telemetry: Option<Arc<TermTelemetry>>,
    /// Enable query result caching
    enable_caching: bool,
    /// Maximum cache size in bytes
    max_cache_size: usize,
    /// Current cache size in bytes
    current_cache_size: usize,
}

/// Cached query result with metadata
#[derive(Debug, Clone)]
struct CachedResult {
    /// The cached data
    data: Vec<RecordBatch>,
    /// When the result was cached
    cached_at: Instant,
    /// Size in bytes
    size_bytes: usize,
}

impl MultiSourceValidator {
    /// Create a new multi-source validator.
    pub fn new() -> Self {
        Self::with_context(SessionContext::new())
    }

    /// Create a new multi-source validator with a custom session context.
    pub fn with_context(ctx: SessionContext) -> Self {
        Self {
            ctx,
            sources: HashMap::new(),
            query_cache: HashMap::new(),
            telemetry: None,
            enable_caching: true,
            max_cache_size: 100 * 1024 * 1024, // 100MB default
            current_cache_size: 0,
        }
    }

    /// Set telemetry for observability.
    pub fn with_telemetry(mut self, telemetry: Arc<TermTelemetry>) -> Self {
        self.telemetry = Some(telemetry);
        self
    }

    /// Enable or disable query result caching.
    pub fn with_caching(mut self, enable: bool) -> Self {
        self.enable_caching = enable;
        self
    }

    /// Set maximum cache size in bytes.
    pub fn with_max_cache_size(mut self, size_bytes: usize) -> Self {
        self.max_cache_size = size_bytes;
        self
    }

    /// Add a data source to the validator.
    ///
    /// # Arguments
    ///
    /// * `name` - The name to register the source as
    /// * `source` - The data source to register
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use term_guard::core::MultiSourceValidator;
    /// # use term_guard::sources::CsvSource;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut validator = MultiSourceValidator::new();
    /// validator.add_source("orders", CsvSource::new("orders.csv")?).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip(self, source, name))]
    pub async fn add_source<S: DataSource + 'static>(
        &mut self,
        name: impl Into<String>,
        source: S,
    ) -> Result<()> {
        let name = name.into();
        info!("Adding data source: {}", name);

        let source = Arc::new(source);

        // Register with DataFusion context
        source
            .register_with_telemetry(&self.ctx, &name, self.telemetry.as_ref())
            .await
            .map_err(|e| {
                TermError::data_source(
                    "multi_source",
                    format!("Failed to register source '{name}': {e}"),
                )
            })?;

        self.sources.insert(name.clone(), source);
        info!("Successfully added data source: {}", name);

        Ok(())
    }

    /// Get the DataFusion session context.
    ///
    /// This provides direct access to the underlying context for advanced use cases.
    pub fn context(&self) -> &SessionContext {
        &self.ctx
    }

    /// Get a registered data source by name.
    pub fn get_source(&self, name: &str) -> Option<&Arc<dyn DataSource>> {
        self.sources.get(name)
    }

    /// List all registered data sources.
    pub fn list_sources(&self) -> Vec<String> {
        self.sources.keys().cloned().collect()
    }

    /// Run a validation suite across registered data sources.
    ///
    /// # Arguments
    ///
    /// * `suite` - The validation suite to run
    ///
    /// # Returns
    ///
    /// The validation results from running the suite
    #[instrument(skip(self, suite), fields(suite_name = %suite.name()))]
    pub async fn run_suite(&self, suite: &ValidationSuite) -> Result<ValidationResult> {
        let span = span!(Level::INFO, "multi_source_validation", suite = %suite.name());
        let _enter = span.enter();

        info!(
            "Running validation suite '{}' with {} registered sources",
            suite.name(),
            self.sources.len()
        );

        // Clear expired cache entries if caching is enabled
        if self.enable_caching {
            self.cleanup_cache();
        }

        // Run the suite with our context
        let result = suite.run(&self.ctx).await?;

        match &result {
            ValidationResult::Success { report, .. } => {
                info!(
                    "Validation suite '{}' succeeded: {} checks passed",
                    suite.name(),
                    report.metrics.total_checks
                );
            }
            ValidationResult::Failure { report } => {
                info!(
                    "Validation suite '{}' failed: {} issues found",
                    suite.name(),
                    report.issues.len()
                );
            }
        }

        Ok(result)
    }

    /// Execute a SQL query with optional caching.
    ///
    /// This method is used internally by constraints for performance optimization.
    #[instrument(skip(self))]
    pub async fn execute_query(&mut self, sql: &str) -> Result<Vec<RecordBatch>> {
        // Generate cache key from SQL query using hash
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        sql.hash(&mut hasher);
        let cache_key = format!("{:x}", hasher.finish());

        // Check cache if enabled
        if self.enable_caching {
            if let Some(cached) = self.query_cache.get(&cache_key) {
                debug!("Cache hit for query");
                return Ok(cached.data.clone());
            }
        }

        debug!("Executing query: {}", sql);

        // Execute query
        let df = self.ctx.sql(sql).await.map_err(|e| {
            TermError::data_source("multi_source", format!("Query execution failed: {e}"))
        })?;

        let batches = df.collect().await.map_err(|e| {
            TermError::data_source("multi_source", format!("Failed to collect results: {e}"))
        })?;

        // Cache result if enabled
        if self.enable_caching {
            self.cache_result(cache_key, batches.clone());
        }

        Ok(batches)
    }

    /// Cache a query result.
    fn cache_result(&mut self, key: String, data: Vec<RecordBatch>) {
        let size_bytes = data.iter().map(|batch| batch.get_array_memory_size()).sum();

        // Check if adding this would exceed cache size
        if self.current_cache_size + size_bytes > self.max_cache_size {
            // Evict oldest entries until we have space
            self.evict_cache_entries(size_bytes);
        }

        let cached = CachedResult {
            data,
            cached_at: Instant::now(),
            size_bytes,
        };

        self.current_cache_size += size_bytes;
        self.query_cache.insert(key, cached);
    }

    /// Evict cache entries to make room for new data.
    fn evict_cache_entries(&mut self, needed_bytes: usize) {
        // Simple LRU eviction - remove oldest entries
        let mut entries_to_remove = Vec::new();

        {
            let mut entries: Vec<_> = self.query_cache.iter().collect();
            entries.sort_by_key(|(_, cached)| cached.cached_at);

            for (key, cached) in entries {
                if self.current_cache_size + needed_bytes <= self.max_cache_size {
                    break;
                }

                entries_to_remove.push((key.clone(), cached.size_bytes));
            }
        }

        // Now remove the entries after collecting them
        for (key, size) in entries_to_remove {
            self.query_cache.remove(&key);
            self.current_cache_size -= size;
            debug!("Evicted cache entry to free {} bytes", size);
        }
    }

    /// Clean up expired cache entries.
    fn cleanup_cache(&self) {
        // This is a placeholder for more sophisticated cache management
        // Currently we only evict based on size, but could add TTL-based eviction
        debug!(
            "Cache cleanup: {} entries, {} bytes",
            self.query_cache.len(),
            self.current_cache_size
        );
    }

    /// Get cache statistics.
    pub fn cache_stats(&self) -> CacheStats {
        CacheStats {
            entries: self.query_cache.len(),
            size_bytes: self.current_cache_size,
            max_size_bytes: self.max_cache_size,
            hit_rate: 0.0, // Would need to track hits/misses for this
        }
    }
}

impl Default for MultiSourceValidator {
    fn default() -> Self {
        Self::new()
    }
}

/// Cache statistics for monitoring.
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Number of cached entries
    pub entries: usize,
    /// Current cache size in bytes
    pub size_bytes: usize,
    /// Maximum cache size in bytes
    pub max_size_bytes: usize,
    /// Cache hit rate (0.0 to 1.0)
    pub hit_rate: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::CsvSource;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_test_csv(data: &str) -> Result<NamedTempFile> {
        let mut temp_file = NamedTempFile::with_suffix(".csv")?;
        write!(temp_file, "{data}")?;
        temp_file.flush()?;
        Ok(temp_file)
    }

    #[tokio::test]
    async fn test_multi_source_validator_creation() {
        let validator = MultiSourceValidator::new();
        assert_eq!(validator.sources.len(), 0);
        assert!(validator.enable_caching);
    }

    #[tokio::test]
    async fn test_add_source() -> Result<()> {
        let mut validator = MultiSourceValidator::new();

        let csv_data = "id,name\n1,Alice\n2,Bob";
        let temp_file = create_test_csv(csv_data)?;
        let source = CsvSource::new(temp_file.path().to_string_lossy().to_string())?;

        validator.add_source("test_data", source).await?;

        assert_eq!(validator.sources.len(), 1);
        assert!(validator.get_source("test_data").is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_list_sources() -> Result<()> {
        let mut validator = MultiSourceValidator::new();

        let csv_data = "id,value\n1,100";
        let temp_file1 = create_test_csv(csv_data)?;
        let temp_file2 = create_test_csv(csv_data)?;

        validator
            .add_source(
                "source1",
                CsvSource::new(temp_file1.path().to_string_lossy().to_string())?,
            )
            .await?;
        validator
            .add_source(
                "source2",
                CsvSource::new(temp_file2.path().to_string_lossy().to_string())?,
            )
            .await?;

        let sources = validator.list_sources();
        assert_eq!(sources.len(), 2);
        assert!(sources.contains(&"source1".to_string()));
        assert!(sources.contains(&"source2".to_string()));

        Ok(())
    }

    #[tokio::test]
    async fn test_cache_configuration() {
        let validator = MultiSourceValidator::new()
            .with_caching(false)
            .with_max_cache_size(1024 * 1024);

        assert!(!validator.enable_caching);
        assert_eq!(validator.max_cache_size, 1024 * 1024);
    }

    #[tokio::test]
    async fn test_cache_stats() {
        let validator = MultiSourceValidator::new();
        let stats = validator.cache_stats();

        assert_eq!(stats.entries, 0);
        assert_eq!(stats.size_bytes, 0);
        assert_eq!(stats.max_size_bytes, 100 * 1024 * 1024);
    }
}
