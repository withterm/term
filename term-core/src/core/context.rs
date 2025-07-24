//! DataFusion context management for Term validation library.
//!
//! This module provides [`TermContext`], an abstraction layer over DataFusion's
//! [`SessionContext`] with optimized settings for data validation workloads.

use crate::prelude::*;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::memory_pool::{FairSpillPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::instrument;

/// Configuration for creating a [`TermContext`].
#[derive(Debug, Clone)]
pub struct TermContextConfig {
    /// Batch size for query execution
    pub batch_size: usize,
    /// Target number of partitions for parallel execution
    pub target_partitions: usize,
    /// Maximum memory for query execution (in bytes)
    pub max_memory: usize,
    /// Memory fraction to use before spilling (0.0 to 1.0)
    pub memory_fraction: f64,
}

impl Default for TermContextConfig {
    fn default() -> Self {
        Self {
            batch_size: 8192,
            target_partitions: std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(4),
            max_memory: 2 * 1024 * 1024 * 1024, // 2GB
            memory_fraction: 0.9,
        }
    }
}

/// A managed DataFusion context for Term validation operations.
///
/// `TermContext` wraps DataFusion's [`SessionContext`] and provides:
/// - Optimized default settings for data validation workloads
/// - Memory management with configurable limits
/// - Table registration helpers with tracking
/// - Automatic resource cleanup
///
/// # Examples
///
/// ```rust,ignore
/// use term_core::core::TermContext;
///
/// # async fn example() -> Result<()> {
/// // Create context with default settings
/// let ctx = TermContext::new()?;
///
/// // Register a table
/// ctx.register_csv("users", "data/users.csv").await?;
///
/// // Use the underlying SessionContext for queries
/// let df = ctx.inner().sql("SELECT COUNT(*) FROM users").await?;
/// # Ok(())
/// # }
/// ```
pub struct TermContext {
    inner: SessionContext,
    pub(crate) tables: HashMap<String, Arc<dyn TableProvider>>,
    config: TermContextConfig,
}

impl TermContext {
    /// Creates a new context with default configuration.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use term_core::core::TermContext;
    ///
    /// let ctx = TermContext::new()?;
    /// ```
    #[instrument]
    pub fn new() -> Result<Self> {
        Self::with_config(TermContextConfig::default())
    }

    /// Creates a new context with custom configuration.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use term_core::core::{TermContext, TermContextConfig};
    ///
    /// let config = TermContextConfig {
    ///     batch_size: 16384,
    ///     max_memory: 4 * 1024 * 1024 * 1024, // 4GB
    ///     ..Default::default()
    /// };
    ///
    /// let ctx = TermContext::with_config(config)?;
    /// ```
    #[instrument(skip(config))]
    pub fn with_config(config: TermContextConfig) -> Result<Self> {
        // Create session configuration
        let session_config = SessionConfig::new()
            .with_batch_size(config.batch_size)
            .with_target_partitions(config.target_partitions)
            .with_information_schema(true);

        // Create memory pool
        let memory_pool = Arc::new(FairSpillPool::new(config.max_memory)) as Arc<dyn MemoryPool>;

        // Create runtime environment
        let runtime_env = RuntimeEnvBuilder::new()
            .with_memory_pool(memory_pool)
            .with_temp_file_path(std::env::temp_dir())
            .build()
            .map(Arc::new)?;

        // Create session context
        let inner = SessionContext::new_with_config_rt(session_config, runtime_env);

        Ok(Self {
            inner,
            tables: HashMap::new(),
            config,
        })
    }

    /// Returns a reference to the underlying DataFusion [`SessionContext`].
    ///
    /// This allows direct access to all DataFusion functionality while
    /// still benefiting from Term's resource management.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// # use term_core::core::TermContext;
    /// # async fn example(ctx: &TermContext) -> Result<()> {
    /// let df = ctx.inner().sql("SELECT * FROM data").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn inner(&self) -> &SessionContext {
        &self.inner
    }

    /// Returns a mutable reference to the underlying [`SessionContext`].
    pub fn inner_mut(&mut self) -> &mut SessionContext {
        &mut self.inner
    }

    /// Returns the configuration used to create this context.
    pub fn config(&self) -> &TermContextConfig {
        &self.config
    }

    /// Returns the names of all registered tables.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// # use term_core::core::TermContext;
    /// # async fn example(ctx: &TermContext) -> Result<()> {
    /// let tables = ctx.registered_tables();
    /// println!("Registered tables: {:?}", tables);
    /// # Ok(())
    /// # }
    /// ```
    pub fn registered_tables(&self) -> Vec<&str> {
        self.tables.keys().map(|s| s.as_str()).collect()
    }

    /// Checks if a table is registered.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// # use term_core::core::TermContext;
    /// # async fn example(ctx: &TermContext) -> Result<()> {
    /// if ctx.has_table("users") {
    ///     println!("Users table is registered");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn has_table(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    /// Registers a CSV file as a table.
    ///
    /// This is a convenience method that reads a CSV file and registers it
    /// as a table in the context.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// # use term_core::core::TermContext;
    /// # async fn example() -> Result<()> {
    /// let mut ctx = TermContext::new()?;
    /// ctx.register_csv("users", "data/users.csv").await?;
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip(self))]
    pub async fn register_csv(&mut self, name: &str, path: &str) -> Result<()> {
        self.inner
            .register_csv(name, path, Default::default())
            .await?;

        // Track the table
        let source = self.inner.table_provider(name).await?;
        self.tables.insert(name.to_string(), source);

        Ok(())
    }

    /// Registers a Parquet file as a table.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// # use term_core::core::TermContext;
    /// # async fn example() -> Result<()> {
    /// let mut ctx = TermContext::new()?;
    /// ctx.register_parquet("events", "data/events.parquet").await?;
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip(self))]
    pub async fn register_parquet(&mut self, name: &str, path: &str) -> Result<()> {
        self.inner
            .register_parquet(name, path, Default::default())
            .await?;

        // Track the table
        let source = self.inner.table_provider(name).await?;
        self.tables.insert(name.to_string(), source);

        Ok(())
    }

    /// Deregisters a table from the context.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// # use term_core::core::TermContext;
    /// # async fn example(ctx: &mut TermContext) -> Result<()> {
    /// ctx.deregister_table("users")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn deregister_table(&mut self, name: &str) -> Result<()> {
        self.inner.deregister_table(name)?;
        self.tables.remove(name);
        Ok(())
    }

    /// Registers a table directly and tracks it.
    ///
    /// This is a lower-level method that allows registering any TableProvider
    /// directly. The table is automatically tracked for cleanup.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// # use term_core::core::TermContext;
    /// # use datafusion::datasource::MemTable;
    /// # async fn example(ctx: &mut TermContext, table: Arc<MemTable>) -> Result<()> {
    /// ctx.register_table_provider("data", table).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip(self, provider))]
    pub async fn register_table_provider(
        &mut self,
        name: &str,
        provider: Arc<dyn TableProvider>,
    ) -> Result<()> {
        self.inner.register_table(name, provider.clone())?;
        self.tables.insert(name.to_string(), provider);
        Ok(())
    }

    /// Clears all registered tables.
    ///
    /// This is useful for resetting the context between validation runs.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// # use term_core::core::TermContext;
    /// # fn example(ctx: &mut TermContext) -> Result<()> {
    /// ctx.clear_tables()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn clear_tables(&mut self) -> Result<()> {
        let table_names: Vec<_> = self.tables.keys().cloned().collect();
        for name in table_names {
            self.deregister_table(&name)?;
        }
        Ok(())
    }
}

/// Ensure proper cleanup when the context is dropped.
impl Drop for TermContext {
    fn drop(&mut self) {
        // Clear all tables to ensure proper cleanup
        if let Err(e) = self.clear_tables() {
            tracing::warn!("Failed to clear tables during TermContext drop: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;

    #[test]
    fn test_default_config() {
        let config = TermContextConfig::default();
        assert_eq!(config.batch_size, 8192);
        assert_eq!(
            config.target_partitions,
            std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(4)
        );
        assert_eq!(config.max_memory, 2 * 1024 * 1024 * 1024);
        assert_eq!(config.memory_fraction, 0.9);
    }

    #[tokio::test]
    async fn test_context_creation() {
        let ctx = TermContext::new().unwrap();
        assert!(ctx.registered_tables().is_empty());
    }

    #[tokio::test]
    async fn test_context_with_custom_config() {
        let config = TermContextConfig {
            batch_size: 16384,
            max_memory: 4 * 1024 * 1024 * 1024,
            ..Default::default()
        };

        let ctx = TermContext::with_config(config.clone()).unwrap();
        assert_eq!(ctx.config().batch_size, 16384);
        assert_eq!(ctx.config().max_memory, 4 * 1024 * 1024 * 1024);
    }

    #[tokio::test]
    async fn test_table_registration() {
        let mut ctx = TermContext::new().unwrap();

        // Create test data
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        )
        .unwrap();

        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();

        // Register table
        ctx.register_table_provider("users", Arc::new(table))
            .await
            .unwrap();

        assert!(ctx.has_table("users"));
        assert_eq!(ctx.registered_tables(), vec!["users"]);
    }

    #[tokio::test]
    async fn test_table_deregistration() {
        let mut ctx = TermContext::new().unwrap();

        // Create and register test table
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();

        ctx.register_table_provider("test", Arc::new(table))
            .await
            .unwrap();

        assert!(ctx.has_table("test"));

        // Deregister
        ctx.deregister_table("test").unwrap();
        assert!(!ctx.has_table("test"));
        assert!(ctx.registered_tables().is_empty());
    }

    #[tokio::test]
    async fn test_clear_tables() {
        let mut ctx = TermContext::new().unwrap();

        // Register multiple tables
        for i in 0..3 {
            let name = format!("table{i}");
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![i]))])
                    .unwrap();
            let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();

            ctx.register_table_provider(&name, Arc::new(table))
                .await
                .unwrap();
        }

        assert_eq!(ctx.registered_tables().len(), 3);

        // Clear all
        ctx.clear_tables().unwrap();
        assert!(ctx.registered_tables().is_empty());
    }
}
