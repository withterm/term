//! Parquet file source implementation.

use super::DataSource;
use crate::prelude::*;
use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::*;
use std::sync::Arc;
use tracing::instrument;

/// Options for configuring Parquet file reading.
#[derive(Debug, Clone, Default)]
pub struct ParquetOptions {
    /// Schema to use (if None, will be read from file metadata)
    pub schema: Option<Arc<Schema>>,
    /// Whether to use pruning based on Parquet statistics
    pub enable_pruning: bool,
    /// Batch size for reading
    pub batch_size: usize,
    /// Maximum number of threads to use for reading
    pub max_threads: Option<usize>,
}

impl ParquetOptions {
    /// Creates default options with pruning enabled.
    pub fn new() -> Self {
        Self {
            schema: None,
            enable_pruning: true,
            batch_size: 8192,
            max_threads: None,
        }
    }
}

/// A Parquet file data source with metadata reading and efficient querying.
///
/// # Examples
///
/// ```rust,ignore
/// use term_core::sources::{ParquetSource, ParquetOptions};
///
/// # async fn example() -> Result<()> {
/// // Simple Parquet file
/// let source = ParquetSource::new("data/events.parquet")?;
///
/// // Parquet with custom options
/// let options = ParquetOptions {
///     enable_pruning: false,
///     batch_size: 16384,
///     ..Default::default()
/// };
/// let source = ParquetSource::with_options("data/events.parquet", options)?;
///
/// // Multiple files with glob pattern
/// let source = ParquetSource::from_glob("data/year=2023/*.parquet").await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ParquetSource {
    paths: Vec<String>,
    options: ParquetOptions,
    metadata_schema: Option<Arc<Schema>>,
}

impl ParquetSource {
    /// Creates a new Parquet source from a single file path.
    pub fn new(path: impl Into<String>) -> Result<Self> {
        Ok(Self {
            paths: vec![path.into()],
            options: ParquetOptions::new(),
            metadata_schema: None,
        })
    }

    /// Creates a new Parquet source with custom options.
    pub fn with_options(path: impl Into<String>, options: ParquetOptions) -> Result<Self> {
        Ok(Self {
            paths: vec![path.into()],
            options,
            metadata_schema: None,
        })
    }

    /// Creates a Parquet source from multiple file paths.
    pub fn from_paths(paths: Vec<String>) -> Result<Self> {
        if paths.is_empty() {
            return Err(TermError::Configuration(
                "At least one path must be provided".to_string(),
            ));
        }
        Ok(Self {
            paths,
            options: ParquetOptions::new(),
            metadata_schema: None,
        })
    }

    /// Creates a Parquet source from a glob pattern.
    pub async fn from_glob(pattern: impl Into<String>) -> Result<Self> {
        let patterns = vec![pattern.into()];
        let paths = super::expand_globs(&patterns).await?;
        Self::from_paths(paths)
    }

    /// Creates a Parquet source from multiple glob patterns.
    pub async fn from_globs(patterns: Vec<String>) -> Result<Self> {
        let paths = super::expand_globs(&patterns).await?;
        Self::from_paths(paths)
    }

    /// Sets custom options for this Parquet source.
    pub fn with_custom_options(mut self, options: ParquetOptions) -> Self {
        self.options = options;
        self
    }

    /// Reads schema from Parquet file metadata.
    #[instrument(skip(self))]
    #[allow(dead_code)]
    async fn read_metadata_schema(&mut self) -> Result<Arc<Schema>> {
        if let Some(schema) = &self.options.schema {
            return Ok(schema.clone());
        }

        if let Some(schema) = &self.metadata_schema {
            return Ok(schema.clone());
        }

        // Create a temporary context for schema inference
        let ctx = SessionContext::new();

        // For Parquet, we can read schema directly from file metadata
        let first_path = &self.paths[0];
        let options = ParquetReadOptions::default();
        let df = ctx.read_parquet(first_path, options).await?;
        let schema = df.schema().inner().clone();

        self.metadata_schema = Some(schema.clone());
        Ok(schema)
    }
}

#[async_trait]
impl DataSource for ParquetSource {
    #[instrument(skip(self, ctx, telemetry), fields(table_name = %table_name, source_type = "parquet", file_count = self.paths.len()))]
    async fn register_with_telemetry(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        telemetry: Option<&Arc<TermTelemetry>>,
    ) -> Result<()> {
        // Create telemetry span for data source loading
        let mut _datasource_span = if let Some(tel) = telemetry {
            tel.start_datasource_span("parquet", table_name)
        } else {
            TermSpan::noop()
        };
        // Handle multiple paths
        if self.paths.len() == 1 {
            // Single file - use register_parquet for simplicity
            let mut options = ParquetReadOptions::default();
            if let Some(schema) = &self.options.schema {
                options = options.schema(schema);
            }

            ctx.register_parquet(table_name, &self.paths[0], options)
                .await?;
        } else {
            // Multiple files - use ListingTable
            // For multiple files, we need to use the directory path, not a specific file
            let first_path = std::path::Path::new(&self.paths[0]);
            let dir_path = first_path
                .parent()
                .ok_or_else(|| TermError::Configuration("Invalid file path".to_string()))?;
            let dir_path_str = dir_path.to_str().ok_or_else(|| {
                TermError::Configuration("Path contains invalid UTF-8".to_string())
            })?;
            let table_path = ListingTableUrl::parse(dir_path_str)?;

            let format = ParquetFormat::new().with_enable_pruning(self.options.enable_pruning);

            let listing_options =
                ListingOptions::new(Arc::new(format)).with_file_extension(".parquet");

            // Infer schema if not provided
            let schema = if let Some(schema) = &self.options.schema {
                schema.clone()
            } else {
                // Infer schema using a mutable clone
                let mut source_clone = self.clone();
                source_clone.read_metadata_schema().await?
            };

            let config = ListingTableConfig::new(table_path)
                .with_listing_options(listing_options)
                .with_schema(schema);

            let table = ListingTable::try_new(config)?;
            ctx.register_table(table_name, Arc::new(table))?;
        }

        Ok(())
    }

    fn schema(&self) -> Option<&Arc<Schema>> {
        self.options
            .schema
            .as_ref()
            .or(self.metadata_schema.as_ref())
    }

    fn description(&self) -> String {
        if self.paths.len() == 1 {
            let path = &self.paths[0];
            format!("Parquet file: {path}")
        } else {
            let count = self.paths.len();
            format!("Parquet files: {count} files")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::ArrowWriter;
    use std::fs::File;
    use tempfile::NamedTempFile;

    fn create_test_parquet() -> NamedTempFile {
        let file = NamedTempFile::with_suffix(".parquet").unwrap();

        // Create schema
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        // Create data
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        )
        .unwrap();

        // Write to Parquet
        let props = Default::default();
        let file_handle = File::create(file.path()).unwrap();
        let mut writer = ArrowWriter::try_new(file_handle, schema, props).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        file
    }

    #[tokio::test]
    async fn test_parquet_source_single_file() {
        let file = create_test_parquet();
        let source = ParquetSource::new(file.path().to_str().unwrap()).unwrap();

        assert_eq!(source.paths.len(), 1);
        assert!(source.description().contains("Parquet file"));
    }

    #[tokio::test]
    async fn test_parquet_source_with_options() {
        let file = create_test_parquet();
        let options = ParquetOptions {
            enable_pruning: false,
            batch_size: 16384,
            ..Default::default()
        };

        let source = ParquetSource::with_options(file.path().to_str().unwrap(), options).unwrap();
        assert!(!source.options.enable_pruning);
        assert_eq!(source.options.batch_size, 16384);
    }

    #[tokio::test]
    async fn test_parquet_source_multiple_files() {
        let file1 = create_test_parquet();
        let file2 = create_test_parquet();

        let paths = vec![
            file1.path().to_str().unwrap().to_string(),
            file2.path().to_str().unwrap().to_string(),
        ];

        let source = ParquetSource::from_paths(paths).unwrap();
        assert_eq!(source.paths.len(), 2);
        assert!(source.description().contains("2 files"));
    }

    #[tokio::test]
    async fn test_parquet_metadata_reading() {
        let file = create_test_parquet();
        let mut source = ParquetSource::new(file.path().to_str().unwrap()).unwrap();

        let schema = source.read_metadata_schema().await.unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }

    #[tokio::test]
    async fn test_parquet_registration() {
        let file = create_test_parquet();
        let source = ParquetSource::new(file.path().to_str().unwrap()).unwrap();

        let ctx = SessionContext::new();
        source.register(&ctx, "test_table").await.unwrap();

        // Verify table is registered
        let df = ctx
            .sql("SELECT COUNT(*) as count FROM test_table")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert!(!batches.is_empty());
    }
}
