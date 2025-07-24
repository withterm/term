//! CSV file source implementation.

use super::{CompressionType, DataSource};
use crate::prelude::*;
use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::*;
use std::sync::Arc;
use tracing::{debug, info, instrument};

/// Options for configuring CSV file reading.
#[derive(Debug, Clone)]
pub struct CsvOptions {
    /// Whether the CSV file has a header row
    pub has_header: bool,
    /// Field delimiter (default: ',')
    pub delimiter: u8,
    /// Quote character (default: '"')
    pub quote: u8,
    /// Escape character (default: None)
    pub escape: Option<u8>,
    /// Comment prefix (lines starting with this are ignored)
    pub comment: Option<u8>,
    /// Schema to use (if None, will be inferred)
    pub schema: Option<Arc<Schema>>,
    /// Compression type (default: Auto)
    pub compression: CompressionType,
    /// Maximum records to read for schema inference
    pub schema_infer_max_records: usize,
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            has_header: true,
            delimiter: b',',
            quote: b'"',
            escape: None,
            comment: None,
            schema: None,
            compression: CompressionType::Auto,
            schema_infer_max_records: 1000,
        }
    }
}

/// A CSV file data source with schema inference and compression support.
///
/// # Examples
///
/// ```rust,ignore
/// use term_core::sources::{CsvSource, CsvOptions};
///
/// # async fn example() -> Result<()> {
/// // Simple CSV file
/// let source = CsvSource::new("data/users.csv")?;
///
/// // CSV with custom options
/// let options = CsvOptions {
///     delimiter: b'\t',
///     has_header: false,
///     ..Default::default()
/// };
/// let source = CsvSource::with_options("data/users.tsv", options)?;
///
/// // Multiple files with glob pattern
/// let source = CsvSource::from_glob("data/*.csv")?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct CsvSource {
    paths: Vec<String>,
    options: CsvOptions,
    inferred_schema: Option<Arc<Schema>>,
}

impl CsvSource {
    /// Creates a new CSV source from a single file path.
    pub fn new(path: impl Into<String>) -> Result<Self> {
        Ok(Self {
            paths: vec![path.into()],
            options: CsvOptions::default(),
            inferred_schema: None,
        })
    }

    /// Creates a new CSV source with custom options.
    pub fn with_options(path: impl Into<String>, options: CsvOptions) -> Result<Self> {
        Ok(Self {
            paths: vec![path.into()],
            options,
            inferred_schema: None,
        })
    }

    /// Creates a CSV source from multiple file paths.
    pub fn from_paths(paths: Vec<String>) -> Result<Self> {
        if paths.is_empty() {
            return Err(TermError::Configuration(
                "At least one path must be provided".to_string(),
            ));
        }
        Ok(Self {
            paths,
            options: CsvOptions::default(),
            inferred_schema: None,
        })
    }

    /// Creates a CSV source from a glob pattern.
    pub async fn from_glob(pattern: impl Into<String>) -> Result<Self> {
        let patterns = vec![pattern.into()];
        let paths = super::expand_globs(&patterns).await?;
        Self::from_paths(paths)
    }

    /// Creates a CSV source from multiple glob patterns.
    pub async fn from_globs(patterns: Vec<String>) -> Result<Self> {
        let paths = super::expand_globs(&patterns).await?;
        Self::from_paths(paths)
    }

    /// Sets custom options for this CSV source.
    pub fn with_custom_options(mut self, options: CsvOptions) -> Self {
        self.options = options;
        self
    }

    /// Infers schema from the CSV files.
    #[instrument(skip(self))]
    #[allow(dead_code)]
    async fn infer_schema(&mut self) -> Result<Arc<Schema>> {
        if let Some(schema) = &self.options.schema {
            return Ok(schema.clone());
        }

        if let Some(schema) = &self.inferred_schema {
            return Ok(schema.clone());
        }

        // Use DataFusion's CSV format for schema inference
        let _format = CsvFormat::default()
            .with_has_header(self.options.has_header)
            .with_delimiter(self.options.delimiter)
            .with_quote(self.options.quote)
            .with_escape(self.options.escape)
            .with_comment(self.options.comment)
            .with_schema_infer_max_rec(self.options.schema_infer_max_records);

        // Create a temporary context for schema inference
        let ctx = SessionContext::new();

        // For schema inference, we can use the first file path
        let first_path = &self.paths[0];
        let schema = if first_path.ends_with(".csv") {
            // For .csv files, use read_csv
            let csv_options = CsvReadOptions::new()
                .has_header(self.options.has_header)
                .delimiter(self.options.delimiter)
                .schema_infer_max_records(self.options.schema_infer_max_records);

            let df = ctx.read_csv(first_path, csv_options).await?;
            df.schema().inner().clone()
        } else {
            // For non-.csv files (e.g., .tsv), use ListingTable
            let first_path_obj = std::path::Path::new(first_path);
            let dir_path = first_path_obj
                .parent()
                .ok_or_else(|| TermError::Configuration("Invalid file path".to_string()))?;
            let dir_path_str = dir_path.to_str().ok_or_else(|| {
                TermError::Configuration("Path contains invalid UTF-8".to_string())
            })?;
            let table_path = ListingTableUrl::parse(dir_path_str)?;

            let extension = if first_path.ends_with(".tsv") {
                ".tsv"
            } else if first_path.ends_with(".txt") {
                ".txt"
            } else {
                ".csv"
            };

            let format = CsvFormat::default()
                .with_has_header(self.options.has_header)
                .with_delimiter(self.options.delimiter)
                .with_quote(self.options.quote)
                .with_escape(self.options.escape)
                .with_comment(self.options.comment)
                .with_schema_infer_max_rec(self.options.schema_infer_max_records);

            let listing_options =
                ListingOptions::new(Arc::new(format)).with_file_extension(extension);

            let config = ListingTableConfig::new(table_path).with_listing_options(listing_options);
            let table = ListingTable::try_new(config)?;

            // Register temporarily to infer schema
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| TermError::Internal(format!("Failed to get system time: {e}")))?
                .as_nanos();
            let temp_table_name = format!("_temp_schema_inference_{timestamp}");
            ctx.register_table(&temp_table_name, Arc::new(table))?;

            let df = ctx.table(&temp_table_name).await?;
            let schema = df.schema().inner().clone();

            // Deregister the temporary table
            ctx.deregister_table(&temp_table_name)?;

            schema
        };

        self.inferred_schema = Some(schema.clone());
        Ok(schema)
    }
}

#[async_trait]
impl DataSource for CsvSource {
    #[instrument(skip(self, ctx, telemetry), fields(
        table.name = %table_name,
        source.type = "csv",
        source.files = self.paths.len(),
        csv.delimiter = %self.options.delimiter as char,
        csv.has_header = self.options.has_header
    ))]
    async fn register_with_telemetry(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        telemetry: Option<&Arc<TermTelemetry>>,
    ) -> Result<()> {
        info!(
            table.name = %table_name,
            source.type = "csv",
            source.paths = ?self.paths,
            csv.delimiter = %self.options.delimiter as char,
            csv.has_header = self.options.has_header,
            csv.compression = ?self.options.compression,
            "Registering CSV data source"
        );

        // Create telemetry span for data source loading
        let mut _datasource_span = if let Some(tel) = telemetry {
            tel.start_datasource_span("csv", table_name)
        } else {
            TermSpan::noop()
        };
        // Create CSV format configuration
        let mut format = CsvFormat::default()
            .with_has_header(self.options.has_header)
            .with_delimiter(self.options.delimiter)
            .with_quote(self.options.quote);

        if let Some(escape) = self.options.escape {
            format = format.with_escape(Some(escape));
        }
        if let Some(comment) = self.options.comment {
            format = format.with_comment(Some(comment));
        }

        // Handle single vs multiple paths
        if self.paths.len() == 1 {
            let path = &self.paths[0];

            // For single files ending with .csv, use register_csv for simplicity
            if path.ends_with(".csv") {
                let mut csv_options = CsvReadOptions::new()
                    .has_header(self.options.has_header)
                    .delimiter(self.options.delimiter)
                    .quote(self.options.quote)
                    .schema_infer_max_records(self.options.schema_infer_max_records);

                if let Some(escape) = self.options.escape {
                    csv_options = csv_options.escape(escape);
                }
                if let Some(comment) = self.options.comment {
                    csv_options = csv_options.comment(comment);
                }
                if let Some(schema) = &self.options.schema {
                    csv_options = csv_options.schema(schema);
                }

                ctx.register_csv(table_name, path, csv_options).await?;
            } else {
                // Single non-.csv file (like .tsv) - use ListingTable with specific file
                let table_path = ListingTableUrl::parse(path)?;

                // Determine the file extension
                let extension = if path.ends_with(".tsv") {
                    ".tsv".to_string()
                } else if path.ends_with(".txt") {
                    ".txt".to_string()
                } else {
                    // Get the extension from the path
                    std::path::Path::new(path)
                        .extension()
                        .and_then(|ext| ext.to_str())
                        .map(|ext| format!(".{ext}"))
                        .unwrap_or_else(|| ".csv".to_string())
                };

                let listing_options =
                    ListingOptions::new(Arc::new(format)).with_file_extension(&extension);

                // Infer schema if not provided
                let config = if let Some(schema) = &self.options.schema {
                    ListingTableConfig::new(table_path)
                        .with_listing_options(listing_options)
                        .with_schema(schema.clone())
                } else {
                    ListingTableConfig::new(table_path)
                        .with_listing_options(listing_options)
                        .infer_schema(&ctx.state())
                        .await?
                };

                let table = ListingTable::try_new(config)?;
                ctx.register_table(table_name, Arc::new(table))?;
            }
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

            // Determine the file extension from the actual files
            let extension = if self.paths[0].ends_with(".tsv") {
                ".tsv"
            } else if self.paths[0].ends_with(".txt") {
                ".txt"
            } else {
                ".csv"
            };

            let listing_options =
                ListingOptions::new(Arc::new(format)).with_file_extension(extension);

            // Infer schema if not provided
            let schema = if let Some(schema) = &self.options.schema {
                schema.clone()
            } else {
                // Infer schema using a mutable clone
                let mut source_clone = self.clone();
                source_clone.infer_schema().await?
            };

            let config = ListingTableConfig::new(table_path)
                .with_listing_options(listing_options)
                .with_schema(schema);

            let table = ListingTable::try_new(config)?;
            ctx.register_table(table_name, Arc::new(table))?;
        }

        debug!(
            table.name = %table_name,
            source.type = "csv",
            source.files = self.paths.len(),
            "CSV data source registered successfully"
        );

        Ok(())
    }

    fn schema(&self) -> Option<&Arc<Schema>> {
        self.options
            .schema
            .as_ref()
            .or(self.inferred_schema.as_ref())
    }

    fn description(&self) -> String {
        if self.paths.len() == 1 {
            let path = &self.paths[0];
            format!("CSV file: {path}")
        } else {
            let count = self.paths.len();
            format!("CSV files: {count} files")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    async fn create_test_csv() -> NamedTempFile {
        let mut file = NamedTempFile::with_suffix(".csv").unwrap();
        writeln!(file, "id,name,age").unwrap();
        writeln!(file, "1,Alice,30").unwrap();
        writeln!(file, "2,Bob,25").unwrap();
        writeln!(file, "3,Charlie,35").unwrap();
        file.flush().unwrap();
        file
    }

    #[tokio::test]
    async fn test_csv_source_single_file() {
        let file = create_test_csv().await;
        let source = CsvSource::new(file.path().to_str().unwrap()).unwrap();

        assert_eq!(source.paths.len(), 1);
        assert!(source.description().contains("CSV file"));
    }

    #[tokio::test]
    async fn test_csv_source_with_options() {
        let file = create_test_csv().await;
        let options = CsvOptions {
            delimiter: b'\t',
            has_header: false,
            ..Default::default()
        };

        let source = CsvSource::with_options(file.path().to_str().unwrap(), options).unwrap();
        assert_eq!(source.options.delimiter, b'\t');
        assert!(!source.options.has_header);
    }

    #[tokio::test]
    async fn test_csv_source_multiple_files() {
        let file1 = create_test_csv().await;
        let file2 = create_test_csv().await;

        let paths = vec![
            file1.path().to_str().unwrap().to_string(),
            file2.path().to_str().unwrap().to_string(),
        ];

        let source = CsvSource::from_paths(paths).unwrap();
        assert_eq!(source.paths.len(), 2);
        assert!(source.description().contains("2 files"));
    }

    #[tokio::test]
    async fn test_csv_source_empty_paths() {
        let result = CsvSource::from_paths(vec![]);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_csv_registration() {
        let file = create_test_csv().await;
        let source = CsvSource::new(file.path().to_str().unwrap()).unwrap();

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
