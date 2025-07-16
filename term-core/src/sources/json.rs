//! JSON and NDJSON file source implementation.

use super::{CompressionType, DataSource};
use crate::prelude::*;
use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::*;
use std::sync::Arc;
use tracing::instrument;

/// Format type for JSON files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonFormatType {
    /// Line-delimited JSON (one JSON object per line)
    NdJson,
    /// Regular JSON (single JSON object or array)
    Json,
}

impl JsonFormatType {
    /// Detects JSON format from file extension.
    pub fn from_path(path: &str) -> Self {
        let lower = path.to_lowercase();
        // Remove compression extensions first
        let without_compression =
            if lower.ends_with(".gz") || lower.ends_with(".zst") || lower.ends_with(".bz2") {
                &lower[..lower.rfind('.').unwrap_or(lower.len())]
            } else {
                &lower
            };

        if without_compression.ends_with(".ndjson") || without_compression.ends_with(".jsonl") {
            Self::NdJson
        } else {
            Self::Json
        }
    }
}

/// Options for configuring JSON file reading.
#[derive(Debug, Clone)]
pub struct JsonOptions {
    /// JSON format type
    pub format: JsonFormatType,
    /// Schema to use (if None, will be inferred)
    pub schema: Option<Arc<Schema>>,
    /// Compression type (default: Auto)
    pub compression: CompressionType,
    /// Maximum records to read for schema inference
    pub schema_infer_max_records: usize,
}

impl Default for JsonOptions {
    fn default() -> Self {
        Self {
            format: JsonFormatType::NdJson,
            schema: None,
            compression: CompressionType::Auto,
            schema_infer_max_records: 1000,
        }
    }
}

/// A JSON/NDJSON file data source with schema inference and compression support.
///
/// # Examples
///
/// ```rust,ignore
/// use term_core::sources::{JsonSource, JsonOptions, JsonFormatType};
///
/// # async fn example() -> Result<()> {
/// // NDJSON file (auto-detected)
/// let source = JsonSource::new("data/events.ndjson")?;
///
/// // Regular JSON with custom options
/// let options = JsonOptions {
///     format: JsonFormatType::Json,
///     ..Default::default()
/// };
/// let source = JsonSource::with_options("data/config.json", options)?;
///
/// // Compressed NDJSON files with glob
/// let source = JsonSource::from_glob("logs/*.jsonl.gz").await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct JsonSource {
    paths: Vec<String>,
    options: JsonOptions,
    inferred_schema: Option<Arc<Schema>>,
}

impl JsonSource {
    /// Creates a new JSON source from a single file path.
    pub fn new(path: impl Into<String>) -> Result<Self> {
        let path_str = path.into();
        let format = JsonFormatType::from_path(&path_str);

        Ok(Self {
            paths: vec![path_str],
            options: JsonOptions {
                format,
                ..Default::default()
            },
            inferred_schema: None,
        })
    }

    /// Creates a new JSON source with custom options.
    pub fn with_options(path: impl Into<String>, options: JsonOptions) -> Result<Self> {
        Ok(Self {
            paths: vec![path.into()],
            options,
            inferred_schema: None,
        })
    }

    /// Creates a JSON source from multiple file paths.
    pub fn from_paths(paths: Vec<String>) -> Result<Self> {
        if paths.is_empty() {
            return Err(TermError::Configuration(
                "At least one path must be provided".to_string(),
            ));
        }

        // Auto-detect format from first file
        let format = JsonFormatType::from_path(&paths[0]);

        Ok(Self {
            paths,
            options: JsonOptions {
                format,
                ..Default::default()
            },
            inferred_schema: None,
        })
    }

    /// Creates a JSON source from a glob pattern.
    pub async fn from_glob(pattern: impl Into<String>) -> Result<Self> {
        let patterns = vec![pattern.into()];
        let paths = super::expand_globs(&patterns).await?;
        Self::from_paths(paths)
    }

    /// Creates a JSON source from multiple glob patterns.
    pub async fn from_globs(patterns: Vec<String>) -> Result<Self> {
        let paths = super::expand_globs(&patterns).await?;
        Self::from_paths(paths)
    }

    /// Sets custom options for this JSON source.
    pub fn with_custom_options(mut self, options: JsonOptions) -> Self {
        self.options = options;
        self
    }

    /// Infers schema from the JSON files.
    #[instrument(skip(self))]
    async fn infer_schema(&self) -> Result<Arc<Schema>> {
        if let Some(schema) = &self.options.schema {
            return Ok(schema.clone());
        }

        if let Some(schema) = &self.inferred_schema {
            return Ok(schema.clone());
        }

        // Create a temporary context for schema inference
        let ctx = SessionContext::new();

        // For JSON, infer schema by reading the first file
        let first_path = &self.paths[0];
        let schema = if self.options.format == JsonFormatType::NdJson {
            // For NDJSON, we need to handle different extensions
            if first_path.ends_with(".json") {
                let mut options = NdJsonReadOptions::default();
                options.schema_infer_max_records = self.options.schema_infer_max_records;
                let df = ctx.read_json(first_path, options).await?;
                df.schema().inner().clone()
            } else {
                // For .ndjson or .jsonl files, try to read as NDJSON with read_json
                let mut options = NdJsonReadOptions::default();
                options.schema_infer_max_records = self.options.schema_infer_max_records;

                // Try to read the file directly to infer schema
                match ctx.read_json(first_path, options).await {
                    Ok(df) => df.schema().inner().clone(),
                    Err(_) => {
                        // If that fails, fall back to creating a minimal schema
                        // This is a workaround for the permission issue
                        return Err(TermError::DataSource {
                            source_type: "JSON".to_string(),
                            message: "Unable to infer schema from NDJSON file. Please provide an explicit schema.".to_string(),
                            source: None,
                        });
                    }
                }
            }
        } else {
            // For regular JSON, we need a different approach
            // DataFusion doesn't have built-in regular JSON support
            // So we'll just return an error for now
            return Err(TermError::NotSupported(
                "Regular JSON format is not yet supported. Please use NDJSON format.".to_string(),
            ));
        };

        Ok(schema)
    }
}

#[async_trait]
impl DataSource for JsonSource {
    #[instrument(skip(self, ctx, telemetry), fields(table_name = %table_name, source_type = "json", file_count = self.paths.len()))]
    async fn register_with_telemetry(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        telemetry: Option<&Arc<TermTelemetry>>,
    ) -> Result<()> {
        // Create telemetry span for data source loading
        let mut _datasource_span = if let Some(tel) = telemetry {
            tel.start_datasource_span("json", table_name)
        } else {
            TermSpan::noop()
        };
        // Handle single vs multiple paths
        if self.paths.len() == 1 {
            let path = &self.paths[0];

            // For single NDJSON files with .json extension, use register_json
            if path.ends_with(".json") && self.options.format == JsonFormatType::NdJson {
                let mut options = NdJsonReadOptions::default();
                options.schema = self.options.schema.as_deref();
                options.schema_infer_max_records = self.options.schema_infer_max_records;

                ctx.register_json(table_name, path, options).await?;
            } else if path.ends_with(".ndjson") || path.ends_with(".jsonl") {
                // For .ndjson/.jsonl files, create a ListingTable with the specific file
                let table_path = ListingTableUrl::parse(path)?;

                // Use the actual file extension
                let extension = if path.ends_with(".ndjson") {
                    ".ndjson"
                } else {
                    ".jsonl"
                };

                let format = JsonFormat::default();
                let listing_options =
                    ListingOptions::new(Arc::new(format)).with_file_extension(extension);

                // Create config with schema if provided
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
            } else {
                // For regular JSON files
                return Err(TermError::NotSupported(
                    "Regular JSON format is not yet supported. Please use NDJSON format."
                        .to_string(),
                ));
            }
        } else {
            // Multiple files - register each file separately and create a union
            // First, infer or get the schema
            let schema = if let Some(schema) = &self.options.schema {
                schema.clone()
            } else {
                // Infer schema from the first file
                self.infer_schema().await?
            };

            // Register each file as a separate table
            let mut table_names = Vec::new();
            for (i, path) in self.paths.iter().enumerate() {
                let temp_table_name = format!("__{}_temp_{}", table_name, i);

                if path.ends_with(".json") && self.options.format == JsonFormatType::NdJson {
                    let mut options = NdJsonReadOptions::default();
                    options.schema = Some(&schema);
                    options.schema_infer_max_records = self.options.schema_infer_max_records;
                    ctx.register_json(&temp_table_name, path, options).await?;
                } else if path.ends_with(".ndjson") || path.ends_with(".jsonl") {
                    // For .ndjson/.jsonl files, create a ListingTable with the specific file
                    let table_path = ListingTableUrl::parse(path)?;

                    let extension = if path.ends_with(".ndjson") {
                        ".ndjson"
                    } else {
                        ".jsonl"
                    };

                    let format = JsonFormat::default();
                    let listing_options =
                        ListingOptions::new(Arc::new(format)).with_file_extension(extension);

                    let config = ListingTableConfig::new(table_path)
                        .with_listing_options(listing_options)
                        .with_schema(schema.clone());

                    let table = ListingTable::try_new(config)?;
                    ctx.register_table(&temp_table_name, Arc::new(table))?;
                } else {
                    return Err(TermError::NotSupported(
                        "Regular JSON format is not yet supported. Please use NDJSON format."
                            .to_string(),
                    ));
                }

                table_names.push(temp_table_name);
            }

            // Create a union of all the temporary tables
            if !table_names.is_empty() {
                let union_sql = table_names
                    .iter()
                    .map(|name| format!("SELECT * FROM {}", name))
                    .collect::<Vec<_>>()
                    .join(" UNION ALL ");

                let df = ctx.sql(&union_sql).await?;
                ctx.register_table(table_name, df.into_view())?;

                // Clean up temporary tables
                for temp_name in table_names {
                    ctx.deregister_table(&temp_name)?;
                }
            }
        }

        Ok(())
    }

    fn schema(&self) -> Option<&Arc<Schema>> {
        self.options
            .schema
            .as_ref()
            .or(self.inferred_schema.as_ref())
    }

    fn description(&self) -> String {
        let format_str = match self.options.format {
            JsonFormatType::NdJson => "NDJSON",
            JsonFormatType::Json => "JSON",
        };

        if self.paths.len() == 1 {
            format!("{} file: {}", format_str, self.paths[0])
        } else {
            format!("{} files: {} files", format_str, self.paths.len())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_test_ndjson() -> NamedTempFile {
        let mut file = NamedTempFile::with_suffix(".ndjson").unwrap();
        writeln!(file, r#"{{"id": 1, "name": "Alice", "age": 30}}"#).unwrap();
        writeln!(file, r#"{{"id": 2, "name": "Bob", "age": 25}}"#).unwrap();
        writeln!(file, r#"{{"id": 3, "name": "Charlie", "age": 35}}"#).unwrap();
        file.flush().unwrap();
        file
    }

    fn create_test_json() -> NamedTempFile {
        let mut file = NamedTempFile::with_suffix(".json").unwrap();
        writeln!(
            file,
            r#"[
            {{"id": 1, "name": "Alice", "age": 30}},
            {{"id": 2, "name": "Bob", "age": 25}},
            {{"id": 3, "name": "Charlie", "age": 35}}
        ]"#
        )
        .unwrap();
        file.flush().unwrap();
        file
    }

    #[test]
    fn test_format_detection() {
        assert_eq!(JsonFormatType::from_path("data.json"), JsonFormatType::Json);
        assert_eq!(
            JsonFormatType::from_path("data.ndjson"),
            JsonFormatType::NdJson
        );
        assert_eq!(
            JsonFormatType::from_path("data.jsonl"),
            JsonFormatType::NdJson
        );
        assert_eq!(
            JsonFormatType::from_path("data.json.gz"),
            JsonFormatType::Json
        );
        assert_eq!(
            JsonFormatType::from_path("data.ndjson.gz"),
            JsonFormatType::NdJson
        );
    }

    #[tokio::test]
    async fn test_json_source_single_file() {
        let file = create_test_ndjson();
        let source = JsonSource::new(file.path().to_str().unwrap()).unwrap();

        assert_eq!(source.paths.len(), 1);
        assert_eq!(source.options.format, JsonFormatType::NdJson);
        assert!(source.description().contains("NDJSON file"));
    }

    #[tokio::test]
    async fn test_json_source_with_options() {
        let file = create_test_json();
        let options = JsonOptions {
            format: JsonFormatType::Json,
            schema_infer_max_records: 500,
            ..Default::default()
        };

        let source = JsonSource::with_options(file.path().to_str().unwrap(), options).unwrap();
        assert_eq!(source.options.format, JsonFormatType::Json);
        assert_eq!(source.options.schema_infer_max_records, 500);
    }

    #[tokio::test]
    async fn test_json_source_multiple_files() {
        let file1 = create_test_ndjson();
        let file2 = create_test_ndjson();

        let paths = vec![
            file1.path().to_str().unwrap().to_string(),
            file2.path().to_str().unwrap().to_string(),
        ];

        let source = JsonSource::from_paths(paths).unwrap();
        assert_eq!(source.paths.len(), 2);
        assert!(source.description().contains("2 files"));
    }

    #[tokio::test]
    async fn test_ndjson_registration() {
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

        let file = create_test_ndjson();

        // Provide schema since JsonFormat may not support inference in DataFusion 48.0
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
        ]));

        let options = JsonOptions {
            schema: Some(schema),
            ..Default::default()
        };

        let source = JsonSource::with_options(file.path().to_str().unwrap(), options).unwrap();

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
