//! Data source connectors for Term validation library.
//!
//! This module provides implementations for various data sources including
//! file formats (CSV, Parquet, JSON) with support for compression and glob patterns.

use crate::prelude::*;
use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::prelude::SessionContext;
use std::fmt::Debug;
use std::sync::Arc;

mod csv;
mod json;
mod parquet;

// #[cfg(feature = "database")]
// mod database;

#[cfg(feature = "cloud-storage")]
mod cloud;

pub use csv::{CsvOptions, CsvSource};
pub use json::{JsonOptions, JsonSource};
pub use parquet::{ParquetOptions, ParquetSource};

// #[cfg(feature = "database")]
// pub use database::{DatabaseConfig, DatabaseSource};

// #[cfg(all(feature = "database", feature = "postgres"))]
// pub use database::PostgresSource;

// #[cfg(all(feature = "database", feature = "mysql"))]
// pub use database::MySqlSource;

// #[cfg(all(feature = "database", feature = "sqlite"))]
// pub use database::SqliteSource;

#[cfg(feature = "cloud-storage")]
pub use cloud::{AzureConfig, GcsConfig, S3Config};

#[cfg(all(feature = "cloud-storage", feature = "s3"))]
pub use cloud::{S3Auth, S3Source};

#[cfg(all(feature = "cloud-storage", feature = "gcs"))]
pub use cloud::{GcsAuth, GcsSource};

#[cfg(all(feature = "cloud-storage", feature = "azure"))]
pub use cloud::{AzureAuth, AzureBlobSource};

/// A data source that can be registered with a DataFusion context.
///
/// This trait defines the interface for all data sources in the Term library.
/// Implementations should handle schema inference, compression detection, and
/// efficient data loading.
///
/// # Examples
///
/// ```rust,ignore
/// use term_guard::sources::{DataSource, CsvSource};
///
/// # async fn example() -> Result<()> {
/// let source = CsvSource::new("data/users.csv")?;
/// let ctx = SessionContext::new();
/// source.register(&ctx, "users").await?;
/// # Ok(())
/// # }
/// ```
#[async_trait]
pub trait DataSource: Debug + Send + Sync {
    /// Registers this data source with the given session context.
    ///
    /// This method should handle:
    /// - Schema inference if not explicitly provided
    /// - Compression detection and handling
    /// - Efficient data loading
    /// - Telemetry spans for data loading operations
    ///
    /// # Arguments
    ///
    /// * `ctx` - The DataFusion session context to register with
    /// * `table_name` - The name to register the table as
    /// * `telemetry` - Optional telemetry configuration for tracing data loading
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure
    async fn register(&self, ctx: &SessionContext, table_name: &str) -> Result<()> {
        self.register_with_telemetry(ctx, table_name, None).await
    }

    /// Registers this data source with telemetry support.
    ///
    /// This is the main implementation method that data sources should override.
    /// The default `register` method delegates to this with `None` telemetry.
    async fn register_with_telemetry(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        telemetry: Option<&Arc<TermTelemetry>>,
    ) -> Result<()>;

    /// Returns the schema of this data source if known.
    ///
    /// This may return `None` if schema inference hasn't been performed yet.
    fn schema(&self) -> Option<&Arc<Schema>>;

    /// Returns a human-readable description of this data source.
    fn description(&self) -> String;
}

/// Common compression formats supported by file sources.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    /// No compression
    None,
    /// Gzip compression
    Gzip,
    /// Zstandard compression
    Zstd,
    /// Bzip2 compression
    Bzip2,
    /// LZ4 compression
    Lz4,
    /// Snappy compression
    Snappy,
    /// Automatic detection based on file extension
    Auto,
}

impl CompressionType {
    /// Detects compression type from file path extension.
    pub fn from_path(path: &str) -> Self {
        let lower = path.to_lowercase();
        if lower.ends_with(".gz") || lower.ends_with(".gzip") {
            Self::Gzip
        } else if lower.ends_with(".zst") || lower.ends_with(".zstd") {
            Self::Zstd
        } else if lower.ends_with(".bz2") || lower.ends_with(".bzip2") {
            Self::Bzip2
        } else if lower.ends_with(".lz4") {
            Self::Lz4
        } else if lower.ends_with(".snappy") || lower.ends_with(".sz") {
            Self::Snappy
        } else {
            Self::None
        }
    }

    /// Returns the file extension for this compression type.
    pub fn extension(&self) -> &'static str {
        match self {
            Self::None => "",
            Self::Gzip => ".gz",
            Self::Zstd => ".zst",
            Self::Bzip2 => ".bz2",
            Self::Lz4 => ".lz4",
            Self::Snappy => ".snappy",
            Self::Auto => "",
        }
    }
}

/// Utility function to expand glob patterns into file paths.
pub(crate) async fn expand_globs(patterns: &[String]) -> Result<Vec<String>> {
    use glob::glob;

    let mut paths = Vec::new();
    for pattern in patterns {
        let matches = glob(pattern).map_err(|e| {
            TermError::Configuration(format!("Invalid glob pattern '{pattern}': {e}"))
        })?;

        for entry in matches {
            let path = entry
                .map_err(|e| TermError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

            if path.is_file() {
                if let Some(path_str) = path.to_str() {
                    paths.push(path_str.to_string());
                }
            }
        }
    }

    if paths.is_empty() {
        return Err(TermError::DataSource {
            source_type: "file".to_string(),
            message: "No files found matching glob patterns".to_string(),
            source: None,
        });
    }

    Ok(paths)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_detection() {
        assert_eq!(
            CompressionType::from_path("data.csv"),
            CompressionType::None
        );
        assert_eq!(
            CompressionType::from_path("data.csv.gz"),
            CompressionType::Gzip
        );
        assert_eq!(
            CompressionType::from_path("data.CSV.GZ"),
            CompressionType::Gzip
        );
        assert_eq!(
            CompressionType::from_path("data.csv.zst"),
            CompressionType::Zstd
        );
        assert_eq!(
            CompressionType::from_path("data.csv.bz2"),
            CompressionType::Bzip2
        );
        assert_eq!(
            CompressionType::from_path("data.csv.lz4"),
            CompressionType::Lz4
        );
        assert_eq!(
            CompressionType::from_path("data.csv.snappy"),
            CompressionType::Snappy
        );
    }

    #[test]
    fn test_compression_extension() {
        assert_eq!(CompressionType::None.extension(), "");
        assert_eq!(CompressionType::Gzip.extension(), ".gz");
        assert_eq!(CompressionType::Zstd.extension(), ".zst");
        assert_eq!(CompressionType::Bzip2.extension(), ".bz2");
        assert_eq!(CompressionType::Lz4.extension(), ".lz4");
        assert_eq!(CompressionType::Snappy.extension(), ".snappy");
    }
}
