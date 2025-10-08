#![deny(clippy::all)]

mod check;
mod constraints;
mod data_source;
mod sources;
mod types;
mod validation_suite;

pub use check::{Check, CheckBuilder};
pub use data_source::{DataSource, DataSourceBuilder};
pub use sources::datafusion::DataFusionContext;
pub use sources::file::{CsvOptions, FileDataSource, JsonOptions, ParquetOptions};
pub use sources::memory::MemoryDataSource;
pub use sources::{DataSourceType, UnifiedDataSource};
pub use types::{Level, ValidationReport, ValidationResult};
pub use validation_suite::{ValidationSuite, ValidationSuiteBuilder};

#[cfg(feature = "cloud-storage")]
pub use sources::cloud::{AzureOptions, CloudDataSource, GcsOptions, S3Options};
