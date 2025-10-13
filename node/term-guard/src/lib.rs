#![deny(clippy::all)]

mod check;
mod data_source;
mod types;
mod validation_suite;

use napi::bindgen_prelude::*;
use napi_derive::napi;

// Re-export the main types for the NAPI interface
pub use check::{Check, CheckBuilder};
pub use data_source::{DataSource, DataSourceBuilder};
pub use types::{
    ConstraintStatus, Level, PerformanceMetrics, ValidationIssue, ValidationReport,
    ValidationResult,
};
pub use validation_suite::{ValidationSuite, ValidationSuiteBuilder};

#[napi]
pub fn hello_term() -> String {
    "Hello from Term Guard! Data validation powered by Rust.".to_string()
}

#[napi]
pub fn get_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

#[napi(object)]
pub struct ValidationInfo {
    pub name: String,
    pub version: String,
    pub rust_version: String,
}

#[napi]
pub fn get_info() -> ValidationInfo {
    ValidationInfo {
        name: "term-guard".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        rust_version: "1.70+".to_string(),
    }
}

/// Example usage function demonstrating the full API
#[napi]
pub async fn validate_sample_data(path: String) -> Result<String> {
    // Create a data source from a CSV file
    let data_source = DataSource::from_csv(path).await?;

    // Create some checks
    let mut builder = CheckBuilder::new("completeness_check".to_string());
    builder.description("Check for data completeness".to_string());
    let completeness_check = builder.is_complete("column1".to_string(), Some(0.95))?;

    // Build a validation suite  
    let mut suite_builder = ValidationSuiteBuilder::new("sample_suite".to_string());
    suite_builder.description("Sample validation suite".to_string());
    suite_builder.add_check(&completeness_check);
    let suite = suite_builder.build()?;

    // Run the validation
    let result = suite.run(&data_source).await?;

    Ok(format!(
        "Validation {}: {} checks passed, {} failed",
        result.status, result.report.passed_checks, result.report.failed_checks
    ))
}
