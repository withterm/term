//! # Term - Data Validation for Rust
//!
//! Term is a powerful data validation library inspired by AWS Deequ, providing
//! comprehensive data quality checks without requiring Apache Spark. It leverages
//! DataFusion for efficient query execution and includes built-in observability
//! through OpenTelemetry.
//!
//! ## Overview
//!
//! Term enables you to define and run data quality validations on your datasets,
//! helping you ensure data correctness, completeness, and consistency. Whether
//! you're validating data in ETL pipelines, ensuring data quality in analytics
//! workflows, or monitoring data drift in production, Term provides the tools
//! you need.
//!
//! ## Quick Start
//!
//! ```rust
//! use term_guard::prelude::*;
//! use term_guard::core::{ValidationSuite, Check, Level, ConstraintStatus, builder_extensions::CompletenessOptions};
//! use term_guard::constraints::Assertion;
//! use datafusion::prelude::*;
//!
//! # async fn example() -> std::result::Result<(), Box<dyn std::error::Error>> {
//! // Create a validation suite
//! let suite = ValidationSuite::builder("user_data_validation")
//!     .check(
//!         Check::builder("critical_checks")
//!             .level(Level::Error)
//!             .completeness("user_id", CompletenessOptions::full().into_constraint_options())     // No nulls allowed
//!             .validates_uniqueness(vec!["user_id"], 1.0) // Must be unique
//!             .completeness("email", CompletenessOptions::threshold(0.95).into_constraint_options())      // 95% non-null
//!             .build()
//!     )
//!     .check(
//!         Check::builder("data_quality")
//!             .level(Level::Warning)
//!             .validates_regex("email", r"^[^@]+@[^@]+$", 0.98)
//!             .statistic("age", term_guard::constraints::StatisticType::Min, Assertion::GreaterThanOrEqual(0.0))
//!             .statistic("age", term_guard::constraints::StatisticType::Max, Assertion::LessThanOrEqual(120.0))
//!             .build()
//!     )
//!     .build();
//!
//! // Create a DataFusion context with your data
//! let ctx = SessionContext::new();
//! // ... register your data tables ...
//!
//! // Run validation
//! let results = suite.run(&ctx).await?;
//!
//! // Check results
//! match &results {
//!     term_guard::core::ValidationResult::Success { report, .. } => {
//!         println!("Validation succeeded!");
//!         println!("Total checks: {}", report.metrics.total_checks);
//!     }
//!     term_guard::core::ValidationResult::Failure { report } => {
//!         println!("Validation failed!");
//!         for issue in &report.issues {
//!             println!("{}: {}", issue.check_name, issue.message);
//!         }
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Key Features
//!
//! ### Comprehensive Validation Constraints
//!
//! - **Completeness**: Check for null values and missing data
//! - **Uniqueness**: Ensure values are unique (single or multi-column)
//! - **Patterns**: Validate data against regex patterns
//! - **Statistics**: Min, max, mean, sum, standard deviation checks
//! - **Data Types**: Ensure consistent data types
//! - **Custom SQL**: Define complex validation logic with SQL expressions
//!
//! ### Performance Optimization
//!
//! Term includes a query optimizer that dramatically improves performance:
//!
//! ```rust,no_run
//! use term_guard::core::ValidationSuite;
//!
//! let suite = ValidationSuite::builder("optimized_validation")
//!     .with_optimizer(true)  // Enable query optimization
//!     // .check(/* your checks */)
//!     .build();
//! ```
//!
//! The optimizer combines multiple constraints into single queries when possible,
//! reducing table scans and improving performance by up to 15x for suites with
//! many constraints.
//!
//! ### Multiple Data Sources
//!
//! Term supports various data sources through the `sources` module:
//!
//! - CSV files
//! - Parquet files
//! - JSON files
//! - PostgreSQL databases
//! - Cloud storage (S3, Azure Blob, Google Cloud Storage)
//!
//! ### Observability
//!
//! Built-in OpenTelemetry integration provides:
//!
//! - Distributed tracing for validation runs
//! - Metrics for constraint evaluation performance
//! - Structured logging with the `tracing` crate
//!
//! ```rust,ignore
//! use term_guard::telemetry::TermTelemetry;
//! use opentelemetry::trace::Tracer;
//!
//! // User configures their own tracer
//! let tracer = opentelemetry_jaeger::new_agent_pipeline()
//!     .with_service_name("data-validation")
//!     .install_simple()?;
//!     
//! let telemetry = TermTelemetry::new(tracer);
//! ```
//!
//! ## Architecture
//!
//! Term is built on a modular architecture:
//!
//! - **`analyzers`**: Advanced data analysis framework including:
//!   - Type Inference Engine: Automatic data type detection with confidence scores
//!   - Column Profiler: Three-pass algorithm for comprehensive column analysis
//!   - Basic & Advanced Analyzers: Metrics computation (mean, entropy, correlation, etc.)
//! - **`core`**: Core types like `Check`, `ValidationSuite`, and `ConstraintResult`
//! - **`constraints`**: All validation constraint implementations
//! - **`sources`**: Data source connectors and loaders
//! - **`optimizer`**: Query optimization engine
//! - **`telemetry`**: OpenTelemetry integration
//! - **`formatters`**: Result formatting utilities
//!
//! ## Examples
//!
//! See the `examples` directory for complete examples:
//!
//! - `basic_validation.rs`: Simple validation example
//! - `tpc_h_validation.rs`: TPC-H benchmark data validation
//! - `cloud_storage_example.rs`: Validating data in cloud storage
//! - `deequ_migration.rs`: Migrating from Deequ to Term
//!
//! ## Migration from Deequ
//!
//! Term provides similar APIs to Deequ, making migration straightforward:
//!
//! ```rust
//! use term_guard::core::{Check, builder_extensions::CompletenessOptions};
//! use term_guard::constraints::Assertion;
//!
//! // Deequ-style checks in Term
//! let check = Check::builder("data_quality")
//!     .has_size(Assertion::GreaterThan(1000.0))
//!     .completeness("id", CompletenessOptions::full().into_constraint_options())
//!     .completeness("name", CompletenessOptions::threshold(0.98).into_constraint_options())
//!     .validates_uniqueness(vec!["id"], 1.0)
//!     .build();
//! ```

pub mod analyzers;
pub mod constraints;
pub mod core;
pub mod error;
pub mod formatters;
pub mod logging;
pub mod optimizer;
pub mod prelude;
pub mod security;
pub mod sources;
pub mod telemetry;

#[cfg(test)]
pub mod test_helpers;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_fixtures;
