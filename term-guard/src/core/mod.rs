//! Core validation types for the Term data quality library.
//!
//! This module provides the fundamental types for defining and executing
//! data validation suites, including checks, constraints, and results.
//!
//! ## Overview
//!
//! The core module contains the essential building blocks for data validation:
//!
//! - **[`ValidationSuite`]**: A collection of checks to run against your data
//! - **[`Check`]**: A named group of related constraints with a severity level
//! - **[`Constraint`]**: Individual validation rules (implemented in the `constraints` module)
//! - **[`Level`]**: Severity levels for checks (Error, Warning, Info)
//! - **[`ValidationResult`]**: Results from running a validation suite
//!
//! ## Architecture
//!
//! ```text
//! ValidationSuite
//!     ├── Check (Level: Error)
//!     │   ├── Constraint 1
//!     │   └── Constraint 2
//!     └── Check (Level: Warning)
//!         ├── Constraint 3
//!         └── Constraint 4
//! ```
//!
//! ## Example
//!
//! ```rust
//! use term_guard::core::{ValidationSuite, Check, Level, ValidationResult};
//! use term_guard::core::builder_extensions::{CompletenessOptions, StatisticalOptions};
//! use term_guard::constraints::{Assertion, FormatType, FormatOptions};
//! use datafusion::prelude::*;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Build a validation suite using the unified API
//! let suite = ValidationSuite::builder("customer_validation")
//!     .description("Validate customer data quality")
//!     .check(
//!         Check::builder("critical_fields")
//!             .level(Level::Error)
//!             .description("Critical fields must be valid")
//!             // Unified API for completeness
//!             .completeness("customer_id", CompletenessOptions::full().into_constraint_options())
//!             .completeness("email", CompletenessOptions::threshold(0.99).into_constraint_options())
//!             // Convenience method for primary key validation
//!             .primary_key(vec!["customer_id"])
//!             .build()
//!     )
//!     .check(
//!         Check::builder("data_quality")
//!             .level(Level::Warning)
//!             // Format validation API
//!             .has_format("email", FormatType::Email, 0.95, FormatOptions::default())
//!             // Combined statistics in one query
//!             .statistics(
//!                 "age",
//!                 StatisticalOptions::new()
//!                     .min(Assertion::GreaterThanOrEqual(18.0))
//!                     .max(Assertion::LessThan(120.0))
//!             )?
//!             .build()
//!     )
//!     .build();
//!
//! // Create context and register data
//! let ctx = SessionContext::new();
//! // ... register your customer table ...
//!
//! // Run validation
//! let results = suite.run(&ctx).await?;
//!
//! // Process results
//! match results {
//!     ValidationResult::Success { report, .. } => {
//!         println!("All validations passed!");
//!         for issue in &report.issues {
//!             if issue.level == Level::Warning {
//!                 println!("Warning: {}", issue.message);
//!             }
//!         }
//!     }
//!     ValidationResult::Failure { report } => {
//!         println!("Validation failures:");
//!         for issue in &report.issues {
//!             if issue.level == Level::Error {
//!                 println!("Error in '{}': {}", issue.check_name, issue.message);
//!             }
//!         }
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Constraint Status
//!
//! Each constraint evaluation returns a status:
//!
//! - **Success**: The constraint passed
//! - **Failure**: The constraint failed
//! - **Skipped**: The constraint was skipped (e.g., no data)
//!
//! ## Performance Considerations
//!
//! - Constraints within a check may be optimized to run in a single query
//! - Use the `with_optimizer(true)` option on ValidationSuite for best performance
//! - Group related constraints in the same Check when possible

mod check;
mod constraint;
mod context;
mod level;
mod logical;
mod multi_source;
mod result;
mod suite;
mod unified;
pub mod validation_context;

pub mod builder_extensions;

pub use check::{Check, CheckBuilder};
pub use constraint::{Constraint, ConstraintMetadata, ConstraintResult, ConstraintStatus};
pub use context::{TermContext, TermContextConfig};
pub use level::Level;
pub use logical::{ColumnSpec, ConstraintOptionsBuilder, LogicalOperator, LogicalResult};
pub use multi_source::{CacheStats, MultiSourceValidator};
pub use result::{ValidationIssue, ValidationMetrics, ValidationReport, ValidationResult};
pub use suite::{ValidationSuite, ValidationSuiteBuilder};
pub use unified::{ConstraintOptions, UnifiedCompletenessBase, UnifiedConstraint};
pub use validation_context::{current_validation_context, ValidationContext};
