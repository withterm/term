//! Built-in constraint implementations for data validation.
//!
//! This module provides a comprehensive collection of constraints for validating
//! data quality across various dimensions. Each constraint implements the
//! [`Constraint`](crate::core::Constraint) trait and can be used within a
//! [`Check`](crate::core::Check).
//!
//! ## Unified API Overview
//!
//! Term uses a **unified constraint system** that consolidates similar validation
//! types into powerful, flexible constraints. This approach:
//!
//! - **Reduces API surface** while increasing functionality
//! - **Improves performance** through query optimization
//! - **Provides consistency** across different validation types
//!
//! ### Core Unified Constraints
//!
//! 1. **[`CompletenessConstraint`]** - Handles all completeness validations
//!    - Single column: `complete()`, `with_threshold()`
//!    - Multiple columns: `with_operator()` for AND/OR logic
//!    - Complex patterns: `with_options()` for advanced scenarios
//!
//! 2. **[`UniquenessConstraint`]** - Covers uniqueness-related checks
//!    - Full uniqueness: `full_uniqueness()`
//!    - Uniqueness ratio: `unique_value_ratio()`
//!    - Distinctness: `distinctness()`
//!    - Primary key: `primary_key()`
//!
//! 3. **[`StatisticalConstraint`]** - All statistical validations
//!    - Basic stats: `min()`, `max()`, `mean()`, `sum()`
//!    - Advanced stats: `standard_deviation()`, `variance()`
//!    - Quantiles: `median()`, `percentile()`
//!
//! 4. **[`FormatConstraint`]** - Pattern and format validation
//!    - Regex patterns: `regex()`
//!    - Email validation: `email()`
//!    - URL validation: `url()`
//!    - Credit card detection: `credit_card()`
//!
//! 5. **[`LengthConstraint`]** - String length validations
//!    - Minimum length: `min()`
//!    - Maximum length: `max()`
//!    - Exact length: `exactly()`
//!    - Range: `between()`
//!
//! ## Usage Examples
//!
//! ### Basic Validation Suite
//!
//! ```rust
//! use term_guard::prelude::*;
//! use term_guard::core::{Check, ValidationSuite};
//! use term_guard::constraints::{CompletenessConstraint, UniquenessConstraint, StatisticalConstraint, StatisticType, Assertion};
//!
//! # async fn example() -> Result<()> {
//! let suite = ValidationSuite::builder("data_quality")
//!     .check(
//!         Check::builder("completeness")
//!             .constraint(CompletenessConstraint::complete("id"))
//!             .constraint(CompletenessConstraint::with_threshold("email", 0.95))
//!             .build()
//!     )
//!     .check(
//!         Check::builder("uniqueness")
//!             .constraint(UniquenessConstraint::primary_key(vec!["id"]).unwrap())
//!             .constraint(UniquenessConstraint::unique_value_ratio(vec!["email"], Assertion::GreaterThan(0.98)).unwrap())
//!             .build()
//!     )
//!     .check(
//!         Check::builder("statistics")
//!             .constraint(StatisticalConstraint::new("age", StatisticType::Min, Assertion::GreaterThanOrEqual(0.0)).unwrap())
//!             .constraint(StatisticalConstraint::new("age", StatisticType::Max, Assertion::LessThan(150.0)).unwrap())
//!             .build()
//!     )
//!     .build();
//! # Ok(())
//! # }
//! ```
//!
//! ### Using the Fluent API
//!
//! ```rust
//! use term_guard::prelude::*;
//! use term_guard::core::{Check, builder_extensions::*};
//! use term_guard::constraints::{Assertion, FormatOptions, FormatType};
//!
//! # async fn example() -> Result<()> {
//! let check = Check::builder("user_validation")
//!     // Completeness checks
//!     .completeness("user_id", CompletenessOptions::full().into_constraint_options())
//!     .completeness("email", CompletenessOptions::threshold(0.99).into_constraint_options())
//!     
//!     // Statistical checks
//!     .statistics(
//!         "age",
//!         StatisticalOptions::new()
//!             .min(Assertion::GreaterThanOrEqual(13.0))
//!             .max(Assertion::LessThanOrEqual(120.0))
//!             .mean(Assertion::Between(25.0, 65.0))
//!     )?
//!     
//!     // Format validation
//!     .has_format("email", FormatType::Email, 0.99, FormatOptions::default())
//!     
//!     .build();
//! # Ok(())
//! # }
//! ```
//!
//! ### Custom Constraints
//!
//! For business-specific rules, use [`CustomSqlConstraint`]:
//!
//! ```rust
//! use term_guard::constraints::CustomSqlConstraint;
//!
//! # fn example() -> term_guard::prelude::Result<()> {
//! let constraint = CustomSqlConstraint::new(
//!     "discount_amount <= total_amount * 0.5",
//!     Some("Discount cannot exceed 50% of total")
//! )?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Performance Optimization
//!
//! The unified constraint system enables powerful optimizations:
//!
//! ```rust
//! use term_guard::prelude::*;
//! use term_guard::core::{Check, ValidationSuite};
//! use term_guard::constraints::{StatisticalConstraint, StatisticType, Assertion};
//!
//! # async fn example() -> Result<()> {
//! // These constraints will be combined into a single query
//! let check = Check::builder("optimized")
//!     .constraint(StatisticalConstraint::new("price", StatisticType::Min, Assertion::GreaterThan(0.0)).unwrap())
//!     .constraint(StatisticalConstraint::new("price", StatisticType::Max, Assertion::LessThan(1000.0)).unwrap())
//!     .constraint(StatisticalConstraint::new("price", StatisticType::Mean, Assertion::Between(50.0, 200.0)).unwrap())
//!     .build();
//!
//! // Enable optimizer for best performance
//! let suite = ValidationSuite::builder("suite")
//!     .with_optimizer(true)
//!     .check(check)
//!     .build();
//! # Ok(())
//! # }
//! ```
//!
//! ## Constraint Categories
//!
//! ### Data Completeness
//! - [`CompletenessConstraint`] - Null value validation
//! - [`SizeConstraint`] - Row count validation
//!
//! ### Data Uniqueness  
//! - [`UniquenessConstraint`] - Duplicate detection
//! - [`ApproxCountDistinctConstraint`] - Approximate distinct counts
//!
//! ### Statistical Analysis
//! - [`StatisticalConstraint`] - Statistical measures
//! - [`QuantileConstraint`] - Percentile analysis
//! - [`CorrelationConstraint`] - Column relationships
//! - [`HistogramConstraint`] - Value distribution
//!
//! ### Pattern & Format
//! - [`FormatConstraint`] - Pattern matching
//! - [`LengthConstraint`] - String length validation
//! - [`DataTypeConstraint`] - Type validation
//!
//! ### Custom Rules
//! - [`CustomSqlConstraint`] - SQL expressions
//! - [`ColumnCountConstraint`] - Schema validation
//!
//! ## Best Practices
//!
//! 1. **Use unified constraints** - They provide better performance and consistency
//! 2. **Group related checks** - Organize constraints into logical checks
//! 3. **Enable optimization** - Use `with_optimizer(true)` for large datasets
//! 4. **Set appropriate thresholds** - Not all data needs 100% compliance
//! 5. **Monitor performance** - Use telemetry features for production systems

// Module declarations - only non-deprecated modules
mod approx_count_distinct;
mod assertion;
mod column_count;
mod completeness;
mod correlation;
mod cross_table_sum;
mod custom_sql;
mod datatype;
mod foreign_key;
mod format;
mod histogram;
mod join_coverage;
mod length;
mod quantile;
mod size;
mod statistics;
mod temporal_ordering;
mod uniqueness;
mod values;

// Public exports
pub use approx_count_distinct::ApproxCountDistinctConstraint;
pub use assertion::Assertion;
pub use column_count::ColumnCountConstraint;
pub use completeness::CompletenessConstraint;
pub use correlation::{CorrelationConstraint, CorrelationType};
pub use cross_table_sum::CrossTableSumConstraint;
pub use custom_sql::CustomSqlConstraint;
pub use datatype::{
    DataTypeConstraint, DataTypeValidation, NumericValidation, StringTypeValidation,
    TemporalValidation,
};
pub use foreign_key::ForeignKeyConstraint;
pub use format::{FormatConstraint, FormatOptions, FormatType};
pub use histogram::{Histogram, HistogramAssertion, HistogramBucket, HistogramConstraint};
pub use join_coverage::{CoverageType, JoinCoverageConstraint};
pub use length::{LengthAssertion, LengthConstraint};
pub use quantile::{QuantileConstraint, QuantileMethod};
pub use size::SizeConstraint;
pub use statistics::{MultiStatisticalConstraint, StatisticType, StatisticalConstraint};
pub use temporal_ordering::{TemporalOrderingConstraint, TemporalValidationType};
pub use uniqueness::{NullHandling, UniquenessConstraint, UniquenessOptions, UniquenessType};
pub use values::ContainmentConstraint;
