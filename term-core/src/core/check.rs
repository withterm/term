//! Check type and builder for grouping constraints.
#![allow(deprecated)] // Allow deprecated constraints for backward compatibility
//!
//! This module provides the [`Check`] type and [`CheckBuilder`] for creating validation checks.
//! Starting with v0.2.0, we provide a new unified API through the `builder_extensions` module
//! that offers improved consistency and performance.
//!
//! ## New Unified API Example (v0.2.0+)
//!
//! ```rust
//! use term_core::core::{Check, Level};
//! use term_core::core::builder_extensions::{CompletenessOptions, StatisticalOptions};
//! use term_core::constraints::{Assertion, FormatType, FormatOptions};
//!
//! # use term_core::prelude::*;
//! # fn example() -> Result<Check> {
//! let check = Check::builder("user_validation")
//!     .level(Level::Error)
//!     // Completeness with options
//!     .completeness("user_id", CompletenessOptions::full().into_constraint_options())
//!     .completeness("email", CompletenessOptions::threshold(0.95).into_constraint_options())
//!     // Format validation
//!     .has_format("email", FormatType::Email, 0.95, FormatOptions::default())
//!     // Combined statistics (single query)
//!     .statistics(
//!         "age",
//!         StatisticalOptions::new()
//!             .min(Assertion::GreaterThanOrEqual(18.0))
//!             .max(Assertion::LessThan(100.0))
//!             .mean(Assertion::Between(25.0, 65.0))
//!     )?
//!     // Convenience method
//!     .primary_key(vec!["user_id"])
//!     .build();
//! # Ok(check)
//! # }
//! ```

use super::{constraint::BoxedConstraint, Constraint, Level};
use crate::constraints::{
    ApproxCountDistinctConstraint, Assertion, ColumnCountConstraint, CorrelationConstraint,
    CustomSqlConstraint, DataTypeConstraint, FormatConstraint, FormatOptions, FormatType,
    HistogramAssertion, HistogramConstraint, NullHandling, QuantileConstraint, SizeConstraint,
    UniquenessConstraint, UniquenessOptions, UniquenessType,
};
use std::sync::Arc;

/// A validation check containing one or more constraints.
///
/// A `Check` groups related constraints together and assigns them a severity level.
/// Checks are the building blocks of validation suites. When a check runs, all its
/// constraints are evaluated, and the check fails if any constraint fails.
///
/// # Examples
///
/// ## Basic Check
///
/// ```rust
/// use term_core::core::{Check, Level};
///
/// let check = Check::builder("user_data_quality")
///     .level(Level::Error)
///     .description("Validates user data quality")
///     .build();
/// ```
///
/// ## Check with Constraints
///
/// ```rust
/// use term_core::core::{Check, Level, ConstraintOptions};
/// use term_core::constraints::{Assertion, UniquenessType, StatisticType, FormatType, FormatOptions};
///
/// let check = Check::builder("customer_validation")
///     .level(Level::Error)
///     .description("Ensure customer data integrity")
///     // Completeness checks using unified API
///     .completeness("customer_id", ConstraintOptions::new().with_threshold(1.0))
///     .completeness("email", ConstraintOptions::new().with_threshold(0.99))
///     // Uniqueness checks using unified API
///     .validates_uniqueness(vec!["customer_id"], 1.0)
///     .validates_uniqueness(vec!["email", "region"], 1.0)
///     // Pattern validation using format
///     .has_format("phone", FormatType::Regex(r"^\+?\d{3}-\d{3}-\d{4}$".to_string()), 0.95, FormatOptions::default())
///     // Range checks using statistic
///     .statistic("age", StatisticType::Min, Assertion::GreaterThanOrEqual(18.0))
///     .statistic("age", StatisticType::Max, Assertion::LessThanOrEqual(120.0))
///     .build();
/// ```
///
/// ## Data Quality Check
///
/// ```rust
/// use term_core::core::{Check, Level};
/// use term_core::constraints::{Assertion, StatisticType};
///
/// let check = Check::builder("data_types_and_formats")
///     .level(Level::Warning)
///     // Ensure consistent data types using the unified API
///     .has_consistent_data_type("order_date", 0.99)
///     .has_consistent_data_type("product_id", 0.95)
///     // String length validation
///     .has_min_length("password", 8)
///     .has_max_length("username", 20)
///     // Check for PII
///     .validates_credit_card("comments", 0.0, true)  // Should be 0%
///     .validates_email("email_field", 0.98)
///     // Statistical checks
///     .statistic("order_value", StatisticType::Mean, Assertion::Between(50.0, 500.0))
///     .statistic("response_time", StatisticType::StandardDeviation, Assertion::LessThan(100.0))
///     .build();
/// ```
///
/// ## Enhanced Format Validation
///
/// ```rust
/// use term_core::core::{Check, Level};
/// use term_core::constraints::FormatOptions;
///
/// let check = Check::builder("enhanced_format_validation")
///     .level(Level::Error)
///     // Basic format validation
///     .validates_email("email", 0.95)
///     .validates_url("website", 0.90, false)
///     .validates_phone("phone", 0.85, Some("US"))
///     // Enhanced format validation with options
///     .validates_email_with_options(
///         "secondary_email",
///         0.80,
///         FormatOptions::lenient()  // Case insensitive, trimming, nulls allowed
///     )
///     .validates_url_with_options(
///         "dev_url",
///         0.75,
///         true, // allow localhost
///         FormatOptions::case_insensitive().trim_before_check(true)
///     )
///     .validates_regex_with_options(
///         "product_code",
///         r"^[A-Z]{2}\d{4}$",
///         0.98,
///         FormatOptions::strict()  // Case sensitive, no nulls, no trimming
///     )
///     .build();
/// ```
#[derive(Debug, Clone)]
pub struct Check {
    /// The name of the check
    name: String,
    /// The severity level of the check
    level: Level,
    /// Optional description of what this check validates
    description: Option<String>,
    /// The constraints that make up this check
    constraints: Vec<Arc<dyn Constraint>>,
}

impl Check {
    /// Creates a new builder for constructing a check.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the check
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::Check;
    ///
    /// let builder = Check::builder("data_quality");
    /// ```
    pub fn builder(name: impl Into<String>) -> CheckBuilder {
        CheckBuilder::new(name)
    }

    /// Returns the name of the check.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the severity level of the check.
    pub fn level(&self) -> Level {
        self.level
    }

    /// Returns the description of the check if available.
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    /// Returns the constraints in this check.
    pub fn constraints(&self) -> &[Arc<dyn Constraint>] {
        &self.constraints
    }
}

/// Builder for constructing `Check` instances.
///
/// # Examples
///
/// ```rust
/// use term_core::core::{Check, Level};
///
/// let check = Check::builder("completeness_check")
///     .level(Level::Error)
///     .description("Ensures all required fields are present")
///     .build();
/// ```
#[derive(Debug)]
pub struct CheckBuilder {
    name: String,
    level: Level,
    description: Option<String>,
    constraints: Vec<Arc<dyn Constraint>>,
}

impl CheckBuilder {
    /// Creates a new check builder with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            level: Level::default(),
            description: None,
            constraints: Vec::new(),
        }
    }

    /// Sets the severity level for the check.
    ///
    /// # Arguments
    ///
    /// * `level` - The severity level
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    ///
    /// let check = Check::builder("critical_check")
    ///     .level(Level::Error)
    ///     .build();
    /// ```
    pub fn level(mut self, level: Level) -> Self {
        self.level = level;
        self
    }

    /// Sets the description for the check.
    ///
    /// # Arguments
    ///
    /// * `description` - A description of what this check validates
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Adds a constraint to the check.
    ///
    /// # Arguments
    ///
    /// * `constraint` - The constraint to add
    pub fn constraint(mut self, constraint: impl Constraint + 'static) -> Self {
        self.constraints.push(Arc::new(constraint));
        self
    }

    /// Adds a boxed constraint to the check.
    ///
    /// # Arguments
    ///
    /// * `constraint` - The boxed constraint to add
    pub fn boxed_constraint(mut self, constraint: BoxedConstraint) -> Self {
        self.constraints.push(Arc::from(constraint));
        self
    }

    /// Adds multiple constraints to the check.
    ///
    /// # Arguments
    ///
    /// * `constraints` - An iterator of constraints to add
    pub fn constraints<I>(mut self, constraints: I) -> Self
    where
        I: IntoIterator<Item = BoxedConstraint>,
    {
        self.constraints
            .extend(constraints.into_iter().map(Arc::from));
        self
    }

    // Builder methods

    /// Adds a constraint that checks the dataset size (row count).
    ///
    /// # Arguments
    ///
    /// * `assertion` - The assertion to evaluate against the row count
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    /// use term_core::constraints::Assertion;
    ///
    /// let check = Check::builder("size_validation")
    ///     .level(Level::Error)
    ///     .has_size(Assertion::GreaterThan(1000.0))
    ///     .build();
    /// ```
    pub fn has_size(mut self, assertion: Assertion) -> Self {
        self.constraints
            .push(Arc::new(SizeConstraint::new(assertion)));
        self
    }

    /// Adds a constraint that checks the number of columns in the dataset.
    ///
    /// This constraint validates that the dataset has the expected number of columns
    /// by examining the schema.
    ///
    /// # Arguments
    ///
    /// * `assertion` - The assertion to evaluate against the column count
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    /// use term_core::constraints::Assertion;
    ///
    /// let check = Check::builder("schema_validation")
    ///     .level(Level::Error)
    ///     .has_column_count(Assertion::Equals(15.0))
    ///     .has_column_count(Assertion::GreaterThanOrEqual(10.0))
    ///     .build();
    /// ```
    pub fn has_column_count(mut self, assertion: Assertion) -> Self {
        self.constraints
            .push(Arc::new(ColumnCountConstraint::new(assertion)));
        self
    }

    /// Adds a constraint that checks the approximate count of distinct values in a column.
    ///
    /// Uses DataFusion's APPROX_DISTINCT function which provides an approximate count
    /// using HyperLogLog algorithm. This is much faster than exact COUNT(DISTINCT)
    /// for large datasets while maintaining accuracy within 2-3% error margin.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to count distinct values in
    /// * `assertion` - The assertion to evaluate against the approximate distinct count
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    /// use term_core::constraints::Assertion;
    ///
    /// let check = Check::builder("cardinality_validation")
    ///     .level(Level::Warning)
    ///     // High cardinality check (e.g., user IDs)
    ///     .has_approx_count_distinct("user_id", Assertion::GreaterThan(1000000.0))
    ///     // Low cardinality check (e.g., country codes)
    ///     .has_approx_count_distinct("country_code", Assertion::LessThan(200.0))
    ///     .build();
    /// ```
    pub fn has_approx_count_distinct(
        mut self,
        column: impl Into<String>,
        assertion: Assertion,
    ) -> Self {
        self.constraints
            .push(Arc::new(ApproxCountDistinctConstraint::new(
                column, assertion,
            )));
        self
    }

    /// Adds a constraint that checks an approximate quantile of a column.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to check
    /// * `quantile` - The quantile to compute (0.0 to 1.0)
    /// * `assertion` - The assertion to evaluate against the quantile value
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    /// use term_core::constraints::Assertion;
    ///
    /// let check = Check::builder("quantile_validation")
    ///     .level(Level::Warning)
    ///     .has_approx_quantile("response_time", 0.95, Assertion::LessThan(1000.0))
    ///     .build();
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if quantile is not between 0.0 and 1.0
    pub fn has_approx_quantile(
        mut self,
        column: impl Into<String>,
        quantile: f64,
        assertion: Assertion,
    ) -> Self {
        self.constraints.push(Arc::new(
            QuantileConstraint::percentile(column, quantile, assertion)
                .expect("Invalid quantile parameters"),
        ));
        self
    }

    /// Adds a constraint that checks the mutual information between two columns.
    ///
    /// # Arguments
    ///
    /// * `column1` - The first column
    /// * `column2` - The second column
    /// * `assertion` - The assertion to evaluate against the mutual information
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    /// use term_core::constraints::Assertion;
    ///
    /// let check = Check::builder("mi_validation")
    ///     .level(Level::Info)
    ///     .has_mutual_information("feature1", "feature2", Assertion::GreaterThan(0.5))
    ///     .build();
    /// ```
    pub fn has_mutual_information(
        mut self,
        column1: impl Into<String>,
        column2: impl Into<String>,
        assertion: Assertion,
    ) -> Self {
        self.constraints.push(Arc::new(
            CorrelationConstraint::mutual_information(column1, column2, 10, assertion)
                .expect("Invalid mutual information parameters"),
        ));
        self
    }

    /// Adds a constraint that checks the correlation between two columns.
    ///
    /// # Arguments
    ///
    /// * `column1` - The first column
    /// * `column2` - The second column
    /// * `assertion` - The assertion to evaluate against the correlation
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    /// use term_core::constraints::Assertion;
    ///
    /// let check = Check::builder("correlation_validation")
    ///     .level(Level::Warning)
    ///     .has_correlation("height", "weight", Assertion::GreaterThan(0.7))
    ///     .build();
    /// ```
    pub fn has_correlation(
        mut self,
        column1: impl Into<String>,
        column2: impl Into<String>,
        assertion: Assertion,
    ) -> Self {
        self.constraints.push(Arc::new(
            CorrelationConstraint::pearson(column1, column2, assertion)
                .expect("Invalid correlation parameters"),
        ));
        self
    }

    /// Adds a constraint that checks minimum string length.
    ///
    /// **Note**: Consider using the new `length()` method for more flexibility.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to check
    /// * `min_length` - The minimum acceptable string length
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    /// use term_core::constraints::LengthAssertion;
    ///
    /// // Old API:
    /// let check = Check::builder("password_validation")
    ///     .level(Level::Error)
    ///     .has_min_length("password", 8)
    ///     .build();
    ///
    /// // New unified API (recommended):
    /// let check = Check::builder("password_validation")
    ///     .level(Level::Error)
    ///     .length("password", LengthAssertion::Min(8))
    ///     .build();
    /// ```
    pub fn has_min_length(mut self, column: impl Into<String>, min_length: usize) -> Self {
        use crate::constraints::LengthConstraint;
        self.constraints
            .push(Arc::new(LengthConstraint::min(column, min_length)));
        self
    }

    /// Adds a constraint that checks maximum string length.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to check
    /// * `max_length` - The maximum acceptable string length
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    ///
    /// let check = Check::builder("username_validation")
    ///     .level(Level::Error)
    ///     .has_max_length("username", 20)
    ///     .build();
    /// ```
    pub fn has_max_length(mut self, column: impl Into<String>, max_length: usize) -> Self {
        use crate::constraints::LengthConstraint;
        self.constraints
            .push(Arc::new(LengthConstraint::max(column, max_length)));
        self
    }

    /// Adds a constraint that checks string length is between bounds (inclusive).
    ///
    /// # Arguments
    ///  
    /// * `column` - The column to check
    /// * `min_length` - The minimum acceptable string length
    /// * `max_length` - The maximum acceptable string length
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    ///
    /// let check = Check::builder("description_validation")
    ///     .level(Level::Warning)
    ///     .has_length_between("description", 10, 500)
    ///     .build();
    /// ```
    pub fn has_length_between(
        mut self,
        column: impl Into<String>,
        min_length: usize,
        max_length: usize,
    ) -> Self {
        use crate::constraints::LengthConstraint;
        self.constraints.push(Arc::new(LengthConstraint::between(
            column, min_length, max_length,
        )));
        self
    }

    /// Adds a constraint that checks string has exact length.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to check  
    /// * `length` - The required exact string length
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    ///
    /// let check = Check::builder("code_validation")
    ///     .level(Level::Error)
    ///     .has_exact_length("verification_code", 6)
    ///     .build();
    /// ```
    pub fn has_exact_length(mut self, column: impl Into<String>, length: usize) -> Self {
        use crate::constraints::LengthConstraint;
        self.constraints
            .push(Arc::new(LengthConstraint::exactly(column, length)));
        self
    }

    /// Adds a constraint that checks strings are not empty.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to check
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    ///
    /// let check = Check::builder("name_validation")
    ///     .level(Level::Error)
    ///     .is_not_empty("name")
    ///     .build();
    /// ```
    pub fn is_not_empty(mut self, column: impl Into<String>) -> Self {
        use crate::constraints::LengthConstraint;
        self.constraints
            .push(Arc::new(LengthConstraint::not_empty(column)));
        self
    }

    /// Adds a constraint that checks data type consistency.
    ///
    /// This analyzes the actual data types present in a column and reports on consistency,
    /// helping identify columns with mixed types.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to check
    /// * `threshold` - The minimum ratio of values that must have the most common type (0.0 to 1.0)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    ///
    /// let check = Check::builder("consistency_validation")
    ///     .level(Level::Warning)
    ///     .has_consistent_data_type("user_id", 0.95)
    ///     .build();
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if threshold is not between 0.0 and 1.0
    pub fn has_consistent_data_type(mut self, column: impl Into<String>, threshold: f64) -> Self {
        self.constraints.push(Arc::new(
            DataTypeConstraint::type_consistency(column, threshold)
                .expect("Invalid data type consistency parameters"),
        ));
        self
    }

    /// Adds a constraint that evaluates a custom SQL expression.
    ///
    /// This allows users to define custom validation logic using SQL expressions.
    /// The expression should evaluate to a boolean value for each row.
    /// For safety, the expression cannot contain data-modifying operations.
    ///
    /// # Arguments
    ///
    /// * `sql_expression` - The SQL expression to evaluate (must return boolean)
    /// * `hint` - Optional hint message to provide context when the constraint fails
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    ///
    /// let check = Check::builder("business_rules")
    ///     .level(Level::Error)
    ///     .satisfies("price > 0 AND price < 1000000", Some("Price must be positive and reasonable"))
    ///     .satisfies("order_date <= ship_date", Some("Orders cannot ship before being placed"))
    ///     .build();
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the SQL expression contains dangerous operations like DROP, DELETE, UPDATE, etc.
    pub fn satisfies(
        mut self,
        sql_expression: impl Into<String>,
        hint: Option<impl Into<String>>,
    ) -> Self {
        self.constraints.push(Arc::new(
            CustomSqlConstraint::new(sql_expression, hint).expect("Invalid SQL expression"),
        ));
        self
    }

    /// Adds a constraint that analyzes value distribution and applies custom assertions.
    ///
    /// This constraint computes a histogram of value frequencies in the specified column
    /// and allows custom assertion functions to validate distribution characteristics.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to analyze
    /// * `assertion` - The assertion function to apply to the histogram
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    /// use term_core::constraints::Histogram;
    /// use std::sync::Arc;
    ///
    /// let check = Check::builder("distribution_validation")
    ///     .level(Level::Warning)
    ///     // No single value should dominate
    ///     .has_histogram("status", Arc::new(|hist: &Histogram| {
    ///         hist.most_common_ratio() < 0.5
    ///     }))
    ///     // Check expected number of categories
    ///     .has_histogram("category", Arc::new(|hist| {
    ///         hist.bucket_count() >= 5 && hist.bucket_count() <= 10
    ///     }))
    ///     .build();
    /// ```
    pub fn has_histogram(
        mut self,
        column: impl Into<String>,
        assertion: HistogramAssertion,
    ) -> Self {
        self.constraints
            .push(Arc::new(HistogramConstraint::new(column, assertion)));
        self
    }

    /// Adds a constraint that analyzes value distribution with a custom description.
    ///
    /// This is similar to `has_histogram` but allows providing a description of what
    /// the assertion checks, which is useful for error messages.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to analyze
    /// * `assertion` - The assertion function to apply to the histogram
    /// * `description` - A description of what the assertion checks
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    /// use term_core::constraints::Histogram;
    /// use std::sync::Arc;
    ///
    /// let check = Check::builder("distribution_validation")
    ///     .level(Level::Error)
    ///     .has_histogram_with_description(
    ///         "age",
    ///         Arc::new(|hist: &Histogram| hist.is_roughly_uniform(2.0)),
    ///         "age distribution is roughly uniform"
    ///     )
    ///     .build();
    /// ```
    pub fn has_histogram_with_description(
        mut self,
        column: impl Into<String>,
        assertion: HistogramAssertion,
        description: impl Into<String>,
    ) -> Self {
        self.constraints
            .push(Arc::new(HistogramConstraint::new_with_description(
                column,
                assertion,
                description,
            )));
        self
    }

    // ========================================================================
    // NEW UNIFIED FORMAT VALIDATION METHODS
    // ========================================================================

    /// Adds a general format validation constraint with full configuration options.
    ///
    /// This is the most flexible format validation method, supporting all format types
    /// and configuration options through the unified FormatConstraint API.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to validate
    /// * `format` - The format type to validate against
    /// * `threshold` - The minimum ratio of values that must match (0.0 to 1.0)
    /// * `options` - Configuration options for the validation
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    /// use term_core::constraints::{FormatType, FormatOptions};
    ///
    /// let check = Check::builder("format_validation")
    ///     .level(Level::Error)
    ///     // Custom regex with case-insensitive matching
    ///     .has_format(
    ///         "phone",
    ///         FormatType::Regex(r"^\+?\d{3}-\d{3}-\d{4}$".to_string()),
    ///         0.95,
    ///         FormatOptions::default().case_sensitive(false).trim_before_check(true)
    ///     )
    ///     // Email validation with custom options
    ///     .has_format(
    ///         "email",
    ///         FormatType::Email,
    ///         0.99,
    ///         FormatOptions::default().null_is_valid(true)
    ///     )
    ///     // UUID validation
    ///     .has_format(
    ///         "user_id",
    ///         FormatType::UUID,
    ///         1.0,
    ///         FormatOptions::default()
    ///     )
    ///     .build();
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error if column name is invalid, threshold is out of range,
    /// or regex pattern is invalid.
    pub fn has_format(
        mut self,
        column: impl Into<String>,
        format: FormatType,
        threshold: f64,
        options: FormatOptions,
    ) -> Self {
        self.constraints.push(Arc::new(
            FormatConstraint::new(column, format, threshold, options)
                .expect("Invalid column, format, threshold, or options"),
        ));
        self
    }

    /// Adds a regex pattern validation constraint.
    ///
    /// This is a convenience method for `has_format()` with `FormatType::Regex`.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to validate
    /// * `pattern` - The regular expression pattern
    /// * `threshold` - The minimum ratio of values that must match (0.0 to 1.0)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    ///
    /// let check = Check::builder("regex_validation")
    ///     .level(Level::Error)
    ///     .validates_regex("phone", r"^\+?\d{3}-\d{3}-\d{4}$", 0.95)
    ///     .validates_regex("product_code", r"^[A-Z]{2}\d{6}$", 1.0)
    ///     .build();
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error if column name is invalid, threshold is out of range,
    /// or regex pattern is invalid.
    pub fn validates_regex(
        mut self,
        column: impl Into<String>,
        pattern: impl Into<String>,
        threshold: f64,
    ) -> Self {
        self.constraints.push(Arc::new(
            FormatConstraint::regex(column, pattern, threshold)
                .expect("Invalid column, pattern, or threshold"),
        ));
        self
    }

    /// Adds an email address validation constraint.
    ///
    /// This is a convenience method for `has_format()` with `FormatType::Email`.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to validate
    /// * `threshold` - The minimum ratio of values that must be valid emails (0.0 to 1.0)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    ///
    /// let check = Check::builder("email_validation")
    ///     .level(Level::Error)
    ///     .validates_email("primary_email", 0.99)
    ///     .validates_email("secondary_email", 0.80)
    ///     .build();
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error if column name is invalid or threshold is out of range.
    pub fn validates_email(mut self, column: impl Into<String>, threshold: f64) -> Self {
        self.constraints.push(Arc::new(
            FormatConstraint::email(column, threshold).expect("Invalid column or threshold"),
        ));
        self
    }

    /// Adds a URL validation constraint.
    ///
    /// This is a convenience method for `has_format()` with `FormatType::Url`.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to validate
    /// * `threshold` - The minimum ratio of values that must be valid URLs (0.0 to 1.0)
    /// * `allow_localhost` - Whether to allow localhost URLs
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    ///
    /// let check = Check::builder("url_validation")
    ///     .level(Level::Error)
    ///     .validates_url("website", 0.90, false)
    ///     .validates_url("dev_endpoint", 0.80, true)
    ///     .build();
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error if column name is invalid or threshold is out of range.
    pub fn validates_url(
        mut self,
        column: impl Into<String>,
        threshold: f64,
        allow_localhost: bool,
    ) -> Self {
        self.constraints.push(Arc::new(
            FormatConstraint::url(column, threshold, allow_localhost)
                .expect("Invalid column or threshold"),
        ));
        self
    }

    /// Adds a credit card number detection constraint.
    ///
    /// This is a convenience method for `has_format()` with `FormatType::CreditCard`.
    /// Note: For PII detection, you typically want a low threshold (e.g., 0.01 or 0.05)
    /// to catch any potential credit card numbers.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to validate
    /// * `threshold` - The maximum ratio of values that can be credit card numbers (0.0 to 1.0)
    /// * `detect_only` - If true, optimizes for detection; if false, for validation
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    ///
    /// let check = Check::builder("pii_detection")
    ///     .level(Level::Error)
    ///     // PII detection - should find very few or no credit cards
    ///     .validates_credit_card("comments", 0.01, true)
    ///     .validates_credit_card("description", 0.0, true)
    ///     // Credit card validation - most values should be valid credit cards
    ///     .validates_credit_card("payment_info", 0.95, false)
    ///     .build();
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error if column name is invalid or threshold is out of range.
    pub fn validates_credit_card(
        mut self,
        column: impl Into<String>,
        threshold: f64,
        detect_only: bool,
    ) -> Self {
        self.constraints.push(Arc::new(
            FormatConstraint::credit_card(column, threshold, detect_only)
                .expect("Invalid column or threshold"),
        ));
        self
    }

    /// Adds a phone number validation constraint.
    ///
    /// This is a convenience method for `has_format()` with `FormatType::Phone`.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to validate
    /// * `threshold` - The minimum ratio of values that must be valid phone numbers (0.0 to 1.0)
    /// * `country` - Optional country code for country-specific validation (e.g., "US", "CA")
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    ///
    /// let check = Check::builder("phone_validation")
    ///     .level(Level::Error)
    ///     .validates_phone("phone", 0.95, Some("US"))
    ///     .validates_phone("international_phone", 0.90, None)
    ///     .build();
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error if column name is invalid or threshold is out of range.
    pub fn validates_phone(
        mut self,
        column: impl Into<String>,
        threshold: f64,
        country: Option<&str>,
    ) -> Self {
        self.constraints.push(Arc::new(
            FormatConstraint::phone(column, threshold, country.map(|s| s.to_string()))
                .expect("Invalid column or threshold"),
        ));
        self
    }

    /// Adds a postal code validation constraint.
    ///
    /// This is a convenience method for `has_format()` with `FormatType::PostalCode`.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to validate
    /// * `threshold` - The minimum ratio of values that must be valid postal codes (0.0 to 1.0)
    /// * `country` - Country code for country-specific validation (e.g., "US", "CA", "UK")
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    ///
    /// let check = Check::builder("postal_code_validation")
    ///     .level(Level::Error)
    ///     .validates_postal_code("zip_code", 0.98, "US")
    ///     .validates_postal_code("postal_code", 0.95, "CA")
    ///     .build();
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error if column name is invalid or threshold is out of range.
    pub fn validates_postal_code(
        mut self,
        column: impl Into<String>,
        threshold: f64,
        country: &str,
    ) -> Self {
        self.constraints.push(Arc::new(
            FormatConstraint::postal_code(column, threshold, country)
                .expect("Invalid column or threshold"),
        ));
        self
    }

    /// Adds a UUID validation constraint.
    ///
    /// This is a convenience method for `has_format()` with `FormatType::UUID`.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to validate
    /// * `threshold` - The minimum ratio of values that must be valid UUIDs (0.0 to 1.0)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    ///
    /// let check = Check::builder("uuid_validation")
    ///     .level(Level::Error)
    ///     .validates_uuid("user_id", 1.0)
    ///     .validates_uuid("session_id", 0.99)
    ///     .build();
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error if column name is invalid or threshold is out of range.
    pub fn validates_uuid(mut self, column: impl Into<String>, threshold: f64) -> Self {
        self.constraints.push(Arc::new(
            FormatConstraint::uuid(column, threshold).expect("Invalid column or threshold"),
        ));
        self
    }

    /// Adds an IPv4 address validation constraint.
    ///
    /// This is a convenience method for `has_format()` with `FormatType::IPv4`.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to validate
    /// * `threshold` - The minimum ratio of values that must be valid IPv4 addresses (0.0 to 1.0)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    ///
    /// let check = Check::builder("ip_validation")
    ///     .level(Level::Error)
    ///     .validates_ipv4("client_ip", 0.98)
    ///     .validates_ipv4("server_ip", 1.0)
    ///     .build();
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error if column name is invalid or threshold is out of range.
    pub fn validates_ipv4(mut self, column: impl Into<String>, threshold: f64) -> Self {
        self.constraints.push(Arc::new(
            FormatConstraint::ipv4(column, threshold).expect("Invalid column or threshold"),
        ));
        self
    }

    /// Adds an IPv6 address validation constraint.
    ///
    /// This is a convenience method for `has_format()` with `FormatType::IPv6`.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to validate
    /// * `threshold` - The minimum ratio of values that must be valid IPv6 addresses (0.0 to 1.0)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    ///
    /// let check = Check::builder("ipv6_validation")
    ///     .level(Level::Error)
    ///     .validates_ipv6("client_ipv6", 0.95)
    ///     .build();
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error if column name is invalid or threshold is out of range.
    pub fn validates_ipv6(mut self, column: impl Into<String>, threshold: f64) -> Self {
        self.constraints.push(Arc::new(
            FormatConstraint::ipv6(column, threshold).expect("Invalid column or threshold"),
        ));
        self
    }

    /// Adds a JSON format validation constraint.
    ///
    /// This is a convenience method for `has_format()` with `FormatType::Json`.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to validate
    /// * `threshold` - The minimum ratio of values that must be valid JSON (0.0 to 1.0)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    ///
    /// let check = Check::builder("json_validation")
    ///     .level(Level::Error)
    ///     .validates_json("metadata", 0.99)
    ///     .validates_json("config", 1.0)
    ///     .build();
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error if column name is invalid or threshold is out of range.
    pub fn validates_json(mut self, column: impl Into<String>, threshold: f64) -> Self {
        self.constraints.push(Arc::new(
            FormatConstraint::json(column, threshold).expect("Invalid column or threshold"),
        ));
        self
    }

    /// Adds an ISO 8601 datetime validation constraint.
    ///
    /// This is a convenience method for `has_format()` with `FormatType::Iso8601DateTime`.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to validate
    /// * `threshold` - The minimum ratio of values that must be valid ISO 8601 datetimes (0.0 to 1.0)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    ///
    /// let check = Check::builder("datetime_validation")
    ///     .level(Level::Error)
    ///     .validates_iso8601_datetime("order_date", 1.0)
    ///     .validates_iso8601_datetime("modified_date", 0.98)
    ///     .build();
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error if column name is invalid or threshold is out of range.
    pub fn validates_iso8601_datetime(mut self, column: impl Into<String>, threshold: f64) -> Self {
        self.constraints.push(Arc::new(
            FormatConstraint::iso8601_datetime(column, threshold)
                .expect("Invalid column or threshold"),
        ));
        self
    }

    // ========================================================================
    // ENHANCED FORMAT VALIDATION METHODS WITH OPTIONS
    // ========================================================================

    /// Adds an enhanced email validation constraint with configurable options.
    ///
    /// This method provides more control than `validates_email()` by supporting
    /// case sensitivity, whitespace trimming, and null handling options.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to validate
    /// * `threshold` - The minimum ratio of values that must be valid emails (0.0 to 1.0)
    /// * `options` - Format validation options
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    /// use term_core::constraints::FormatOptions;
    ///
    /// let check = Check::builder("enhanced_email_validation")
    ///     .level(Level::Error)
    ///     // Case-insensitive email validation with trimming
    ///     .validates_email_with_options(
    ///         "email",
    ///         0.95,
    ///         FormatOptions::new()
    ///             .case_sensitive(false)
    ///             .trim_before_check(true)
    ///             .null_is_valid(false)
    ///     )
    ///     .build();
    /// ```
    pub fn validates_email_with_options(
        mut self,
        column: impl Into<String>,
        threshold: f64,
        options: FormatOptions,
    ) -> Self {
        self.constraints.push(Arc::new(
            FormatConstraint::new(column, FormatType::Email, threshold, options)
                .expect("Invalid column, threshold, or options"),
        ));
        self
    }

    /// Adds an enhanced URL validation constraint with configurable options.
    ///
    /// This method provides more control than `validates_url()` by supporting
    /// case sensitivity, whitespace trimming, and null handling options.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to validate
    /// * `threshold` - The minimum ratio of values that must be valid URLs (0.0 to 1.0)
    /// * `allow_localhost` - Whether to allow localhost URLs
    /// * `options` - Format validation options
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    /// use term_core::constraints::FormatOptions;
    ///
    /// let check = Check::builder("enhanced_url_validation")
    ///     .level(Level::Error)
    ///     // Case-insensitive URL validation with trimming
    ///     .validates_url_with_options(
    ///         "website",
    ///         0.90,
    ///         true, // allow localhost
    ///         FormatOptions::new()
    ///             .case_sensitive(false)
    ///             .trim_before_check(true)
    ///     )
    ///     .build();
    /// ```
    pub fn validates_url_with_options(
        mut self,
        column: impl Into<String>,
        threshold: f64,
        allow_localhost: bool,
        options: FormatOptions,
    ) -> Self {
        self.constraints.push(Arc::new(
            FormatConstraint::new(
                column,
                FormatType::Url { allow_localhost },
                threshold,
                options,
            )
            .expect("Invalid column, threshold, or options"),
        ));
        self
    }

    /// Adds an enhanced phone number validation constraint with configurable options.
    ///
    /// This method provides more control than `validates_phone()` by supporting
    /// case sensitivity, whitespace trimming, and null handling options.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to validate
    /// * `threshold` - The minimum ratio of values that must be valid phone numbers (0.0 to 1.0)
    /// * `country` - Optional country code for region-specific validation
    /// * `options` - Format validation options
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    /// use term_core::constraints::FormatOptions;
    ///
    /// let check = Check::builder("enhanced_phone_validation")
    ///     .level(Level::Error)
    ///     // US phone validation with trimming
    ///     .validates_phone_with_options(
    ///         "phone",
    ///         0.95,
    ///         Some("US".to_string()),
    ///         FormatOptions::new().trim_before_check(true)
    ///     )
    ///     .build();
    /// ```
    pub fn validates_phone_with_options(
        mut self,
        column: impl Into<String>,
        threshold: f64,
        country: Option<String>,
        options: FormatOptions,
    ) -> Self {
        self.constraints.push(Arc::new(
            FormatConstraint::new(column, FormatType::Phone { country }, threshold, options)
                .expect("Invalid column, threshold, or options"),
        ));
        self
    }

    /// Adds an enhanced regex pattern validation constraint with configurable options.
    ///
    /// This method provides more control than `validates_regex()` by supporting
    /// case sensitivity, whitespace trimming, and null handling options.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to validate
    /// * `pattern` - The regular expression pattern
    /// * `threshold` - The minimum ratio of values that must match (0.0 to 1.0)
    /// * `options` - Format validation options
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    /// use term_core::constraints::FormatOptions;
    ///
    /// let check = Check::builder("enhanced_regex_validation")
    ///     .level(Level::Error)
    ///     // Case-insensitive product code validation
    ///     .validates_regex_with_options(
    ///         "product_code",
    ///         r"^[A-Z]{2}\d{4}$",
    ///         0.98,
    ///         FormatOptions::new()
    ///             .case_sensitive(false)
    ///             .trim_before_check(true)
    ///     )
    ///     .build();
    /// ```
    pub fn validates_regex_with_options(
        mut self,
        column: impl Into<String>,
        pattern: impl Into<String>,
        threshold: f64,
        options: FormatOptions,
    ) -> Self {
        self.constraints.push(Arc::new(
            FormatConstraint::new(
                column,
                FormatType::Regex(pattern.into()),
                threshold,
                options,
            )
            .expect("Invalid column, pattern, threshold, or options"),
        ));
        self
    }

    // ========================================================================
    // NEW UNIFIED API METHODS
    // ========================================================================

    /// Adds a unified uniqueness constraint with full control over validation type and options.
    ///
    /// This method provides a comprehensive alternative to `is_unique`, `are_unique`, `has_uniqueness`,
    /// `is_primary_key`, `has_distinctness`, and `has_unique_value_ratio` by supporting all uniqueness
    /// validation types with flexible configuration options.
    ///
    /// # Arguments
    ///
    /// * `columns` - The column(s) to check (single string, vec, or array)
    /// * `uniqueness_type` - The type of uniqueness validation to perform
    /// * `options` - Configuration options for null handling, case sensitivity, etc.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    /// use term_core::constraints::{UniquenessType, UniquenessOptions, NullHandling, Assertion};
    ///
    /// let check = Check::builder("unified_uniqueness")
    ///     // Full uniqueness with threshold
    ///     .uniqueness(
    ///         vec!["user_id"],
    ///         UniquenessType::FullUniqueness { threshold: 1.0 },
    ///         UniquenessOptions::default()
    ///     )
    ///     // Primary key validation
    ///     .uniqueness(
    ///         vec!["order_id", "line_item_id"],
    ///         UniquenessType::PrimaryKey,
    ///         UniquenessOptions::default()
    ///     )
    ///     // Distinctness check
    ///     .uniqueness(
    ///         vec!["category"],
    ///         UniquenessType::Distinctness(Assertion::LessThan(0.1)),
    ///         UniquenessOptions::default()
    ///     )
    ///     // Unique value ratio
    ///     .uniqueness(
    ///         vec!["transaction_id"],
    ///         UniquenessType::UniqueValueRatio(Assertion::GreaterThan(0.99)),
    ///         UniquenessOptions::default()
    ///     )
    ///     // Composite uniqueness with null handling
    ///     .uniqueness(
    ///         vec!["email", "domain"],
    ///         UniquenessType::UniqueComposite {
    ///             threshold: 0.95,
    ///             null_handling: NullHandling::Exclude,
    ///             case_sensitive: false
    ///         },
    ///         UniquenessOptions::new()
    ///             .with_null_handling(NullHandling::Exclude)
    ///             .case_sensitive(false)
    ///     )
    ///     .build();
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error if column names are invalid or thresholds are out of range.
    pub fn uniqueness<I, S>(
        mut self,
        columns: I,
        uniqueness_type: UniquenessType,
        options: UniquenessOptions,
    ) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.constraints.push(Arc::new(
            UniquenessConstraint::new(columns, uniqueness_type, options)
                .expect("Invalid columns, uniqueness type, or options"),
        ));
        self
    }

    /// Adds a full uniqueness constraint for single or multiple columns.
    ///
    /// This is a convenience method for `uniqueness()` with `UniquenessType::FullUniqueness`.
    ///
    /// # Arguments
    ///
    /// * `columns` - The column(s) to check for uniqueness
    /// * `threshold` - The minimum acceptable uniqueness ratio (0.0 to 1.0)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    ///
    /// let check = Check::builder("uniqueness_validation")
    ///     .level(Level::Error)
    ///     .validates_uniqueness(vec!["user_id"], 1.0)
    ///     .validates_uniqueness(vec!["email", "domain"], 0.95)
    ///     .build();
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error if column names are invalid or threshold is out of range.
    pub fn validates_uniqueness<I, S>(mut self, columns: I, threshold: f64) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.constraints.push(Arc::new(
            UniquenessConstraint::new(
                columns,
                UniquenessType::FullUniqueness { threshold },
                UniquenessOptions::default(),
            )
            .expect("Invalid columns or threshold"),
        ));
        self
    }

    /// Adds a distinctness constraint with assertion-based validation.
    ///
    /// This is a convenience method for `uniqueness()` with `UniquenessType::Distinctness`.
    ///
    /// # Arguments
    ///
    /// * `columns` - The column(s) to check for distinctness
    /// * `assertion` - The assertion to apply to the distinctness ratio
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    /// use term_core::constraints::Assertion;
    ///
    /// let check = Check::builder("distinctness_validation")
    ///     .level(Level::Warning)
    ///     .validates_distinctness(vec!["status"], Assertion::LessThan(0.1))
    ///     .validates_distinctness(vec!["user_id"], Assertion::GreaterThan(0.95))
    ///     .build();
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error if column names are invalid.
    pub fn validates_distinctness<I, S>(mut self, columns: I, assertion: Assertion) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.constraints.push(Arc::new(
            UniquenessConstraint::new(
                columns,
                UniquenessType::Distinctness(assertion),
                UniquenessOptions::default(),
            )
            .expect("Invalid columns"),
        ));
        self
    }

    /// Adds a unique value ratio constraint with assertion-based validation.
    ///
    /// This is a convenience method for `uniqueness()` with `UniquenessType::UniqueValueRatio`.
    ///
    /// # Arguments
    ///
    /// * `columns` - The column(s) to check for unique value ratio
    /// * `assertion` - The assertion to apply to the unique value ratio
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    /// use term_core::constraints::Assertion;
    ///
    /// let check = Check::builder("unique_ratio_validation")
    ///     .level(Level::Warning)
    ///     .validates_unique_value_ratio(vec!["transaction_id"], Assertion::GreaterThan(0.99))
    ///     .validates_unique_value_ratio(vec!["category"], Assertion::LessThan(0.01))
    ///     .build();
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error if column names are invalid.
    pub fn validates_unique_value_ratio<I, S>(mut self, columns: I, assertion: Assertion) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.constraints.push(Arc::new(
            UniquenessConstraint::new(
                columns,
                UniquenessType::UniqueValueRatio(assertion),
                UniquenessOptions::default(),
            )
            .expect("Invalid columns"),
        ));
        self
    }

    /// Adds a primary key constraint (unique + non-null).
    ///
    /// This is a convenience method for `uniqueness()` with `UniquenessType::PrimaryKey`.
    ///
    /// # Arguments
    ///
    /// * `columns` - The column(s) that form the primary key
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    ///
    /// let check = Check::builder("primary_key_validation")
    ///     .level(Level::Error)
    ///     .validates_primary_key(vec!["user_id"])
    ///     .validates_primary_key(vec!["order_id", "line_item_id"])
    ///     .build();
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error if column names are invalid.
    pub fn validates_primary_key<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.constraints.push(Arc::new(
            UniquenessConstraint::new(
                columns,
                UniquenessType::PrimaryKey,
                UniquenessOptions::default(),
            )
            .expect("Invalid columns"),
        ));
        self
    }

    /// Adds a uniqueness constraint that allows NULL values with configurable handling.
    ///
    /// This is a convenience method for `uniqueness()` with `UniquenessType::UniqueWithNulls`.
    ///
    /// # Arguments
    ///
    /// * `columns` - The column(s) to check for uniqueness
    /// * `threshold` - The minimum acceptable uniqueness ratio (0.0 to 1.0)
    /// * `null_handling` - How to handle NULL values in uniqueness calculations
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    /// use term_core::constraints::NullHandling;
    ///
    /// let check = Check::builder("null_handling_validation")
    ///     .level(Level::Warning)
    ///     .validates_uniqueness_with_nulls(vec!["optional_id"], 0.9, NullHandling::Exclude)
    ///     .validates_uniqueness_with_nulls(vec!["reference"], 0.8, NullHandling::Include)
    ///     .build();
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error if column names are invalid or threshold is out of range.
    pub fn validates_uniqueness_with_nulls<I, S>(
        mut self,
        columns: I,
        threshold: f64,
        null_handling: NullHandling,
    ) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.constraints.push(Arc::new(
            UniquenessConstraint::new(
                columns,
                UniquenessType::UniqueWithNulls {
                    threshold,
                    null_handling,
                },
                UniquenessOptions::new().with_null_handling(null_handling),
            )
            .expect("Invalid columns or threshold"),
        ));
        self
    }

    /// Adds a completeness constraint using the unified options pattern.
    ///
    /// This method provides a more flexible alternative to `is_complete`, `has_completeness`,
    /// `are_complete`, and `are_any_complete` by supporting arbitrary logical operators
    /// and thresholds.
    ///
    /// # Arguments
    ///
    /// * `columns` - The column(s) to check (single string, vec, or array)
    /// * `options` - Configuration options including threshold and logical operator
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, ConstraintOptions, LogicalOperator};
    ///
    /// let check = Check::builder("unified_completeness")
    ///     // Single column with threshold
    ///     .completeness("email", ConstraintOptions::new().with_threshold(0.95))
    ///     // Multiple columns - all must be complete
    ///     .completeness(
    ///         vec!["first_name", "last_name"],
    ///         ConstraintOptions::new()
    ///             .with_operator(LogicalOperator::All)
    ///             .with_threshold(1.0)
    ///     )
    ///     // At least 2 of 4 contact methods must be 90% complete
    ///     .completeness(
    ///         vec!["email", "phone", "address", "postal_code"],
    ///         ConstraintOptions::new()
    ///             .with_operator(LogicalOperator::AtLeast(2))
    ///             .with_threshold(0.9)
    ///     )
    ///     .build();
    /// ```
    pub fn completeness(
        mut self,
        columns: impl Into<crate::core::ColumnSpec>,
        options: crate::core::ConstraintOptions,
    ) -> Self {
        use crate::constraints::CompletenessConstraint;
        self.constraints
            .push(Arc::new(CompletenessConstraint::new(columns, options)));
        self
    }

    /// Adds a string length constraint using the unified options pattern.
    ///
    /// This method provides a more flexible alternative to the individual length methods
    /// by supporting all length assertion types in a single interface.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to check
    /// * `assertion` - The length assertion (Min, Max, Between, Exactly, NotEmpty)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::Check;
    /// use term_core::constraints::LengthAssertion;
    ///
    /// let check = Check::builder("length_validation")
    ///     .length("password", LengthAssertion::Min(8))
    ///     .length("username", LengthAssertion::Between(3, 20))
    ///     .length("verification_code", LengthAssertion::Exactly(6))
    ///     .length("name", LengthAssertion::NotEmpty)
    ///     .build();
    /// ```
    pub fn length(
        mut self,
        column: impl Into<String>,
        assertion: crate::constraints::LengthAssertion,
    ) -> Self {
        use crate::constraints::LengthConstraint;
        self.constraints
            .push(Arc::new(LengthConstraint::new(column, assertion)));
        self
    }

    /// Adds a statistical constraint using the unified options pattern.
    ///
    /// This method provides a unified interface for all statistical constraints
    /// (min, max, mean, sum, standard deviation) with consistent assertion patterns.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to analyze
    /// * `statistic` - The type of statistic to compute
    /// * `assertion` - The assertion to apply to the statistic
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::Check;
    /// use term_core::constraints::{StatisticType, Assertion};
    ///
    /// let check = Check::builder("statistical_validation")
    ///     .statistic("age", StatisticType::Min, Assertion::GreaterThanOrEqual(0.0))
    ///     .statistic("age", StatisticType::Max, Assertion::LessThanOrEqual(120.0))
    ///     .statistic("salary", StatisticType::Mean, Assertion::Between(50000.0, 100000.0))
    ///     .statistic("response_time", StatisticType::StandardDeviation, Assertion::LessThan(100.0))
    ///     .build();
    /// ```
    pub fn statistic(
        mut self,
        column: impl Into<String>,
        statistic: crate::constraints::StatisticType,
        assertion: Assertion,
    ) -> Self {
        use crate::constraints::StatisticalConstraint;
        self.constraints.push(Arc::new(
            StatisticalConstraint::new(column, statistic, assertion)
                .expect("Invalid column name or statistic"),
        ));
        self
    }

    /// Adds a minimum value constraint for a column.
    ///
    /// This is a convenience method for `statistic()` with `StatisticType::Min`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    /// use term_core::constraints::Assertion;
    ///
    /// let check = Check::builder("age_validation")
    ///     .level(Level::Error)
    ///     .has_min("age", Assertion::GreaterThanOrEqual(0.0))
    ///     .build();
    /// ```
    pub fn has_min(self, column: impl Into<String>, assertion: Assertion) -> Self {
        self.statistic(column, crate::constraints::StatisticType::Min, assertion)
    }

    /// Adds a maximum value constraint for a column.
    ///
    /// This is a convenience method for `statistic()` with `StatisticType::Max`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    /// use term_core::constraints::Assertion;
    ///
    /// let check = Check::builder("age_validation")
    ///     .level(Level::Error)
    ///     .has_max("age", Assertion::LessThanOrEqual(120.0))
    ///     .build();
    /// ```
    pub fn has_max(self, column: impl Into<String>, assertion: Assertion) -> Self {
        self.statistic(column, crate::constraints::StatisticType::Max, assertion)
    }

    /// Adds a mean (average) value constraint for a column.
    ///
    /// This is a convenience method for `statistic()` with `StatisticType::Mean`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    /// use term_core::constraints::Assertion;
    ///
    /// let check = Check::builder("salary_validation")
    ///     .level(Level::Warning)
    ///     .has_mean("salary", Assertion::Between(50000.0, 100000.0))
    ///     .build();
    /// ```
    pub fn has_mean(self, column: impl Into<String>, assertion: Assertion) -> Self {
        self.statistic(column, crate::constraints::StatisticType::Mean, assertion)
    }

    /// Adds a sum constraint for a column.
    ///
    /// This is a convenience method for `statistic()` with `StatisticType::Sum`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    /// use term_core::constraints::Assertion;
    ///
    /// let check = Check::builder("revenue_validation")
    ///     .level(Level::Error)
    ///     .has_sum("revenue", Assertion::GreaterThan(1000000.0))
    ///     .build();
    /// ```
    pub fn has_sum(self, column: impl Into<String>, assertion: Assertion) -> Self {
        self.statistic(column, crate::constraints::StatisticType::Sum, assertion)
    }

    /// Adds a standard deviation constraint for a column.
    ///
    /// This is a convenience method for `statistic()` with `StatisticType::StandardDeviation`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    /// use term_core::constraints::Assertion;
    ///
    /// let check = Check::builder("response_time_validation")
    ///     .level(Level::Warning)
    ///     .has_standard_deviation("response_time", Assertion::LessThan(100.0))
    ///     .build();
    /// ```
    pub fn has_standard_deviation(self, column: impl Into<String>, assertion: Assertion) -> Self {
        self.statistic(
            column,
            crate::constraints::StatisticType::StandardDeviation,
            assertion,
        )
    }

    /// Adds a variance constraint for a column.
    ///
    /// This is a convenience method for `statistic()` with `StatisticType::Variance`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, Level};
    /// use term_core::constraints::Assertion;
    ///
    /// let check = Check::builder("score_validation")
    ///     .level(Level::Warning)
    ///     .has_variance("score", Assertion::LessThan(250.0))
    ///     .build();
    /// ```
    pub fn has_variance(self, column: impl Into<String>, assertion: Assertion) -> Self {
        self.statistic(
            column,
            crate::constraints::StatisticType::Variance,
            assertion,
        )
    }

    /// Adds a constraint using a fluent constraint builder.
    ///
    /// This method provides the most flexible API for building complex constraints
    /// with full access to all unified constraint features.
    ///
    /// # Arguments
    ///
    /// * `constraint` - A constraint built using the fluent API
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::{Check, ConstraintOptions, LogicalOperator};
    /// use term_core::constraints::{CompletenessConstraint, LengthConstraint, LengthAssertion};
    ///
    /// let check = Check::builder("advanced_validation")
    ///     .with_constraint(
    ///         CompletenessConstraint::new(
    ///             vec!["phone", "email"],
    ///             ConstraintOptions::new()
    ///                 .with_operator(LogicalOperator::Any)
    ///                 .with_threshold(0.99)
    ///         )
    ///     )
    ///     .with_constraint(
    ///         LengthConstraint::new("description", LengthAssertion::Between(50, 2000))
    ///     )
    ///     .build();
    /// ```
    pub fn with_constraint(mut self, constraint: impl crate::core::Constraint + 'static) -> Self {
        self.constraints.push(Arc::new(constraint));
        self
    }

    // ========================================================================
    // CONVENIENCE METHODS FOR COMMON PATTERNS
    // ========================================================================

    /// Convenience method for requiring any of multiple columns to be complete.
    ///
    /// Equivalent to `completeness(columns, ConstraintOptions::new().with_operator(LogicalOperator::Any).with_threshold(1.0))`
    /// but more concise for this common pattern.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::Check;
    ///
    /// let check = Check::builder("contact_validation")
    ///     .any_complete(vec!["phone", "email", "address"])
    ///     .build();
    /// ```
    pub fn any_complete<I, S>(self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        use crate::core::{ConstraintOptions, LogicalOperator};
        let cols: Vec<String> = columns.into_iter().map(Into::into).collect();
        self.completeness(
            cols,
            ConstraintOptions::new()
                .with_operator(LogicalOperator::Any)
                .with_threshold(1.0),
        )
    }

    /// Convenience method for requiring at least N columns to meet a threshold.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::Check;
    ///
    /// let check = Check::builder("contact_validation")
    ///     .at_least_complete(2, vec!["email", "phone", "address", "postal_code"], 0.9)
    ///     .build();
    /// ```
    pub fn at_least_complete<I, S>(self, n: usize, columns: I, threshold: f64) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        use crate::core::{ConstraintOptions, LogicalOperator};
        let cols: Vec<String> = columns.into_iter().map(Into::into).collect();
        self.completeness(
            cols,
            ConstraintOptions::new()
                .with_operator(LogicalOperator::AtLeast(n))
                .with_threshold(threshold),
        )
    }

    /// Convenience method for exactly N columns meeting a threshold.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::Check;
    ///
    /// let check = Check::builder("balance_validation")
    ///     .exactly_complete(1, vec!["primary_phone", "secondary_phone"], 1.0)
    ///     .build();
    /// ```
    pub fn exactly_complete<I, S>(self, n: usize, columns: I, threshold: f64) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        use crate::core::{ConstraintOptions, LogicalOperator};
        let cols: Vec<String> = columns.into_iter().map(Into::into).collect();
        self.completeness(
            cols,
            ConstraintOptions::new()
                .with_operator(LogicalOperator::Exactly(n))
                .with_threshold(threshold),
        )
    }

    /// Builds the `Check` instance.
    ///
    /// # Returns
    ///
    /// The constructed `Check`
    pub fn build(self) -> Check {
        Check {
            name: self.name,
            level: self.level,
            description: self.description,
            constraints: self.constraints,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;
    use async_trait::async_trait;
    use datafusion::prelude::*;

    #[derive(Debug)]
    struct DummyConstraint {
        name: String,
    }

    #[async_trait]
    impl Constraint for DummyConstraint {
        async fn evaluate(&self, _ctx: &SessionContext) -> Result<crate::core::ConstraintResult> {
            Ok(crate::core::ConstraintResult::success())
        }

        fn name(&self) -> &str {
            &self.name
        }

        fn metadata(&self) -> crate::core::ConstraintMetadata {
            crate::core::ConstraintMetadata::new()
        }
    }

    #[test]
    fn test_check_builder() {
        let check = Check::builder("test_check")
            .level(Level::Error)
            .description("Test check description")
            .constraint(DummyConstraint {
                name: "constraint1".to_string(),
            })
            .build();

        assert_eq!(check.name(), "test_check");
        assert_eq!(check.level(), Level::Error);
        assert_eq!(check.description(), Some("Test check description"));
        assert_eq!(check.constraints().len(), 1);
    }

    #[test]
    fn test_check_default_level() {
        let check = Check::builder("test_check").build();
        assert_eq!(check.level(), Level::Warning);
    }

    #[test]
    fn test_check_builder_completeness() {
        use crate::core::ConstraintOptions;

        let check = Check::builder("completeness_check")
            .level(Level::Error)
            .completeness("user_id", ConstraintOptions::new().with_threshold(1.0))
            .completeness("email", ConstraintOptions::new().with_threshold(0.95))
            .build();

        assert_eq!(check.name(), "completeness_check");
        assert_eq!(check.level(), Level::Error);
        assert_eq!(check.constraints().len(), 2);
    }

    #[test]
    fn test_check_builder_uniqueness() {
        let check = Check::builder("uniqueness_check")
            .validates_uniqueness(vec!["user_id"], 1.0)
            .validates_uniqueness(vec!["first_name", "last_name"], 1.0)
            .validates_uniqueness(vec!["email"], 0.99)
            .build();

        assert_eq!(check.constraints().len(), 3);
    }

    #[test]
    fn test_check_builder_method_chaining() {
        use crate::core::ConstraintOptions;

        let check = Check::builder("comprehensive_check")
            .level(Level::Error)
            .description("Comprehensive data quality check")
            .completeness("id", ConstraintOptions::new().with_threshold(1.0))
            .completeness("name", ConstraintOptions::new().with_threshold(0.9))
            .validates_uniqueness(vec!["id"], 1.0)
            .validates_uniqueness(vec!["email", "phone"], 1.0)
            .build();

        assert_eq!(check.name(), "comprehensive_check");
        assert_eq!(check.level(), Level::Error);
        assert_eq!(
            check.description(),
            Some("Comprehensive data quality check")
        );
        assert_eq!(check.constraints().len(), 4);
    }

    #[test]
    fn test_check_builder_multiple_completeness() {
        use crate::core::{ConstraintOptions, LogicalOperator};

        let check = Check::builder("multi_completeness_check")
            .completeness(
                vec!["user_id", "email", "name"],
                ConstraintOptions::new()
                    .with_operator(LogicalOperator::All)
                    .with_threshold(1.0),
            )
            .any_complete(vec!["phone", "mobile", "fax"])
            .build();

        assert_eq!(check.constraints().len(), 2);
    }

    #[test]
    #[should_panic(expected = "Threshold must be between 0.0 and 1.0")]
    fn test_check_builder_invalid_completeness_threshold() {
        use crate::core::ConstraintOptions;

        Check::builder("test")
            .completeness("column", ConstraintOptions::new().with_threshold(1.5))
            .build();
    }

    #[test]
    #[should_panic(expected = "Invalid columns or threshold")]
    fn test_check_builder_invalid_uniqueness_threshold() {
        Check::builder("test")
            .validates_uniqueness(vec!["column"], -0.1)
            .build();
    }

    #[test]
    fn test_check_builder_string_length() {
        let check = Check::builder("string_length_check")
            .has_min_length("password", 8)
            .has_max_length("username", 20)
            .build();

        assert_eq!(check.constraints().len(), 2);
    }

    #[test]
    fn test_unified_completeness_api() {
        use crate::core::{ConstraintOptions, LogicalOperator};

        let check = Check::builder("unified_completeness_test")
            // Single column with threshold
            .completeness("email", ConstraintOptions::new().with_threshold(0.95))
            // Multiple columns with ANY operator
            .completeness(
                vec!["phone", "email", "address"],
                ConstraintOptions::new()
                    .with_operator(LogicalOperator::Any)
                    .with_threshold(1.0),
            )
            // At least 2 columns with threshold
            .completeness(
                vec!["a", "b", "c", "d"],
                ConstraintOptions::new()
                    .with_operator(LogicalOperator::AtLeast(2))
                    .with_threshold(0.9),
            )
            .build();

        assert_eq!(check.constraints().len(), 3);
    }

    #[test]
    fn test_unified_length_api() {
        use crate::constraints::LengthAssertion;

        let check = Check::builder("unified_length_test")
            .length("password", LengthAssertion::Min(8))
            .length("username", LengthAssertion::Between(3, 20))
            .length("code", LengthAssertion::Exactly(6))
            .length("name", LengthAssertion::NotEmpty)
            .build();

        assert_eq!(check.constraints().len(), 4);
    }

    #[test]
    fn test_unified_statistics_api() {
        use crate::constraints::{Assertion, StatisticType};

        let check = Check::builder("unified_statistics_test")
            .statistic(
                "age",
                StatisticType::Min,
                Assertion::GreaterThanOrEqual(0.0),
            )
            .statistic("age", StatisticType::Max, Assertion::LessThanOrEqual(120.0))
            .statistic(
                "salary",
                StatisticType::Mean,
                Assertion::Between(50000.0, 100000.0),
            )
            .statistic(
                "response_time",
                StatisticType::StandardDeviation,
                Assertion::LessThan(100.0),
            )
            .build();

        assert_eq!(check.constraints().len(), 4);
    }

    #[test]
    fn test_convenience_methods() {
        let check = Check::builder("convenience_test")
            .any_complete(vec!["phone", "email", "address"])
            .at_least_complete(2, vec!["a", "b", "c", "d"], 0.9)
            .exactly_complete(1, vec!["primary", "secondary"], 1.0)
            .build();

        assert_eq!(check.constraints().len(), 3);
    }

    #[test]
    fn test_with_constraint_method() {
        use crate::constraints::{LengthAssertion, LengthConstraint};

        let constraint = LengthConstraint::new("test", LengthAssertion::Between(5, 50));
        let check = Check::builder("with_constraint_test")
            .with_constraint(constraint)
            .build();

        assert_eq!(check.constraints().len(), 1);
    }

    #[test]
    fn test_enhanced_format_validation_methods() {
        let check = Check::builder("enhanced_format_test")
            // Enhanced email validation with options
            .validates_email_with_options(
                "email",
                0.95,
                FormatOptions::new()
                    .case_sensitive(false)
                    .trim_before_check(true)
                    .null_is_valid(false),
            )
            // Enhanced URL validation with options
            .validates_url_with_options(
                "website",
                0.90,
                true, // allow localhost
                FormatOptions::new()
                    .case_sensitive(false)
                    .trim_before_check(true),
            )
            // Enhanced phone validation with options
            .validates_phone_with_options(
                "phone",
                0.95,
                Some("US".to_string()),
                FormatOptions::new().trim_before_check(true),
            )
            // Enhanced regex validation with options
            .validates_regex_with_options(
                "product_code",
                r"^[A-Z]{2}\d{4}$",
                0.98,
                FormatOptions::new()
                    .case_sensitive(false)
                    .trim_before_check(true),
            )
            .build();

        assert_eq!(check.constraints().len(), 4);
        assert_eq!(check.name(), "enhanced_format_test");
    }

    #[test]
    fn test_enhanced_vs_basic_format_methods() {
        // Test that basic and enhanced methods can be used together
        let check = Check::builder("mixed_format_test")
            // Basic methods
            .validates_email("basic_email", 0.90)
            .validates_url("basic_url", 0.85, false)
            .validates_phone("basic_phone", 0.80, None)
            .validates_regex("basic_pattern", r"^\d+$", 0.75)
            // Enhanced methods
            .validates_email_with_options(
                "enhanced_email",
                0.95,
                FormatOptions::new().case_sensitive(false),
            )
            .validates_url_with_options(
                "enhanced_url",
                0.90,
                true,
                FormatOptions::new().trim_before_check(true),
            )
            .build();

        assert_eq!(check.constraints().len(), 6);
    }
}
