//! Extended builder API for unified constraints.
//!
//! This module provides the new unified constraint API for the Check builder,
//! with improved ergonomics and consistency while maintaining backward compatibility.

use crate::constraints::{
    Assertion, FormatOptions, FormatType, StatisticType, UniquenessOptions, UniquenessType,
};
use crate::core::{CheckBuilder, ConstraintOptions, LogicalOperator};
use crate::prelude::*;

/// Options for completeness constraints with fluent builder pattern.
#[derive(Debug, Clone)]
pub struct CompletenessOptions {
    threshold: Option<f64>,
    operator: LogicalOperator,
    null_is_failure: bool,
}

impl CompletenessOptions {
    /// Creates options that require 100% completeness.
    pub fn full() -> Self {
        Self {
            threshold: Some(1.0),
            operator: LogicalOperator::All,
            null_is_failure: true,
        }
    }

    /// Creates options with a specific threshold.
    pub fn threshold(threshold: f64) -> Self {
        Self {
            threshold: Some(threshold),
            operator: LogicalOperator::All,
            null_is_failure: true,
        }
    }

    /// Creates options where at least N columns must be complete.
    pub fn at_least(n: usize) -> Self {
        Self {
            threshold: None,
            operator: LogicalOperator::AtLeast(n),
            null_is_failure: true,
        }
    }

    /// Creates options where any column being complete satisfies the constraint.
    pub fn any() -> Self {
        Self {
            threshold: None,
            operator: LogicalOperator::Any,
            null_is_failure: true,
        }
    }

    /// Sets the logical operator for multi-column constraints.
    pub fn with_operator(mut self, operator: LogicalOperator) -> Self {
        self.operator = operator;
        self
    }

    /// Sets whether null values should be considered failures.
    pub fn null_handling(mut self, null_is_failure: bool) -> Self {
        self.null_is_failure = null_is_failure;
        self
    }

    /// Converts to ConstraintOptions.
    pub fn into_constraint_options(self) -> ConstraintOptions {
        let mut options = ConstraintOptions::new()
            .with_operator(self.operator)
            .with_flag("null_is_failure", self.null_is_failure);

        if let Some(threshold) = self.threshold {
            options = options.with_threshold(threshold);
        }

        options
    }
}

/// Options for statistical constraints with fluent builder pattern.
#[derive(Debug, Clone)]
pub struct StatisticalOptions {
    statistics: Vec<(StatisticType, Assertion)>,
}

impl Default for StatisticalOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl StatisticalOptions {
    /// Creates a new statistical options builder.
    pub fn new() -> Self {
        Self {
            statistics: Vec::new(),
        }
    }

    /// Adds a minimum value constraint.
    pub fn min(mut self, assertion: Assertion) -> Self {
        self.statistics.push((StatisticType::Min, assertion));
        self
    }

    /// Adds a maximum value constraint.
    pub fn max(mut self, assertion: Assertion) -> Self {
        self.statistics.push((StatisticType::Max, assertion));
        self
    }

    /// Adds a mean/average constraint.
    pub fn mean(mut self, assertion: Assertion) -> Self {
        self.statistics.push((StatisticType::Mean, assertion));
        self
    }

    /// Adds a sum constraint.
    pub fn sum(mut self, assertion: Assertion) -> Self {
        self.statistics.push((StatisticType::Sum, assertion));
        self
    }

    /// Adds a standard deviation constraint.
    pub fn standard_deviation(mut self, assertion: Assertion) -> Self {
        self.statistics
            .push((StatisticType::StandardDeviation, assertion));
        self
    }

    /// Adds a variance constraint.
    pub fn variance(mut self, assertion: Assertion) -> Self {
        self.statistics.push((StatisticType::Variance, assertion));
        self
    }

    /// Adds a median constraint.
    pub fn median(mut self, assertion: Assertion) -> Self {
        self.statistics.push((StatisticType::Median, assertion));
        self
    }

    /// Adds a percentile constraint.
    pub fn percentile(mut self, percentile: f64, assertion: Assertion) -> Self {
        self.statistics
            .push((StatisticType::Percentile(percentile), assertion));
        self
    }

    /// Returns true if multiple statistics are configured.
    pub fn is_multi(&self) -> bool {
        self.statistics.len() > 1
    }

    /// Returns the configured statistics.
    pub fn into_statistics(self) -> Vec<(StatisticType, Assertion)> {
        self.statistics
    }
}

// Note: ConstraintBuilder was removed as it doesn't integrate well with the
// existing builder pattern. Use the individual methods on CheckBuilder instead.

/// Extension methods for CheckBuilder providing the new unified API.
impl CheckBuilder {
    /// Adds statistical constraints using the new unified API.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_guard::core::{Check, builder_extensions::StatisticalOptions};
    /// use term_guard::constraints::Assertion;
    ///
    /// # use term_guard::prelude::*;
    /// # fn example() -> Result<Check> {
    /// // Single statistic
    /// let check = Check::builder("age_stats")
    ///     .statistics(
    ///         "age",
    ///         StatisticalOptions::new()
    ///             .min(Assertion::GreaterThanOrEqual(0.0))
    ///             .max(Assertion::LessThan(150.0))
    ///     )?
    ///     .build();
    ///
    /// // Multiple statistics optimized in one query
    /// let check = Check::builder("response_time_stats")
    ///     .statistics(
    ///         "response_time",
    ///         StatisticalOptions::new()
    ///             .min(Assertion::GreaterThanOrEqual(0.0))
    ///             .max(Assertion::LessThan(5000.0))
    ///             .mean(Assertion::Between(100.0, 1000.0))
    ///             .percentile(0.95, Assertion::LessThan(2000.0))
    ///     )?
    ///     .build();
    /// # Ok(check)
    /// # }
    /// ```
    pub fn statistics(
        self,
        column: impl Into<String>,
        options: StatisticalOptions,
    ) -> Result<Self> {
        let column_str = column.into();
        let stats = options.into_statistics();

        // Use the existing statistic method
        let mut result = self;
        for (stat_type, assertion) in stats {
            result = result.statistic(column_str.clone(), stat_type, assertion);
        }

        Ok(result)
    }

    /// Adds multiple constraints using a fluent builder API.
    ///
    /// Note: This is an advanced API. For simple cases, use the individual builder methods.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_guard::core::Check;
    /// use term_guard::core::builder_extensions::{CompletenessOptions, StatisticalOptions};
    /// use term_guard::constraints::{FormatType, FormatOptions, UniquenessType, UniquenessOptions, Assertion};
    ///
    /// # use term_guard::prelude::*;
    /// let check = Check::builder("user_validation")
    ///     // Use individual methods for constraints
    ///     .completeness("user_id", CompletenessOptions::full().into_constraint_options())
    ///     .completeness("email", CompletenessOptions::threshold(0.95).into_constraint_options())
    ///     .has_format("email", FormatType::Email, 0.95, FormatOptions::default())
    ///     .uniqueness(
    ///         vec!["email"],
    ///         UniquenessType::FullUniqueness { threshold: 1.0 },
    ///         UniquenessOptions::default()
    ///     )
    ///     .build();
    /// ```
    pub fn with_constraints<F>(self, build_fn: F) -> Self
    where
        F: FnOnce(Self) -> Self,
    {
        build_fn(self)
    }
}

/// Convenience methods for common validation patterns.
impl CheckBuilder {
    /// Adds a primary key validation (non-null and unique).
    ///
    /// This is a convenience method that combines completeness and uniqueness constraints.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_guard::core::Check;
    ///
    /// # use term_guard::prelude::*;
    /// # fn example() -> Result<Check> {
    /// let check = Check::builder("primary_key")
    ///     .primary_key(vec!["user_id"])
    ///     .build();
    ///
    /// // Composite primary key
    /// let check = Check::builder("composite_pk")
    ///     .primary_key(vec!["tenant_id", "user_id"])
    ///     .build();
    /// # Ok(check)
    /// # }
    /// ```
    pub fn primary_key<I, S>(self, columns: I) -> Self
    where
        I: IntoIterator<Item = S> + Clone,
        S: Into<String>,
    {
        let columns_vec: Vec<String> = columns.clone().into_iter().map(Into::into).collect();

        self.completeness(
            columns_vec.clone(),
            CompletenessOptions::full().into_constraint_options(),
        )
        .uniqueness(
            columns_vec,
            UniquenessType::FullUniqueness { threshold: 1.0 },
            UniquenessOptions::default(),
        )
    }

    /// Adds email validation with common settings.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_guard::core::Check;
    ///
    /// # use term_guard::prelude::*;
    /// # fn example() -> Result<Check> {
    /// let check = Check::builder("email_validation")
    ///     .email("email_address", 0.95)
    ///     .build();
    /// # Ok(check)
    /// # }
    /// ```
    pub fn email(self, column: impl Into<String>, threshold: f64) -> Self {
        self.has_format(
            column,
            FormatType::Email,
            threshold,
            FormatOptions::new()
                .trim_before_check(true)
                .null_is_valid(false),
        )
    }

    /// Adds URL validation with common settings.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_guard::core::Check;
    ///
    /// # use term_guard::prelude::*;
    /// # fn example() -> Result<Check> {
    /// let check = Check::builder("url_validation")
    ///     .url("website", 0.90)
    ///     .build();
    /// # Ok(check)
    /// # }
    /// ```
    pub fn url(self, column: impl Into<String>, threshold: f64) -> Self {
        self.has_format(
            column,
            FormatType::Url {
                allow_localhost: false,
            },
            threshold,
            FormatOptions::new().trim_before_check(true),
        )
    }

    /// Adds phone number validation.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_guard::core::Check;
    ///
    /// # use term_guard::prelude::*;
    /// # fn example() -> Result<Check> {
    /// let check = Check::builder("phone_validation")
    ///     .phone("contact_phone", 0.90, Some("US"))
    ///     .build();
    /// # Ok(check)
    /// # }
    /// ```
    pub fn phone(
        self,
        column: impl Into<String>,
        threshold: f64,
        country_code: Option<&str>,
    ) -> Self {
        let format_type = FormatType::Phone {
            country: country_code.map(|s| s.to_string()),
        };

        self.has_format(
            column,
            format_type,
            threshold,
            FormatOptions::new().trim_before_check(true),
        )
    }

    /// Adds value range validation (min and max).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_guard::core::Check;
    ///
    /// # use term_guard::prelude::*;
    /// # fn example() -> Result<Check> {
    /// let check = Check::builder("age_range")
    ///     .value_range("age", 0.0, 150.0)?
    ///     .build();
    /// # Ok(check)
    /// # }
    /// ```
    pub fn value_range(self, column: impl Into<String>, min: f64, max: f64) -> Result<Self> {
        self.statistics(
            column,
            StatisticalOptions::new()
                .min(Assertion::GreaterThanOrEqual(min))
                .max(Assertion::LessThanOrEqual(max)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::Check;

    #[test]
    fn test_completeness_options() {
        let full = CompletenessOptions::full();
        let options = full.into_constraint_options();
        assert_eq!(options.threshold_or(0.0), 1.0);
        assert!(options.flag("null_is_failure"));

        let threshold = CompletenessOptions::threshold(0.95);
        let options = threshold.into_constraint_options();
        assert_eq!(options.threshold_or(0.0), 0.95);

        let at_least = CompletenessOptions::at_least(2);
        let options = at_least.into_constraint_options();
        assert!(matches!(
            options.operator_or(LogicalOperator::All),
            LogicalOperator::AtLeast(2)
        ));
    }

    #[test]
    fn test_statistical_options() {
        let options = StatisticalOptions::new()
            .min(Assertion::GreaterThan(0.0))
            .max(Assertion::LessThan(100.0))
            .mean(Assertion::Between(25.0, 75.0));

        assert!(options.is_multi());
        let stats = options.into_statistics();
        assert_eq!(stats.len(), 3);
    }

    #[test]
    fn test_new_builder_api() {
        // Test single column completeness
        let check = Check::builder("test")
            .completeness(
                "user_id",
                CompletenessOptions::full().into_constraint_options(),
            )
            .build();
        assert_eq!(check.constraints().len(), 1);

        // Test multiple column completeness
        let check = Check::builder("test")
            .completeness(
                vec!["email", "phone"],
                CompletenessOptions::at_least(1).into_constraint_options(),
            )
            .build();
        assert_eq!(check.constraints().len(), 1);

        // Test format constraint
        let check = Check::builder("test")
            .has_format("email", FormatType::Email, 0.95, FormatOptions::default())
            .build();
        assert_eq!(check.constraints().len(), 1);

        // Test convenience methods
        let check = Check::builder("test").email("email_field", 0.95).build();
        assert_eq!(check.constraints().len(), 1);
    }
}
