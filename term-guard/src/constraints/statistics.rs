//! Unified statistical constraint that consolidates all statistical checks.
//!
//! This module provides a single, flexible statistical constraint that replaces:
//! - `MinConstraint`
//! - `MaxConstraint`
//! - `MeanConstraint`
//! - `SumConstraint`
//! - `StandardDeviationConstraint`
//!
//! And adds support for new statistics like variance, median, and percentiles.

use crate::constraints::Assertion;
use crate::core::{current_validation_context, Constraint, ConstraintMetadata, ConstraintResult};
use crate::prelude::*;
use crate::security::SqlSecurity;
use arrow::array::Array;
use async_trait::async_trait;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt;
use tracing::instrument;
/// Types of statistics that can be computed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StatisticType {
    /// Minimum value
    Min,
    /// Maximum value
    Max,
    /// Mean/average value
    Mean,
    /// Sum of all values
    Sum,
    /// Standard deviation
    StandardDeviation,
    /// Variance
    Variance,
    /// Median (50th percentile)
    Median,
    /// Specific percentile (0.0 to 1.0)
    Percentile(f64),
}

impl StatisticType {
    /// Returns the SQL function name for this statistic.
    fn sql_function(&self) -> String {
        match self {
            StatisticType::Min => "MIN".to_string(),
            StatisticType::Max => "MAX".to_string(),
            StatisticType::Mean => "AVG".to_string(),
            StatisticType::Sum => "SUM".to_string(),
            StatisticType::StandardDeviation => "STDDEV".to_string(),
            StatisticType::Variance => "VARIANCE".to_string(),
            StatisticType::Median => "APPROX_PERCENTILE_CONT".to_string(),
            StatisticType::Percentile(_) => "APPROX_PERCENTILE_CONT".to_string(),
        }
    }

    /// Returns the SQL expression for this statistic.
    fn sql_expression(&self, column: &str) -> String {
        match self {
            StatisticType::Median => {
                let func = self.sql_function();
                format!("{func}({column}, 0.5)")
            }
            StatisticType::Percentile(p) => {
                let func = self.sql_function();
                format!("{func}({column}, {p})")
            }
            _ => {
                let func = self.sql_function();
                format!("{func}({column})")
            }
        }
    }

    /// Returns a human-readable name for this statistic.
    fn name(&self) -> &str {
        match self {
            StatisticType::Min => "minimum",
            StatisticType::Max => "maximum",
            StatisticType::Mean => "mean",
            StatisticType::Sum => "sum",
            StatisticType::StandardDeviation => "standard deviation",
            StatisticType::Variance => "variance",
            StatisticType::Median => "median",
            StatisticType::Percentile(p) => {
                if (*p - 0.5).abs() < f64::EPSILON {
                    "median"
                } else {
                    "percentile"
                }
            }
        }
    }

    /// Returns the constraint name for backward compatibility.
    fn constraint_name(&self) -> &str {
        match self {
            StatisticType::Min => "min",
            StatisticType::Max => "max",
            StatisticType::Mean => "mean",
            StatisticType::Sum => "sum",
            StatisticType::StandardDeviation => "standard_deviation",
            StatisticType::Variance => "variance",
            StatisticType::Median => "median",
            StatisticType::Percentile(_) => "percentile",
        }
    }
}

impl fmt::Display for StatisticType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StatisticType::Percentile(p) => write!(f, "{}({p})", self.name()),
            _ => write!(f, "{}", self.name()),
        }
    }
}

/// A unified constraint that checks statistical properties of a column.
///
/// This constraint replaces the individual statistical constraints and provides
/// a consistent interface for all statistical checks.
///
/// # Examples
///
/// ```rust
/// use term_guard::constraints::{StatisticalConstraint, StatisticType, Assertion};
/// use term_guard::core::Constraint;
///
/// // Check that mean is between 25 and 35
/// let mean_check = StatisticalConstraint::new(
///     "age",
///     StatisticType::Mean,
///     Assertion::Between(25.0, 35.0)
/// );
///
/// // Check that maximum is less than 100
/// let max_check = StatisticalConstraint::new(
///     "score",
///     StatisticType::Max,
///     Assertion::LessThan(100.0)
/// );
///
/// // Check 95th percentile for SLA
/// let p95_check = StatisticalConstraint::new(
///     "response_time",
///     StatisticType::Percentile(0.95),
///     Assertion::LessThan(1000.0)
/// );
/// ```
#[derive(Debug, Clone)]
pub struct StatisticalConstraint {
    /// The column to compute statistics on
    column: String,
    /// The type of statistic to compute
    statistic: StatisticType,
    /// The assertion to evaluate against the statistic
    assertion: Assertion,
}

impl StatisticalConstraint {
    /// Creates a new statistical constraint.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to check
    /// * `statistic` - The type of statistic to compute
    /// * `assertion` - The assertion to evaluate
    ///
    /// # Errors
    ///
    /// Returns error if column name is invalid or if percentile is out of range.
    pub fn new(
        column: impl Into<String>,
        statistic: StatisticType,
        assertion: Assertion,
    ) -> Result<Self> {
        let column_str = column.into();
        SqlSecurity::validate_identifier(&column_str)?;

        // Validate percentile range
        if let StatisticType::Percentile(p) = &statistic {
            if !(0.0..=1.0).contains(p) {
                return Err(TermError::SecurityError(
                    "Percentile must be between 0.0 and 1.0".to_string(),
                ));
            }
        }

        Ok(Self {
            column: column_str,
            statistic,
            assertion,
        })
    }

    /// Creates a minimum value constraint.
    pub fn min(column: impl Into<String>, assertion: Assertion) -> Result<Self> {
        Self::new(column, StatisticType::Min, assertion)
    }

    /// Creates a maximum value constraint.
    pub fn max(column: impl Into<String>, assertion: Assertion) -> Result<Self> {
        Self::new(column, StatisticType::Max, assertion)
    }

    /// Creates a mean/average constraint.
    pub fn mean(column: impl Into<String>, assertion: Assertion) -> Result<Self> {
        Self::new(column, StatisticType::Mean, assertion)
    }

    /// Creates a sum constraint.
    pub fn sum(column: impl Into<String>, assertion: Assertion) -> Result<Self> {
        Self::new(column, StatisticType::Sum, assertion)
    }

    /// Creates a standard deviation constraint.
    pub fn standard_deviation(column: impl Into<String>, assertion: Assertion) -> Result<Self> {
        Self::new(column, StatisticType::StandardDeviation, assertion)
    }

    /// Creates a variance constraint.
    pub fn variance(column: impl Into<String>, assertion: Assertion) -> Result<Self> {
        Self::new(column, StatisticType::Variance, assertion)
    }

    /// Creates a median constraint.
    pub fn median(column: impl Into<String>, assertion: Assertion) -> Result<Self> {
        Self::new(column, StatisticType::Median, assertion)
    }

    /// Creates a percentile constraint.
    ///
    /// # Errors
    ///
    /// Returns error if column name is invalid or percentile is not between 0.0 and 1.0
    pub fn percentile(
        column: impl Into<String>,
        percentile: f64,
        assertion: Assertion,
    ) -> Result<Self> {
        Self::new(column, StatisticType::Percentile(percentile), assertion)
    }
}

#[async_trait]
impl Constraint for StatisticalConstraint {
    #[instrument(skip(self, ctx), fields(
        column = %self.column,
        statistic = %self.statistic,
        assertion = %self.assertion
    ))]
    async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult> {
        let column_identifier = SqlSecurity::escape_identifier(&self.column)?;
        let stat_expr = self.statistic.sql_expression(&column_identifier);
        // Get the table name from the validation context

        let validation_ctx = current_validation_context();

        let table_name = validation_ctx.table_name();

        let sql = format!("SELECT {stat_expr} as stat_value FROM {table_name}");

        let df = ctx.sql(&sql).await?;
        let batches = df.collect().await?;

        if batches.is_empty() {
            return Ok(ConstraintResult::skipped("No data to validate"));
        }

        let batch = &batches[0];
        if batch.num_rows() == 0 {
            return Ok(ConstraintResult::skipped("No data to validate"));
        }

        // Extract the statistic value - try Int64 first, then Float64
        let value = if let Ok(array) = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| TermError::Internal("Failed to extract statistic value".to_string()))
        {
            if array.is_null(0) {
                let stat_name = self.statistic.name();
                return Ok(ConstraintResult::failure(format!(
                    "{stat_name} is null (no non-null values)"
                )));
            }
            array.value(0) as f64
        } else if let Ok(array) = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .ok_or_else(|| TermError::Internal("Failed to extract statistic value".to_string()))
        {
            if array.is_null(0) {
                let stat_name = self.statistic.name();
                return Ok(ConstraintResult::failure(format!(
                    "{stat_name} is null (no non-null values)"
                )));
            }
            array.value(0)
        } else {
            return Err(TermError::Internal(
                "Failed to extract statistic value".to_string(),
            ));
        };

        if self.assertion.evaluate(value) {
            Ok(ConstraintResult::success_with_metric(value))
        } else {
            Ok(ConstraintResult::failure_with_metric(
                value,
                format!(
                    "{} {value} does not {}",
                    self.statistic.name(),
                    self.assertion
                ),
            ))
        }
    }

    fn name(&self) -> &str {
        self.statistic.constraint_name()
    }

    fn column(&self) -> Option<&str> {
        Some(&self.column)
    }

    fn metadata(&self) -> ConstraintMetadata {
        let mut metadata = ConstraintMetadata::for_column(&self.column)
            .with_description(format!(
                "Checks that {} of {} {}",
                self.statistic.name(),
                self.column,
                self.assertion.description()
            ))
            .with_custom("assertion", self.assertion.to_string())
            .with_custom("statistic_type", self.statistic.to_string())
            .with_custom("constraint_type", "statistical");

        if let StatisticType::Percentile(p) = self.statistic {
            metadata = metadata.with_custom("percentile", p.to_string());
        }

        metadata
    }
}

/// A constraint that can compute multiple statistics in a single query for performance optimization.
///
/// This is useful when you need to validate multiple statistics on the same column,
/// as it reduces the number of table scans required.
///
/// # Examples
///
/// ```rust
/// use term_guard::constraints::{MultiStatisticalConstraint, StatisticType, Assertion};
///
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Check multiple statistics on the same column in one pass
/// let multi_stats = MultiStatisticalConstraint::new(
///     "response_time",
///     vec![
///         (StatisticType::Min, Assertion::GreaterThanOrEqual(0.0)),
///         (StatisticType::Max, Assertion::LessThan(5000.0)),
///         (StatisticType::Mean, Assertion::Between(100.0, 1000.0)),
///         (StatisticType::Percentile(0.95), Assertion::LessThan(2000.0)),
///     ]
/// )?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct MultiStatisticalConstraint {
    column: String,
    statistics: Vec<(StatisticType, Assertion)>,
}

impl MultiStatisticalConstraint {
    /// Creates a new multi-statistical constraint.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to compute statistics on
    /// * `statistics` - A vector of (statistic_type, assertion) pairs to evaluate
    ///
    /// # Errors
    ///
    /// Returns error if column name is invalid or if any percentile is out of range.
    pub fn new(
        column: impl Into<String>,
        statistics: Vec<(StatisticType, Assertion)>,
    ) -> Result<Self> {
        let column_str = column.into();
        SqlSecurity::validate_identifier(&column_str)?;

        // Validate all percentile values
        for (stat, _) in &statistics {
            if let StatisticType::Percentile(p) = stat {
                if !(0.0..=1.0).contains(p) {
                    return Err(TermError::SecurityError(
                        "Percentile must be between 0.0 and 1.0".to_string(),
                    ));
                }
            }
        }

        Ok(Self {
            column: column_str,
            statistics,
        })
    }
}

#[async_trait]
impl Constraint for MultiStatisticalConstraint {
    #[instrument(skip(self, ctx), fields(
        column = %self.column,
        num_statistics = %self.statistics.len()
    ))]
    async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult> {
        let column_identifier = SqlSecurity::escape_identifier(&self.column)?;

        // Build SQL with all statistics computed in one query
        let sql_parts: Vec<String> = self
            .statistics
            .iter()
            .enumerate()
            .map(|(i, (stat, _))| {
                let expr = stat.sql_expression(&column_identifier);
                format!("{expr} as stat_{i}")
            })
            .collect();

        let parts = sql_parts.join(", ");
        // Get the table name from the validation context
        let validation_ctx = current_validation_context();
        let table_name = validation_ctx.table_name();

        let sql = format!("SELECT {parts} FROM {table_name}");

        let df = ctx.sql(&sql).await?;
        let batches = df.collect().await?;

        if batches.is_empty() {
            return Ok(ConstraintResult::skipped("No data to validate"));
        }

        let batch = &batches[0];
        if batch.num_rows() == 0 {
            return Ok(ConstraintResult::skipped("No data to validate"));
        }

        // Check each statistic
        let mut failures = Vec::new();
        let mut all_metrics = Vec::new();

        for (i, (stat_type, assertion)) in self.statistics.iter().enumerate() {
            let column = batch.column(i);

            // Extract value
            let value = if let Some(array) =
                column.as_any().downcast_ref::<arrow::array::Float64Array>()
            {
                if array.is_null(0) {
                    let name = stat_type.name();
                    failures.push(format!("{name} is null"));
                    continue;
                }
                array.value(0)
            } else if let Some(array) = column.as_any().downcast_ref::<arrow::array::Int64Array>() {
                if array.is_null(0) {
                    let name = stat_type.name();
                    failures.push(format!("{name} is null"));
                    continue;
                }
                array.value(0) as f64
            } else {
                let name = stat_type.name();
                failures.push(format!("Failed to compute {name}"));
                continue;
            };

            all_metrics.push((stat_type.name().to_string(), value));

            if !assertion.evaluate(value) {
                failures.push(format!(
                    "{} is {value} which does not {assertion}",
                    stat_type.name()
                ));
            }
        }

        if failures.is_empty() {
            // All assertions passed - return the first metric as representative
            let first_metric = all_metrics.first().map(|(_, v)| *v).unwrap_or(0.0);
            Ok(ConstraintResult::success_with_metric(first_metric))
        } else {
            Ok(ConstraintResult::failure(failures.join("; ")))
        }
    }

    fn name(&self) -> &str {
        "multi_statistical"
    }

    fn column(&self) -> Option<&str> {
        Some(&self.column)
    }

    fn metadata(&self) -> ConstraintMetadata {
        let stat_names: Vec<String> = self
            .statistics
            .iter()
            .map(|(s, _)| s.name().to_string())
            .collect();

        ConstraintMetadata::for_column(&self.column)
            .with_description({
                let stats = stat_names.join(", ");
                format!(
                    "Checks multiple statistics ({stats}) for column {}",
                    self.column
                )
            })
            .with_custom("statistics_count", self.statistics.len().to_string())
            .with_custom("constraint_type", "multi_statistical")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ConstraintStatus;
    use arrow::array::Float64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use std::sync::Arc;

    use crate::test_helpers::evaluate_constraint_with_context;
    async fn create_test_context(values: Vec<Option<f64>>) -> SessionContext {
        let ctx = SessionContext::new();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            true,
        )]));

        let array = Float64Array::from(values);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();

        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("data", Arc::new(provider)).unwrap();

        ctx
    }

    #[tokio::test]
    async fn test_mean_constraint() {
        let ctx = create_test_context(vec![Some(10.0), Some(20.0), Some(30.0)]).await;
        let constraint = StatisticalConstraint::mean("value", Assertion::Equals(20.0)).unwrap();

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(20.0));
    }

    #[tokio::test]
    async fn test_min_max_constraints() {
        let ctx = create_test_context(vec![Some(5.0), Some(10.0), Some(15.0)]).await;

        let min_constraint = StatisticalConstraint::min("value", Assertion::Equals(5.0)).unwrap();
        let result = evaluate_constraint_with_context(&min_constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(5.0));

        let max_constraint = StatisticalConstraint::max("value", Assertion::Equals(15.0)).unwrap();
        let result = evaluate_constraint_with_context(&max_constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(15.0));
    }

    #[tokio::test]
    async fn test_sum_constraint() {
        let ctx = create_test_context(vec![Some(10.0), Some(20.0), Some(30.0)]).await;
        let constraint = StatisticalConstraint::sum("value", Assertion::Equals(60.0)).unwrap();

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(60.0));
    }

    #[tokio::test]
    async fn test_with_nulls() {
        let ctx = create_test_context(vec![Some(10.0), None, Some(20.0)]).await;
        let constraint = StatisticalConstraint::mean("value", Assertion::Equals(15.0)).unwrap();

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(15.0));
    }

    #[tokio::test]
    async fn test_all_nulls() {
        let ctx = create_test_context(vec![None, None, None]).await;
        let constraint = StatisticalConstraint::mean("value", Assertion::Equals(0.0)).unwrap();

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Failure);
        assert!(result.message.unwrap().contains("null"));
    }

    #[test]
    fn test_statistic_type_display() {
        assert_eq!(StatisticType::Min.to_string(), "minimum");
        assert_eq!(StatisticType::Mean.to_string(), "mean");
        assert_eq!(
            StatisticType::Percentile(0.95).to_string(),
            "percentile(0.95)"
        );
        assert_eq!(StatisticType::Median.to_string(), "median");
    }

    #[tokio::test]
    async fn test_multi_statistical_constraint() {
        let ctx = create_test_context(vec![Some(10.0), Some(20.0), Some(30.0), Some(40.0)]).await;

        let constraint = MultiStatisticalConstraint::new(
            "value",
            vec![
                (StatisticType::Min, Assertion::GreaterThanOrEqual(10.0)),
                (StatisticType::Max, Assertion::LessThanOrEqual(40.0)),
                (StatisticType::Mean, Assertion::Equals(25.0)),
                (StatisticType::Sum, Assertion::Equals(100.0)),
            ],
        )
        .unwrap();

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
    }

    #[tokio::test]
    async fn test_multi_statistical_constraint_failure() {
        let ctx = create_test_context(vec![Some(10.0), Some(20.0), Some(30.0)]).await;

        let constraint = MultiStatisticalConstraint::new(
            "value",
            vec![
                (StatisticType::Min, Assertion::Equals(5.0)), // Will fail
                (StatisticType::Max, Assertion::Equals(30.0)),
            ],
        )
        .unwrap();

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Failure);
        assert!(result.message.unwrap().contains("minimum is 10"));
    }

    #[test]
    fn test_invalid_percentile() {
        let result = StatisticalConstraint::new(
            "value",
            StatisticType::Percentile(1.5),
            Assertion::LessThan(100.0),
        );

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Percentile must be between 0.0 and 1.0"));
    }
}
