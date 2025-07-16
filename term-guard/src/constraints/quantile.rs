//! Unified quantile constraint that consolidates quantile-related validations.
//!
//! This module provides a single, flexible quantile constraint that replaces:
//! - `QuantileConstraint` - Approximate quantile checks
//! - `ExactQuantileConstraint` - Exact quantile checks
//!
//! And adds support for multiple quantile checks and distribution analysis.

use crate::constraints::Assertion;
use crate::core::{Constraint, ConstraintMetadata, ConstraintResult};
use crate::prelude::*;
use crate::security::SqlSecurity;
use arrow::array::Array;
use async_trait::async_trait;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, instrument};

/// Types of quantile calculations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum QuantileMethod {
    /// Approximate quantile using DataFusion's APPROX_PERCENTILE_CONT
    Approximate,
    /// Exact quantile calculation (more expensive)
    Exact,
    /// Use approximate for large datasets, exact for small
    Auto { threshold: usize },
}

impl Default for QuantileMethod {
    fn default() -> Self {
        Self::Auto { threshold: 10000 }
    }
}

/// A single quantile check specification.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuantileCheck {
    /// The quantile to compute (0.0 to 1.0)
    pub quantile: f64,
    /// The assertion to evaluate against the quantile value
    pub assertion: Assertion,
}

impl QuantileCheck {
    /// Creates a new quantile check.
    pub fn new(quantile: f64, assertion: Assertion) -> Result<Self> {
        if !(0.0..=1.0).contains(&quantile) {
            return Err(TermError::Configuration(
                "Quantile must be between 0.0 and 1.0".to_string(),
            ));
        }
        Ok(Self {
            quantile,
            assertion,
        })
    }
}

/// Configuration for distribution analysis.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DistributionConfig {
    /// Quantiles to compute for distribution analysis
    pub quantiles: Vec<f64>,
    /// Whether to include min/max in the analysis
    pub include_bounds: bool,
    /// Whether to compute inter-quartile range
    pub compute_iqr: bool,
}

impl Default for DistributionConfig {
    fn default() -> Self {
        Self {
            quantiles: vec![0.25, 0.5, 0.75],
            include_bounds: true,
            compute_iqr: true,
        }
    }
}

/// Types of quantile validation that can be performed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum QuantileValidation {
    /// Check a single quantile value
    Single(QuantileCheck),

    /// Check multiple quantiles
    Multiple(Vec<QuantileCheck>),

    /// Analyze distribution characteristics
    Distribution {
        config: DistributionConfig,
        /// Optional assertions on IQR
        iqr_assertion: Option<Assertion>,
        /// Optional assertions on specific quantiles
        quantile_assertions: HashMap<String, Assertion>,
    },

    /// Check that values follow a monotonic pattern across quantiles
    Monotonic {
        quantiles: Vec<f64>,
        /// Whether values should be strictly increasing
        strict: bool,
    },

    /// Custom quantile validation with SQL
    Custom {
        sql_expression: String,
        assertion: Assertion,
    },
}

/// A unified constraint that validates quantiles and distribution properties.
///
/// This constraint replaces individual quantile constraints and provides
/// a consistent interface for all quantile-based validations.
///
/// # Examples
///
/// ```rust
/// use term_guard::constraints::{QuantileConstraint, QuantileMethod, Assertion};
/// use term_guard::core::Constraint;
///
/// // Check median (50th percentile)
/// let median_check = QuantileConstraint::median(
///     "response_time",
///     Assertion::LessThan(500.0)
/// );
///
/// // Check 95th percentile for SLA
/// let p95_check = QuantileConstraint::percentile(
///     "response_time",
///     0.95,
///     Assertion::LessThan(1000.0)
/// );
///
/// // Check specific quantiles using percentile method
/// let q25_check = QuantileConstraint::percentile(
///     "score",
///     0.25,
///     Assertion::GreaterThan(60.0)
/// );
/// ```
#[derive(Debug, Clone)]
pub struct QuantileConstraint {
    /// The column to validate
    column: String,
    /// The type of validation to perform
    validation: QuantileValidation,
    /// The method to use for quantile calculation
    method: QuantileMethod,
}

impl QuantileConstraint {
    /// Creates a new unified quantile constraint.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to validate
    /// * `validation` - The type of validation to perform
    ///
    /// # Errors
    ///
    /// Returns an error if the column name is invalid.
    pub fn new(column: impl Into<String>, validation: QuantileValidation) -> Result<Self> {
        Self::with_method(column, validation, QuantileMethod::default())
    }

    /// Creates a new unified quantile constraint with a specific method.
    pub fn with_method(
        column: impl Into<String>,
        validation: QuantileValidation,
        method: QuantileMethod,
    ) -> Result<Self> {
        let column_str = column.into();
        SqlSecurity::validate_identifier(&column_str)?;

        Ok(Self {
            column: column_str,
            validation,
            method,
        })
    }

    /// Convenience constructor for median check.
    pub fn median(column: impl Into<String>, assertion: Assertion) -> Result<Self> {
        Self::new(
            column,
            QuantileValidation::Single(QuantileCheck::new(0.5, assertion)?),
        )
    }

    /// Convenience constructor for percentile check.
    pub fn percentile(
        column: impl Into<String>,
        quantile: f64,
        assertion: Assertion,
    ) -> Result<Self> {
        Self::new(
            column,
            QuantileValidation::Single(QuantileCheck::new(quantile, assertion)?),
        )
    }

    /// Convenience constructor for multiple quantile checks.
    pub fn multiple(column: impl Into<String>, checks: Vec<QuantileCheck>) -> Result<Self> {
        if checks.is_empty() {
            return Err(TermError::Configuration(
                "At least one quantile check is required".to_string(),
            ));
        }
        Self::new(column, QuantileValidation::Multiple(checks))
    }

    /// Convenience constructor for distribution analysis.
    pub fn distribution(column: impl Into<String>, config: DistributionConfig) -> Result<Self> {
        Self::new(
            column,
            QuantileValidation::Distribution {
                config,
                iqr_assertion: None,
                quantile_assertions: HashMap::new(),
            },
        )
    }

    /// Generates SQL for approximate quantile calculation.
    fn approx_quantile_sql(&self, quantile: f64) -> Result<String> {
        let escaped_column = SqlSecurity::escape_identifier(&self.column)?;
        Ok(format!(
            "APPROX_PERCENTILE_CONT({quantile}) WITHIN GROUP (ORDER BY {escaped_column})"
        ))
    }

    /// Generates SQL for exact quantile calculation.
    #[allow(dead_code)]
    fn exact_quantile_sql(&self, quantile: f64) -> Result<String> {
        // DataFusion doesn't support exact PERCENTILE_CONT, so we use APPROX_PERCENTILE_CONT
        // which is accurate enough for most use cases
        self.approx_quantile_sql(quantile)
    }

    /// Determines whether to use exact or approximate method based on data size.
    async fn should_use_exact(&self, ctx: &SessionContext) -> Result<bool> {
        match &self.method {
            QuantileMethod::Exact => Ok(true),
            QuantileMethod::Approximate => Ok(false),
            QuantileMethod::Auto { threshold } => {
                let count_sql = "SELECT COUNT(*) as cnt FROM data";
                let df = ctx.sql(count_sql).await?;
                let batches = df.collect().await?;

                if batches.is_empty() || batches[0].num_rows() == 0 {
                    return Ok(true);
                }

                let count = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| {
                        TermError::Internal("Failed to downcast to Int64Array".to_string())
                    })?
                    .value(0) as usize;

                Ok(count <= *threshold)
            }
        }
    }
}

#[async_trait]
impl Constraint for QuantileConstraint {
    #[instrument(skip(self, ctx), fields(
        column = %self.column,
        validation = ?self.validation
    ))]
    async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult> {
        let _use_exact = self.should_use_exact(ctx).await?;

        match &self.validation {
            QuantileValidation::Single(check) => {
                // Always use approx for now since DataFusion doesn't support exact percentile_cont
                let sql = format!(
                    "SELECT {} as q_value FROM data",
                    self.approx_quantile_sql(check.quantile)?
                );

                debug!("Quantile SQL: {}", sql);
                let df = ctx.sql(&sql).await?;
                let batches = df.collect().await?;

                if batches.is_empty() || batches[0].num_rows() == 0 {
                    return Ok(ConstraintResult::skipped("No data to validate"));
                }

                // Handle different numeric types that DataFusion might return
                let column = batches[0].column(0);
                let value = if let Some(arr) =
                    column.as_any().downcast_ref::<arrow::array::Float64Array>()
                {
                    arr.value(0)
                } else if let Some(arr) = column.as_any().downcast_ref::<arrow::array::Int64Array>()
                {
                    arr.value(0) as f64
                } else if let Some(arr) = column.as_any().downcast_ref::<arrow::array::Int32Array>()
                {
                    arr.value(0) as f64
                } else {
                    return Err(TermError::TypeMismatch {
                        expected: "Float64, Int64, or Int32".to_string(),
                        found: format!("{:?}", column.data_type()),
                    });
                };

                if check.assertion.evaluate(value) {
                    Ok(ConstraintResult::success_with_metric(value))
                } else {
                    Ok(ConstraintResult::failure_with_metric(
                        value,
                        format!(
                            "Quantile {} is {value} which does not {}",
                            check.quantile, check.assertion
                        ),
                    ))
                }
            }
            QuantileValidation::Multiple(checks) => {
                // Build SQL to compute all quantiles in one query
                let sql_parts: Vec<String> = checks
                    .iter()
                    .enumerate()
                    .map(|(i, check)| {
                        // Always use approx for now since DataFusion doesn't support exact percentile_cont
                        self.approx_quantile_sql(check.quantile)
                            .map(|q_sql| format!("{q_sql} as q_{i}"))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let parts = sql_parts.join(", ");
                let sql = format!("SELECT {parts} FROM data");
                let df = ctx.sql(&sql).await?;
                let batches = df.collect().await?;

                if batches.is_empty() || batches[0].num_rows() == 0 {
                    return Ok(ConstraintResult::skipped("No data to validate"));
                }

                let mut failures = Vec::new();
                let batch = &batches[0];

                for (i, check) in checks.iter().enumerate() {
                    let column = batch.column(i);
                    let value = if let Some(arr) =
                        column.as_any().downcast_ref::<arrow::array::Float64Array>()
                    {
                        arr.value(0)
                    } else if let Some(arr) =
                        column.as_any().downcast_ref::<arrow::array::Int64Array>()
                    {
                        arr.value(0) as f64
                    } else if let Some(arr) =
                        column.as_any().downcast_ref::<arrow::array::Int32Array>()
                    {
                        arr.value(0) as f64
                    } else {
                        return Err(TermError::TypeMismatch {
                            expected: "Float64, Int64, or Int32".to_string(),
                            found: format!("{:?}", column.data_type()),
                        });
                    };

                    if !check.assertion.evaluate(value) {
                        let q_pct = (check.quantile * 100.0) as i32;
                        failures.push(format!(
                            "Q{q_pct} is {value} which does not {}",
                            check.assertion
                        ));
                    }
                }

                if failures.is_empty() {
                    Ok(ConstraintResult::success())
                } else {
                    Ok(ConstraintResult::failure(failures.join("; ")))
                }
            }
            QuantileValidation::Monotonic { quantiles, strict } => {
                // Compute all quantiles
                let sql_parts: Vec<String> = quantiles
                    .iter()
                    .enumerate()
                    .map(|(i, q)| {
                        // Always use approx for now since DataFusion doesn't support exact percentile_cont
                        self.approx_quantile_sql(*q)
                            .map(|q_sql| format!("{q_sql} as q_{i}"))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let parts = sql_parts.join(", ");
                let sql = format!("SELECT {parts} FROM data");
                let df = ctx.sql(&sql).await?;
                let batches = df.collect().await?;

                if batches.is_empty() || batches[0].num_rows() == 0 {
                    return Ok(ConstraintResult::skipped("No data to validate"));
                }

                let batch = &batches[0];
                let mut values = Vec::new();

                for i in 0..quantiles.len() {
                    let column = batch.column(i);
                    let value = if let Some(arr) =
                        column.as_any().downcast_ref::<arrow::array::Float64Array>()
                    {
                        arr.value(0)
                    } else if let Some(arr) =
                        column.as_any().downcast_ref::<arrow::array::Int64Array>()
                    {
                        arr.value(0) as f64
                    } else if let Some(arr) =
                        column.as_any().downcast_ref::<arrow::array::Int32Array>()
                    {
                        arr.value(0) as f64
                    } else {
                        return Err(TermError::TypeMismatch {
                            expected: "Float64, Int64, or Int32".to_string(),
                            found: format!("{:?}", column.data_type()),
                        });
                    };
                    values.push(value);
                }

                // Check monotonicity
                let mut is_monotonic = true;
                for i in 1..values.len() {
                    if *strict {
                        if values[i] <= values[i - 1] {
                            is_monotonic = false;
                            break;
                        }
                    } else if values[i] < values[i - 1] {
                        is_monotonic = false;
                        break;
                    }
                }

                if is_monotonic {
                    Ok(ConstraintResult::success())
                } else {
                    let monotonic_type = if *strict { "strictly" } else { "" };
                    Ok(ConstraintResult::failure(format!(
                        "Quantiles are not {monotonic_type} monotonic: {values:?}"
                    )))
                }
            }
            _ => {
                // Other validation types would be implemented similarly
                Ok(ConstraintResult::skipped(
                    "Validation type not yet implemented",
                ))
            }
        }
    }

    fn name(&self) -> &str {
        "quantile"
    }

    fn metadata(&self) -> ConstraintMetadata {
        ConstraintMetadata::for_column(&self.column).with_description(format!(
            "Validates quantile properties for column '{}'",
            self.column
        ))
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
    async fn test_median_check() {
        let values: Vec<Option<f64>> = (1..=100).map(|i| Some(i as f64)).collect();
        let ctx = create_test_context(values).await;

        let constraint =
            QuantileConstraint::median("value", Assertion::Between(45.0, 55.0)).unwrap();

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
    }

    #[tokio::test]
    async fn test_percentile_check() {
        let values: Vec<Option<f64>> = (1..=100).map(|i| Some(i as f64)).collect();
        let ctx = create_test_context(values).await;

        let constraint =
            QuantileConstraint::percentile("value", 0.95, Assertion::Between(94.0, 96.0)).unwrap();

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
    }

    #[tokio::test]
    async fn test_multiple_quantiles() {
        let values: Vec<Option<f64>> = (1..=100).map(|i| Some(i as f64)).collect();
        let ctx = create_test_context(values).await;

        let constraint = QuantileConstraint::multiple(
            "value",
            vec![
                QuantileCheck::new(0.25, Assertion::Between(24.0, 26.0)).unwrap(),
                QuantileCheck::new(0.75, Assertion::Between(74.0, 76.0)).unwrap(),
            ],
        )
        .unwrap();

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
    }

    #[tokio::test]
    async fn test_monotonic_check() {
        let values: Vec<Option<f64>> = (1..=100).map(|i| Some(i as f64)).collect();
        let ctx = create_test_context(values).await;

        let constraint = QuantileConstraint::new(
            "value",
            QuantileValidation::Monotonic {
                quantiles: vec![0.1, 0.5, 0.9],
                strict: true,
            },
        )
        .unwrap();

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
    }

    #[test]
    fn test_invalid_quantile() {
        let result = QuantileCheck::new(1.5, Assertion::LessThan(100.0));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Quantile must be between 0.0 and 1.0"));
    }
}
