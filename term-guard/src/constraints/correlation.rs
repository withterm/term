//! Unified correlation constraint that consolidates correlation and relationship validations.
//!
//! This module provides a single, flexible correlation constraint that replaces:
//! - `CorrelationConstraint` - Pearson correlation between columns
//! - `MutualInformationConstraint` - Mutual information between columns
//!
//! And adds support for other correlation types and multi-column relationships.

use crate::constraints::Assertion;
use crate::core::{current_validation_context, Constraint, ConstraintMetadata, ConstraintResult};
use crate::prelude::*;
use crate::security::SqlSecurity;
use arrow::array::Array;
use async_trait::async_trait;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::instrument;
/// Types of correlation that can be computed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CorrelationType {
    /// Pearson correlation coefficient (-1 to 1)
    Pearson,
    /// Spearman rank correlation coefficient
    Spearman,
    /// Kendall's tau correlation
    KendallTau,
    /// Mutual information (non-negative)
    MutualInformation {
        /// Number of bins for discretization
        bins: usize,
    },
    /// Covariance
    Covariance,
    /// Custom correlation using SQL expression
    Custom { sql_expression: String },
}

impl CorrelationType {
    /// Returns a human-readable name for this correlation type.
    fn name(&self) -> &str {
        match self {
            CorrelationType::Pearson => "Pearson correlation",
            CorrelationType::Spearman => "Spearman correlation",
            CorrelationType::KendallTau => "Kendall's tau",
            CorrelationType::MutualInformation { .. } => "mutual information",
            CorrelationType::Covariance => "covariance",
            CorrelationType::Custom { .. } => "custom correlation",
        }
    }

    /// Returns the constraint name for backward compatibility.
    fn constraint_name(&self) -> &str {
        match self {
            CorrelationType::Pearson => "correlation",
            CorrelationType::Spearman => "spearman_correlation",
            CorrelationType::KendallTau => "kendall_correlation",
            CorrelationType::MutualInformation { .. } => "mutual_information",
            CorrelationType::Covariance => "covariance",
            CorrelationType::Custom { .. } => "custom_correlation",
        }
    }
}

/// Configuration for multi-column correlation analysis.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MultiCorrelationConfig {
    /// Columns to analyze (must have at least 2)
    pub columns: Vec<String>,
    /// Type of correlation to compute
    pub correlation_type: CorrelationType,
    /// Whether to compute pairwise correlations
    pub pairwise: bool,
    /// Minimum threshold for correlation strength (optional)
    pub min_correlation: Option<f64>,
}

/// Types of correlation validation that can be performed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CorrelationValidation {
    /// Check correlation between two columns
    Pairwise {
        column1: String,
        column2: String,
        correlation_type: CorrelationType,
        assertion: Assertion,
    },

    /// Check that correlation is within expected range
    Range {
        column1: String,
        column2: String,
        correlation_type: CorrelationType,
        min: f64,
        max: f64,
    },

    /// Check multiple correlations
    MultiColumn(MultiCorrelationConfig),

    /// Check that columns are independent (low correlation)
    Independence {
        column1: String,
        column2: String,
        max_correlation: f64,
    },

    /// Check correlation stability over time/segments
    Stability {
        column1: String,
        column2: String,
        segment_column: String,
        max_variance: f64,
    },
}

/// A unified constraint that validates correlations and relationships between columns.
///
/// This constraint replaces individual correlation constraints and provides
/// a consistent interface for all correlation-based validations.
///
/// # Examples
///
/// ```rust
/// use term_guard::constraints::{CorrelationConstraint, CorrelationType, Assertion};
/// use term_guard::core::Constraint;
///
/// // Check Pearson correlation
/// let pearson_check = CorrelationConstraint::pearson(
///     "height",
///     "weight",
///     Assertion::Between(0.6, 0.9)
/// );
///
/// // Check independence
/// let independence_check = CorrelationConstraint::independence(
///     "user_id",
///     "transaction_amount",
///     0.1
/// );
///
/// // Check mutual information
/// let mi_check = CorrelationConstraint::mutual_information(
///     "category",
///     "price",
///     10, // bins
///     Assertion::GreaterThan(0.5)
/// );
/// ```
#[derive(Debug, Clone)]
pub struct CorrelationConstraint {
    /// The type of validation to perform
    validation: CorrelationValidation,
}

impl CorrelationConstraint {
    /// Creates a new unified correlation constraint.
    ///
    /// # Arguments
    ///
    /// * `validation` - The type of validation to perform
    ///
    /// # Errors
    ///
    /// Returns an error if column names are invalid.
    pub fn new(validation: CorrelationValidation) -> Result<Self> {
        // Validate column names
        match &validation {
            CorrelationValidation::Pairwise {
                column1, column2, ..
            }
            | CorrelationValidation::Range {
                column1, column2, ..
            }
            | CorrelationValidation::Independence {
                column1, column2, ..
            }
            | CorrelationValidation::Stability {
                column1, column2, ..
            } => {
                SqlSecurity::validate_identifier(column1)?;
                SqlSecurity::validate_identifier(column2)?;
            }
            CorrelationValidation::MultiColumn(config) => {
                if config.columns.len() < 2 {
                    return Err(TermError::Configuration(
                        "At least 2 columns required for correlation analysis".to_string(),
                    ));
                }
                for column in &config.columns {
                    SqlSecurity::validate_identifier(column)?;
                }
            }
        }

        Ok(Self { validation })
    }

    /// Convenience constructor for Pearson correlation.
    pub fn pearson(
        column1: impl Into<String>,
        column2: impl Into<String>,
        assertion: Assertion,
    ) -> Result<Self> {
        Self::new(CorrelationValidation::Pairwise {
            column1: column1.into(),
            column2: column2.into(),
            correlation_type: CorrelationType::Pearson,
            assertion,
        })
    }

    /// Convenience constructor for Spearman correlation.
    pub fn spearman(
        column1: impl Into<String>,
        column2: impl Into<String>,
        assertion: Assertion,
    ) -> Result<Self> {
        Self::new(CorrelationValidation::Pairwise {
            column1: column1.into(),
            column2: column2.into(),
            correlation_type: CorrelationType::Spearman,
            assertion,
        })
    }

    /// Convenience constructor for mutual information.
    pub fn mutual_information(
        column1: impl Into<String>,
        column2: impl Into<String>,
        bins: usize,
        assertion: Assertion,
    ) -> Result<Self> {
        Self::new(CorrelationValidation::Pairwise {
            column1: column1.into(),
            column2: column2.into(),
            correlation_type: CorrelationType::MutualInformation { bins },
            assertion,
        })
    }

    /// Convenience constructor for independence check.
    pub fn independence(
        column1: impl Into<String>,
        column2: impl Into<String>,
        max_correlation: f64,
    ) -> Result<Self> {
        if !(0.0..=1.0).contains(&max_correlation) {
            return Err(TermError::Configuration(
                "Max correlation must be between 0.0 and 1.0".to_string(),
            ));
        }
        Self::new(CorrelationValidation::Independence {
            column1: column1.into(),
            column2: column2.into(),
            max_correlation,
        })
    }

    /// Generates SQL for Pearson correlation.
    fn pearson_sql(&self, col1: &str, col2: &str) -> Result<String> {
        let escaped_col1 = SqlSecurity::escape_identifier(col1)?;
        let escaped_col2 = SqlSecurity::escape_identifier(col2)?;

        // Using DataFusion's CORR function
        Ok(format!("CORR({escaped_col1}, {escaped_col2})"))
    }

    /// Generates SQL for covariance.
    fn covariance_sql(&self, col1: &str, col2: &str) -> Result<String> {
        let escaped_col1 = SqlSecurity::escape_identifier(col1)?;
        let escaped_col2 = SqlSecurity::escape_identifier(col2)?;

        // Using DataFusion's COVAR_SAMP function
        Ok(format!("COVAR_SAMP({escaped_col1}, {escaped_col2})"))
    }

    /// Generates SQL for Spearman correlation (rank-based).
    #[allow(dead_code)]
    fn spearman_sql(&self, col1: &str, col2: &str) -> Result<String> {
        let escaped_col1 = SqlSecurity::escape_identifier(col1)?;
        let escaped_col2 = SqlSecurity::escape_identifier(col2)?;

        // Spearman correlation is Pearson correlation of ranks
        // Using window functions to compute ranks
        Ok(format!(
            "CORR(
                RANK() OVER (ORDER BY {escaped_col1}) AS rank1,
                RANK() OVER (ORDER BY {escaped_col2}) AS rank2
            )"
        ))
    }
}

#[async_trait]
impl Constraint for CorrelationConstraint {
    #[instrument(skip(self, ctx), fields(
        validation = ?self.validation
    ))]
    async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult> {
        // Get the table name from the validation context
        let validation_ctx = current_validation_context();
        let table_name = validation_ctx.table_name();
        
        match &self.validation {
            CorrelationValidation::Pairwise {
                column1,
                column2,
                correlation_type,
                assertion,
            } => {
                let sql = match correlation_type {
                    CorrelationType::Pearson => {
                        format!(
                            "SELECT {} as corr_value FROM {table_name}",
                            self.pearson_sql(column1, column2)?
                        )
                    }
                    CorrelationType::Covariance => {
                        format!(
                            "SELECT {} as corr_value FROM {table_name}",
                            self.covariance_sql(column1, column2)?
                        )
                    }
                    CorrelationType::Custom { sql_expression } => {
                        // Basic validation to prevent obvious SQL injection
                        if sql_expression.contains(';')
                            || sql_expression.to_lowercase().contains("drop")
                        {
                            return Ok(ConstraintResult::failure(
                                "Custom SQL expression contains potentially unsafe content",
                            ));
                        }
                        let escaped_col1 = SqlSecurity::escape_identifier(column1)?;
                        let escaped_col2 = SqlSecurity::escape_identifier(column2)?;
                        let expr = sql_expression
                            .replace("{column1}", &escaped_col1)
                            .replace("{column2}", &escaped_col2);
                        format!("SELECT {expr} as corr_value FROM {table_name}")
                    }
                    _ => {
                        // Other correlation types would require more complex implementation
                        return Ok(ConstraintResult::skipped(
                            "Correlation type not yet implemented",
                        ));
                    }
                };

                let df = ctx.sql(&sql).await?;
                let batches = df.collect().await?;

                if batches.is_empty() || batches[0].num_rows() == 0 {
                    return Ok(ConstraintResult::skipped("No data to validate"));
                }

                let value = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .ok_or_else(|| {
                        TermError::Internal("Failed to downcast to Float64Array".to_string())
                    })?
                    .value(0);

                if assertion.evaluate(value) {
                    Ok(ConstraintResult::success_with_metric(value))
                } else {
                    Ok(ConstraintResult::failure_with_metric(
                        value,
                        format!(
                            "{} between {column1} and {column2} is {value} which does not {assertion}",
                            correlation_type.name()
                        ),
                    ))
                }
            }
            CorrelationValidation::Range {
                column1,
                column2,
                correlation_type,
                min,
                max,
            } => {
                // This is essentially the same as Pairwise with a Between assertion
                let result = self
                    .evaluate_with_validation(
                        ctx,
                        &CorrelationValidation::Pairwise {
                            column1: column1.clone(),
                            column2: column2.clone(),
                            correlation_type: correlation_type.clone(),
                            assertion: Assertion::Between(*min, *max),
                        },
                    )
                    .await?;
                Ok(result)
            }
            CorrelationValidation::Independence {
                column1,
                column2,
                max_correlation,
            } => {
                // Get the table name from the validation context

                let validation_ctx = current_validation_context();

                let table_name = validation_ctx.table_name();

                

                let sql = format!(
                    "SELECT ABS({}) as abs_corr FROM {table_name}",
                    self.pearson_sql(column1, column2)?
                );

                let df = ctx.sql(&sql).await?;
                let batches = df.collect().await?;

                if batches.is_empty() || batches[0].num_rows() == 0 {
                    return Ok(ConstraintResult::skipped("No data to validate"));
                }

                let abs_corr = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .ok_or_else(|| {
                        TermError::Internal("Failed to downcast to Float64Array".to_string())
                    })?
                    .value(0);

                if abs_corr <= *max_correlation {
                    Ok(ConstraintResult::success_with_metric(abs_corr))
                } else {
                    Ok(ConstraintResult::failure_with_metric(
                        abs_corr,
                        format!(
                            "Columns {column1} and {column2} have correlation {abs_corr} exceeding independence threshold {max_correlation}"
                        ),
                    ))
                }
            }
            _ => Ok(ConstraintResult::skipped(
                "Validation type not yet implemented",
            )),
        }
    }

    fn name(&self) -> &str {
        match &self.validation {
            CorrelationValidation::Pairwise {
                correlation_type, ..
            } => correlation_type.constraint_name(),
            CorrelationValidation::Range { .. } => "correlation_range",
            CorrelationValidation::Independence { .. } => "independence",
            CorrelationValidation::MultiColumn { .. } => "multi_correlation",
            CorrelationValidation::Stability { .. } => "correlation_stability",
        }
    }

    fn metadata(&self) -> ConstraintMetadata {
        let description = match &self.validation {
            CorrelationValidation::Pairwise {
                column1,
                column2,
                correlation_type,
                ..
            } => format!(
                "Validates {} between '{column1}' and '{column2}'",
                correlation_type.name()
            ),
            CorrelationValidation::Range {
                column1, column2, ..
            } => format!(
                "Validates correlation range between '{column1}' and '{column2}'"
            ),
            CorrelationValidation::Independence {
                column1, column2, ..
            } => format!(
                "Validates independence between '{column1}' and '{column2}'"
            ),
            CorrelationValidation::MultiColumn(config) => format!(
                "Validates correlations among columns: {}",
                config.columns.join(", ")
            ),
            CorrelationValidation::Stability {
                column1,
                column2,
                segment_column,
                ..
            } => format!(
                "Validates correlation stability between '{column1}' and '{column2}' across '{segment_column}'"
            ),
        };

        ConstraintMetadata::new().with_description(description)
    }
}

impl CorrelationConstraint {
    /// Helper method to evaluate with a different validation (for internal use).
    async fn evaluate_with_validation(
        &self,
        ctx: &SessionContext,
        validation: &CorrelationValidation,
    ) -> Result<ConstraintResult> {
        let temp_constraint = Self::new(validation.clone())?;
        temp_constraint.evaluate(ctx).await
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
    async fn create_test_context_correlated() -> SessionContext {
        let ctx = SessionContext::new();

        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Float64, true),
            Field::new("y", DataType::Float64, true),
        ]));

        // Create correlated data: y ≈ 2x + noise
        let mut x_values = Vec::new();
        let mut y_values = Vec::new();

        for i in 0..100 {
            let x = i as f64;
            let y = 2.0 * x + (i % 10) as f64 - 5.0; // Some noise
            x_values.push(Some(x));
            y_values.push(Some(y));
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(x_values)),
                Arc::new(Float64Array::from(y_values)),
            ],
        )
        .unwrap();

        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("data", Arc::new(provider)).unwrap();

        ctx
    }

    async fn create_test_context_independent() -> SessionContext {
        let ctx = SessionContext::new();

        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Float64, true),
            Field::new("y", DataType::Float64, true),
        ]));

        // Create independent data
        let mut x_values = Vec::new();
        let mut y_values = Vec::new();

        for i in 0..100 {
            x_values.push(Some(i as f64));
            y_values.push(Some(((i * 37) % 100) as f64)); // Pseudo-random
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(x_values)),
                Arc::new(Float64Array::from(y_values)),
            ],
        )
        .unwrap();

        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("data", Arc::new(provider)).unwrap();

        ctx
    }

    #[tokio::test]
    async fn test_pearson_correlation() {
        let ctx = create_test_context_correlated().await;

        let constraint =
            CorrelationConstraint::pearson("x", "y", Assertion::GreaterThan(0.9)).unwrap();

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data").await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert!(result.metric.unwrap() > 0.9);
    }

    #[tokio::test]
    async fn test_independence_check() {
        let ctx = create_test_context_independent().await;

        let constraint = CorrelationConstraint::independence("x", "y", 0.3).unwrap();

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data").await.unwrap();
        // Independent data should have low correlation
        assert_eq!(result.status, ConstraintStatus::Success);
    }

    #[tokio::test]
    async fn test_correlation_range() {
        let ctx = create_test_context_correlated().await;

        let constraint = CorrelationConstraint::new(CorrelationValidation::Range {
            column1: "x".to_string(),
            column2: "y".to_string(),
            correlation_type: CorrelationType::Pearson,
            min: 0.8,
            max: 1.0,
        })
        .unwrap();

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data").await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
    }

    #[test]
    fn test_invalid_max_correlation() {
        let result = CorrelationConstraint::independence("x", "y", 1.5);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Max correlation must be between 0.0 and 1.0"));
    }

    #[test]
    fn test_multi_column_validation() {
        let config = MultiCorrelationConfig {
            columns: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            correlation_type: CorrelationType::Pearson,
            pairwise: true,
            min_correlation: Some(0.5),
        };

        let result = CorrelationConstraint::new(CorrelationValidation::MultiColumn(config));
        assert!(result.is_ok());
    }

    #[test]
    fn test_multi_column_too_few() {
        let config = MultiCorrelationConfig {
            columns: vec!["a".to_string()],
            correlation_type: CorrelationType::Pearson,
            pairwise: true,
            min_correlation: None,
        };

        let result = CorrelationConstraint::new(CorrelationValidation::MultiColumn(config));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("At least 2 columns required"));
    }
}
