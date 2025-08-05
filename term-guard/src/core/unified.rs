//! Base traits and types for unified constraints.
//!
//! This module provides the foundation for the unified constraint API,
//! including base traits, common options, and shared functionality.

use super::{ColumnSpec, Constraint, ConstraintResult, LogicalOperator};
use crate::core::current_validation_context;
use crate::prelude::*;
use async_trait::async_trait;
use datafusion::prelude::*;
use std::collections::HashMap;

/// Base trait for unified constraints.
///
/// This trait extends the standard `Constraint` trait with additional
/// functionality needed for unified constraint types.
#[async_trait]
pub trait UnifiedConstraint: Constraint {
    /// Returns the column specification for this constraint.
    fn column_spec(&self) -> &ColumnSpec;

    /// Returns the logical operator used for multi-column evaluation.
    fn logical_operator(&self) -> Option<LogicalOperator> {
        None
    }

    /// Returns configuration options as a map.
    fn options(&self) -> HashMap<String, String> {
        HashMap::new()
    }

    /// Evaluates the constraint for a single column.
    ///
    /// This method should be implemented by constraint types to evaluate
    /// a single column. The default `evaluate` method will call this
    /// for each column and combine results using the logical operator.
    async fn evaluate_column(&self, ctx: &SessionContext, column: &str)
        -> Result<ConstraintResult>;

    /// Default implementation of evaluate that handles multi-column logic.
    async fn evaluate_unified(&self, ctx: &SessionContext) -> Result<ConstraintResult> {
        let columns = self.column_spec().as_vec();

        match columns.len() {
            0 => Ok(ConstraintResult::skipped("No columns specified")),
            1 => {
                // Single column - evaluate directly
                self.evaluate_column(ctx, columns[0]).await
            }
            _ => {
                // Multiple columns - evaluate each and combine
                let mut results = Vec::new();
                let mut metrics = Vec::new();

                for column in &columns {
                    let result = self.evaluate_column(ctx, column).await?;
                    results.push((column.to_string(), result.status.is_success()));
                    if let Some(metric) = result.metric {
                        metrics.push(metric);
                    }
                }

                // Apply logical operator
                let operator = self.logical_operator().unwrap_or(LogicalOperator::All);
                let bools: Vec<bool> = results.iter().map(|(_, b)| *b).collect();
                let combined_result = operator.evaluate(&bools);

                // Calculate combined metric (average)
                let combined_metric = if metrics.is_empty() {
                    None
                } else {
                    Some(metrics.iter().sum::<f64>() / metrics.len() as f64)
                };

                // Generate message
                let message = if combined_result {
                    match operator {
                        LogicalOperator::All => Some(format!(
                            "All {} columns satisfy the constraint",
                            columns.len()
                        )),
                        LogicalOperator::Any => {
                            let passed = results
                                .iter()
                                .filter(|(_, b)| *b)
                                .map(|(c, _)| c.as_str())
                                .collect::<Vec<_>>();
                            Some(format!(
                                "Columns {} satisfy the constraint",
                                passed.join(", ")
                            ))
                        }
                        _ => None,
                    }
                } else {
                    let failed = results
                        .iter()
                        .filter(|(_, b)| !*b)
                        .map(|(c, _)| c.as_str())
                        .collect::<Vec<_>>();
                    Some(format!(
                        "Constraint failed for columns: {}. Required: {}",
                        failed.join(", "),
                        operator.description()
                    ))
                };

                if combined_result {
                    Ok(ConstraintResult {
                        status: crate::core::ConstraintStatus::Success,
                        metric: combined_metric,
                        message,
                    })
                } else {
                    Ok(ConstraintResult {
                        status: crate::core::ConstraintStatus::Failure,
                        metric: combined_metric,
                        message,
                    })
                }
            }
        }
    }
}

/// Common options for constraint configuration.
///
/// This struct provides a builder pattern for common constraint options
/// that can be extended by specific constraint types.
#[derive(Debug, Clone, Default)]
pub struct ConstraintOptions {
    /// The logical operator for multi-column constraints
    pub operator: Option<LogicalOperator>,
    /// Threshold value (interpretation depends on constraint type)
    pub threshold: Option<f64>,
    /// Additional boolean flags
    pub flags: HashMap<String, bool>,
    /// Additional string options
    pub options: HashMap<String, String>,
}

impl ConstraintOptions {
    /// Creates a new options builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the logical operator.
    pub fn with_operator(mut self, operator: LogicalOperator) -> Self {
        self.operator = Some(operator);
        self
    }

    /// Sets the threshold value.
    pub fn with_threshold(mut self, threshold: f64) -> Self {
        self.threshold = Some(threshold);
        self
    }

    /// Sets a boolean flag.
    pub fn with_flag(mut self, name: impl Into<String>, value: bool) -> Self {
        self.flags.insert(name.into(), value);
        self
    }

    /// Sets a string option.
    pub fn with_option(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(name.into(), value.into());
        self
    }

    /// Returns the operator or a default value.
    pub fn operator_or(&self, default: LogicalOperator) -> LogicalOperator {
        self.operator.unwrap_or(default)
    }

    /// Returns the threshold or a default value.
    pub fn threshold_or(&self, default: f64) -> f64 {
        self.threshold.unwrap_or(default)
    }

    /// Returns a flag value or false if not set.
    pub fn flag(&self, name: &str) -> bool {
        self.flags.get(name).copied().unwrap_or(false)
    }

    /// Returns an option value.
    pub fn option(&self, name: &str) -> Option<&str> {
        self.options.get(name).map(|s| s.as_str())
    }
}

/// Helper macro for implementing common constraint patterns.
///
/// This macro reduces boilerplate when implementing unified constraints.
#[macro_export]
macro_rules! impl_unified_constraint {
    ($constraint:ty, $name:expr) => {
        #[async_trait]
        impl Constraint for $constraint {
            async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult> {
                self.evaluate_unified(ctx).await
            }

            fn name(&self) -> &str {
                $name
            }

            fn column(&self) -> Option<&str> {
                match &self.columns {
                    ColumnSpec::Single(col) => Some(col),
                    ColumnSpec::Multiple(_) => None,
                }
            }

            fn metadata(&self) -> ConstraintMetadata {
                let mut metadata = match &self.columns {
                    ColumnSpec::Single(col) => ConstraintMetadata::for_column(col),
                    ColumnSpec::Multiple(cols) => ConstraintMetadata::for_columns(cols),
                };

                if let Some(op) = self.logical_operator() {
                    metadata = metadata.with_custom("operator", op.description());
                }

                metadata
            }
        }
    };
}

/// Base implementation for unified completeness-style constraints.
///
/// This provides common functionality for constraints that check
/// completeness, non-null ratios, or similar metrics.
pub struct UnifiedCompletenessBase {
    pub columns: ColumnSpec,
    pub threshold: f64,
    pub operator: LogicalOperator,
}

impl UnifiedCompletenessBase {
    /// Evaluates completeness for a single column.
    pub async fn evaluate_completeness(
        &self,
        ctx: &SessionContext,
        column: &str,
    ) -> Result<(f64, i64, i64)> {
        // Get the table name from the validation context

        let validation_ctx = current_validation_context();

        let table_name = validation_ctx.table_name();

        

        let sql = format!(
            "SELECT 
                COUNT(*) as total_count,
                COUNT({column}) as non_null_count
             FROM {table_name}"
        );

        let df = ctx.sql(&sql).await?;
        let batches = df.collect().await?;

        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Ok((0.0, 0, 0));
        }

        let batch = &batches[0];
        let total_count = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| TermError::Internal("Failed to extract total count".to_string()))?
            .value(0);

        let non_null_count = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| TermError::Internal("Failed to extract non-null count".to_string()))?
            .value(0);

        let completeness = if total_count > 0 {
            non_null_count as f64 / total_count as f64
        } else {
            0.0
        };

        Ok((completeness, non_null_count, total_count))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constraint_options_builder() {
        let options = ConstraintOptions::new()
            .with_operator(LogicalOperator::Any)
            .with_threshold(0.95)
            .with_flag("case_sensitive", true)
            .with_option("pattern", "[A-Z]+");

        assert_eq!(
            options.operator_or(LogicalOperator::All),
            LogicalOperator::Any
        );
        assert_eq!(options.threshold_or(1.0), 0.95);
        assert!(options.flag("case_sensitive"));
        assert!(!options.flag("unknown_flag"));
        assert_eq!(options.option("pattern"), Some("[A-Z]+"));
        assert_eq!(options.option("unknown"), None);
    }

    #[test]
    fn test_column_spec_with_options() {
        let single = ColumnSpec::Single("user_id".to_string());
        let multiple = ColumnSpec::Multiple(vec!["email".to_string(), "phone".to_string()]);

        let options = ConstraintOptions::new().with_operator(LogicalOperator::Any);

        // Test that options work with both single and multiple columns
        assert_eq!(single.len(), 1);
        assert_eq!(multiple.len(), 2);
        assert_eq!(
            options.operator_or(LogicalOperator::All),
            LogicalOperator::Any
        );
    }
}
