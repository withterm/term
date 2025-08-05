//! Completeness constraint for validating non-null ratios in data.
//!
//! This module provides a flexible completeness constraint that supports:
//! - Single column completeness checks
//! - Multiple column completeness with logical operators
//! - Configurable thresholds for partial completeness

use crate::core::{
    current_validation_context, ColumnSpec, Constraint, ConstraintMetadata, ConstraintOptions,
    ConstraintResult, LogicalOperator, UnifiedConstraint,
};
use crate::prelude::*;
use crate::security::SqlSecurity;
use async_trait::async_trait;
use datafusion::prelude::*;
use tracing::{debug, instrument};

/// A constraint that checks the completeness (non-null ratio) of one or more columns.
///
/// This constraint supports:
/// - Single column completeness checks
/// - Multiple column completeness with logical operators (All, Any, AtLeast, etc.)
/// - Configurable thresholds
/// - Flexible configuration through ConstraintOptions
///
/// # Examples
///
/// ```rust
/// use term_guard::constraints::CompletenessConstraint;
/// use term_guard::core::LogicalOperator;
///
/// // Single column - 100% complete (equivalent to is_complete)
/// let constraint = CompletenessConstraint::complete("user_id");
///
/// // Single column with threshold (equivalent to has_completeness)
/// let constraint = CompletenessConstraint::with_threshold("email", 0.95);
///
/// // Multiple columns - all must be complete (equivalent to are_complete)
/// let constraint = CompletenessConstraint::with_operator(
///     vec!["first_name", "last_name"],
///     LogicalOperator::All,
///     1.0,
/// );
///
/// // Multiple columns - any must be complete (equivalent to are_any_complete)
/// let constraint = CompletenessConstraint::with_operator(
///     vec!["phone", "email", "address"],
///     LogicalOperator::Any,
///     1.0,
/// );
///
/// // New capability: At least 2 columns must be 90% complete
/// let constraint = CompletenessConstraint::with_operator(
///     vec!["email", "phone", "address", "postal_code"],
///     LogicalOperator::AtLeast(2),
///     0.9,
/// );
/// ```
#[derive(Debug, Clone)]
pub struct CompletenessConstraint {
    /// The columns to check for completeness
    columns: ColumnSpec,
    /// The minimum acceptable completeness ratio (0.0 to 1.0)
    threshold: f64,
    /// The logical operator for combining results
    operator: LogicalOperator,
}

impl CompletenessConstraint {
    /// Creates a new unified completeness constraint.
    ///
    /// # Arguments
    ///
    /// * `columns` - The column(s) to check (accepts single string or vector)
    /// * `options` - Configuration options including threshold and operator
    ///
    /// # Panics
    ///
    /// Panics if threshold is not between 0.0 and 1.0
    pub fn new(columns: impl Into<ColumnSpec>, options: ConstraintOptions) -> Self {
        let threshold = options.threshold_or(1.0);
        assert!(
            (0.0..=1.0).contains(&threshold),
            "Threshold must be between 0.0 and 1.0"
        );

        Self {
            columns: columns.into(),
            threshold,
            operator: options.operator_or(LogicalOperator::All),
        }
    }

    /// Creates a constraint with a specific threshold.
    /// Convenience method for the common case.
    pub fn with_threshold(columns: impl Into<ColumnSpec>, threshold: f64) -> Self {
        Self::new(columns, ConstraintOptions::new().with_threshold(threshold))
    }

    /// Creates a constraint requiring 100% completeness.
    /// Convenience method equivalent to `is_complete`.
    pub fn complete(columns: impl Into<ColumnSpec>) -> Self {
        Self::new(columns, ConstraintOptions::new().with_threshold(1.0))
    }

    /// Creates a constraint for multiple columns with a specific operator.
    /// Convenience method for multi-column checks.
    pub fn with_operator(
        columns: impl Into<ColumnSpec>,
        operator: LogicalOperator,
        threshold: f64,
    ) -> Self {
        Self::new(
            columns,
            ConstraintOptions::new()
                .with_operator(operator)
                .with_threshold(threshold),
        )
    }
}

#[async_trait]
impl UnifiedConstraint for CompletenessConstraint {
    fn column_spec(&self) -> &ColumnSpec {
        &self.columns
    }

    fn logical_operator(&self) -> Option<LogicalOperator> {
        Some(self.operator)
    }

    #[instrument(skip(self, ctx), fields(
        constraint.name = %self.name(),
        constraint.threshold = %self.threshold,
        constraint.operator = %self.operator
    ))]
    async fn evaluate_column(
        &self,
        ctx: &SessionContext,
        column: &str,
    ) -> Result<ConstraintResult> {
        debug!(
            constraint.name = %self.name(),
            constraint.column = %column,
            constraint.threshold = %self.threshold,
            "Evaluating completeness for single column"
        );

        // Validate and escape column identifier
        SqlSecurity::validate_identifier(column)?;
        let column_identifier = SqlSecurity::escape_identifier(column)?;

        // Get the table name from the validation context
        let validation_ctx = current_validation_context();
        // Use the original table name since it's already been validated
        let table_name = validation_ctx.table_name();

        // Build SQL query to calculate completeness
        // Note: table_name is safe because it was validated when creating ValidationContext
        let sql = format!(
            "SELECT 
                COUNT(*) as total_count,
                COUNT({column_identifier}) as non_null_count
             FROM {table_name}"
        );

        // Execute query
        let df = ctx.sql(&sql).await?;
        let batches = df.collect().await?;

        // Extract results
        if batches.is_empty() {
            debug!(
                constraint.name = %self.name(),
                constraint.column = %column,
                skip.reason = "No data to validate",
                "Skipping constraint due to empty result set"
            );
            return Ok(ConstraintResult::skipped("No data to validate"));
        }

        let batch = &batches[0];
        if batch.num_rows() == 0 {
            return Ok(ConstraintResult::skipped("No data to validate"));
        }

        let total_count = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| TermError::Internal("Failed to extract total count".to_string()))?
            .value(0) as f64;

        if total_count == 0.0 {
            debug!(
                constraint.name = %self.name(),
                constraint.column = %column,
                skip.reason = "No data to validate",
                data.rows = 0,
                "Skipping constraint due to zero rows"
            );
            return Ok(ConstraintResult::skipped("No data to validate"));
        }

        let non_null_count = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| TermError::Internal("Failed to extract non-null count".to_string()))?
            .value(0) as f64;

        // Calculate completeness ratio
        let completeness = non_null_count / total_count;

        // Determine result based on threshold
        if completeness >= self.threshold {
            debug!(
                constraint.name = %self.name(),
                constraint.column = %column,
                constraint.threshold = %self.threshold,
                result.completeness = %format!("{completeness:.4}"),
                result.non_null_count = non_null_count as i64,
                result.total_count = total_count as i64,
                result.status = "success",
                "Completeness constraint passed for column"
            );
            Ok(ConstraintResult::success_with_metric(completeness))
        } else {
            debug!(
                constraint.name = %self.name(),
                constraint.column = %column,
                constraint.threshold = %self.threshold,
                result.completeness = %format!("{completeness:.4}"),
                result.non_null_count = non_null_count as i64,
                result.total_count = total_count as i64,
                result.status = "failure",
                "Completeness constraint failed for column"
            );
            Ok(ConstraintResult::failure_with_metric(
                completeness,
                format!(
                    "Column '{column}' completeness {:.2}% is below threshold {:.2}%",
                    completeness * 100.0,
                    self.threshold * 100.0
                ),
            ))
        }
    }
}

#[async_trait]
impl Constraint for CompletenessConstraint {
    async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult> {
        self.evaluate_unified(ctx).await
    }

    fn name(&self) -> &str {
        "completeness"
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

        let operator_desc = match &self.columns {
            ColumnSpec::Single(_) => String::new(),
            ColumnSpec::Multiple(_) => format!(" ({})", self.operator.description()),
        };

        metadata = metadata
            .with_description(format!(
                "Checks that {}{operator_desc} have at least {:.1}% completeness",
                match &self.columns {
                    ColumnSpec::Single(_) => "column",
                    ColumnSpec::Multiple(_) => "columns",
                },
                self.threshold * 100.0
            ))
            .with_custom("threshold", self.threshold.to_string())
            .with_custom("constraint_type", "data_quality");

        if let ColumnSpec::Multiple(_) = &self.columns {
            metadata = metadata.with_custom("operator", self.operator.description());
        }

        metadata
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ConstraintStatus;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use std::sync::Arc;

    use crate::test_helpers::evaluate_constraint_with_context;
    async fn create_test_context(
        columns: Vec<&str>,
        data: Vec<Vec<Option<i64>>>,
    ) -> SessionContext {
        let ctx = SessionContext::new();

        // Create schema
        let fields: Vec<Field> = columns
            .iter()
            .map(|&name| Field::new(name, DataType::Int64, true))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        // Create arrays
        let arrays: Vec<Arc<dyn arrow::array::Array>> = (0..columns.len())
            .map(|col_idx| {
                let values: Vec<Option<i64>> = data.iter().map(|row| row[col_idx]).collect();
                Arc::new(Int64Array::from(values)) as Arc<dyn arrow::array::Array>
            })
            .collect();

        // Create batch
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();

        // Register as table
        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("data", Arc::new(provider)).unwrap();

        ctx
    }

    #[tokio::test]
    async fn test_single_column_complete() {
        let ctx = create_test_context(
            vec!["id"],
            vec![vec![Some(1)], vec![Some(2)], vec![Some(3)], vec![Some(4)]],
        )
        .await;

        let constraint = CompletenessConstraint::complete("id");

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(1.0));
    }

    #[tokio::test]
    async fn test_single_column_with_threshold() {
        let ctx = create_test_context(
            vec!["email"],
            vec![
                vec![Some(1)],
                vec![Some(2)],
                vec![None],
                vec![Some(4)],
                vec![Some(5)],
            ],
        )
        .await;

        let constraint = CompletenessConstraint::with_threshold("email", 0.8);

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(0.8)); // 4 out of 5
    }

    #[tokio::test]
    async fn test_single_column_below_threshold() {
        let ctx = create_test_context(
            vec!["phone"],
            vec![vec![Some(1)], vec![None], vec![None], vec![Some(4)]],
        )
        .await;

        let constraint = CompletenessConstraint::with_threshold("phone", 0.8);

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Failure);
        assert_eq!(result.metric, Some(0.5)); // 2 out of 4
        assert!(result.message.is_some());
        assert!(result.message.unwrap().contains("50.00%"));
    }

    #[tokio::test]
    async fn test_multiple_columns_all_operator() {
        let ctx = create_test_context(
            vec!["first_name", "last_name"],
            vec![
                vec![Some(1), Some(10)],
                vec![Some(2), Some(20)],
                vec![Some(3), Some(30)],
            ],
        )
        .await;

        let constraint = CompletenessConstraint::new(
            vec!["first_name", "last_name"],
            ConstraintOptions::new()
                .with_operator(LogicalOperator::All)
                .with_threshold(1.0),
        );

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(1.0));
    }

    #[tokio::test]
    async fn test_multiple_columns_all_operator_failure() {
        let ctx = create_test_context(
            vec!["col1", "col2", "col3"],
            vec![
                vec![Some(1), None, Some(100)],
                vec![Some(2), Some(20), Some(200)],
                vec![Some(3), Some(30), Some(300)],
            ],
        )
        .await;

        let constraint = CompletenessConstraint::new(
            vec!["col1", "col2", "col3"],
            ConstraintOptions::new()
                .with_operator(LogicalOperator::All)
                .with_threshold(1.0),
        );

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Failure);
        assert!(result.message.is_some());
        assert!(result.message.unwrap().contains("col2"));
    }

    #[tokio::test]
    async fn test_multiple_columns_any_operator() {
        let ctx = create_test_context(
            vec!["phone", "email", "address"],
            vec![
                vec![Some(1), None, None],
                vec![None, Some(2), None],
                vec![None, None, None],
            ],
        )
        .await;

        let constraint = CompletenessConstraint::with_operator(
            vec!["phone", "email", "address"],
            LogicalOperator::Any,
            0.3, // 30% complete
        );

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        // phone: 1/3 = 0.33, email: 1/3 = 0.33, address: 0/3 = 0
        // At least one column (phone and email) meets the 30% threshold
    }

    #[tokio::test]
    async fn test_multiple_columns_at_least_operator() {
        let ctx = create_test_context(
            vec!["col1", "col2", "col3", "col4"],
            vec![
                vec![Some(1), Some(10), None, Some(100)],
                vec![Some(2), Some(20), Some(200), None],
                vec![Some(3), Some(30), Some(300), Some(3000)],
                vec![Some(4), Some(40), Some(400), Some(4000)],
            ],
        )
        .await;

        // col1: 100%, col2: 100%, col3: 75%, col4: 75%
        // At least 2 columns should be 80% complete
        let constraint = CompletenessConstraint::with_operator(
            vec!["col1", "col2", "col3", "col4"],
            LogicalOperator::AtLeast(2),
            0.8,
        );

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        // 2 columns (col1 and col2) are >= 80% complete
    }

    #[tokio::test]
    async fn test_multiple_columns_exactly_operator() {
        let ctx = create_test_context(
            vec!["a", "b", "c"],
            vec![
                vec![Some(1), Some(10), None],
                vec![Some(2), None, None],
                vec![Some(3), Some(30), None],
            ],
        )
        .await;

        // a: 100%, b: 66.7%, c: 0%
        // Exactly 1 column should be 100% complete
        let constraint = CompletenessConstraint::with_operator(
            vec!["a", "b", "c"],
            LogicalOperator::Exactly(1),
            1.0,
        );

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
    }

    #[tokio::test]
    async fn test_empty_data() {
        let ctx = create_test_context(vec!["id"], vec![]).await;
        let constraint = CompletenessConstraint::complete("id");

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Skipped);
    }

    #[test]
    #[should_panic(expected = "Threshold must be between 0.0 and 1.0")]
    fn test_invalid_threshold() {
        CompletenessConstraint::with_threshold("col", 1.5);
    }

    #[tokio::test]
    async fn test_metadata() {
        let single = CompletenessConstraint::with_threshold("email", 0.95);
        let metadata1 = single.metadata();
        assert_eq!(metadata1.columns, vec!["email"]);
        assert!(metadata1.description.is_some());
        assert!(metadata1.description.unwrap().contains("95.0%"));

        let multiple =
            CompletenessConstraint::with_operator(vec!["a", "b"], LogicalOperator::Any, 0.8);
        let metadata2 = multiple.metadata();
        assert_eq!(metadata2.columns, vec!["a", "b"]);
        assert!(metadata2.description.is_some());
        assert!(metadata2.description.unwrap().contains("any"));
        assert_eq!(metadata2.custom.get("operator"), Some(&"any".to_string()));
    }
}
