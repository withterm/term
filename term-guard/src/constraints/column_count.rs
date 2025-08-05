//! Column count validation constraint.

use crate::constraints::Assertion;
use crate::core::{Constraint, ConstraintMetadata, ConstraintResult, ConstraintStatus};
use crate::prelude::*;
use async_trait::async_trait;
use datafusion::prelude::*;
use tracing::instrument;
/// A constraint that validates the number of columns in a dataset.
///
/// This constraint checks if the column count of the dataset meets the specified assertion.
/// It uses DataFusion's schema to determine the number of columns.
///
/// # Examples
///
/// ```rust
/// use term_guard::constraints::{Assertion, ColumnCountConstraint};
/// use term_guard::core::Constraint;
///
/// // Check for exactly 15 columns
/// let constraint = ColumnCountConstraint::new(Assertion::Equals(15.0));
/// assert_eq!(constraint.name(), "column_count");
///
/// // Check for at least 10 columns
/// let constraint = ColumnCountConstraint::new(Assertion::GreaterThanOrEqual(10.0));
/// ```
#[derive(Debug, Clone)]
pub struct ColumnCountConstraint {
    assertion: Assertion,
}

impl ColumnCountConstraint {
    /// Creates a new column count constraint.
    ///
    /// # Arguments
    ///
    /// * `assertion` - The assertion to apply to the column count
    pub fn new(assertion: Assertion) -> Self {
        Self { assertion }
    }
}

#[async_trait]
impl Constraint for ColumnCountConstraint {
    #[instrument(skip(self, ctx), fields(assertion = ?self.assertion))]
    async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult> {
        // Get the table from the context
        let df = ctx.table("data").await.map_err(|e| {
            TermError::constraint_evaluation(
                self.name(),
                format!("Failed to access table 'data': {e}"),
            )
        })?;

        // Get the column count from the schema
        let column_count = df.schema().fields().len() as f64;

        // Evaluate the assertion
        let assertion_result = self.assertion.evaluate(column_count);

        let status = if assertion_result {
            ConstraintStatus::Success
        } else {
            ConstraintStatus::Failure
        };

        let message = if status == ConstraintStatus::Failure {
            Some(format!(
                "Column count {column_count} does not satisfy assertion {}",
                self.assertion.description()
            ))
        } else {
            None
        };

        Ok(ConstraintResult {
            status,
            metric: Some(column_count),
            message,
        })
    }

    fn name(&self) -> &str {
        "column_count"
    }

    fn column(&self) -> Option<&str> {
        None // This constraint operates on the entire dataset, not a specific column
    }

    fn metadata(&self) -> ConstraintMetadata {
        ConstraintMetadata::default()
            .with_description(format!(
                "Checks that the dataset has {} columns",
                self.assertion.description()
            ))
            .with_custom("assertion", self.assertion.description())
            .with_custom("constraint_type", "schema")
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
    async fn create_test_context_with_columns(num_columns: usize) -> SessionContext {
        let ctx = SessionContext::new();

        // Create a schema with the specified number of columns
        let fields: Vec<Field> = (0..num_columns)
            .map(|i| Field::new(format!("col_{i}"), DataType::Int64, true))
            .collect();

        let schema = Arc::new(Schema::new(fields));

        // Create arrays for each column
        let arrays: Vec<Arc<dyn arrow::array::Array>> = (0..num_columns)
            .map(|_| {
                Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)]))
                    as Arc<dyn arrow::array::Array>
            })
            .collect();

        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("data", Arc::new(provider)).unwrap();

        ctx
    }

    #[tokio::test]
    async fn test_column_count_equals() {
        let ctx = create_test_context_with_columns(5).await;

        // Test exact match
        let constraint = ColumnCountConstraint::new(Assertion::Equals(5.0));
        let result = evaluate_constraint_with_context(&constraint, &ctx, "data").await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(5.0));

        // Test mismatch
        let constraint = ColumnCountConstraint::new(Assertion::Equals(10.0));
        let result = evaluate_constraint_with_context(&constraint, &ctx, "data").await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Failure);
        assert_eq!(result.metric, Some(5.0));
        assert!(result.message.is_some());
    }

    #[tokio::test]
    async fn test_column_count_greater_than() {
        let ctx = create_test_context_with_columns(8).await;

        // Test success case
        let constraint = ColumnCountConstraint::new(Assertion::GreaterThan(5.0));
        let result = evaluate_constraint_with_context(&constraint, &ctx, "data").await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);

        // Test failure case
        let constraint = ColumnCountConstraint::new(Assertion::GreaterThan(10.0));
        let result = evaluate_constraint_with_context(&constraint, &ctx, "data").await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Failure);
    }

    #[tokio::test]
    async fn test_column_count_less_than() {
        let ctx = create_test_context_with_columns(3).await;

        // Test success case
        let constraint = ColumnCountConstraint::new(Assertion::LessThan(5.0));
        let result = evaluate_constraint_with_context(&constraint, &ctx, "data").await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);

        // Test failure case
        let constraint = ColumnCountConstraint::new(Assertion::LessThan(2.0));
        let result = evaluate_constraint_with_context(&constraint, &ctx, "data").await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Failure);
    }

    #[tokio::test]
    async fn test_column_count_between() {
        let ctx = create_test_context_with_columns(7).await;

        // Test within range
        let constraint = ColumnCountConstraint::new(Assertion::Between(5.0, 10.0));
        let result = evaluate_constraint_with_context(&constraint, &ctx, "data").await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);

        // Test outside range
        let constraint = ColumnCountConstraint::new(Assertion::Between(10.0, 15.0));
        let result = evaluate_constraint_with_context(&constraint, &ctx, "data").await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Failure);
    }

    #[tokio::test]
    async fn test_single_column_dataset() {
        let ctx = create_test_context_with_columns(1).await;

        let constraint = ColumnCountConstraint::new(Assertion::Equals(1.0));
        let result = evaluate_constraint_with_context(&constraint, &ctx, "data").await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(1.0));
    }

    #[tokio::test]
    async fn test_large_column_count() {
        let ctx = create_test_context_with_columns(100).await;

        let constraint = ColumnCountConstraint::new(Assertion::GreaterThanOrEqual(100.0));
        let result = evaluate_constraint_with_context(&constraint, &ctx, "data").await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(100.0));
    }

    #[tokio::test]
    async fn test_missing_table() {
        let ctx = SessionContext::new();
        // Don't register any table

        let constraint = ColumnCountConstraint::new(Assertion::Equals(5.0));
        let result = evaluate_constraint_with_context(&constraint, &ctx, "data").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_metadata() {
        let constraint = ColumnCountConstraint::new(Assertion::Between(10.0, 20.0));
        let metadata = constraint.metadata();

        assert!(metadata
            .description
            .unwrap_or_default()
            .contains("between 10 and 20"));
        assert_eq!(
            metadata.custom.get("constraint_type"),
            Some(&"schema".to_string())
        );
    }
}
