//! Size constraint for checking row counts.

use crate::constraints::Assertion;
use crate::core::{current_validation_context, Constraint, ConstraintMetadata, ConstraintResult};
use crate::prelude::*;
use async_trait::async_trait;
use datafusion::prelude::*;
use tracing::{debug, instrument};
/// A constraint that checks the size (row count) of the data.
///
/// This constraint validates that the total number of rows in the dataset
/// meets the specified assertion criteria.
///
/// # Examples
///
/// ```rust
/// use term_guard::constraints::{SizeConstraint, Assertion};
/// use term_guard::core::Constraint;
///
/// // Check that we have exactly 1000 rows
/// let constraint = SizeConstraint::new(Assertion::Equals(1000.0));
/// assert_eq!(constraint.name(), "size");
///
/// // Check that we have at least 100 rows
/// let constraint = SizeConstraint::new(Assertion::GreaterThanOrEqual(100.0));
///
/// // Check that row count is between 1000 and 10000
/// let constraint = SizeConstraint::new(Assertion::Between(1000.0, 10000.0));
/// ```
#[derive(Debug, Clone)]
pub struct SizeConstraint {
    /// The assertion to evaluate against the row count
    assertion: Assertion,
}

impl SizeConstraint {
    /// Creates a new size constraint with the specified assertion.
    ///
    /// # Arguments
    ///
    /// * `assertion` - The assertion to evaluate against the row count
    pub fn new(assertion: Assertion) -> Self {
        Self { assertion }
    }
}

#[async_trait]
impl Constraint for SizeConstraint {
    #[instrument(skip(self, ctx), fields(
        constraint.name = %self.name(),
        constraint.assertion = %self.assertion
    ))]
    async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult> {
        debug!(
            constraint.name = %self.name(),
            constraint.assertion = %self.assertion,
            "Starting size constraint evaluation"
        );
        // Build SQL query to count rows
        // Get the table name from the validation context

        let validation_ctx = current_validation_context();

        let table_name = validation_ctx.table_name();

        

        let sql = format!("SELECT COUNT(*) as row_count FROM {table_name}");

        // Execute query
        let df = ctx.sql(&sql).await?;
        let batches = df.collect().await?;

        // Extract results
        if batches.is_empty() {
            debug!(
                constraint.name = %self.name(),
                skip.reason = "No data to validate",
                "Skipping constraint due to empty result set"
            );
            return Ok(ConstraintResult::skipped("No data to validate"));
        }

        let batch = &batches[0];

        // Check if the query returned any rows
        if batch.num_rows() == 0 {
            return Ok(ConstraintResult::skipped("No data to validate"));
        }

        let row_count = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| TermError::Internal("Failed to extract row count".to_string()))?
            .value(0) as f64;

        // Evaluate assertion
        if self.assertion.evaluate(row_count) {
            debug!(
                constraint.name = %self.name(),
                constraint.assertion = %self.assertion,
                result.row_count = row_count as i64,
                result.status = "success",
                "Size constraint passed"
            );
            Ok(ConstraintResult::success_with_metric(row_count))
        } else {
            debug!(
                constraint.name = %self.name(),
                constraint.assertion = %self.assertion,
                result.row_count = row_count as i64,
                result.status = "failure",
                "Size constraint failed"
            );
            Ok(ConstraintResult::failure_with_metric(
                row_count,
                format!("Size {row_count} does not {}", self.assertion),
            ))
        }
    }

    fn name(&self) -> &str {
        "size"
    }

    fn metadata(&self) -> ConstraintMetadata {
        ConstraintMetadata::new()
            .with_description(format!(
                "Checks that the dataset size {}",
                self.assertion.description()
            ))
            .with_custom("assertion", self.assertion.to_string())
            .with_custom("constraint_type", "statistical")
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
    async fn create_test_context(num_rows: usize) -> SessionContext {
        let ctx = SessionContext::new();

        // Create schema
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));

        // Create data
        let values: Vec<i64> = (0..num_rows as i64).collect();
        let array = Int64Array::from(values);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();

        // Register as table
        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("data", Arc::new(provider)).unwrap();

        ctx
    }

    #[tokio::test]
    async fn test_size_equals() {
        let ctx = create_test_context(100).await;
        let constraint = SizeConstraint::new(Assertion::Equals(100.0));

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data").await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(100.0));
    }

    #[tokio::test]
    async fn test_size_greater_than() {
        let ctx = create_test_context(50).await;
        let constraint = SizeConstraint::new(Assertion::GreaterThan(25.0));

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data").await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(50.0));
    }

    #[tokio::test]
    async fn test_size_between() {
        let ctx = create_test_context(75).await;
        let constraint = SizeConstraint::new(Assertion::Between(50.0, 100.0));

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data").await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(75.0));
    }

    #[tokio::test]
    async fn test_size_failure() {
        let ctx = create_test_context(10).await;
        let constraint = SizeConstraint::new(Assertion::GreaterThan(50.0));

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data").await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Failure);
        assert_eq!(result.metric, Some(10.0));
    }

    #[tokio::test]
    async fn test_empty_data() {
        let ctx = create_test_context(0).await;
        let constraint = SizeConstraint::new(Assertion::Equals(0.0));

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data").await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(0.0));
    }
}
