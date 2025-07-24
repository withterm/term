//! Approximate count distinct validation constraint.

use crate::constraints::Assertion;
use crate::core::{Constraint, ConstraintMetadata, ConstraintResult, ConstraintStatus};
use crate::prelude::*;
use async_trait::async_trait;
use datafusion::prelude::*;
use tracing::instrument;

/// A constraint that validates the approximate count of distinct values in a column.
///
/// This constraint uses DataFusion's APPROX_DISTINCT function which provides
/// an approximate count using HyperLogLog algorithm. This is much faster than
/// exact COUNT(DISTINCT) for large datasets while maintaining accuracy within
/// 2-3% error margin.
///
/// # Examples
///
/// ```rust
/// use term_core::constraints::{ApproxCountDistinctConstraint, Assertion};
/// use term_core::core::Constraint;
///
/// // Check for high cardinality (e.g., user IDs)
/// let constraint = ApproxCountDistinctConstraint::new("user_id", Assertion::GreaterThan(1000000.0));
/// assert_eq!(constraint.name(), "approx_count_distinct");
///
/// // Check for low cardinality (e.g., country codes)
/// let constraint = ApproxCountDistinctConstraint::new("country_code", Assertion::LessThan(200.0));
/// ```
#[derive(Debug, Clone)]
pub struct ApproxCountDistinctConstraint {
    column: String,
    assertion: Assertion,
}

impl ApproxCountDistinctConstraint {
    /// Creates a new approximate count distinct constraint.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to count distinct values in
    /// * `assertion` - The assertion to apply to the approximate distinct count
    pub fn new<S: Into<String>>(column: S, assertion: Assertion) -> Self {
        Self {
            column: column.into(),
            assertion,
        }
    }
}

#[async_trait]
impl Constraint for ApproxCountDistinctConstraint {
    #[instrument(skip(self, ctx), fields(column = %self.column, assertion = ?self.assertion))]
    async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult> {
        // Build the SQL query using APPROX_DISTINCT
        let sql = format!(
            "SELECT APPROX_DISTINCT({}) as approx_distinct_count FROM data",
            self.column
        );

        let df = ctx.sql(&sql).await.map_err(|e| {
            TermError::constraint_evaluation(
                self.name(),
                format!("Failed to execute approximate count distinct query: {e}"),
            )
        })?;

        let batches = df.collect().await?;

        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Ok(ConstraintResult::skipped("No data to validate"));
        }

        let batch = &batches[0];

        // Extract the approximate distinct count
        let approx_count = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .ok_or_else(|| {
                TermError::constraint_evaluation(
                    self.name(),
                    "Failed to extract approximate distinct count from result",
                )
            })?
            .value(0) as f64;

        // Evaluate the assertion
        let assertion_result = self.assertion.evaluate(approx_count);

        let status = if assertion_result {
            ConstraintStatus::Success
        } else {
            ConstraintStatus::Failure
        };

        let message = if status == ConstraintStatus::Failure {
            Some(format!(
                "Approximate distinct count {approx_count} does not satisfy assertion {} for column '{}'",
                self.assertion.description(),
                self.column
            ))
        } else {
            None
        };

        Ok(ConstraintResult {
            status,
            metric: Some(approx_count),
            message,
        })
    }

    fn name(&self) -> &str {
        "approx_count_distinct"
    }

    fn column(&self) -> Option<&str> {
        Some(&self.column)
    }

    fn metadata(&self) -> ConstraintMetadata {
        ConstraintMetadata::for_column(&self.column)
            .with_description(format!(
                "Checks that the approximate distinct count of column '{}' {}",
                self.column,
                self.assertion.description()
            ))
            .with_custom("assertion", self.assertion.description())
            .with_custom("constraint_type", "cardinality")
            .with_custom("algorithm", "HyperLogLog")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ConstraintStatus;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use std::sync::Arc;

    async fn create_test_context_with_data(values: Vec<Option<i64>>) -> SessionContext {
        let ctx = SessionContext::new();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "test_col",
            DataType::Int64,
            true,
        )]));

        let array = Int64Array::from(values);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();

        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("data", Arc::new(provider)).unwrap();

        ctx
    }

    async fn create_string_context_with_data(values: Vec<Option<&str>>) -> SessionContext {
        let ctx = SessionContext::new();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "test_col",
            DataType::Utf8,
            true,
        )]));

        let array = StringArray::from(values);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();

        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("data", Arc::new(provider)).unwrap();

        ctx
    }

    #[tokio::test]
    async fn test_high_cardinality() {
        // All unique values
        let values: Vec<Option<i64>> = (0..1000).map(Some).collect();
        let ctx = create_test_context_with_data(values).await;

        let constraint =
            ApproxCountDistinctConstraint::new("test_col", Assertion::GreaterThan(990.0));
        let result = constraint.evaluate(&ctx).await.unwrap();

        assert_eq!(result.status, ConstraintStatus::Success);
        // APPROX_DISTINCT should be close to 1000
        assert!(result.metric.unwrap() > 990.0);
    }

    #[tokio::test]
    async fn test_low_cardinality() {
        // Only a few distinct values repeated many times
        let mut values = Vec::new();
        for _ in 0..100 {
            values.push(Some(1));
            values.push(Some(2));
            values.push(Some(3));
        }
        let ctx = create_test_context_with_data(values).await;

        let constraint = ApproxCountDistinctConstraint::new("test_col", Assertion::LessThan(10.0));
        let result = constraint.evaluate(&ctx).await.unwrap();

        assert_eq!(result.status, ConstraintStatus::Success);
        // Should be approximately 3
        assert!(result.metric.unwrap() < 10.0);
    }

    #[tokio::test]
    async fn test_with_nulls() {
        // Mix of values and nulls
        let values = vec![
            Some(1),
            None,
            Some(2),
            None,
            Some(3),
            None,
            Some(1),
            Some(2),
            Some(3),
            None,
        ];
        let ctx = create_test_context_with_data(values).await;

        // APPROX_DISTINCT should count distinct non-null values (approximately 3)
        let constraint =
            ApproxCountDistinctConstraint::new("test_col", Assertion::Between(2.0, 5.0));
        let result = constraint.evaluate(&ctx).await.unwrap();

        assert_eq!(result.status, ConstraintStatus::Success);
        let metric = result.metric.unwrap();
        assert!((2.0..=5.0).contains(&metric));
    }

    #[tokio::test]
    async fn test_constraint_failure() {
        // Create data with moderate cardinality
        let values: Vec<Option<i64>> = (0..50).map(|i| Some(i % 10)).collect();
        let ctx = create_test_context_with_data(values).await;

        // Expect high cardinality but data has low cardinality (~10 distinct values)
        let constraint =
            ApproxCountDistinctConstraint::new("test_col", Assertion::GreaterThan(100.0));
        let result = constraint.evaluate(&ctx).await.unwrap();

        assert_eq!(result.status, ConstraintStatus::Failure);
        assert!(result.metric.unwrap() < 20.0);
        assert!(result.message.is_some());
    }

    #[tokio::test]
    async fn test_string_column() {
        let values = vec![
            Some("apple"),
            Some("banana"),
            Some("cherry"),
            Some("apple"),
            Some("banana"),
            Some("date"),
            Some("elderberry"),
            None,
        ];
        let ctx = create_string_context_with_data(values).await;

        // Should have approximately 5 distinct non-null values
        let constraint =
            ApproxCountDistinctConstraint::new("test_col", Assertion::Between(4.0, 6.0));
        let result = constraint.evaluate(&ctx).await.unwrap();

        assert_eq!(result.status, ConstraintStatus::Success);
    }

    #[tokio::test]
    async fn test_empty_data() {
        let ctx = create_test_context_with_data(vec![]).await;

        // APPROX_DISTINCT returns 0 for empty data, not skipped
        let constraint = ApproxCountDistinctConstraint::new("test_col", Assertion::Equals(0.0));
        let result = constraint.evaluate(&ctx).await.unwrap();

        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(0.0));
    }

    #[tokio::test]
    async fn test_all_null_values() {
        let values = vec![None, None, None, None, None];
        let ctx = create_test_context_with_data(values).await;

        // APPROX_DISTINCT of all nulls should be 0
        let constraint = ApproxCountDistinctConstraint::new("test_col", Assertion::Equals(0.0));
        let result = constraint.evaluate(&ctx).await.unwrap();

        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(0.0));
    }

    #[tokio::test]
    async fn test_accuracy_comparison() {
        // Create a dataset large enough to see HyperLogLog in action
        // but small enough to verify accuracy
        let mut values = Vec::new();
        for i in 0..10000 {
            values.push(Some(i % 1000)); // 1000 distinct values
        }
        let ctx = create_test_context_with_data(values).await;

        let constraint =
            ApproxCountDistinctConstraint::new("test_col", Assertion::Between(970.0, 1030.0));
        let result = constraint.evaluate(&ctx).await.unwrap();

        assert_eq!(result.status, ConstraintStatus::Success);
        // Should be within 3% of 1000
        let metric = result.metric.unwrap();
        assert!((970.0..=1030.0).contains(&metric));
    }

    #[tokio::test]
    async fn test_metadata() {
        let constraint =
            ApproxCountDistinctConstraint::new("user_id", Assertion::GreaterThan(1000000.0));
        let metadata = constraint.metadata();

        assert_eq!(metadata.columns, vec!["user_id".to_string()]);
        let description = metadata.description.unwrap_or_default();
        assert!(description.contains("approximate distinct count"));
        assert!(description.contains("greater than 1000000"));
        assert_eq!(
            metadata.custom.get("algorithm"),
            Some(&"HyperLogLog".to_string())
        );
        assert_eq!(
            metadata.custom.get("constraint_type"),
            Some(&"cardinality".to_string())
        );
    }
}
