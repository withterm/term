//! Type consistency validation constraints.

use crate::core::{Constraint, ConstraintMetadata, ConstraintResult, ConstraintStatus};
use crate::prelude::*;
use arrow::array::Array;
use async_trait::async_trait;
use datafusion::prelude::*;
use tracing::instrument;

/// A constraint that checks for data type consistency in a column.
///
/// Unlike `DataTypeConstraint` which checks for a specific expected type,
/// this constraint analyzes the actual data types present in a column and
/// reports on consistency, helping identify columns with mixed types.
///
/// # Examples
///
/// ```rust
/// use term_guard::constraints::DataTypeConsistencyConstraint;
/// use term_guard::core::Constraint;
///
/// // Check that at least 95% of values have the same data type
/// let constraint = DataTypeConsistencyConstraint::new("user_id", 0.95);
/// assert_eq!(constraint.name(), "data_type_consistency");
///
/// // Require perfect type consistency
/// let constraint = DataTypeConsistencyConstraint::new("amount", 1.0);
/// ```
#[derive(Debug, Clone)]
pub struct DataTypeConsistencyConstraint {
    column: String,
    threshold: f64,
}

impl DataTypeConsistencyConstraint {
    /// Creates a new data type consistency constraint.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to check
    /// * `threshold` - The minimum ratio of values that must have the most common type (0.0 to 1.0)
    ///
    /// # Panics
    ///
    /// Panics if threshold is not between 0.0 and 1.0
    pub fn new(column: impl Into<String>, threshold: f64) -> Self {
        assert!(
            (0.0..=1.0).contains(&threshold),
            "Threshold must be between 0.0 and 1.0"
        );
        Self {
            column: column.into(),
            threshold,
        }
    }
}

#[async_trait]
impl Constraint for DataTypeConsistencyConstraint {
    #[instrument(skip(self, ctx), fields(column = %self.column, threshold = %self.threshold))]
    async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult> {
        // First, get type distribution
        let type_dist_sql = format!(
            "WITH type_analysis AS (
                SELECT 
                    CASE 
                        WHEN {} IS NULL THEN 'null'
                        WHEN {} ~ '^-?\\d+$' THEN 'integer'
                        WHEN {} ~ '^-?\\d*\\.?\\d+([eE][+-]?\\d+)?$' THEN 'float'
                        WHEN {} ~ '^(true|false|TRUE|FALSE|True|False|0|1)$' THEN 'boolean'
                        WHEN {} ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}$' THEN 'date'
                        WHEN {} ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}[ T]\\d{{2}}:\\d{{2}}:\\d{{2}}' THEN 'timestamp'
                        ELSE 'string'
                    END as detected_type
                FROM data
            )
            SELECT 
                detected_type,
                COUNT(*) as type_count
            FROM type_analysis
            GROUP BY detected_type
            ORDER BY type_count DESC",
            self.column, self.column, self.column, self.column, self.column, self.column
        );

        let df = ctx.sql(&type_dist_sql).await?;
        let batches = df.collect().await?;

        if batches.is_empty() {
            return Ok(ConstraintResult::skipped("No data to validate"));
        }

        let batch = &batches[0];
        if batch.num_rows() == 0 {
            return Ok(ConstraintResult::skipped("No data to validate"));
        }

        // Extract type distribution
        let types = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| TermError::Internal("Failed to extract type names".to_string()))?;

        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| TermError::Internal("Failed to extract type counts".to_string()))?;

        // Calculate totals and find dominant type
        let mut total_count = 0i64;
        let mut _null_count = 0i64;
        let mut non_null_count = 0i64;
        let mut max_type_count = 0i64;
        let mut dominant_type = "";
        let mut distinct_type_count = 0i64;
        let mut type_distribution = Vec::new();

        for i in 0..batch.num_rows() {
            if !types.is_null(i) {
                let type_name = types.value(i);
                let count = counts.value(i);
                total_count += count;

                if type_name == "null" {
                    _null_count = count;
                } else {
                    non_null_count += count;
                    distinct_type_count += 1;
                    if count > max_type_count {
                        max_type_count = count;
                        dominant_type = type_name;
                    }
                }
            }
        }

        // Build type distribution report
        for i in 0..batch.num_rows() {
            if !types.is_null(i) {
                let type_name = types.value(i);
                let count = counts.value(i);
                let percentage = (count as f64 / total_count as f64) * 100.0;
                type_distribution.push(format!("{type_name}: {percentage:.1}%"));
            }
        }

        if non_null_count == 0 {
            return Ok(ConstraintResult::skipped("No non-null data to validate"));
        }

        // Calculate consistency ratio based on non-null values
        let consistency_ratio = max_type_count as f64 / non_null_count as f64;

        let type_report = type_distribution.join(", ");

        if consistency_ratio >= self.threshold {
            Ok(ConstraintResult {
                status: ConstraintStatus::Success,
                metric: Some(consistency_ratio),
                message: Some(format!(
                    "Column has consistent type '{}' ({:.1}% of non-null values). Type distribution: [{}]",
                    dominant_type,
                    consistency_ratio * 100.0,
                    type_report
                )),
            })
        } else {
            Ok(ConstraintResult::failure_with_metric(
                consistency_ratio,
                format!(
                    "Type consistency {:.1}% is below threshold {:.1}%. Found {} distinct types. Type distribution: [{}]",
                    consistency_ratio * 100.0,
                    self.threshold * 100.0,
                    distinct_type_count,
                    type_report
                ),
            ))
        }
    }

    fn name(&self) -> &str {
        "data_type_consistency"
    }

    fn column(&self) -> Option<&str> {
        Some(&self.column)
    }

    fn metadata(&self) -> ConstraintMetadata {
        ConstraintMetadata::for_column(&self.column)
            .with_description(format!(
                "Checks that at least {:.1}% of values in '{}' have the same data type",
                self.threshold * 100.0,
                self.column
            ))
            .with_custom("threshold", self.threshold.to_string())
            .with_custom("constraint_type", "consistency")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ConstraintStatus;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use std::sync::Arc;

    async fn create_test_context(values: Vec<Option<&str>>) -> SessionContext {
        let ctx = SessionContext::new();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "mixed_col",
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
    async fn test_consistent_integers() {
        let values = vec![Some("123"), Some("456"), Some("789"), Some("0")];
        let ctx = create_test_context(values).await;

        let constraint = DataTypeConsistencyConstraint::new("mixed_col", 0.95);

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(1.0)); // All integers
        assert!(result.message.as_ref().unwrap().contains("integer"));
    }

    #[tokio::test]
    async fn test_mixed_types() {
        let values = vec![
            Some("123"),    // integer
            Some("456.78"), // float
            Some("hello"),  // string
            Some("true"),   // boolean
        ];
        let ctx = create_test_context(values).await;

        let constraint = DataTypeConsistencyConstraint::new("mixed_col", 0.8);

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Failure);
        assert_eq!(result.metric, Some(0.25)); // Each type appears once
        assert!(result
            .message
            .as_ref()
            .unwrap()
            .contains("4 distinct types"));
    }

    #[tokio::test]
    async fn test_mostly_consistent() {
        let values = vec![
            Some("123"),
            Some("456"),
            Some("789"),
            Some("hello"), // One string among integers
        ];
        let ctx = create_test_context(values).await;

        let constraint = DataTypeConsistencyConstraint::new("mixed_col", 0.7);

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(0.75)); // 3 out of 4 are integers
    }

    #[tokio::test]
    async fn test_with_nulls() {
        let values = vec![Some("123"), None, Some("456"), None, Some("789")];
        let ctx = create_test_context(values).await;

        let constraint = DataTypeConsistencyConstraint::new("mixed_col", 0.95);

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(1.0)); // All non-null values are integers
        assert!(result.message.as_ref().unwrap().contains("null: 40.0%"));
    }

    #[tokio::test]
    async fn test_date_consistency() {
        let values = vec![
            Some("2024-01-01"),
            Some("2024-01-02"),
            Some("2024-01-03"),
            Some("not-a-date"),
        ];
        let ctx = create_test_context(values).await;

        let constraint = DataTypeConsistencyConstraint::new("mixed_col", 0.7);

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(0.75)); // 3 out of 4 are dates
        assert!(result.message.as_ref().unwrap().contains("date"));
    }

    #[test]
    #[should_panic(expected = "Threshold must be between 0.0 and 1.0")]
    fn test_invalid_threshold() {
        DataTypeConsistencyConstraint::new("col", 1.5);
    }
}
