//! Value-based validation constraints.

use crate::core::{current_validation_context, Constraint, ConstraintMetadata, ConstraintResult};
use crate::prelude::*;
use arrow::array::Array;
use async_trait::async_trait;
use datafusion::prelude::*;
use tracing::instrument;
/// Supported data types for validation.
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum DataType {
    /// Integer values
    Integer,
    /// Floating point values
    Float,
    /// Boolean values
    Boolean,
    /// Date values (ISO format)
    Date,
    /// Timestamp values
    Timestamp,
    /// String values
    String,
}

impl DataType {
    /// Returns the SQL pattern to match this data type.
    fn pattern(&self) -> &str {
        match self {
            DataType::Integer => r"^-?\d+$",
            DataType::Float => r"^-?\d*\.?\d+([eE][+-]?\d+)?$",
            DataType::Boolean => r"^(true|false|TRUE|FALSE|True|False|0|1)$",
            DataType::Date => r"^\d{4}-\d{2}-\d{2}$",
            DataType::Timestamp => r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}",
            DataType::String => r".*", // Any string
        }
    }

    /// Returns a human-readable name for this data type.
    fn name(&self) -> &str {
        match self {
            DataType::Integer => "integer",
            DataType::Float => "float",
            DataType::Boolean => "boolean",
            DataType::Date => "date",
            DataType::Timestamp => "timestamp",
            DataType::String => "string",
        }
    }
}

/// A constraint that checks if values in a column match a specific data type.
///
/// This constraint validates that a minimum percentage of values in a column
/// conform to the expected data type format.
///
/// # Examples
///
/// ```rust
/// use term_guard::constraints::DataTypeConstraint;
/// use term_guard::core::Constraint;
///
/// // Note: DataTypeConstraint is actually in the datatype module
/// // This example won't compile as shown
/// ```
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct DataTypeConstraint {
    column: String,
    data_type: DataType,
    threshold: f64,
}

#[allow(dead_code)]
impl DataTypeConstraint {
    /// Creates a new data type constraint.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to check
    /// * `data_type` - The expected data type
    /// * `threshold` - The minimum ratio of values that must match the type (0.0 to 1.0)
    ///
    /// # Panics
    ///
    /// Panics if threshold is not between 0.0 and 1.0
    pub fn new(column: impl Into<String>, data_type: DataType, threshold: f64) -> Self {
        assert!(
            (0.0..=1.0).contains(&threshold),
            "Threshold must be between 0.0 and 1.0"
        );
        Self {
            column: column.into(),
            data_type,
            threshold,
        }
    }
}

#[async_trait]
impl Constraint for DataTypeConstraint {
    #[instrument(skip(self, ctx), fields(column = %self.column, data_type = %self.data_type.name(), threshold = %self.threshold))]
    async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult> {
        let pattern = self.data_type.pattern();

        // Get the table name from the validation context

        let validation_ctx = current_validation_context();

        let table_name = validation_ctx.table_name();

        let sql = format!(
            "SELECT 
                COUNT(CASE WHEN {} ~ '{pattern}' THEN 1 END) as matches,
                COUNT(*) as total
             FROM {table_name}
             WHERE {} IS NOT NULL",
            self.column, self.column
        );

        let df = ctx.sql(&sql).await?;
        let batches = df.collect().await?;

        if batches.is_empty() {
            return Ok(ConstraintResult::skipped("No data to validate"));
        }

        let batch = &batches[0];
        if batch.num_rows() == 0 {
            return Ok(ConstraintResult::skipped("No data to validate"));
        }

        let matches = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| TermError::Internal("Failed to extract match count".to_string()))?
            .value(0) as f64;

        let total = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| TermError::Internal("Failed to extract total count".to_string()))?
            .value(0) as f64;

        if total == 0.0 {
            return Ok(ConstraintResult::skipped("No non-null data to validate"));
        }

        let type_ratio = matches / total;

        if type_ratio >= self.threshold {
            Ok(ConstraintResult::success_with_metric(type_ratio))
        } else {
            Ok(ConstraintResult::failure_with_metric(
                type_ratio,
                format!(
                    "Data type conformance {type_ratio} is below threshold {}",
                    self.threshold
                ),
            ))
        }
    }

    fn name(&self) -> &str {
        "data_type"
    }

    fn column(&self) -> Option<&str> {
        Some(&self.column)
    }

    fn metadata(&self) -> ConstraintMetadata {
        ConstraintMetadata::for_column(&self.column)
            .with_description(format!(
                "Checks that at least {:.1}% of values in '{}' conform to {} type",
                self.threshold * 100.0,
                self.column,
                self.data_type.name()
            ))
            .with_custom("data_type", self.data_type.name())
            .with_custom("threshold", self.threshold.to_string())
            .with_custom("constraint_type", "data_type")
    }
}

/// A constraint that checks if values in a column are contained in a set of allowed values.
///
/// This constraint validates that all non-null values in a column are members
/// of the specified set of allowed values.
///
/// # Examples
///
/// ```rust
/// use term_guard::constraints::ContainmentConstraint;
/// use term_guard::core::Constraint;
///
/// // Check that status values are only "active", "inactive", or "pending"
/// let constraint = ContainmentConstraint::new("status", vec!["active", "inactive", "pending"]);
/// assert_eq!(constraint.name(), "containment");
/// ```
#[derive(Debug, Clone)]
pub struct ContainmentConstraint {
    column: String,
    allowed_values: Vec<String>,
}

impl ContainmentConstraint {
    /// Creates a new containment constraint.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to check
    /// * `allowed_values` - The set of allowed values
    pub fn new<I, S>(column: impl Into<String>, allowed_values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            column: column.into(),
            allowed_values: allowed_values.into_iter().map(Into::into).collect(),
        }
    }
}

#[async_trait]
impl Constraint for ContainmentConstraint {
    #[instrument(skip(self, ctx), fields(column = %self.column, allowed_count = %self.allowed_values.len()))]
    async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult> {
        // Get the table name from the validation context
        let validation_ctx = current_validation_context();
        let table_name = validation_ctx.table_name();

        // Create IN clause with allowed values
        let values_list = self
            .allowed_values
            .iter()
            .map(|v| format!("'{}'", v.replace('\'', "''"))) // Escape single quotes
            .collect::<Vec<_>>()
            .join(", ");

        let sql = format!(
            "SELECT 
                COUNT(CASE WHEN {} IN ({values_list}) THEN 1 END) as valid_values,
                COUNT(*) as total
             FROM {table_name}
             WHERE {} IS NOT NULL",
            self.column, self.column
        );

        let df = ctx.sql(&sql).await?;
        let batches = df.collect().await?;

        if batches.is_empty() {
            return Ok(ConstraintResult::skipped("No data to validate"));
        }

        let batch = &batches[0];
        if batch.num_rows() == 0 {
            return Ok(ConstraintResult::skipped("No data to validate"));
        }

        let valid_values = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| TermError::Internal("Failed to extract valid count".to_string()))?
            .value(0) as f64;

        let total = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| TermError::Internal("Failed to extract total count".to_string()))?
            .value(0) as f64;

        if total == 0.0 {
            return Ok(ConstraintResult::skipped("No non-null data to validate"));
        }

        let containment_ratio = valid_values / total;

        if containment_ratio == 1.0 {
            Ok(ConstraintResult::success_with_metric(containment_ratio))
        } else {
            let invalid_count = total - valid_values;
            Ok(ConstraintResult::failure_with_metric(
                containment_ratio,
                format!("{invalid_count} values are not in the allowed set"),
            ))
        }
    }

    fn name(&self) -> &str {
        "containment"
    }

    fn column(&self) -> Option<&str> {
        Some(&self.column)
    }

    fn metadata(&self) -> ConstraintMetadata {
        ConstraintMetadata::for_column(&self.column)
            .with_description(format!(
                "Checks that all values in '{}' are contained in the allowed set",
                self.column
            ))
            .with_custom(
                "allowed_values",
                format!("[{}]", self.allowed_values.join(", ")),
            )
            .with_custom("constraint_type", "containment")
    }
}

/// A constraint that checks if all values in a column are non-negative.
///
/// This constraint validates that all numeric values in a column are >= 0.
///
/// # Examples
///
/// ```rust,ignore
/// // NonNegativeConstraint is not exported
/// use term_guard::constraints::NonNegativeConstraint;
/// use term_guard::core::Constraint;
///
/// // Check that all age values are non-negative
/// let constraint = NonNegativeConstraint::new("age");
/// assert_eq!(constraint.name(), "non_negative");
/// ```
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct NonNegativeConstraint {
    column: String,
}

#[allow(dead_code)]
impl NonNegativeConstraint {
    /// Creates a new non-negative constraint.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to check
    pub fn new(column: impl Into<String>) -> Self {
        Self {
            column: column.into(),
        }
    }
}

#[async_trait]
impl Constraint for NonNegativeConstraint {
    #[instrument(skip(self, ctx), fields(column = %self.column))]
    async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult> {
        // Get the table name from the validation context
        let validation_ctx = current_validation_context();
        let table_name = validation_ctx.table_name();

        // Check if all values are >= 0
        let sql = format!(
            "SELECT 
                COUNT(CASE WHEN CAST({} AS DOUBLE) >= 0 THEN 1 END) as non_negative,
                COUNT(*) as total
             FROM {table_name}
             WHERE {} IS NOT NULL",
            self.column, self.column
        );

        let df = ctx.sql(&sql).await?;
        let batches = df.collect().await?;

        if batches.is_empty() {
            return Ok(ConstraintResult::skipped("No data to validate"));
        }

        let batch = &batches[0];
        if batch.num_rows() == 0 {
            return Ok(ConstraintResult::skipped("No data to validate"));
        }

        let non_negative = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| TermError::Internal("Failed to extract non-negative count".to_string()))?
            .value(0) as f64;

        let total = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| TermError::Internal("Failed to extract total count".to_string()))?
            .value(0) as f64;

        if total == 0.0 {
            return Ok(ConstraintResult::skipped("No non-null data to validate"));
        }

        let non_negative_ratio = non_negative / total;

        if non_negative_ratio == 1.0 {
            Ok(ConstraintResult::success_with_metric(non_negative_ratio))
        } else {
            let negative_count = total - non_negative;
            Ok(ConstraintResult::failure_with_metric(
                non_negative_ratio,
                format!("{negative_count} values are negative"),
            ))
        }
    }

    fn name(&self) -> &str {
        "non_negative"
    }

    fn column(&self) -> Option<&str> {
        Some(&self.column)
    }

    fn metadata(&self) -> ConstraintMetadata {
        ConstraintMetadata::for_column(&self.column)
            .with_description(format!(
                "Checks that all values in '{}' are non-negative",
                self.column
            ))
            .with_custom("constraint_type", "value_range")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ConstraintStatus;
    use arrow::array::{Float64Array, StringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use std::sync::Arc;

    use crate::test_helpers::evaluate_constraint_with_context;
    async fn create_string_test_context(values: Vec<Option<&str>>) -> SessionContext {
        let ctx = SessionContext::new();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "text_col",
            ArrowDataType::Utf8,
            true,
        )]));

        let array = StringArray::from(values);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();

        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("data", Arc::new(provider)).unwrap();

        ctx
    }

    async fn create_numeric_test_context(values: Vec<Option<f64>>) -> SessionContext {
        let ctx = SessionContext::new();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "num_col",
            ArrowDataType::Float64,
            true,
        )]));

        let array = Float64Array::from(values);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();

        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("data", Arc::new(provider)).unwrap();

        ctx
    }

    #[tokio::test]
    async fn test_data_type_integer() {
        let values = vec![Some("123"), Some("456"), Some("not_number"), Some("789")];
        let ctx = create_string_test_context(values).await;

        let constraint = DataTypeConstraint::new("text_col", DataType::Integer, 0.7);

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(0.75)); // 3 out of 4 are integers
    }

    #[tokio::test]
    async fn test_data_type_float() {
        let values = vec![Some("123.45"), Some("67.89"), Some("invalid"), Some("100")];
        let ctx = create_string_test_context(values).await;

        let constraint = DataTypeConstraint::new("text_col", DataType::Float, 0.7);

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(0.75)); // 3 out of 4 are floats
    }

    #[tokio::test]
    async fn test_data_type_boolean() {
        let values = vec![Some("true"), Some("false"), Some("invalid"), Some("1")];
        let ctx = create_string_test_context(values).await;

        let constraint = DataTypeConstraint::new("text_col", DataType::Boolean, 0.7);

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(0.75)); // 3 out of 4 are booleans
    }

    #[tokio::test]
    async fn test_containment_constraint() {
        let values = vec![
            Some("active"),
            Some("inactive"),
            Some("pending"),
            Some("invalid_status"),
        ];
        let ctx = create_string_test_context(values).await;

        let constraint = ContainmentConstraint::new(
            "text_col",
            vec!["active", "inactive", "pending", "archived"],
        );

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Failure);
        assert_eq!(result.metric, Some(0.75)); // 3 out of 4 are in allowed set
    }

    #[tokio::test]
    async fn test_containment_all_valid() {
        let values = vec![Some("active"), Some("inactive"), Some("pending")];
        let ctx = create_string_test_context(values).await;

        let constraint = ContainmentConstraint::new(
            "text_col",
            vec!["active", "inactive", "pending", "archived"],
        );

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(1.0)); // All values are in allowed set
    }

    #[tokio::test]
    async fn test_non_negative_constraint() {
        let values = vec![Some(1.0), Some(0.0), Some(5.5), Some(100.0)];
        let ctx = create_numeric_test_context(values).await;

        let constraint = NonNegativeConstraint::new("num_col");

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(1.0)); // All values are non-negative
    }

    #[tokio::test]
    async fn test_non_negative_with_negative() {
        let values = vec![Some(1.0), Some(-2.0), Some(5.5), Some(100.0)];
        let ctx = create_numeric_test_context(values).await;

        let constraint = NonNegativeConstraint::new("num_col");

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Failure);
        assert_eq!(result.metric, Some(0.75)); // 3 out of 4 are non-negative
    }

    #[tokio::test]
    async fn test_with_nulls() {
        let values = vec![Some("active"), None, Some("inactive"), None];
        let ctx = create_string_test_context(values).await;

        let constraint = ContainmentConstraint::new("text_col", vec!["active", "inactive"]);

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(1.0)); // All non-null values are valid
    }

    #[test]
    #[should_panic(expected = "Threshold must be between 0.0 and 1.0")]
    fn test_invalid_threshold() {
        DataTypeConstraint::new("col", DataType::Integer, 1.5);
    }
}
