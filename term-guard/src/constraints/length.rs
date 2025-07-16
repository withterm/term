//! Unified string length constraint that consolidates all length checks.
//!
//! This module provides a single, flexible length constraint that replaces:
//! - `MinLengthConstraint`
//! - `MaxLengthConstraint`
//!
//! And adds support for new patterns like between, exactly, and not_empty.

use crate::core::{current_validation_context, Constraint, ConstraintResult, ConstraintStatus};
use crate::error::Result;
use crate::security::SqlSecurity;
use arrow::array::Array;
use async_trait::async_trait;
use datafusion::execution::context::SessionContext;
use serde::{Deserialize, Serialize};
use std::fmt;
use tracing::instrument;
/// Types of length assertions that can be made.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LengthAssertion {
    /// String must be at least this length
    Min(usize),
    /// String must be at most this length
    Max(usize),
    /// String length must be between min and max (inclusive)
    Between(usize, usize),
    /// String must be exactly this length
    Exactly(usize),
    /// String must not be empty (convenience for Min(1))
    NotEmpty,
}

impl LengthAssertion {
    /// Returns the SQL condition for this length assertion.
    fn sql_condition(&self, column: &str) -> String {
        match self {
            LengthAssertion::Min(min) => format!("LENGTH({column}) >= {min}"),
            LengthAssertion::Max(max) => format!("LENGTH({column}) <= {max}"),
            LengthAssertion::Between(min, max) => {
                format!("LENGTH({column}) >= {min} AND LENGTH({column}) <= {max}")
            }
            LengthAssertion::Exactly(len) => format!("LENGTH({column}) = {len}"),
            LengthAssertion::NotEmpty => format!("LENGTH({column}) >= 1"),
        }
    }

    /// Returns a human-readable name for this assertion.
    fn name(&self) -> &str {
        match self {
            LengthAssertion::Min(_) => "min_length",
            LengthAssertion::Max(_) => "max_length",
            LengthAssertion::Between(_, _) => "length_between",
            LengthAssertion::Exactly(_) => "exact_length",
            LengthAssertion::NotEmpty => "not_empty",
        }
    }

    /// Returns a human-readable description for this assertion.
    fn description(&self) -> String {
        match self {
            LengthAssertion::Min(min) => format!("at least {min} characters"),
            LengthAssertion::Max(max) => format!("at most {max} characters"),
            LengthAssertion::Between(min, max) => format!("between {min} and {max} characters"),
            LengthAssertion::Exactly(len) => format!("exactly {len} characters"),
            LengthAssertion::NotEmpty => "not empty".to_string(),
        }
    }
}

impl fmt::Display for LengthAssertion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.description())
    }
}

/// A unified constraint that checks string length properties of a column.
///
/// This constraint replaces the individual length constraints and provides
/// a consistent interface for all length-related checks.
///
/// # Examples
///
/// ```rust
/// use term_guard::constraints::{LengthConstraint, LengthAssertion};
/// use term_guard::core::Constraint;
///
/// // Check minimum length
/// let min_constraint = LengthConstraint::new("password", LengthAssertion::Min(8));
///
/// // Check maximum length  
/// let max_constraint = LengthConstraint::new("username", LengthAssertion::Max(20));
///
/// // Check length is between bounds
/// let between_constraint = LengthConstraint::new("description", LengthAssertion::Between(10, 500));
///
/// // Check exact length
/// let exact_constraint = LengthConstraint::new("code", LengthAssertion::Exactly(6));
///
/// // Check not empty
/// let not_empty_constraint = LengthConstraint::new("name", LengthAssertion::NotEmpty);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LengthConstraint {
    /// The column to check length on
    column: String,
    /// The length assertion to evaluate
    assertion: LengthAssertion,
}

impl LengthConstraint {
    /// Creates a new length constraint.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to check
    /// * `assertion` - The length assertion to evaluate
    pub fn new(column: impl Into<String>, assertion: LengthAssertion) -> Self {
        Self {
            column: column.into(),
            assertion,
        }
    }

    /// Creates a minimum length constraint.
    pub fn min(column: impl Into<String>, min_length: usize) -> Self {
        Self::new(column, LengthAssertion::Min(min_length))
    }

    /// Creates a maximum length constraint.
    pub fn max(column: impl Into<String>, max_length: usize) -> Self {
        Self::new(column, LengthAssertion::Max(max_length))
    }

    /// Creates a length constraint that checks if the string length is between bounds (inclusive).
    pub fn between(column: impl Into<String>, min_length: usize, max_length: usize) -> Self {
        assert!(min_length <= max_length, "min_length must be <= max_length");
        Self::new(column, LengthAssertion::Between(min_length, max_length))
    }

    /// Creates a constraint that checks for exact length.
    pub fn exactly(column: impl Into<String>, length: usize) -> Self {
        Self::new(column, LengthAssertion::Exactly(length))
    }

    /// Creates a constraint that checks the string is not empty.
    pub fn not_empty(column: impl Into<String>) -> Self {
        Self::new(column, LengthAssertion::NotEmpty)
    }
}

#[async_trait]
impl Constraint for LengthConstraint {
    #[instrument(skip(self, ctx), fields(
        column = %self.column,
        assertion = %self.assertion
    ))]
    async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult> {
        let column_identifier = SqlSecurity::escape_identifier(&self.column)?;
        let condition = self.assertion.sql_condition(&column_identifier);

        // Get the table name from the validation context

        let validation_ctx = current_validation_context();

        let table_name = validation_ctx.table_name();

        let sql = format!(
            "SELECT 
                COUNT(CASE WHEN {condition} OR {column_identifier} IS NULL THEN 1 END) * 1.0 / NULLIF(COUNT(*), 0) as ratio
            FROM {table_name}"
        );

        let df = ctx.sql(&sql).await?;
        let batches = df.collect().await?;

        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Ok(ConstraintResult::skipped("No data to validate"));
        }

        let ratio_array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .ok_or_else(|| {
                crate::error::TermError::constraint_evaluation(
                    self.name(),
                    "Failed to extract ratio from result",
                )
            })?;

        // If ratio is NULL, it means no data (COUNT(*) = 0)
        if ratio_array.is_null(0) {
            return Ok(ConstraintResult::skipped("No data to validate"));
        }

        let ratio = ratio_array.value(0);

        let status = if ratio >= 1.0 {
            ConstraintStatus::Success
        } else {
            ConstraintStatus::Failure
        };

        let message = if status == ConstraintStatus::Failure {
            Some(format!(
                "Length constraint failed: {:.2}% of values are {}",
                ratio * 100.0,
                self.assertion.description()
            ))
        } else {
            None
        };

        Ok(ConstraintResult {
            status,
            metric: Some(ratio),
            message,
        })
    }

    fn name(&self) -> &str {
        self.assertion.name()
    }

    fn column(&self) -> Option<&str> {
        Some(&self.column)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::record_batch::RecordBatch;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    use crate::test_helpers::evaluate_constraint_with_context;
    async fn create_test_context(data: Vec<Option<&str>>) -> SessionContext {
        let ctx = SessionContext::new();
        let string_data = StringArray::from(data);
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(string_data)]).unwrap();
        ctx.register_batch("data", batch).unwrap();
        ctx
    }

    #[tokio::test]
    async fn test_min_length_constraint() {
        let ctx = create_test_context(vec![
            Some("hello"),   // length 5
            Some("world"),   // length 5
            Some("testing"), // length 7
            Some("great"),   // length 5
            None,            // NULL
        ])
        .await;

        let constraint = LengthConstraint::min("text", 5);
        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();

        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(1.0)); // All values meet criteria
        assert_eq!(constraint.name(), "min_length");
    }

    #[tokio::test]
    async fn test_min_length_constraint_failure() {
        let ctx = create_test_context(vec![
            Some("hi"),      // length 2
            Some("hello"),   // length 5
            Some("a"),       // length 1
            Some("testing"), // length 7
            None,            // NULL
        ])
        .await;

        let constraint = LengthConstraint::min("text", 5);
        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();

        assert_eq!(result.status, ConstraintStatus::Failure);
        assert_eq!(result.metric, Some(0.6)); // 3/5 values meet criteria
        assert!(result.message.unwrap().contains("at least 5 characters"));
    }

    #[tokio::test]
    async fn test_max_length_constraint() {
        let ctx = create_test_context(vec![Some("hi"), Some("hey"), Some("test"), None]).await;

        let constraint = LengthConstraint::max("text", 10);
        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();

        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(1.0));
        assert_eq!(constraint.name(), "max_length");
    }

    #[tokio::test]
    async fn test_max_length_constraint_failure() {
        let ctx = create_test_context(vec![
            Some("short"),
            Some("this is a very long string that exceeds the limit"),
            Some("ok"),
            None,
        ])
        .await;

        let constraint = LengthConstraint::max("text", 10);
        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();

        assert_eq!(result.status, ConstraintStatus::Failure);
        assert_eq!(result.metric, Some(0.75)); // 3/4 values meet criteria
        assert!(result.message.unwrap().contains("at most 10 characters"));
    }

    #[tokio::test]
    async fn test_between_length_constraint() {
        let ctx = create_test_context(vec![
            Some("hello"),                // length 5 - within range
            Some("testing"),              // length 7 - within range
            Some("hi"),                   // length 2 - too short
            Some("this is way too long"), // length 18 - too long
            None,
        ])
        .await;

        let constraint = LengthConstraint::between("text", 3, 10);
        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();

        assert_eq!(result.status, ConstraintStatus::Failure);
        assert_eq!(result.metric, Some(0.6)); // 3/5 values meet criteria (2 within range + 1 NULL)
        assert_eq!(constraint.name(), "length_between");
        assert!(result
            .message
            .unwrap()
            .contains("between 3 and 10 characters"));
    }

    #[tokio::test]
    async fn test_exactly_length_constraint() {
        let ctx = create_test_context(vec![
            Some("hello"),   // length 5 - exact match
            Some("world"),   // length 5 - exact match
            Some("test"),    // length 4 - too short
            Some("testing"), // length 7 - too long
            None,
        ])
        .await;

        let constraint = LengthConstraint::exactly("text", 5);
        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();

        assert_eq!(result.status, ConstraintStatus::Failure);
        assert_eq!(result.metric, Some(0.6)); // 3/5 values meet criteria (2 exact + 1 NULL)
        assert_eq!(constraint.name(), "exact_length");
        assert!(result.message.unwrap().contains("exactly 5 characters"));
    }

    #[tokio::test]
    async fn test_not_empty_constraint() {
        let ctx = create_test_context(vec![
            Some("hello"),
            Some("a"), // length 1 - not empty
            Some(""),  // length 0 - empty!
            Some("testing"),
            None, // NULL - considered valid
        ])
        .await;

        let constraint = LengthConstraint::not_empty("text");
        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();

        assert_eq!(result.status, ConstraintStatus::Failure);
        assert_eq!(result.metric, Some(0.8)); // 4/5 values meet criteria
        assert_eq!(constraint.name(), "not_empty");
        assert!(result.message.unwrap().contains("not empty"));
    }

    #[tokio::test]
    async fn test_utf8_multibyte_characters() {
        let ctx = create_test_context(vec![
            Some("hello"), // ASCII, length 5
            Some("你好"),  // Chinese, length 2 characters
            Some("🦀🔥"),  // Emojis, length 2 characters
            Some("café"),  // Accented, length 4
            None,
        ])
        .await;

        // DataFusion's LENGTH function counts characters, not bytes
        let constraint = LengthConstraint::min("text", 2);
        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();

        assert_eq!(result.status, ConstraintStatus::Success);
        // All non-null values have at least 2 characters
    }

    #[tokio::test]
    async fn test_all_null_values() {
        let ctx = create_test_context(vec![None, None, None]).await;

        let constraint = LengthConstraint::min("text", 5);
        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();

        // All NULL values should be considered as meeting the constraint
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(1.0));
    }

    #[tokio::test]
    async fn test_empty_data() {
        let ctx = create_test_context(vec![]).await;

        let constraint = LengthConstraint::min("text", 5);
        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();

        assert_eq!(result.status, ConstraintStatus::Skipped);
    }

    #[test]
    fn test_length_assertion_display() {
        assert_eq!(LengthAssertion::Min(5).to_string(), "at least 5 characters");
        assert_eq!(
            LengthAssertion::Max(10).to_string(),
            "at most 10 characters"
        );
        assert_eq!(
            LengthAssertion::Between(3, 8).to_string(),
            "between 3 and 8 characters"
        );
        assert_eq!(
            LengthAssertion::Exactly(6).to_string(),
            "exactly 6 characters"
        );
        assert_eq!(LengthAssertion::NotEmpty.to_string(), "not empty");
    }

    #[test]
    #[should_panic(expected = "min_length must be <= max_length")]
    fn test_invalid_between_constraint() {
        LengthConstraint::between("test", 10, 5); // min > max should panic
    }
}
