//! Custom SQL validation constraints.

use crate::core::{current_validation_context, Constraint, ConstraintMetadata, ConstraintResult};
use crate::prelude::*;
use crate::security::SqlSecurity;
use async_trait::async_trait;
use datafusion::prelude::*;
use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::sync::RwLock;
use tracing::instrument;
/// Cache for compiled regex patterns to avoid recompiling
static REGEX_CACHE: Lazy<RwLock<HashMap<String, Regex>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// A constraint that evaluates custom SQL expressions.
///
/// This constraint allows users to define custom validation logic using SQL expressions
/// while preventing dangerous operations like DROP, DELETE, UPDATE, etc.
///
/// # Examples
///
/// ```rust
/// use term_guard::constraints::CustomSqlConstraint;
/// use term_guard::core::Constraint;
///
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Check that values in a column meet a custom condition
/// let constraint = CustomSqlConstraint::new("price > 0 AND price < 1000000", None::<String>)?;
/// assert_eq!(constraint.name(), "custom_sql");
///
/// // With a custom hint message
/// let constraint = CustomSqlConstraint::new(
///     "order_date <= ship_date",
///     Some("Shipping date must be after or equal to order date")
/// )?;
/// # Ok(())
/// # }
/// # example().unwrap();
/// ```
#[derive(Debug, Clone)]
pub struct CustomSqlConstraint {
    expression: String,
    hint: Option<String>,
}

impl CustomSqlConstraint {
    /// Creates a new custom SQL constraint.
    ///
    /// # Arguments
    ///
    /// * `expression` - The SQL expression to evaluate (should return a boolean)
    /// * `hint` - Optional hint message to provide context when the constraint fails
    ///
    /// # Errors
    ///
    /// Returns an error if the SQL expression contains dangerous operations
    pub fn new(expression: impl Into<String>, hint: Option<impl Into<String>>) -> Result<Self> {
        let expression = expression.into();

        // Validate the SQL expression for safety using both local and security module validation
        validate_sql_expression(&expression)?;
        SqlSecurity::validate_sql_expression(&expression)?;

        Ok(Self {
            expression,
            hint: hint.map(Into::into),
        })
    }

    /// Attempts to create a new custom SQL constraint, returning an error if validation fails.
    ///
    /// # Arguments
    ///
    /// * `expression` - The SQL expression to evaluate
    /// * `hint` - Optional hint message
    ///
    /// # Returns
    ///
    /// A Result containing the constraint or a validation error
    pub fn try_new(expression: impl Into<String>, hint: Option<impl Into<String>>) -> Result<Self> {
        let expression = expression.into();

        // Validate the SQL expression for safety using both local and security module validation
        validate_sql_expression(&expression)?;
        SqlSecurity::validate_sql_expression(&expression)?;

        Ok(Self {
            expression,
            hint: hint.map(Into::into),
        })
    }
}

/// Validates that a SQL expression doesn't contain dangerous operations.
///
/// This function checks for keywords that could modify data or schema,
/// ensuring the expression is read-only.
fn validate_sql_expression(sql: &str) -> Result<()> {
    // Convert to uppercase for case-insensitive comparison
    let sql_upper = sql.to_uppercase();

    // Define dangerous keywords that should not be allowed
    let dangerous_keywords: HashSet<&str> = [
        "DROP",
        "DELETE",
        "INSERT",
        "UPDATE",
        "CREATE",
        "ALTER",
        "TRUNCATE",
        "GRANT",
        "REVOKE",
        "EXECUTE",
        "EXEC",
        "CALL",
        "MERGE",
        "REPLACE",
        "RENAME",
        "MODIFY",
        "SET",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT",
        "BEGIN",
        "START",
        "TRANSACTION",
        "LOCK",
        "UNLOCK",
    ]
    .iter()
    .copied()
    .collect();

    // Check for dangerous keywords
    for keyword in dangerous_keywords {
        // Use word boundaries to avoid false positives (e.g., "UPDATE" in "UPDATED_AT")
        let pattern = format!(r"\b{keyword}\b");

        // Check cache first
        let matches = {
            let cache = REGEX_CACHE.read().map_err(|_| {
                TermError::Internal("Failed to acquire read lock on regex cache".to_string())
            })?;

            if let Some(regex) = cache.get(&pattern) {
                regex.is_match(&sql_upper)
            } else {
                // Need to compile and cache the regex
                drop(cache);
                let mut write_cache = REGEX_CACHE.write().map_err(|_| {
                    TermError::Internal("Failed to acquire write lock on regex cache".to_string())
                })?;

                let regex = Regex::new(&pattern).map_err(|e| {
                    TermError::Internal(format!("Failed to compile regex pattern: {e}"))
                })?;
                let is_match = regex.is_match(&sql_upper);
                write_cache.insert(pattern.clone(), regex);
                is_match
            }
        };

        if matches {
            return Err(TermError::validation_failed(
                "custom_sql",
                format!("SQL expression contains forbidden operation: {keyword}"),
            ));
        }
    }

    // Check for semicolons which could be used to inject multiple statements
    if sql.contains(';') {
        return Err(TermError::validation_failed(
            "custom_sql",
            "SQL expression cannot contain semicolons",
        ));
    }

    // Check for comment sequences that could be used to bypass validation
    if sql.contains("--") || sql.contains("/*") || sql.contains("*/") {
        return Err(TermError::validation_failed(
            "custom_sql",
            "SQL expression cannot contain comments",
        ));
    }

    Ok(())
}

#[async_trait]
impl Constraint for CustomSqlConstraint {
    #[instrument(skip(self, ctx), fields(expression = %self.expression))]
    async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult> {
        // Wrap the expression in a query that counts rows where the condition is true
        // Get the table name from the validation context

        let validation_ctx = current_validation_context();

        let table_name = validation_ctx.table_name();

        let sql = format!(
            "SELECT 
                COUNT(CASE WHEN {} THEN 1 END) as satisfied,
                COUNT(*) as total
             FROM {table_name}",
            self.expression
        );

        // Try to execute the SQL
        let df = match ctx.sql(&sql).await {
            Ok(df) => df,
            Err(e) => {
                // Return a clear error message for SQL errors
                return Ok(ConstraintResult::failure(format!(
                    "SQL expression error: {e}. Expression: '{}'",
                    self.expression
                )));
            }
        };

        let batches = match df.collect().await {
            Ok(batches) => batches,
            Err(e) => {
                // Return a clear error message for execution errors
                return Ok(ConstraintResult::failure(format!(
                    "SQL execution error: {e}. Expression: '{}'",
                    self.expression
                )));
            }
        };

        if batches.is_empty() {
            return Ok(ConstraintResult::skipped("No data to validate"));
        }

        let batch = &batches[0];
        if batch.num_rows() == 0 {
            return Ok(ConstraintResult::skipped("No data to validate"));
        }

        // Extract results
        let satisfied = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| TermError::Internal("Failed to extract satisfied count".to_string()))?
            .value(0) as f64;

        let total = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| TermError::Internal("Failed to extract total count".to_string()))?
            .value(0) as f64;

        if total == 0.0 {
            return Ok(ConstraintResult::skipped("No data to validate"));
        }

        let satisfaction_ratio = satisfied / total;

        if satisfaction_ratio == 1.0 {
            Ok(ConstraintResult::success_with_metric(satisfaction_ratio))
        } else {
            let failed_count = total - satisfied;
            let message = if let Some(hint) = &self.hint {
                format!("{hint} ({} rows failed the condition)", failed_count as i64)
            } else {
                format!(
                    "Custom SQL condition not satisfied for {} rows. Expression: '{}'",
                    failed_count as i64, self.expression
                )
            };

            Ok(ConstraintResult::failure_with_metric(
                satisfaction_ratio,
                message,
            ))
        }
    }

    fn name(&self) -> &str {
        "custom_sql"
    }

    fn metadata(&self) -> ConstraintMetadata {
        let mut metadata = ConstraintMetadata::new()
            .with_description(format!(
                "Checks that all rows satisfy the SQL expression: {}",
                self.expression
            ))
            .with_custom("expression", self.expression.clone())
            .with_custom("constraint_type", "custom");

        if let Some(hint) = &self.hint {
            metadata = metadata.with_custom("hint", hint.clone());
        }

        metadata
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ConstraintStatus;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use std::sync::Arc;

    use crate::test_helpers::evaluate_constraint_with_context;
    async fn create_test_context() -> SessionContext {
        let ctx = SessionContext::new();

        let schema = Arc::new(Schema::new(vec![
            Field::new("price", DataType::Float64, true),
            Field::new("quantity", DataType::Int64, true),
            Field::new("status", DataType::Utf8, true),
        ]));

        let price_array =
            Float64Array::from(vec![Some(10.5), Some(25.0), Some(5.0), Some(100.0), None]);
        let quantity_array = Int64Array::from(vec![Some(5), Some(10), Some(0), Some(20), Some(15)]);
        let status_array = StringArray::from(vec![
            Some("active"),
            Some("active"),
            Some("inactive"),
            Some("active"),
            Some("pending"),
        ]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(price_array),
                Arc::new(quantity_array),
                Arc::new(status_array),
            ],
        )
        .unwrap();

        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("data", Arc::new(provider)).unwrap();

        ctx
    }

    #[test]
    fn test_sql_validation_accepts_safe_expressions() {
        // These should all be accepted
        assert!(validate_sql_expression("price > 0").is_ok());
        assert!(validate_sql_expression("quantity BETWEEN 1 AND 100").is_ok());
        assert!(validate_sql_expression("status = 'active' AND price < 1000").is_ok());
        assert!(validate_sql_expression("LENGTH(name) > 3").is_ok());
        assert!(validate_sql_expression("order_date <= ship_date").is_ok());
    }

    #[test]
    fn test_sql_validation_rejects_dangerous_operations() {
        // These should all be rejected
        assert!(validate_sql_expression("DROP TABLE users").is_err());
        assert!(validate_sql_expression("DELETE FROM {table_name} WHERE 1=1").is_err());
        assert!(validate_sql_expression("UPDATE data SET price = 0").is_err());
        assert!(validate_sql_expression("price > 0; DROP TABLE data").is_err());
        assert!(validate_sql_expression("INSERT INTO data VALUES (1, 2, 3)").is_err());
        assert!(validate_sql_expression("CREATE TABLE new_table (id INT)").is_err());
        assert!(validate_sql_expression("ALTER TABLE data ADD COLUMN new_col").is_err());
        assert!(validate_sql_expression("TRUNCATE TABLE data").is_err());
        assert!(validate_sql_expression("-- comment\nprice > 0").is_err());
        assert!(validate_sql_expression("price > 0 /* comment */").is_err());
    }

    #[test]
    fn test_sql_validation_case_insensitive() {
        // Should reject regardless of case
        assert!(validate_sql_expression("drop table users").is_err());
        assert!(validate_sql_expression("DeLeTe FROM {table_name}").is_err());
        assert!(validate_sql_expression("UpDaTe data SET x = 1").is_err());
    }

    #[test]
    fn test_sql_validation_word_boundaries() {
        // Should not reject if keyword is part of a larger word
        assert!(validate_sql_expression("updated_at > '2024-01-01'").is_ok());
        assert!(validate_sql_expression("is_deleted = false").is_ok());
        assert!(validate_sql_expression("created_by = 'admin'").is_ok());
    }

    #[tokio::test]
    async fn test_custom_sql_with_nulls_expression() {
        let ctx = create_test_context().await;

        let constraint = CustomSqlConstraint::new("price > 0", None::<String>).unwrap();

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Failure);
        assert_eq!(result.metric, Some(0.8)); // 4 out of 5 rows satisfy (NULL doesn't satisfy)
    }

    #[tokio::test]
    async fn test_custom_sql_all_satisfy() {
        let ctx = create_test_context().await;

        // Using quantity > -1 will be true for all rows (all quantities are >= 0)
        let constraint = CustomSqlConstraint::new("quantity >= 0", None::<String>).unwrap();

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(1.0)); // All rows satisfy
    }

    #[tokio::test]
    async fn test_custom_sql_partial_satisfy() {
        let ctx = create_test_context().await;

        let constraint =
            CustomSqlConstraint::new("quantity > 0", Some("Quantity must be positive")).unwrap();

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Failure);
        assert_eq!(result.metric, Some(0.8)); // 4 out of 5 have quantity > 0
        assert!(result
            .message
            .as_ref()
            .unwrap()
            .contains("Quantity must be positive"));
        assert!(result.message.as_ref().unwrap().contains("1 rows failed"));
    }

    #[tokio::test]
    async fn test_custom_sql_complex_expression() {
        let ctx = create_test_context().await;

        let constraint = CustomSqlConstraint::new(
            "status = 'active' AND price >= 10",
            Some("Active items must have price >= 10"),
        )
        .unwrap();

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Failure);
        // Only 3 rows have status='active' AND price >= 10
        assert_eq!(result.metric, Some(0.6));
    }

    #[tokio::test]
    async fn test_custom_sql_with_nulls() {
        let ctx = create_test_context().await;

        let constraint = CustomSqlConstraint::new("price IS NOT NULL", None::<String>).unwrap();

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Failure);
        assert_eq!(result.metric, Some(0.8)); // 4 out of 5 have non-null price
    }

    #[tokio::test]
    async fn test_custom_sql_invalid_expression() {
        let ctx = create_test_context().await;

        let constraint = CustomSqlConstraint::new("invalid_column > 0", None::<String>).unwrap();

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Failure);
        assert!(result
            .message
            .as_ref()
            .unwrap()
            .contains("SQL expression error"));
    }

    #[test]
    fn test_new_returns_error_on_dangerous_sql_new() {
        let result = CustomSqlConstraint::new("DROP TABLE data", None::<String>);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("forbidden operation: DROP"));
    }

    #[test]
    fn test_try_new_returns_error_on_dangerous_sql() {
        let result = CustomSqlConstraint::try_new("DELETE FROM {table_name}", None::<String>);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("forbidden operation: DELETE"));
    }
}
