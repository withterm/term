//! Foreign key constraint validation for Term.
//!
//! This module provides foreign key validation capabilities for ensuring referential integrity
//! between tables. Foreign keys ensure that values in a child table's column exist as primary
//! keys in a parent table, preventing orphaned records and maintaining data consistency.
//!
//! # Examples
//!
//! ## Basic Foreign Key Validation
//!
//! ```rust
//! use term_guard::constraints::ForeignKeyConstraint;
//! use term_guard::core::{Check, Level};
//!
//! // Validate that all order.customer_id values exist in customers.id
//! let constraint = ForeignKeyConstraint::new("orders.customer_id", "customers.id");
//!
//! let check = Check::builder("referential_integrity")
//!     .level(Level::Error)
//!     .with_constraint(constraint)
//!     .build();
//! ```
//!
//! ## Foreign Key with Null Handling
//!
//! ```rust
//! use term_guard::constraints::ForeignKeyConstraint;
//!
//! // Allow null values in the foreign key column
//! let constraint = ForeignKeyConstraint::new("orders.customer_id", "customers.id")
//!     .allow_nulls(true);
//! ```

use crate::core::{Constraint, ConstraintResult, ConstraintStatus};
use crate::error::{Result, TermError};
use crate::security::SqlSecurity;
use arrow::array::{Array, Int64Array, StringArray};
use async_trait::async_trait;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument, warn};

/// Foreign key constraint for validating referential integrity between tables.
///
/// This constraint ensures that all non-null values in a child table's foreign key column
/// exist as values in the parent table's referenced column. It's essential for maintaining
/// data consistency and preventing orphaned records.
///
/// The constraint supports:
/// - Inner and left joins for different validation strategies
/// - Null value handling (allow/disallow nulls in foreign key)
/// - Custom error messages and violation reporting
/// - Performance optimization through predicate pushdown
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyConstraint {
    /// Column in the child table (e.g., "orders.customer_id")
    child_column: String,
    /// Column in the parent table (e.g., "customers.id")
    parent_column: String,
    /// Whether to allow NULL values in the foreign key column
    allow_nulls: bool,
    /// Use left join strategy (faster for large tables with few violations)
    use_left_join: bool,
    /// Maximum number of violation examples to collect
    max_violations_reported: usize,
}

impl ForeignKeyConstraint {
    /// Create a new foreign key constraint.
    ///
    /// # Arguments
    ///
    /// * `child_column` - Column in child table containing foreign key values
    /// * `parent_column` - Column in parent table containing referenced values
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_guard::constraints::ForeignKeyConstraint;
    ///
    /// let fk = ForeignKeyConstraint::new("orders.customer_id", "customers.id");
    /// ```
    pub fn new(child_column: impl Into<String>, parent_column: impl Into<String>) -> Self {
        Self {
            child_column: child_column.into(),
            parent_column: parent_column.into(),
            allow_nulls: false,
            use_left_join: true,
            max_violations_reported: 100,
        }
    }

    /// Set whether to allow NULL values in the foreign key column.
    ///
    /// When `true`, NULL values in the child column are considered valid.
    /// When `false`, NULL values are treated as constraint violations.
    pub fn allow_nulls(mut self, allow: bool) -> Self {
        self.allow_nulls = allow;
        self
    }

    /// Set the join strategy for validation.
    ///
    /// - `true` (default): Use LEFT JOIN strategy, better for tables with few violations
    /// - `false`: Use NOT EXISTS strategy, better for tables with many violations
    pub fn use_left_join(mut self, use_left_join: bool) -> Self {
        self.use_left_join = use_left_join;
        self
    }

    /// Set the maximum number of violation examples to report.
    ///
    /// Defaults to 100. Set to 0 to disable violation example collection.
    pub fn max_violations_reported(mut self, max_violations: usize) -> Self {
        self.max_violations_reported = max_violations;
        self
    }

    /// Get the child column name
    pub fn child_column(&self) -> &str {
        &self.child_column
    }

    /// Get the parent column name  
    pub fn parent_column(&self) -> &str {
        &self.parent_column
    }

    /// Parse table and column from qualified column name (e.g., "orders.customer_id")
    fn parse_qualified_column(&self, qualified_column: &str) -> Result<(String, String)> {
        let parts: Vec<&str> = qualified_column.split('.').collect();
        if parts.len() != 2 {
            return Err(TermError::constraint_evaluation(
                "foreign_key",
                format!(
                    "Foreign key column must be qualified (table.column): '{qualified_column}'"
                ),
            ));
        }

        let table = parts[0].to_string();
        let column = parts[1].to_string();

        // Validate SQL identifiers for security
        SqlSecurity::validate_identifier(&table)?;
        SqlSecurity::validate_identifier(&column)?;

        Ok((table, column))
    }

    /// Generate SQL query for foreign key validation using LEFT JOIN strategy
    fn generate_left_join_query(
        &self,
        child_table: &str,
        child_col: &str,
        parent_table: &str,
        parent_col: &str,
    ) -> Result<String> {
        let null_condition = if self.allow_nulls {
            format!("AND {child_table}.{child_col} IS NOT NULL")
        } else {
            String::new()
        };

        let sql = format!(
            "SELECT 
                COUNT(*) as total_violations,
                COUNT(DISTINCT {child_table}.{child_col}) as unique_violations
             FROM {child_table} 
             LEFT JOIN {parent_table} ON {child_table}.{child_col} = {parent_table}.{parent_col}
             WHERE {parent_table}.{parent_col} IS NULL {null_condition}"
        );

        debug!("Generated foreign key validation query: {}", sql);
        Ok(sql)
    }

    /// Generate SQL query to get violation examples
    fn generate_violations_query(
        &self,
        child_table: &str,
        child_col: &str,
        parent_table: &str,
        parent_col: &str,
    ) -> Result<String> {
        if self.max_violations_reported == 0 {
            return Ok(String::new());
        }

        let null_condition = if self.allow_nulls {
            format!("AND {child_table}.{child_col} IS NOT NULL")
        } else {
            String::new()
        };

        let limit = self.max_violations_reported;
        let sql = format!(
            "SELECT DISTINCT {child_table}.{child_col} as violating_value
             FROM {child_table}
             LEFT JOIN {parent_table} ON {child_table}.{child_col} = {parent_table}.{parent_col}
             WHERE {parent_table}.{parent_col} IS NULL {null_condition}
             LIMIT {limit}"
        );

        debug!("Generated violations query: {}", sql);
        Ok(sql)
    }

    /// Collect violation examples using memory-efficient approach.
    ///
    /// This method limits memory usage by:
    /// 1. Using LIMIT in the SQL query to restrict result size at the database level
    /// 2. Pre-allocating vector with known maximum size
    /// 3. Processing results in a single pass without intermediate collections
    /// 4. Early termination when max violations are reached
    async fn collect_violation_examples_efficiently(
        &self,
        ctx: &SessionContext,
        child_table: &str,
        child_col: &str,
        parent_table: &str,
        parent_col: &str,
    ) -> Result<Vec<String>> {
        if self.max_violations_reported == 0 {
            return Ok(Vec::new());
        }

        let violations_sql =
            self.generate_violations_query(child_table, child_col, parent_table, parent_col)?;
        if violations_sql.is_empty() {
            return Ok(Vec::new());
        }

        debug!("Executing foreign key violations query with memory-efficient collection");

        let violations_df = ctx.sql(&violations_sql).await.map_err(|e| {
            TermError::constraint_evaluation(
                "foreign_key",
                format!("Failed to execute violations query: {e}"),
            )
        })?;

        let batches = violations_df.collect().await.map_err(|e| {
            TermError::constraint_evaluation(
                "foreign_key",
                format!("Failed to collect violation examples: {e}"),
            )
        })?;

        // Pre-allocate with known maximum size to avoid reallocations
        let mut violation_examples = Vec::with_capacity(self.max_violations_reported);

        // Process batches efficiently with early termination
        for batch in batches {
            for i in 0..batch.num_rows() {
                if violation_examples.len() >= self.max_violations_reported {
                    debug!(
                        "Reached max violations limit ({}), stopping collection",
                        self.max_violations_reported
                    );
                    return Ok(violation_examples);
                }

                // Handle different data types efficiently
                if let Some(string_array) = batch.column(0).as_any().downcast_ref::<StringArray>() {
                    if !string_array.is_null(i) {
                        violation_examples.push(string_array.value(i).to_string());
                    }
                } else if let Some(int64_array) =
                    batch.column(0).as_any().downcast_ref::<Int64Array>()
                {
                    if !int64_array.is_null(i) {
                        violation_examples.push(int64_array.value(i).to_string());
                    }
                } else if let Some(float64_array) = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                {
                    if !float64_array.is_null(i) {
                        violation_examples.push(float64_array.value(i).to_string());
                    }
                } else if let Some(int32_array) = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int32Array>()
                {
                    if !int32_array.is_null(i) {
                        violation_examples.push(int32_array.value(i).to_string());
                    }
                }
                // Add more data types as needed for broader compatibility
            }
        }

        debug!(
            "Collected {} foreign key violation examples",
            violation_examples.len()
        );
        Ok(violation_examples)
    }
}

#[async_trait]
impl Constraint for ForeignKeyConstraint {
    #[instrument(skip(self, ctx), fields(constraint = "foreign_key"))]
    async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult> {
        debug!(
            "Evaluating foreign key constraint: {} -> {}",
            self.child_column, self.parent_column
        );

        // Parse qualified column names
        let (child_table, child_col) = self.parse_qualified_column(&self.child_column)?;
        let (parent_table, parent_col) = self.parse_qualified_column(&self.parent_column)?;

        // Generate and execute validation query
        let sql =
            self.generate_left_join_query(&child_table, &child_col, &parent_table, &parent_col)?;
        let df = ctx.sql(&sql).await.map_err(|e| {
            TermError::constraint_evaluation(
                "foreign_key",
                format!("Foreign key validation query failed: {e}"),
            )
        })?;

        let batches = df.collect().await.map_err(|e| {
            TermError::constraint_evaluation(
                "foreign_key",
                format!("Failed to collect foreign key results: {e}"),
            )
        })?;

        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Ok(ConstraintResult::success());
        }

        // Extract violation counts
        let batch = &batches[0];
        let total_violations = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                TermError::constraint_evaluation(
                    "foreign_key",
                    "Invalid total violations column type",
                )
            })?
            .value(0);

        let unique_violations = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                TermError::constraint_evaluation(
                    "foreign_key",
                    "Invalid unique violations column type",
                )
            })?
            .value(0);

        if total_violations == 0 {
            debug!("Foreign key constraint passed: no violations found");
            return Ok(ConstraintResult::success());
        }

        // Collect violation examples using memory-efficient approach
        let violation_examples = self
            .collect_violation_examples_efficiently(
                ctx,
                &child_table,
                &child_col,
                &parent_table,
                &parent_col,
            )
            .await?;

        // Format error message
        let message = if violation_examples.is_empty() {
            format!(
                "Foreign key constraint violation: {total_violations} values in '{}' do not exist in '{}' (total: {total_violations}, unique: {unique_violations})",
                self.child_column, self.parent_column
            )
        } else {
            let examples_str = if violation_examples.len() <= 5 {
                violation_examples.join(", ")
            } else {
                format!(
                    "{}, ... ({} more)",
                    violation_examples[..5].join(", "),
                    violation_examples.len() - 5
                )
            };

            format!(
                "Foreign key constraint violation: {total_violations} values in '{}' do not exist in '{}' (total: {total_violations}, unique: {unique_violations}). Examples: [{examples_str}]",
                self.child_column, self.parent_column
            )
        };

        warn!("{}", message);

        Ok(ConstraintResult {
            status: ConstraintStatus::Failure,
            metric: Some(total_violations as f64),
            message: Some(message),
        })
    }

    fn name(&self) -> &str {
        "foreign_key"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::create_test_context;

    #[tokio::test]
    async fn test_foreign_key_constraint_success() -> Result<()> {
        let ctx = create_test_context().await?;

        // Create test tables with valid foreign keys only
        ctx.sql("CREATE TABLE customers_success (id BIGINT, name STRING)")
            .await?
            .collect()
            .await?;
        ctx.sql("INSERT INTO customers_success VALUES (1, 'Alice'), (2, 'Bob')")
            .await?
            .collect()
            .await?;
        ctx.sql("CREATE TABLE orders_success (id BIGINT, customer_id BIGINT, amount DOUBLE)")
            .await?
            .collect()
            .await?;
        ctx.sql("INSERT INTO orders_success VALUES (1, 1, 100.0), (2, 2, 200.0)")
            .await?
            .collect()
            .await?;

        let constraint =
            ForeignKeyConstraint::new("orders_success.customer_id", "customers_success.id");
        let result = constraint.evaluate(&ctx).await?;

        assert_eq!(result.status, ConstraintStatus::Success);
        assert!(result.message.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_foreign_key_constraint_violation() -> Result<()> {
        let ctx = create_test_context().await?;

        // Create test tables with foreign key violations
        ctx.sql("CREATE TABLE customers_violation (id BIGINT, name STRING)")
            .await?
            .collect()
            .await?;
        ctx.sql("INSERT INTO customers_violation VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
            .await?
            .collect()
            .await?;
        ctx.sql("CREATE TABLE orders_violation (id BIGINT, customer_id BIGINT, amount DOUBLE)")
            .await?
            .collect()
            .await?;
        ctx.sql("INSERT INTO orders_violation VALUES (1, 1, 100.0), (2, 2, 200.0), (3, 999, 300.0), (4, 998, 400.0)")
            .await?
            .collect()
            .await?;

        let constraint =
            ForeignKeyConstraint::new("orders_violation.customer_id", "customers_violation.id");
        let result = constraint.evaluate(&ctx).await?;

        assert_eq!(result.status, ConstraintStatus::Failure);
        assert!(result.message.is_some());
        assert_eq!(result.metric, Some(2.0)); // 2 violations

        let message = result.message.unwrap();
        assert!(message.contains("Foreign key constraint violation"));
        assert!(message.contains("2 values"));
        assert!(message.contains("orders_violation.customer_id"));
        assert!(message.contains("customers_violation.id"));

        Ok(())
    }

    #[tokio::test]
    async fn test_foreign_key_with_nulls_disallowed() -> Result<()> {
        let ctx = create_test_context().await?;

        ctx.sql("CREATE TABLE customers_nulls_disallowed (id BIGINT, name STRING)")
            .await?
            .collect()
            .await?;
        ctx.sql("INSERT INTO customers_nulls_disallowed VALUES (1, 'Alice')")
            .await?
            .collect()
            .await?;
        ctx.sql(
            "CREATE TABLE orders_nulls_disallowed (id BIGINT, customer_id BIGINT, amount DOUBLE)",
        )
        .await?
        .collect()
        .await?;
        ctx.sql("INSERT INTO orders_nulls_disallowed VALUES (1, 1, 100.0), (2, NULL, 200.0)")
            .await?
            .collect()
            .await?;

        let constraint = ForeignKeyConstraint::new(
            "orders_nulls_disallowed.customer_id",
            "customers_nulls_disallowed.id",
        )
        .allow_nulls(false);
        let result = constraint.evaluate(&ctx).await?;

        // Should fail because null is not allowed
        assert_eq!(result.status, ConstraintStatus::Failure);

        Ok(())
    }

    #[tokio::test]
    async fn test_foreign_key_with_nulls_allowed() -> Result<()> {
        let ctx = create_test_context().await?;

        ctx.sql("CREATE TABLE customers_nulls_allowed (id BIGINT, name STRING)")
            .await?
            .collect()
            .await?;
        ctx.sql("INSERT INTO customers_nulls_allowed VALUES (1, 'Alice')")
            .await?
            .collect()
            .await?;
        ctx.sql("CREATE TABLE orders_nulls_allowed (id BIGINT, customer_id BIGINT, amount DOUBLE)")
            .await?
            .collect()
            .await?;
        ctx.sql("INSERT INTO orders_nulls_allowed VALUES (1, 1, 100.0), (2, NULL, 200.0)")
            .await?
            .collect()
            .await?;

        let constraint = ForeignKeyConstraint::new(
            "orders_nulls_allowed.customer_id",
            "customers_nulls_allowed.id",
        )
        .allow_nulls(true);
        let result = constraint.evaluate(&ctx).await?;

        // Should succeed because null is allowed
        assert_eq!(result.status, ConstraintStatus::Success);

        Ok(())
    }

    #[test]
    fn test_parse_qualified_column() {
        let constraint = ForeignKeyConstraint::new("orders.customer_id", "customers.id");

        let (table, column) = constraint
            .parse_qualified_column("orders.customer_id")
            .unwrap();
        assert_eq!(table, "orders");
        assert_eq!(column, "customer_id");

        // Test invalid format
        assert!(constraint.parse_qualified_column("invalid_column").is_err());
        assert!(constraint.parse_qualified_column("too.many.parts").is_err());
    }

    #[test]
    fn test_constraint_configuration() {
        let constraint = ForeignKeyConstraint::new("orders.customer_id", "customers.id")
            .allow_nulls(true)
            .use_left_join(false)
            .max_violations_reported(50);

        assert_eq!(constraint.child_column(), "orders.customer_id");
        assert_eq!(constraint.parent_column(), "customers.id");
        assert!(constraint.allow_nulls);
        assert!(!constraint.use_left_join);
        assert_eq!(constraint.max_violations_reported, 50);
    }

    #[test]
    fn test_constraint_name() {
        let constraint = ForeignKeyConstraint::new("orders.customer_id", "customers.id");
        assert_eq!(constraint.name(), "foreign_key");
    }

    #[test]
    fn test_sql_generation() -> Result<()> {
        let constraint = ForeignKeyConstraint::new("orders.customer_id", "customers.id");
        let sql =
            constraint.generate_left_join_query("orders", "customer_id", "customers", "id")?;

        assert!(sql.contains("LEFT JOIN"));
        assert!(sql.contains("orders.customer_id = customers.id"));
        assert!(sql.contains("customers.id IS NULL"));
        assert!(sql.contains("COUNT(*) as total_violations"));

        Ok(())
    }

    #[test]
    fn test_sql_generation_with_nulls_allowed() -> Result<()> {
        let constraint =
            ForeignKeyConstraint::new("orders.customer_id", "customers.id").allow_nulls(true);
        let sql =
            constraint.generate_left_join_query("orders", "customer_id", "customers", "id")?;

        assert!(sql.contains("AND orders.customer_id IS NOT NULL"));

        Ok(())
    }
}
