//! Join coverage constraint for validating join quality in Term.
//!
//! This module provides join coverage validation capabilities for assessing the quality of table joins,
//! ensuring that expected match rates are achieved and identifying potential data quality issues
//! in relationships between tables.
//!
//! # Examples
//!
//! ## Basic Join Coverage Validation
//!
//! ```rust
//! use term_guard::constraints::JoinCoverageConstraint;
//! use term_guard::core::{Check, Level};
//!
//! // Validate that 95% of sales records have matching customers
//! let constraint = JoinCoverageConstraint::new("sales", "customers")
//!     .on("customer_id", "id")
//!     .expect_match_rate(0.95);
//!
//! let check = Check::builder("join_quality")
//!     .level(Level::Warning)
//!     .with_constraint(constraint)
//!     .build();
//! ```
//!
//! ## Join Coverage with Multiple Join Keys
//!
//! ```rust
//! use term_guard::constraints::JoinCoverageConstraint;
//!
//! // Validate join coverage on composite keys
//! let constraint = JoinCoverageConstraint::new("orders", "products")
//!     .on_multiple(vec![("product_id", "id"), ("variant", "variant_code")])
//!     .expect_match_rate(0.98);
//! ```

use crate::core::{Constraint, ConstraintResult, ConstraintStatus};
use crate::error::{Result, TermError};
use crate::security::SqlSecurity;
use arrow::array::{Array, Float64Array};
use async_trait::async_trait;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument, warn};

/// Join coverage constraint for validating the quality of table joins.
///
/// This constraint measures what percentage of rows from the left table successfully
/// join with the right table, helping identify:
/// - Missing reference data
/// - Data quality issues in foreign key relationships
/// - Incomplete data loads
/// - Orphaned records
///
/// The constraint supports:
/// - Single and composite join keys
/// - Configurable expected match rates
/// - Detailed mismatch reporting
/// - Inner, left, and right join coverage analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinCoverageConstraint {
    /// Left table name
    left_table: String,
    /// Right table name
    right_table: String,
    /// Join key pairs [(left_column, right_column)]
    join_keys: Vec<(String, String)>,
    /// Expected match rate (0.0 to 1.0)
    expected_match_rate: f64,
    /// Type of coverage to check
    coverage_type: CoverageType,
    /// Whether to count distinct values only
    distinct_only: bool,
    /// Maximum number of unmatched examples to report
    max_examples_reported: usize,
}

/// Type of join coverage to validate
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CoverageType {
    /// Check coverage from left table perspective (left join)
    LeftCoverage,
    /// Check coverage from right table perspective (right join)
    RightCoverage,
    /// Check bidirectional coverage (both directions)
    BidirectionalCoverage,
}

impl JoinCoverageConstraint {
    /// Create a new join coverage constraint.
    ///
    /// # Arguments
    ///
    /// * `left_table` - Name of the left table in the join
    /// * `right_table` - Name of the right table in the join
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_guard::constraints::JoinCoverageConstraint;
    ///
    /// let constraint = JoinCoverageConstraint::new("orders", "customers");
    /// ```
    pub fn new(left_table: impl Into<String>, right_table: impl Into<String>) -> Self {
        Self {
            left_table: left_table.into(),
            right_table: right_table.into(),
            join_keys: Vec::new(),
            expected_match_rate: 1.0,
            coverage_type: CoverageType::LeftCoverage,
            distinct_only: false,
            max_examples_reported: 100,
        }
    }

    /// Set the join key columns.
    ///
    /// # Arguments
    ///
    /// * `left_column` - Column in the left table
    /// * `right_column` - Column in the right table
    pub fn on(mut self, left_column: impl Into<String>, right_column: impl Into<String>) -> Self {
        self.join_keys = vec![(left_column.into(), right_column.into())];
        self
    }

    /// Set multiple join key columns for composite joins.
    ///
    /// # Arguments
    ///
    /// * `keys` - Vector of (left_column, right_column) pairs
    pub fn on_multiple(mut self, keys: Vec<(impl Into<String>, impl Into<String>)>) -> Self {
        self.join_keys = keys
            .into_iter()
            .map(|(l, r)| (l.into(), r.into()))
            .collect();
        self
    }

    /// Set the expected match rate.
    ///
    /// # Arguments
    ///
    /// * `rate` - Expected percentage of rows that should match (0.0 to 1.0)
    pub fn expect_match_rate(mut self, rate: f64) -> Self {
        self.expected_match_rate = rate.clamp(0.0, 1.0);
        self
    }

    /// Set the coverage type to check.
    pub fn coverage_type(mut self, coverage_type: CoverageType) -> Self {
        self.coverage_type = coverage_type;
        self
    }

    /// Count only distinct values when calculating coverage.
    pub fn distinct_only(mut self, distinct: bool) -> Self {
        self.distinct_only = distinct;
        self
    }

    /// Set the maximum number of unmatched examples to report.
    pub fn max_examples_reported(mut self, max_examples: usize) -> Self {
        self.max_examples_reported = max_examples;
        self
    }

    /// Validate table and column names for security.
    fn validate_identifiers(&self) -> Result<()> {
        SqlSecurity::validate_identifier(&self.left_table)?;
        SqlSecurity::validate_identifier(&self.right_table)?;

        for (left_col, right_col) in &self.join_keys {
            SqlSecurity::validate_identifier(left_col)?;
            SqlSecurity::validate_identifier(right_col)?;
        }

        Ok(())
    }

    /// Generate SQL query for join coverage analysis.
    fn generate_coverage_query(&self) -> Result<String> {
        self.validate_identifiers()?;

        if self.join_keys.is_empty() {
            return Err(TermError::constraint_evaluation(
                "join_coverage",
                "No join keys specified. Use .on() or .on_multiple() to set join keys",
            ));
        }

        let join_condition = self
            .join_keys
            .iter()
            .map(|(l, r)| format!("{}.{l} = {}.{r}", self.left_table, self.right_table))
            .collect::<Vec<_>>()
            .join(" AND ");

        let count_expr = if self.distinct_only {
            format!(
                "COUNT(DISTINCT {}.{})",
                self.left_table, self.join_keys[0].0
            )
        } else {
            "COUNT(*)".to_string()
        };

        let sql = match self.coverage_type {
            CoverageType::LeftCoverage => {
                // Check how many rows from left table have matches in right table
                format!(
                    "WITH coverage_stats AS (
                        SELECT 
                            {count_expr} as total_left,
                            SUM(CASE WHEN {}.{} IS NOT NULL THEN 1 ELSE 0 END) as matched_left
                        FROM {} 
                        LEFT JOIN {} ON {join_condition}
                    )
                    SELECT 
                        total_left,
                        matched_left,
                        CAST(matched_left AS DOUBLE) / CAST(total_left AS DOUBLE) as match_rate
                    FROM coverage_stats",
                    self.right_table, self.join_keys[0].1, self.left_table, self.right_table
                )
            }
            CoverageType::RightCoverage => {
                // Check how many rows from right table have matches in left table
                format!(
                    "WITH coverage_stats AS (
                        SELECT 
                            {count_expr} as total_right,
                            SUM(CASE WHEN {}.{} IS NOT NULL THEN 1 ELSE 0 END) as matched_right
                        FROM {} 
                        RIGHT JOIN {} ON {join_condition}
                    )
                    SELECT 
                        total_right,
                        matched_right,
                        CAST(matched_right AS DOUBLE) / CAST(total_right AS DOUBLE) as match_rate
                    FROM coverage_stats",
                    self.left_table, self.join_keys[0].0, self.right_table, self.left_table
                )
            }
            CoverageType::BidirectionalCoverage => {
                // Check coverage in both directions
                format!(
                    "WITH left_coverage AS (
                        SELECT 
                            COUNT(*) as total_left,
                            SUM(CASE WHEN {}.{} IS NOT NULL THEN 1 ELSE 0 END) as matched_left
                        FROM {} 
                        LEFT JOIN {} ON {join_condition}
                    ),
                    right_coverage AS (
                        SELECT 
                            COUNT(*) as total_right,
                            SUM(CASE WHEN {}.{} IS NOT NULL THEN 1 ELSE 0 END) as matched_right
                        FROM {} 
                        RIGHT JOIN {} ON {join_condition}
                    )
                    SELECT 
                        l.total_left,
                        l.matched_left,
                        r.total_right,
                        r.matched_right,
                        LEAST(
                            CAST(l.matched_left AS DOUBLE) / CAST(l.total_left AS DOUBLE),
                            CAST(r.matched_right AS DOUBLE) / CAST(r.total_right AS DOUBLE)
                        ) as match_rate
                    FROM left_coverage l, right_coverage r",
                    self.right_table,
                    self.join_keys[0].1,
                    self.left_table,
                    self.right_table,
                    self.left_table,
                    self.join_keys[0].0,
                    self.right_table,
                    self.left_table
                )
            }
        };

        debug!("Generated join coverage query: {}", sql);
        Ok(sql)
    }

    /// Generate query to find unmatched examples.
    fn generate_unmatched_query(&self) -> Result<String> {
        if self.max_examples_reported == 0 {
            return Ok(String::new());
        }

        self.validate_identifiers()?;

        let join_condition = self
            .join_keys
            .iter()
            .map(|(l, r)| format!("{}.{l} = {}.{r}", self.left_table, self.right_table))
            .collect::<Vec<_>>()
            .join(" AND ");

        let key_columns = self
            .join_keys
            .iter()
            .map(|(l, _)| format!("{}.{l}", self.left_table))
            .collect::<Vec<_>>()
            .join(", ");

        let sql = format!(
            "SELECT DISTINCT {key_columns} 
             FROM {} 
             LEFT JOIN {} ON {join_condition}
             WHERE {}.{} IS NULL
             LIMIT {}",
            self.left_table,
            self.right_table,
            self.right_table,
            self.join_keys[0].1,
            self.max_examples_reported
        );

        Ok(sql)
    }
}

#[async_trait]
impl Constraint for JoinCoverageConstraint {
    #[instrument(skip(self, ctx), fields(constraint = "join_coverage"))]
    async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult> {
        debug!(
            "Evaluating join coverage: {} <-> {} on {:?}",
            self.left_table, self.right_table, self.join_keys
        );

        // Generate and execute coverage query
        let sql = self.generate_coverage_query()?;
        let df = ctx.sql(&sql).await.map_err(|e| {
            TermError::constraint_evaluation(
                "join_coverage",
                format!("Join coverage query failed: {e}"),
            )
        })?;

        let batches = df.collect().await.map_err(|e| {
            TermError::constraint_evaluation(
                "join_coverage",
                format!("Failed to collect join coverage results: {e}"),
            )
        })?;

        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Err(TermError::constraint_evaluation(
                "join_coverage",
                "No results from join coverage query",
            ));
        }

        // Extract match rate from results
        let batch = &batches[0];
        let match_rate_col = batch
            .column(batch.num_columns() - 1) // Last column is always match_rate
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                TermError::constraint_evaluation("join_coverage", "Invalid match rate column type")
            })?;

        let match_rate = match_rate_col.value(0);

        debug!(
            "Join coverage: {:.2}% (expected: {:.2}%)",
            match_rate * 100.0,
            self.expected_match_rate * 100.0
        );

        // Check if match rate meets expectations
        if match_rate >= self.expected_match_rate {
            return Ok(ConstraintResult::success_with_metric(match_rate));
        }

        // Generate detailed failure message
        let coverage_desc = match self.coverage_type {
            CoverageType::LeftCoverage => format!("{} -> {}", self.left_table, self.right_table),
            CoverageType::RightCoverage => format!("{} <- {}", self.left_table, self.right_table),
            CoverageType::BidirectionalCoverage => {
                format!("{} <-> {}", self.left_table, self.right_table)
            }
        };

        // Try to get unmatched examples
        let unmatched_query = self.generate_unmatched_query()?;
        let examples_msg = if !unmatched_query.is_empty() {
            match ctx.sql(&unmatched_query).await {
                Ok(df) => match df.collect().await {
                    Ok(batches) if !batches.is_empty() && batches[0].num_rows() > 0 => {
                        let examples_count = batches[0].num_rows();
                        format!(" ({examples_count} unmatched examples found)")
                    }
                    _ => String::new(),
                },
                _ => String::new(),
            }
        } else {
            String::new()
        };

        let message = format!(
            "Join coverage constraint failed: {coverage_desc} coverage is {:.2}% (expected: {:.2}%){examples_msg}",
            match_rate * 100.0,
            self.expected_match_rate * 100.0
        );

        warn!("{}", message);

        Ok(ConstraintResult {
            status: ConstraintStatus::Failure,
            metric: Some(match_rate),
            message: Some(message),
        })
    }

    fn name(&self) -> &str {
        "join_coverage"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::create_test_context;

    #[tokio::test]
    async fn test_join_coverage_success() -> Result<()> {
        let ctx = create_test_context().await?;

        // Create test tables with full coverage
        ctx.sql("CREATE TABLE orders_cov (id BIGINT, customer_id BIGINT)")
            .await?
            .collect()
            .await?;
        ctx.sql("INSERT INTO orders_cov VALUES (1, 1), (2, 2), (3, 3)")
            .await?
            .collect()
            .await?;
        ctx.sql("CREATE TABLE customers_cov (id BIGINT, name STRING)")
            .await?
            .collect()
            .await?;
        ctx.sql("INSERT INTO customers_cov VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
            .await?
            .collect()
            .await?;

        let constraint = JoinCoverageConstraint::new("orders_cov", "customers_cov")
            .on("customer_id", "id")
            .expect_match_rate(1.0);

        let result = constraint.evaluate(&ctx).await?;
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(1.0));

        Ok(())
    }

    #[tokio::test]
    async fn test_join_coverage_partial() -> Result<()> {
        let ctx = create_test_context().await?;

        // Create test tables with partial coverage
        ctx.sql("CREATE TABLE orders_partial (id BIGINT, customer_id BIGINT)")
            .await?
            .collect()
            .await?;
        ctx.sql("INSERT INTO orders_partial VALUES (1, 1), (2, 2), (3, 999)")
            .await?
            .collect()
            .await?;
        ctx.sql("CREATE TABLE customers_partial (id BIGINT, name STRING)")
            .await?
            .collect()
            .await?;
        ctx.sql("INSERT INTO customers_partial VALUES (1, 'Alice'), (2, 'Bob')")
            .await?
            .collect()
            .await?;

        let constraint = JoinCoverageConstraint::new("orders_partial", "customers_partial")
            .on("customer_id", "id")
            .expect_match_rate(0.6); // Expecting 60% coverage

        let result = constraint.evaluate(&ctx).await?;
        assert_eq!(result.status, ConstraintStatus::Success);
        assert!((result.metric.unwrap() - 0.666).abs() < 0.01); // ~66.7% actual coverage

        Ok(())
    }

    #[tokio::test]
    async fn test_join_coverage_failure() -> Result<()> {
        let ctx = create_test_context().await?;

        // Create test tables with low coverage
        ctx.sql("CREATE TABLE orders_low (id BIGINT, customer_id BIGINT)")
            .await?
            .collect()
            .await?;
        ctx.sql("INSERT INTO orders_low VALUES (1, 999), (2, 998), (3, 997)")
            .await?
            .collect()
            .await?;
        ctx.sql("CREATE TABLE customers_low (id BIGINT, name STRING)")
            .await?
            .collect()
            .await?;
        ctx.sql("INSERT INTO customers_low VALUES (1, 'Alice')")
            .await?
            .collect()
            .await?;

        let constraint = JoinCoverageConstraint::new("orders_low", "customers_low")
            .on("customer_id", "id")
            .expect_match_rate(0.9); // Expecting 90% coverage but will get 0%

        let result = constraint.evaluate(&ctx).await?;
        assert_eq!(result.status, ConstraintStatus::Failure);
        assert_eq!(result.metric, Some(0.0));
        assert!(result.message.is_some());

        Ok(())
    }

    #[test]
    fn test_constraint_configuration() {
        let constraint = JoinCoverageConstraint::new("orders", "customers")
            .on("customer_id", "id")
            .expect_match_rate(0.95)
            .coverage_type(CoverageType::BidirectionalCoverage)
            .distinct_only(true)
            .max_examples_reported(50);

        assert_eq!(constraint.left_table, "orders");
        assert_eq!(constraint.right_table, "customers");
        assert_eq!(constraint.expected_match_rate, 0.95);
        assert_eq!(
            constraint.coverage_type,
            CoverageType::BidirectionalCoverage
        );
        assert!(constraint.distinct_only);
        assert_eq!(constraint.max_examples_reported, 50);
    }

    #[test]
    fn test_composite_keys() {
        let constraint = JoinCoverageConstraint::new("orders", "products")
            .on_multiple(vec![("product_id", "id"), ("variant", "variant_code")])
            .expect_match_rate(0.98);

        assert_eq!(constraint.join_keys.len(), 2);
        assert_eq!(
            constraint.join_keys[0],
            ("product_id".to_string(), "id".to_string())
        );
        assert_eq!(
            constraint.join_keys[1],
            ("variant".to_string(), "variant_code".to_string())
        );
    }
}
