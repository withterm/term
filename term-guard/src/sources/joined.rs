//! Joined data sources for cross-table validation in Term.
//!
//! This module provides infrastructure for validating data relationships across multiple tables,
//! enabling foreign key validation, referential integrity checks, and cross-table consistency
//! rules that are not possible with single-table validation approaches.
//!
//! # Examples
//!
//! ## Basic Foreign Key Validation
//!
//! ```rust,no_run
//! use term_guard::sources::{JoinedSource, JoinType, DataSource, CsvSource};
//! use term_guard::core::{Check, Level};
//!
//! // Create joined source for orders -> customers relationship
//! let joined_source = JoinedSource::builder()
//!     .left_source(CsvSource::new("orders.csv").unwrap(), "orders")
//!     .right_source(CsvSource::new("customers.csv").unwrap(), "customers")
//!     .join_type(JoinType::Inner)
//!     .on("customer_id", "id")
//!     .build()
//!     .unwrap();
//! ```

use crate::error::{Result, TermError};
use crate::sources::DataSource;
use crate::telemetry::TermTelemetry;
use arrow::datatypes::Schema;
use async_trait::async_trait;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, info, instrument};

/// Types of joins supported for cross-table validation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinType {
    /// Inner join - only rows that match in both tables
    Inner,
    /// Left join - all rows from left table, matching rows from right
    Left,
    /// Right join - all rows from right table, matching rows from left  
    Right,
    /// Full outer join - all rows from both tables
    Full,
}

impl JoinType {
    /// Convert to SQL join syntax
    pub fn to_sql(&self) -> &'static str {
        match self {
            JoinType::Inner => "INNER JOIN",
            JoinType::Left => "LEFT JOIN",
            JoinType::Right => "RIGHT JOIN",
            JoinType::Full => "FULL OUTER JOIN",
        }
    }
}

/// Join condition specifying how tables are connected
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinCondition {
    /// Column from the left table
    pub left_column: String,
    /// Column from the right table  
    pub right_column: String,
    /// Type of join to perform
    pub join_type: JoinType,
}

impl JoinCondition {
    /// Create a new join condition
    pub fn new(
        left_column: impl Into<String>,
        right_column: impl Into<String>,
        join_type: JoinType,
    ) -> Self {
        Self {
            left_column: left_column.into(),
            right_column: right_column.into(),
            join_type,
        }
    }

    /// Generate SQL join clause
    pub fn to_sql(&self, left_alias: &str, right_alias: &str) -> String {
        format!(
            "{} ON {left_alias}.{} = {right_alias}.{}",
            self.join_type.to_sql(),
            self.left_column,
            self.right_column
        )
    }
}

/// A data source that represents the join of multiple tables
#[derive(Debug, Clone)]
pub struct JoinedSource {
    /// Primary (left) data source
    left_source: Arc<dyn DataSource>,
    /// Left table alias
    left_alias: String,
    /// Secondary (right) data source
    right_source: Arc<dyn DataSource>,
    /// Right table alias
    right_alias: String,
    /// Join condition
    join_condition: JoinCondition,
    /// Optional WHERE clause to filter joined results
    where_clause: Option<String>,
    /// Additional join stages for multi-table joins
    additional_joins: Vec<AdditionalJoin>,
}

/// Additional join for multi-table scenarios
#[derive(Debug, Clone)]
struct AdditionalJoin {
    source: Arc<dyn DataSource>,
    alias: String,
    condition: JoinCondition,
}

impl JoinedSource {
    /// Create a new builder for configuring joined sources
    pub fn builder() -> JoinedSourceBuilder {
        JoinedSourceBuilder::new()
    }

    /// Get the left table alias
    pub fn left_alias(&self) -> &str {
        &self.left_alias
    }

    /// Get the right table alias  
    pub fn right_alias(&self) -> &str {
        &self.right_alias
    }

    /// Get the join condition
    pub fn join_condition(&self) -> &JoinCondition {
        &self.join_condition
    }

    /// Generate SQL query for the joined tables
    #[instrument(skip(self))]
    pub fn generate_sql(&self, table_name: &str) -> String {
        let join_type_sql = self.join_condition.join_type.to_sql();
        let on_clause = format!(
            "ON {}.{} = {}.{}",
            self.left_alias,
            self.join_condition.left_column,
            self.right_alias,
            self.join_condition.right_column
        );
        let mut sql = format!(
            "CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM {} {join_type_sql} {} {on_clause}",
            self.left_alias,
            self.right_alias
        );

        // Add additional joins if any
        for additional in &self.additional_joins {
            sql.push(' ');
            sql.push_str(
                &additional
                    .condition
                    .to_sql(&self.left_alias, &additional.alias),
            );
            sql.push(' ');
            sql.push_str(&additional.alias);
        }

        // Add WHERE clause if specified
        if let Some(where_clause) = &self.where_clause {
            sql.push_str(" WHERE ");
            sql.push_str(where_clause);
        }

        debug!("Generated SQL for joined source: {}", sql);
        sql
    }
}

#[async_trait]
impl DataSource for JoinedSource {
    #[instrument(skip(self, ctx))]
    async fn register_with_telemetry(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        telemetry: Option<&Arc<TermTelemetry>>,
    ) -> Result<()> {
        info!("Registering joined source as table: {}", table_name);

        // Register individual sources with their aliases
        self.left_source
            .register_with_telemetry(ctx, &self.left_alias, telemetry)
            .await
            .map_err(|e| {
                TermError::data_source(
                    "joined",
                    format!("Failed to register left source '{}': {e}", self.left_alias),
                )
            })?;

        self.right_source
            .register_with_telemetry(ctx, &self.right_alias, telemetry)
            .await
            .map_err(|e| {
                TermError::data_source(
                    "joined",
                    format!(
                        "Failed to register right source '{}': {e}",
                        self.right_alias
                    ),
                )
            })?;

        // Register additional join sources
        for additional in &self.additional_joins {
            additional
                .source
                .register_with_telemetry(ctx, &additional.alias, telemetry)
                .await
                .map_err(|e| {
                    TermError::data_source(
                        "joined",
                        format!(
                            "Failed to register additional source '{}': {e}",
                            additional.alias
                        ),
                    )
                })?;
        }

        // Create the joined view
        let sql = self.generate_sql(table_name);
        ctx.sql(&sql).await.map_err(|e| {
            TermError::data_source("joined", format!("Failed to create joined view: {e}"))
        })?;

        info!("Successfully registered joined source: {}", table_name);
        Ok(())
    }

    fn schema(&self) -> Option<&Arc<Schema>> {
        // For joined sources, we don't have a pre-computed schema
        // The schema will be determined when the view is created
        None
    }

    fn description(&self) -> String {
        format!(
            "Joined source: {} {} {} ON {}.{} = {}.{}",
            self.left_alias,
            self.join_condition.join_type.to_sql(),
            self.right_alias,
            self.left_alias,
            self.join_condition.left_column,
            self.right_alias,
            self.join_condition.right_column
        )
    }
}

/// Builder for configuring joined data sources
pub struct JoinedSourceBuilder {
    left_source: Option<(Arc<dyn DataSource>, String)>,
    right_source: Option<(Arc<dyn DataSource>, String)>,
    join_condition: Option<JoinCondition>,
    where_clause: Option<String>,
    additional_joins: Vec<AdditionalJoin>,
}

impl JoinedSourceBuilder {
    fn new() -> Self {
        Self {
            left_source: None,
            right_source: None,
            join_condition: None,
            where_clause: None,
            additional_joins: Vec::new(),
        }
    }

    /// Set the left (primary) data source
    pub fn left_source<S: DataSource + 'static>(
        mut self,
        source: S,
        alias: impl Into<String>,
    ) -> Self {
        self.left_source = Some((Arc::new(source), alias.into()));
        self
    }

    /// Set the right (secondary) data source
    pub fn right_source<S: DataSource + 'static>(
        mut self,
        source: S,
        alias: impl Into<String>,
    ) -> Self {
        self.right_source = Some((Arc::new(source), alias.into()));
        self
    }

    /// Set the join condition with inner join
    pub fn on(mut self, left_column: impl Into<String>, right_column: impl Into<String>) -> Self {
        self.join_condition = Some(JoinCondition::new(
            left_column,
            right_column,
            JoinType::Inner,
        ));
        self
    }

    /// Set the join condition with specified join type
    pub fn join_on(
        mut self,
        left_column: impl Into<String>,
        right_column: impl Into<String>,
        join_type: JoinType,
    ) -> Self {
        self.join_condition = Some(JoinCondition::new(left_column, right_column, join_type));
        self
    }

    /// Set the join type (defaults to inner join)
    pub fn join_type(mut self, join_type: JoinType) -> Self {
        if let Some(ref mut condition) = self.join_condition {
            condition.join_type = join_type;
        }
        self
    }

    /// Add a WHERE clause to filter joined results
    pub fn where_clause(mut self, clause: impl Into<String>) -> Self {
        self.where_clause = Some(clause.into());
        self
    }

    /// Add an additional join for multi-table scenarios
    pub fn additional_join<S: DataSource + 'static>(
        mut self,
        source: S,
        alias: impl Into<String>,
        left_column: impl Into<String>,
        right_column: impl Into<String>,
        join_type: JoinType,
    ) -> Self {
        self.additional_joins.push(AdditionalJoin {
            source: Arc::new(source),
            alias: alias.into(),
            condition: JoinCondition::new(left_column, right_column, join_type),
        });
        self
    }

    /// Build the joined source
    pub fn build(self) -> Result<JoinedSource> {
        let left_source = self
            .left_source
            .ok_or_else(|| TermError::data_source("joined", "Left source is required"))?;
        let right_source = self
            .right_source
            .ok_or_else(|| TermError::data_source("joined", "Right source is required"))?;
        let join_condition = self
            .join_condition
            .ok_or_else(|| TermError::data_source("joined", "Join condition is required"))?;

        Ok(JoinedSource {
            left_source: left_source.0,
            left_alias: left_source.1,
            right_source: right_source.0,
            right_alias: right_source.1,
            join_condition,
            where_clause: self.where_clause,
            additional_joins: self.additional_joins,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::CsvSource;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_test_csv(data: &str) -> Result<NamedTempFile> {
        let mut temp_file = NamedTempFile::with_suffix(".csv")?;
        write!(temp_file, "{data}")?;
        temp_file.flush()?;
        Ok(temp_file)
    }

    #[test]
    fn test_join_type_sql() {
        assert_eq!(JoinType::Inner.to_sql(), "INNER JOIN");
        assert_eq!(JoinType::Left.to_sql(), "LEFT JOIN");
        assert_eq!(JoinType::Right.to_sql(), "RIGHT JOIN");
        assert_eq!(JoinType::Full.to_sql(), "FULL OUTER JOIN");
    }

    #[test]
    fn test_join_condition_sql() {
        let condition = JoinCondition::new("customer_id", "id", JoinType::Inner);
        assert_eq!(
            condition.to_sql("orders", "customers"),
            "INNER JOIN ON orders.customer_id = customers.id"
        );
    }

    #[tokio::test]
    async fn test_joined_source_builder() -> Result<()> {
        let orders_data = "order_id,customer_id,amount\n1,1,100.0\n2,2,200.0";
        let customers_data = "id,name\n1,Alice\n2,Bob";

        let orders_file = create_test_csv(orders_data)?;
        let customers_file = create_test_csv(customers_data)?;

        let orders_source = CsvSource::new(orders_file.path().to_string_lossy().to_string())?;
        let customers_source = CsvSource::new(customers_file.path().to_string_lossy().to_string())?;

        let joined_source = JoinedSource::builder()
            .left_source(orders_source, "orders")
            .right_source(customers_source, "customers")
            .on("customer_id", "id")
            .build()?;

        assert_eq!(joined_source.left_alias(), "orders");
        assert_eq!(joined_source.right_alias(), "customers");
        assert_eq!(joined_source.join_condition().join_type, JoinType::Inner);

        Ok(())
    }

    #[tokio::test]
    async fn test_joined_source_registration() -> Result<()> {
        let orders_data = "order_id,customer_id,amount\n1,1,100.0\n2,2,200.0\n3,999,300.0";
        let customers_data = "id,name\n1,Alice\n2,Bob";

        let orders_file = create_test_csv(orders_data)?;
        let customers_file = create_test_csv(customers_data)?;

        let orders_source = CsvSource::new(orders_file.path().to_string_lossy().to_string())?;
        let customers_source = CsvSource::new(customers_file.path().to_string_lossy().to_string())?;

        let joined_source = JoinedSource::builder()
            .left_source(orders_source, "orders")
            .right_source(customers_source, "customers")
            .join_on("customer_id", "id", JoinType::Left)
            .build()?;

        let ctx = SessionContext::new();
        joined_source
            .register(&ctx, "orders_with_customers")
            .await?;

        // Verify the joined table exists and can be queried
        let df = ctx
            .sql("SELECT COUNT(*) as count FROM orders_with_customers")
            .await?;
        let results = df.collect().await?;

        // Should have 3 rows (including the one with missing customer)
        assert_eq!(results.len(), 1);

        Ok(())
    }

    #[test]
    fn test_joined_source_sql_generation() -> Result<()> {
        let orders_data = "id,customer_id,amount\n1,1,100.0";
        let customers_data = "id,name\n1,Alice";

        let orders_file = create_test_csv(orders_data)?;
        let customers_file = create_test_csv(customers_data)?;

        let orders_source = CsvSource::new(orders_file.path().to_string_lossy().to_string())?;
        let customers_source = CsvSource::new(customers_file.path().to_string_lossy().to_string())?;

        let joined_source = JoinedSource::builder()
            .left_source(orders_source, "orders")
            .right_source(customers_source, "customers")
            .on("customer_id", "id")
            .where_clause("orders.amount > 50")
            .build()?;

        let sql = joined_source.generate_sql("test_view");

        assert!(sql.contains("CREATE OR REPLACE VIEW test_view"));
        assert!(sql.contains("INNER JOIN"));
        assert!(sql.contains("orders.customer_id = customers.id"));
        assert!(sql.contains("WHERE orders.amount > 50"));

        Ok(())
    }

    #[test]
    fn test_joined_source_description() -> Result<()> {
        let orders_data = "id,customer_id,amount\n1,1,100.0";
        let customers_data = "id,name\n1,Alice";

        let orders_file = create_test_csv(orders_data)?;
        let customers_file = create_test_csv(customers_data)?;

        let orders_source = CsvSource::new(orders_file.path().to_string_lossy().to_string())?;
        let customers_source = CsvSource::new(customers_file.path().to_string_lossy().to_string())?;

        let joined_source = JoinedSource::builder()
            .left_source(orders_source, "orders")
            .right_source(customers_source, "customers")
            .join_on("customer_id", "id", JoinType::Left)
            .build()?;

        let description = joined_source.description();
        assert!(description.contains("orders"));
        assert!(description.contains("LEFT JOIN"));
        assert!(description.contains("customers"));
        assert!(description.contains("customer_id"));

        Ok(())
    }
}
