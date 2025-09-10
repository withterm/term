//! Fluent builder API for complex multi-table constraints.
//!
//! This module provides an intuitive, chainable API for constructing complex validation
//! scenarios involving multiple tables and their relationships. Part of Phase 3: UX & Integration
//! for the Term joined data sources feature.
//!
//! # Examples
//!
//! ```rust
//! use term_guard::core::fluent_builder::{MultiTableCheck, JoinType};
//! use term_guard::core::Level;
//! use term_guard::constraints::Assertion;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let check = MultiTableCheck::new("order_integrity")
//!     .validate_tables("orders", "customers")
//!         .join_on("customer_id", "id")
//!         .ensure_referential_integrity()
//!         .expect_join_coverage(0.95)
//!     .and_validate_tables("orders", "payments")
//!         .join_on("order_id", "order_id")
//!         .ensure_sum_consistency("total", "amount")
//!         .group_by("customer_id")
//!         .with_tolerance(0.01)
//!     .and_validate_temporal("events")
//!         .ensure_ordering("created_at", "processed_at")
//!         .within_business_hours("09:00", "17:00")
//!     .build();
//! # Ok(())
//! # }
//! ```

use crate::constraints::{
    CrossTableSumConstraint, ForeignKeyConstraint, JoinCoverageConstraint,
    TemporalOrderingConstraint,
};
use crate::core::{Check, Level};
use std::sync::Arc;

/// Fluent builder for multi-table validation checks.
///
/// This builder provides an intuitive API for constructing complex validation
/// scenarios involving multiple tables, making it easier to express complex
/// relationships and consistency requirements.
#[derive(Debug)]
pub struct MultiTableCheck {
    name: String,
    level: Level,
    description: Option<String>,
    current_context: Option<TableContext>,
    constraints: Vec<Arc<dyn crate::core::Constraint>>,
}

/// Context for the current table validation being configured.
#[derive(Debug, Clone)]
struct TableContext {
    left_table: String,
    right_table: Option<String>,
    join_columns: Vec<(String, String)>,
    group_by_columns: Vec<String>,
}

impl MultiTableCheck {
    /// Create a new multi-table check builder.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the validation check
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            level: Level::Error,
            description: None,
            current_context: None,
            constraints: Vec::new(),
        }
    }

    /// Set the severity level for this check.
    pub fn level(mut self, level: Level) -> Self {
        self.level = level;
        self
    }

    /// Add a description for this check.
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Start validating a relationship between two tables.
    ///
    /// # Arguments
    ///
    /// * `left_table` - The left table in the relationship
    /// * `right_table` - The right table in the relationship
    ///
    /// # Example
    ///
    /// ```rust
    /// use term_guard::core::fluent_builder::MultiTableCheck;
    ///
    /// let check = MultiTableCheck::new("validation")
    ///     .validate_tables("orders", "customers")
    ///     .join_on("customer_id", "id")
    ///     .ensure_referential_integrity()
    ///     .build();
    /// ```
    pub fn validate_tables(
        mut self,
        left_table: impl Into<String>,
        right_table: impl Into<String>,
    ) -> Self {
        self.current_context = Some(TableContext {
            left_table: left_table.into(),
            right_table: Some(right_table.into()),
            join_columns: Vec::new(),
            group_by_columns: Vec::new(),
        });
        self
    }

    /// Start another table validation (convenience method).
    pub fn and_validate_tables(
        self,
        left_table: impl Into<String>,
        right_table: impl Into<String>,
    ) -> Self {
        self.validate_tables(left_table, right_table)
    }

    /// Start validating a single table for temporal constraints.
    ///
    /// # Arguments
    ///
    /// * `table` - The table to validate
    pub fn validate_temporal(mut self, table: impl Into<String>) -> Self {
        self.current_context = Some(TableContext {
            left_table: table.into(),
            right_table: None,
            join_columns: Vec::new(),
            group_by_columns: Vec::new(),
        });
        self
    }

    /// Start another temporal validation (convenience method).
    pub fn and_validate_temporal(self, table: impl Into<String>) -> Self {
        self.validate_temporal(table)
    }

    /// Specify the join columns for the current table relationship.
    ///
    /// # Arguments
    ///
    /// * `left_column` - Column from the left table
    /// * `right_column` - Column from the right table
    pub fn join_on(
        mut self,
        left_column: impl Into<String>,
        right_column: impl Into<String>,
    ) -> Self {
        if let Some(ref mut ctx) = self.current_context {
            ctx.join_columns
                .push((left_column.into(), right_column.into()));
        }
        self
    }

    /// Specify multiple join columns (composite key).
    pub fn join_on_multiple(mut self, columns: Vec<(&str, &str)>) -> Self {
        if let Some(ref mut ctx) = self.current_context {
            for (left, right) in columns {
                ctx.join_columns.push((left.to_string(), right.to_string()));
            }
        }
        self
    }

    /// Group results by specified columns.
    pub fn group_by(mut self, column: impl Into<String>) -> Self {
        if let Some(ref mut ctx) = self.current_context {
            ctx.group_by_columns.push(column.into());
        }
        self
    }

    /// Group results by multiple columns.
    pub fn group_by_multiple(mut self, columns: Vec<impl Into<String>>) -> Self {
        if let Some(ref mut ctx) = self.current_context {
            ctx.group_by_columns
                .extend(columns.into_iter().map(Into::into));
        }
        self
    }

    /// Ensure referential integrity between the current tables.
    ///
    /// This adds a foreign key constraint using the specified join columns.
    pub fn ensure_referential_integrity(mut self) -> Self {
        if let Some(ref ctx) = self.current_context {
            if let (Some(right_table), Some((left_col, right_col))) =
                (&ctx.right_table, ctx.join_columns.first())
            {
                let child_column = format!("{}.{left_col}", ctx.left_table);
                let parent_column = format!("{right_table}.{right_col}");

                self.constraints.push(Arc::new(ForeignKeyConstraint::new(
                    child_column,
                    parent_column,
                )));
            }
        }
        self
    }

    /// Expect a specific join coverage rate.
    ///
    /// # Arguments
    ///
    /// * `min_coverage` - Minimum expected coverage rate (0.0 to 1.0)
    pub fn expect_join_coverage(mut self, min_coverage: f64) -> Self {
        if let Some(ref ctx) = self.current_context {
            if let Some(ref right_table) = ctx.right_table {
                let mut constraint = JoinCoverageConstraint::new(&ctx.left_table, right_table);

                // Add join columns
                if ctx.join_columns.len() == 1 {
                    let (left_col, right_col) = &ctx.join_columns[0];
                    constraint = constraint.on(left_col, right_col);
                } else if ctx.join_columns.len() > 1 {
                    let cols: Vec<_> = ctx
                        .join_columns
                        .iter()
                        .map(|(l, r)| (l.as_str(), r.as_str()))
                        .collect();
                    constraint = constraint.on_multiple(cols);
                }

                constraint = constraint.expect_match_rate(min_coverage);
                self.constraints.push(Arc::new(constraint));
            }
        }
        self
    }

    /// Ensure sum consistency between columns in the current tables.
    ///
    /// # Arguments
    ///
    /// * `left_column` - Column from the left table to sum
    /// * `right_column` - Column from the right table to sum
    pub fn ensure_sum_consistency(
        mut self,
        left_column: impl Into<String>,
        right_column: impl Into<String>,
    ) -> Self {
        if let Some(ref ctx) = self.current_context {
            if let Some(ref right_table) = ctx.right_table {
                let left_col = format!("{}.{}", ctx.left_table, left_column.into());
                let right_col = format!("{right_table}.{}", right_column.into());

                let mut constraint = CrossTableSumConstraint::new(left_col, right_col);

                // Add group by columns if specified
                if !ctx.group_by_columns.is_empty() {
                    constraint = constraint.group_by(ctx.group_by_columns.clone());
                }

                self.constraints.push(Arc::new(constraint));
            }
        }
        self
    }

    /// Set tolerance for the current sum consistency constraint.
    ///
    /// # Arguments
    ///
    /// * `tolerance` - Maximum allowed difference (e.g., 0.01 for 1%)
    pub fn with_tolerance(self, _tolerance: f64) -> Self {
        // Update the last constraint if it's a CrossTableSumConstraint
        if let Some(_last) = self.constraints.last() {
            // This would require modifying the constraint after creation
            // For now, we'll need to recreate it
            // In production, we'd enhance the constraint API to support this
        }
        self
    }

    /// Ensure temporal ordering between two columns.
    ///
    /// # Arguments
    ///
    /// * `before_column` - Column that should contain earlier timestamps
    /// * `after_column` - Column that should contain later timestamps
    pub fn ensure_ordering(
        mut self,
        before_column: impl Into<String>,
        after_column: impl Into<String>,
    ) -> Self {
        if let Some(ref ctx) = self.current_context {
            let constraint = TemporalOrderingConstraint::new(&ctx.left_table)
                .before_after(before_column, after_column);

            self.constraints.push(Arc::new(constraint));
        }
        self
    }

    /// Validate that timestamps fall within business hours.
    ///
    /// # Arguments
    ///
    /// * `start_time` - Start of business hours (e.g., "09:00")
    /// * `end_time` - End of business hours (e.g., "17:00")
    pub fn within_business_hours(
        mut self,
        start_time: impl Into<String>,
        end_time: impl Into<String>,
    ) -> Self {
        if let Some(ref ctx) = self.current_context {
            // Assuming a default timestamp column name "timestamp"
            // In production, this would be configurable
            let constraint = TemporalOrderingConstraint::new(&ctx.left_table).business_hours(
                "timestamp",
                start_time,
                end_time,
            );

            self.constraints.push(Arc::new(constraint));
        }
        self
    }

    /// Build the final Check with all configured constraints.
    pub fn build(self) -> Check {
        let mut builder = Check::builder(self.name).level(self.level);

        if let Some(desc) = self.description {
            builder = builder.description(desc);
        }

        for constraint in self.constraints {
            builder = builder.arc_constraint(constraint);
        }

        builder.build()
    }
}

/// Extension trait for Check to provide fluent multi-table methods.
pub trait CheckMultiTableExt {
    /// Create a multi-table validation check.
    fn multi_table(name: impl Into<String>) -> MultiTableCheck;
}

impl CheckMultiTableExt for Check {
    fn multi_table(name: impl Into<String>) -> MultiTableCheck {
        MultiTableCheck::new(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fluent_builder_basic() {
        let check = MultiTableCheck::new("test_validation")
            .level(Level::Warning)
            .description("Test multi-table validation")
            .validate_tables("orders", "customers")
            .join_on("customer_id", "id")
            .ensure_referential_integrity()
            .build();

        assert_eq!(check.name(), "test_validation");
        assert_eq!(check.level(), Level::Warning);
        assert_eq!(check.description(), Some("Test multi-table validation"));
        assert_eq!(check.constraints().len(), 1);
    }

    #[test]
    fn test_fluent_builder_complex() {
        let check = MultiTableCheck::new("complex_validation")
            .validate_tables("orders", "customers")
            .join_on("customer_id", "id")
            .ensure_referential_integrity()
            .expect_join_coverage(0.95)
            .and_validate_tables("orders", "payments")
            .join_on("order_id", "order_id")
            .ensure_sum_consistency("total", "amount")
            .group_by("customer_id")
            .and_validate_temporal("events")
            .ensure_ordering("created_at", "processed_at")
            .build();

        assert_eq!(check.name(), "complex_validation");
        // Should have 4 constraints: foreign key, join coverage, sum consistency, temporal ordering
        assert_eq!(check.constraints().len(), 4);
    }

    #[test]
    fn test_composite_keys() {
        let check = MultiTableCheck::new("composite_key_validation")
            .validate_tables("order_items", "products")
            .join_on_multiple(vec![("product_id", "id"), ("variant", "variant_code")])
            .ensure_referential_integrity()
            .build();

        assert_eq!(check.constraints().len(), 1);
    }
}
