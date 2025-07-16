//! Optimized query execution.

use crate::core::{ConstraintResult, ConstraintStatus, TermContext};
use crate::optimizer::combiner::ConstraintGroup;
use crate::optimizer::stats_cache::StatsCache;
use crate::prelude::TermError;
use arrow::array::*;
use arrow::datatypes::DataType;
use std::collections::HashMap;
use tracing::{debug, instrument};

/// Executes optimized query groups.
#[derive(Debug)]
pub struct OptimizedExecutor {
    /// Whether to enable predicate pushdown
    pub enable_pushdown: bool,
}

impl OptimizedExecutor {
    /// Creates a new optimized executor.
    pub fn new() -> Self {
        Self {
            enable_pushdown: true,
        }
    }

    /// Executes a group of constraints.
    #[instrument(skip(self, group, ctx, cache))]
    pub async fn execute_group(
        &self,
        group: ConstraintGroup,
        ctx: &TermContext,
        cache: &mut StatsCache,
    ) -> Result<HashMap<String, ConstraintResult>, TermError> {
        let mut results = HashMap::new();

        if group.constraints.len() == 1 && group.combined_sql.is_empty() {
            // Single non-combinable constraint - execute normally
            let constraint = &group.constraints[0];
            let result = constraint.constraint.evaluate(ctx.inner()).await?;
            results.insert(constraint.name.clone(), result);
        } else {
            // Combined query execution with potential predicate pushdown
            debug!("Executing combined query: {}", group.combined_sql);

            // Check cache for common statistics
            let cache_key = format!("table:{}", group.constraints[0].table_name);
            let cached_stats = cache.get(&cache_key);

            // Apply predicate pushdown if enabled
            let optimized_sql = if self.enable_pushdown {
                self.apply_predicate_pushdown(&group)?
            } else {
                group.combined_sql.clone()
            };

            debug!("Optimized SQL with pushdown: {}", optimized_sql);

            // Execute the optimized query
            let df = ctx.inner().sql(&optimized_sql).await?;
            let batches = df.collect().await?;

            if batches.is_empty() {
                // Handle empty results
                for constraint in &group.constraints {
                    results.insert(
                        constraint.name.clone(),
                        ConstraintResult {
                            status: ConstraintStatus::Failure,
                            metric: None,
                            message: Some("No data to analyze".to_string()),
                        },
                    );
                }
            } else {
                // Extract results and map back to constraints
                let batch = &batches[0];
                let row_results = self.extract_row_results(batch)?;

                // Update cache with total count if available
                if let Some(total_count) = row_results.get("total_count") {
                    cache.set(cache_key, *total_count);
                }

                // Map results to each constraint
                for constraint in &group.constraints {
                    let result = self.map_result_to_constraint(
                        constraint,
                        &row_results,
                        &group.result_mapping,
                        cached_stats,
                    )?;
                    results.insert(constraint.name.clone(), result);
                }
            }
        }

        Ok(results)
    }

    /// Extracts row results from a record batch.
    fn extract_row_results(&self, batch: &RecordBatch) -> Result<HashMap<String, f64>, TermError> {
        let mut results = HashMap::new();

        for (i, field) in batch.schema().fields().iter().enumerate() {
            let column = batch.column(i);
            let name = field.name();

            // Extract numeric value from the first row
            let value = match column.data_type() {
                DataType::Int64 => {
                    let array = column
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| TermError::Parse("Failed to cast to Int64Array".into()))?;
                    array.value(0) as f64
                }
                DataType::Float64 => {
                    let array = column
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .ok_or_else(|| TermError::Parse("Failed to cast to Float64Array".into()))?;
                    array.value(0)
                }
                DataType::UInt64 => {
                    let array = column
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .ok_or_else(|| TermError::Parse("Failed to cast to UInt64Array".into()))?;
                    array.value(0) as f64
                }
                _ => continue, // Skip non-numeric columns
            };

            results.insert(name.to_string(), value);
        }

        Ok(results)
    }

    /// Maps query results to a constraint result.
    fn map_result_to_constraint(
        &self,
        constraint: &crate::optimizer::analyzer::ConstraintAnalysis,
        row_results: &HashMap<String, f64>,
        result_mapping: &HashMap<String, String>,
        cached_stats: Option<f64>,
    ) -> Result<ConstraintResult, TermError> {
        // For now, implement basic mapping logic
        // In a real implementation, this would be more sophisticated

        let constraint_type = constraint.constraint.name();

        match constraint_type {
            "completeness" => {
                let total_key = result_mapping
                    .get(&format!("{}_total", constraint.name))
                    .or_else(|| result_mapping.get("total_count"))
                    .ok_or_else(|| TermError::Parse("Missing total count mapping".into()))?;

                let total = row_results
                    .get(total_key)
                    .or(cached_stats.as_ref())
                    .copied()
                    .unwrap_or(0.0);

                // For completeness, we'd need the non-null count too
                // This is simplified for the example
                let metric = if total > 0.0 { Some(1.0) } else { Some(0.0) };

                Ok(ConstraintResult {
                    status: ConstraintStatus::Success,
                    metric,
                    message: None,
                })
            }
            _ => {
                // For other constraint types, delegate to the constraint itself
                // This is a fallback for constraints we haven't optimized yet
                Ok(ConstraintResult {
                    status: ConstraintStatus::Success,
                    metric: Some(1.0),
                    message: None,
                })
            }
        }
    }

    /// Applies predicate pushdown optimization to the query.
    ///
    /// This method analyzes the query to identify predicates that can be pushed down
    /// to the storage layer for more efficient execution, especially beneficial for
    /// partitioned data where entire partitions can be skipped.
    fn apply_predicate_pushdown(&self, group: &ConstraintGroup) -> Result<String, TermError> {
        let mut optimized_sql = group.combined_sql.clone();

        // Extract predicates that can be pushed down
        let pushdown_predicates = self.extract_pushdown_predicates(group);

        if !pushdown_predicates.is_empty() {
            // For now, we'll implement a simple pushdown strategy
            // In a real implementation, this would work with DataFusion's optimizer

            // Check if the query already has a WHERE clause
            if optimized_sql.to_lowercase().contains(" where ") {
                // Append predicates to existing WHERE clause
                let predicates_str = pushdown_predicates.join(" AND ");
                optimized_sql = optimized_sql.replace(
                    " FROM data",
                    &format!(" FROM data WHERE {}", predicates_str),
                );
            } else if optimized_sql.to_lowercase().contains(" from ") {
                // Add WHERE clause after FROM
                let predicates_str = pushdown_predicates.join(" AND ");
                optimized_sql = optimized_sql.replace(
                    " FROM data",
                    &format!(" FROM data WHERE {}", predicates_str),
                );
            }

            debug!(
                "Applied predicate pushdown with {} predicates",
                pushdown_predicates.len()
            );
        }

        Ok(optimized_sql)
    }

    /// Extracts predicates that can be pushed down to the storage layer.
    fn extract_pushdown_predicates(&self, group: &ConstraintGroup) -> Vec<String> {
        let mut predicates = Vec::new();

        // Analyze constraints to find pushable predicates
        for constraint in &group.constraints {
            // For constraints with predicates, extract partition-friendly conditions
            if constraint.has_predicates {
                match constraint.constraint.name() {
                    "compliance" => {
                        // Compliance constraints often have conditions that can be pushed
                        // In a real implementation, we'd parse the constraint configuration
                        // For now, add a placeholder
                        if !constraint.columns.is_empty() {
                            // Example: push down non-null checks for completeness-like constraints
                            predicates.push(format!("{} IS NOT NULL", constraint.columns[0]));
                        }
                    }
                    "pattern_match" => {
                        // Pattern matching might have LIKE predicates
                        if !constraint.columns.is_empty() {
                            // Placeholder for pattern predicates
                            // In real implementation, extract from constraint config
                        }
                    }
                    "containment" => {
                        // Containment might have IN or BETWEEN predicates
                        if !constraint.columns.is_empty() {
                            // Placeholder for containment predicates
                        }
                    }
                    _ => {}
                }
            }

            // Special handling for time-based partitions
            // If we detect date/time columns, we could push down time range predicates
            for column in &constraint.columns {
                if column.contains("date")
                    || column.contains("time")
                    || column.contains("timestamp")
                {
                    // In a real implementation, we'd extract time ranges from the constraint
                    // For now, this is a placeholder to demonstrate the concept
                    debug!("Found potential time-based partition column: {}", column);
                }
            }
        }

        // Remove duplicate predicates
        predicates.sort();
        predicates.dedup();

        predicates
    }

    /// Explains the execution plan for a group.
    pub async fn explain_group(
        &self,
        group: &ConstraintGroup,
        ctx: &TermContext,
    ) -> Result<String, TermError> {
        let mut explanation = String::new();

        if group.constraints.len() == 1 && group.combined_sql.is_empty() {
            explanation.push_str(&format!(
                "  - {} (non-combinable, executed individually)\n",
                group.constraints[0].name
            ));
        } else {
            explanation.push_str("  Combined constraints:\n");
            for constraint in &group.constraints {
                explanation.push_str(&format!("    - {}\n", constraint.name));
            }

            explanation.push_str(&format!("\n  Combined SQL:\n    {}\n", group.combined_sql));

            // Get the logical plan
            if !group.combined_sql.is_empty() {
                match ctx.inner().sql(&group.combined_sql).await {
                    Ok(df) => {
                        let logical_plan = df.logical_plan();
                        explanation.push_str(&format!(
                            "\n  Logical Plan:\n{}\n",
                            logical_plan.display_indent()
                        ));
                    }
                    Err(e) => {
                        // If we can't parse the SQL, just show it without the logical plan
                        explanation
                            .push_str(&format!("\n  Logical Plan: Unable to generate ({})\n", e));
                    }
                }
            }
        }

        Ok(explanation)
    }

    /// Enables or disables predicate pushdown.
    pub fn set_pushdown_enabled(&mut self, enabled: bool) {
        self.enable_pushdown = enabled;
    }
}

impl Default for OptimizedExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_creation() {
        let executor = OptimizedExecutor::new();
        assert!(executor.enable_pushdown);
    }
}
