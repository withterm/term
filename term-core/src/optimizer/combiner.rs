//! Query combination strategies for optimization.

use crate::optimizer::analyzer::{AggregationType, ConstraintAnalysis};
use crate::prelude::TermError;
use std::collections::{HashMap, HashSet};

/// A group of constraints that can be executed together.
#[derive(Debug)]
pub struct ConstraintGroup {
    /// The constraints in this group
    pub constraints: Vec<ConstraintAnalysis>,
    /// The combined SQL query for this group
    pub combined_sql: String,
    /// Mapping of result columns to constraint names
    pub result_mapping: HashMap<String, String>,
}

/// Combines compatible constraints into optimized query groups.
#[derive(Debug)]
pub struct QueryCombiner {
    /// Maximum number of constraints to combine in a single query
    max_group_size: usize,
}

impl QueryCombiner {
    /// Creates a new query combiner.
    pub fn new() -> Self {
        Self {
            max_group_size: 20, // Reasonable default to avoid overly complex queries
        }
    }

    /// Groups constraints by optimization strategy.
    pub fn group_constraints(
        &self,
        analyses: Vec<ConstraintAnalysis>,
    ) -> Result<Vec<ConstraintGroup>, TermError> {
        let mut groups = Vec::new();
        let mut processed = HashSet::new();

        // Group by table first
        let by_table = self.group_by_table(analyses);

        for (table, table_constraints) in by_table {
            // Within each table, group combinable constraints
            let combinable: Vec<_> = table_constraints
                .iter()
                .filter(|a| a.is_combinable && !processed.contains(&a.name))
                .cloned()
                .collect();

            if !combinable.is_empty() {
                // Create groups of compatible constraints
                let compatible_groups = self.find_compatible_groups(&combinable);

                for group in compatible_groups {
                    let combined = self.combine_group(&table, group)?;
                    for constraint in &combined.constraints {
                        processed.insert(constraint.name.clone());
                    }
                    groups.push(combined);
                }
            }

            // Handle non-combinable constraints individually
            for analysis in table_constraints {
                if !analysis.is_combinable && !processed.contains(&analysis.name) {
                    processed.insert(analysis.name.clone());
                    let individual = self.create_individual_group(analysis)?;
                    groups.push(individual);
                }
            }
        }

        Ok(groups)
    }

    /// Groups constraints by table name.
    fn group_by_table(
        &self,
        analyses: Vec<ConstraintAnalysis>,
    ) -> HashMap<String, Vec<ConstraintAnalysis>> {
        let mut by_table: HashMap<String, Vec<ConstraintAnalysis>> = HashMap::new();

        for analysis in analyses {
            by_table
                .entry(analysis.table_name.clone())
                .or_default()
                .push(analysis);
        }

        by_table
    }

    /// Finds groups of compatible constraints.
    fn find_compatible_groups(
        &self,
        constraints: &[ConstraintAnalysis],
    ) -> Vec<Vec<ConstraintAnalysis>> {
        let mut groups = Vec::new();
        let mut current_group = Vec::new();
        let mut used_columns = HashSet::new();
        let mut used_aggregations = HashSet::new();

        for constraint in constraints {
            // Check if this constraint is compatible with the current group
            let is_compatible = current_group.is_empty()
                || (self.has_compatible_aggregations(&constraint.aggregations, &used_aggregations)
                    && !self.has_column_conflicts(&constraint.columns, &used_columns)
                    && current_group.len() < self.max_group_size);

            if is_compatible {
                // Add to current group
                for agg in &constraint.aggregations {
                    used_aggregations.insert(agg.clone());
                }
                for col in &constraint.columns {
                    used_columns.insert(col.clone());
                }
                current_group.push(constraint.clone());
            } else {
                // Start a new group
                if !current_group.is_empty() {
                    groups.push(current_group);
                }
                current_group = vec![constraint.clone()];
                used_columns.clear();
                used_aggregations.clear();
                for agg in &constraint.aggregations {
                    used_aggregations.insert(agg.clone());
                }
                for col in &constraint.columns {
                    used_columns.insert(col.clone());
                }
            }
        }

        if !current_group.is_empty() {
            groups.push(current_group);
        }

        groups
    }

    /// Checks if aggregations are compatible.
    fn has_compatible_aggregations(
        &self,
        new_aggs: &[AggregationType],
        existing_aggs: &HashSet<AggregationType>,
    ) -> bool {
        // Simple compatibility check - can be made more sophisticated
        new_aggs.iter().all(|agg| {
            existing_aggs.is_empty()
                || existing_aggs.contains(agg)
                || matches!(agg, AggregationType::Count) // COUNT is always compatible
        })
    }

    /// Checks for column conflicts.
    fn has_column_conflicts(&self, new_cols: &[String], existing_cols: &HashSet<String>) -> bool {
        // For now, no conflicts if columns don't overlap too much
        let overlap = new_cols
            .iter()
            .filter(|col| existing_cols.contains(*col))
            .count();
        overlap > new_cols.len() / 2
    }

    /// Combines a group of constraints into a single optimized query.
    fn combine_group(
        &self,
        table: &str,
        constraints: Vec<ConstraintAnalysis>,
    ) -> Result<ConstraintGroup, TermError> {
        let mut select_parts = vec!["COUNT(*) as total_count".to_string()];
        let mut result_mapping = HashMap::new();

        // Add total_count mapping for all constraints that need it
        for constraint in &constraints {
            if constraint.aggregations.contains(&AggregationType::Count) {
                result_mapping.insert(
                    format!("{}_total", constraint.name),
                    "total_count".to_string(),
                );
            }
        }

        // Build SELECT clause with all needed aggregations
        for (i, constraint) in constraints.iter().enumerate() {
            for (j, agg) in constraint.aggregations.iter().enumerate() {
                if matches!(agg, AggregationType::Count) {
                    continue; // Already handled with total_count
                }

                let col_name = if constraint.columns.is_empty() {
                    "*".to_string()
                } else {
                    constraint.columns[0].clone() // Simplified
                };

                let alias = format!("{}_{}_{}_{}", constraint.name, i, agg_to_sql(agg), j);
                let sql_expr = match agg {
                    AggregationType::CountDistinct => {
                        format!("COUNT(DISTINCT {}) as {}", col_name, alias)
                    }
                    AggregationType::Sum => format!("SUM({}) as {}", col_name, alias),
                    AggregationType::Avg => format!("AVG({}) as {}", col_name, alias),
                    AggregationType::Min => format!("MIN({}) as {}", col_name, alias),
                    AggregationType::Max => format!("MAX({}) as {}", col_name, alias),
                    AggregationType::StdDev => format!("STDDEV({}) as {}", col_name, alias),
                    AggregationType::Variance => format!("VARIANCE({}) as {}", col_name, alias),
                    _ => continue,
                };

                select_parts.push(sql_expr);
                result_mapping.insert(format!("{}_{}", constraint.name, agg_to_sql(agg)), alias);
            }
        }

        let combined_sql = format!("SELECT {} FROM {}", select_parts.join(", "), table);

        Ok(ConstraintGroup {
            constraints,
            combined_sql,
            result_mapping,
        })
    }

    /// Creates a group for a single non-combinable constraint.
    fn create_individual_group(
        &self,
        analysis: ConstraintAnalysis,
    ) -> Result<ConstraintGroup, TermError> {
        // For non-combinable constraints, we'll let them execute their own queries
        let result_mapping = HashMap::new();

        Ok(ConstraintGroup {
            constraints: vec![analysis],
            combined_sql: String::new(), // Will use constraint's own SQL
            result_mapping,
        })
    }

    /// Sets the maximum group size.
    pub fn set_max_group_size(&mut self, size: usize) {
        self.max_group_size = size;
    }
}

/// Converts aggregation type to SQL function name.
fn agg_to_sql(agg: &AggregationType) -> &'static str {
    match agg {
        AggregationType::Count => "count",
        AggregationType::CountDistinct => "count_distinct",
        AggregationType::Sum => "sum",
        AggregationType::Avg => "avg",
        AggregationType::Min => "min",
        AggregationType::Max => "max",
        AggregationType::StdDev => "stddev",
        AggregationType::Variance => "variance",
    }
}

impl Default for QueryCombiner {
    fn default() -> Self {
        Self::new()
    }
}

// TODO: Fix tests once Completeness constraint is made public
#[cfg(test)]
mod tests {
    use super::*;
    // use crate::constraints::completeness::Completeness;

    #[test]
    fn test_combiner_creation() {
        let combiner = QueryCombiner::new();
        assert_eq!(combiner.max_group_size, 20);
    }

    // TODO: Re-enable once Completeness is made public
    // #[test]
    // fn test_group_by_table() {
    //     let combiner = QueryCombiner::new();
    //
    //     let analyses = vec![
    //         ConstraintAnalysis {
    //             name: "c1".to_string(),
    //             constraint: Arc::new(Completeness::new("col1")),
    //             table_name: "data".to_string(),
    //             aggregations: vec![AggregationType::Count],
    //             columns: vec!["col1".to_string()],
    //             has_predicates: false,
    //             is_combinable: true,
    //         },
    //         ConstraintAnalysis {
    //             name: "c2".to_string(),
    //             constraint: Arc::new(Completeness::new("col2")),
    //             table_name: "data".to_string(),
    //             aggregations: vec![AggregationType::Count],
    //             columns: vec!["col2".to_string()],
    //             has_predicates: false,
    //             is_combinable: true,
    //         },
    //     ];
    //
    //     let by_table = combiner.group_by_table(analyses);
    //     assert_eq!(by_table.len(), 1);
    //     assert_eq!(by_table.get("data").unwrap().len(), 2);
    // }
}
