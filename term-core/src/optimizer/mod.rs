//! Query optimization for validation constraints.
//!
//! This module provides optimization strategies to improve the performance of
//! validation queries by:
//! - Combining multiple checks into single table scans
//! - Implementing predicate pushdown for partitioned data
//! - Caching statistics across validation runs
//! - Providing query plan explanations for debugging

use crate::core::{Check, Constraint, ConstraintResult, TermContext};
use crate::prelude::TermError;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::instrument;

pub mod analyzer;
pub mod combiner;
pub mod executor;
pub mod stats_cache;

pub use analyzer::QueryAnalyzer;
pub use combiner::QueryCombiner;
pub use executor::OptimizedExecutor;
pub use stats_cache::StatsCache;

/// Query optimizer for validation constraints.
#[derive(Debug)]
pub struct QueryOptimizer {
    analyzer: QueryAnalyzer,
    combiner: QueryCombiner,
    executor: OptimizedExecutor,
    stats_cache: StatsCache,
}

impl QueryOptimizer {
    /// Creates a new query optimizer.
    pub fn new() -> Self {
        Self {
            analyzer: QueryAnalyzer::new(),
            combiner: QueryCombiner::new(),
            executor: OptimizedExecutor::new(),
            stats_cache: StatsCache::new(),
        }
    }

    /// Optimizes and executes a set of checks.
    ///
    /// This method:
    /// 1. Analyzes all constraints to identify optimization opportunities
    /// 2. Groups constraints by table and compatible operations
    /// 3. Combines queries to minimize table scans
    /// 4. Executes optimized queries and maps results back to constraints
    ///
    /// # Arguments
    ///
    /// * `checks` - The validation checks to optimize and execute
    /// * `ctx` - The Term context containing the DataFusion session
    ///
    /// # Returns
    ///
    /// A map of constraint names to their results.
    #[instrument(skip(self, checks, ctx))]
    pub async fn optimize_and_execute(
        &mut self,
        checks: &[Check],
        ctx: &TermContext,
    ) -> Result<HashMap<String, Vec<ConstraintResult>>, TermError> {
        // Extract all constraints from checks
        let constraints = self.extract_constraints(checks);

        // Analyze constraints to identify optimization opportunities
        let analysis = self.analyzer.analyze(&constraints)?;

        // Group constraints by optimization strategy
        let groups = self.combiner.group_constraints(analysis)?;

        // Execute optimized queries
        let mut results = HashMap::new();

        for group in groups {
            let group_results = self
                .executor
                .execute_group(group, ctx, &mut self.stats_cache)
                .await?;
            results.extend(group_results);
        }

        // Map results back to checks
        Ok(self.map_results_to_checks(checks, results))
    }

    /// Extracts all constraints from checks.
    fn extract_constraints(&self, checks: &[Check]) -> Vec<(String, Arc<dyn Constraint>)> {
        let mut constraints = Vec::new();

        for check in checks {
            for constraint in check.constraints() {
                let check_name = check.name();
                let constraint_name = constraint.name();
                let name = format!("{check_name}.{constraint_name}");
                constraints.push((name, constraint.clone()));
            }
        }

        constraints
    }

    /// Maps constraint results back to their respective checks.
    fn map_results_to_checks(
        &self,
        checks: &[Check],
        constraint_results: HashMap<String, ConstraintResult>,
    ) -> HashMap<String, Vec<ConstraintResult>> {
        let mut check_results = HashMap::new();

        for check in checks {
            let mut results = Vec::new();

            for constraint in check.constraints() {
                let check_name = check.name();
                let constraint_name = constraint.name();
                let name = format!("{check_name}.{constraint_name}");
                if let Some(result) = constraint_results.get(&name) {
                    results.push(result.clone());
                }
            }

            check_results.insert(check.name().to_string(), results);
        }

        check_results
    }

    /// Explains the query optimization plan for debugging.
    ///
    /// This provides detailed information about:
    /// - Which constraints were grouped together
    /// - What optimizations were applied
    /// - The resulting query plans
    pub async fn explain_plan(
        &mut self,
        checks: &[Check],
        ctx: &TermContext,
    ) -> Result<String, TermError> {
        let constraints = self.extract_constraints(checks);
        let analysis = self.analyzer.analyze(&constraints)?;
        let groups = self.combiner.group_constraints(analysis.clone())?;

        let mut explanation = String::new();
        explanation.push_str("Query Optimization Plan\n");
        explanation.push_str("======================\n\n");

        // Summary statistics
        explanation.push_str(&format!("Total Checks: {}\n", checks.len()));
        explanation.push_str(&format!("Total Constraints: {}\n", constraints.len()));
        explanation.push_str(&format!("Optimized Groups: {}\n", groups.len()));

        let combinable = analysis.iter().filter(|a| a.is_combinable).count();

        explanation.push_str(&format!("Combinable Constraints: {combinable}\n"));
        explanation.push_str(&format!(
            "Optimization Ratio: {:.1}%\n\n",
            if constraints.is_empty() {
                0.0
            } else {
                (groups.len() as f64 / constraints.len() as f64) * 100.0
            }
        ));

        // Detailed group information
        for (i, group) in groups.iter().enumerate() {
            explanation.push_str(&format!(
                "Group {}: {} constraints\n",
                i + 1,
                group.constraints.len()
            ));
            explanation.push_str(&format!("  Table: {}\n", group.constraints[0].table_name));

            // Show optimization benefits
            if group.constraints.len() > 1 {
                explanation.push_str(&format!(
                    "  Benefit: {} table scans reduced to 1\n",
                    group.constraints.len()
                ));
            }

            // Show predicate pushdown info if applicable
            let has_predicates = group.constraints.iter().any(|c| c.has_predicates);
            if has_predicates && self.executor.enable_pushdown {
                explanation.push_str("  Predicate Pushdown: Enabled\n");
            }

            explanation.push_str(&self.executor.explain_group(group, ctx).await?);
            explanation.push('\n');
        }

        // Cache statistics
        let cache_stats = self.stats_cache.stats();
        explanation.push_str("Cache Statistics\n");
        explanation.push_str("----------------\n");
        explanation.push_str(&format!("  Total Entries: {}\n", cache_stats.total_entries));
        explanation.push_str(&format!(
            "  Active Entries: {}\n",
            cache_stats.active_entries
        ));
        explanation.push_str(&format!(
            "  Expired Entries: {}\n\n",
            cache_stats.expired_entries
        ));

        Ok(explanation)
    }
}

impl Default for QueryOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod basic_tests {
    use super::*;

    #[test]
    fn test_optimizer_creation() {
        let optimizer = QueryOptimizer::new();
        // Basic sanity check
        assert!(std::ptr::eq(&optimizer.analyzer, &optimizer.analyzer));
    }
}
