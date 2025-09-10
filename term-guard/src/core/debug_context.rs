//! Debug context and comprehensive error reporting for multi-table validation.
//!
//! This module provides detailed debugging and error reporting capabilities for
//! cross-table validation scenarios, helping developers quickly identify and resolve
//! issues in complex validation setups. Part of Phase 3: UX & Integration.
//!
//! # Features
//!
//! - SQL query logging and explanation
//! - Constraint execution timeline
//! - Performance profiling
//! - Detailed error context with suggestions
//! - Visual representation of table relationships
//!
//! # Example
//!
//! ```rust
//! use term_guard::core::debug_context::{DebugContext, DebugLevel};
//! use term_guard::core::{ValidationSuite, Check};
//! use datafusion::prelude::*;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let ctx = SessionContext::new();
//!
//! // Enable debug context
//! let debug_ctx = DebugContext::new()
//!     .with_level(DebugLevel::Detailed)
//!     .with_query_logging(true)
//!     .with_performance_tracking(true);
//!
//! let suite = ValidationSuite::builder("validation")
//!     .with_debug_context(debug_ctx)
//!     .check(/* ... */)
//!     .build();
//!
//! let result = suite.run(&ctx).await?;
//!
//! // Access debug information
//! if let Some(debug_info) = result.debug_info() {
//!     println!("Execution timeline: {:#?}", debug_info.timeline);
//!     println!("SQL queries executed: {:#?}", debug_info.queries);
//! }
//! # Ok(())
//! # }
//! ```

use crate::core::ConstraintResult;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{debug, trace};

/// Debug level for validation execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DebugLevel {
    /// No debug information collected
    None,
    /// Basic information (constraint names, pass/fail)
    Basic,
    /// Detailed information (SQL queries, timings)
    Detailed,
    /// Verbose information (all intermediate results)
    Verbose,
}

/// Debug context for validation execution.
#[derive(Debug, Clone)]
pub struct DebugContext {
    level: DebugLevel,
    log_queries: bool,
    track_performance: bool,
    capture_intermediate_results: bool,
    collector: Arc<Mutex<DebugCollector>>,
}

impl Default for DebugContext {
    fn default() -> Self {
        Self::new()
    }
}

impl DebugContext {
    /// Create a new debug context with default settings.
    pub fn new() -> Self {
        Self {
            level: DebugLevel::None,
            log_queries: false,
            track_performance: false,
            capture_intermediate_results: false,
            collector: Arc::new(Mutex::new(DebugCollector::new())),
        }
    }

    /// Set the debug level.
    pub fn with_level(mut self, level: DebugLevel) -> Self {
        self.level = level;
        // Auto-enable features based on level
        match level {
            DebugLevel::None => {
                self.log_queries = false;
                self.track_performance = false;
                self.capture_intermediate_results = false;
            }
            DebugLevel::Basic => {
                self.track_performance = true;
            }
            DebugLevel::Detailed => {
                self.log_queries = true;
                self.track_performance = true;
            }
            DebugLevel::Verbose => {
                self.log_queries = true;
                self.track_performance = true;
                self.capture_intermediate_results = true;
            }
        }
        self
    }

    /// Enable or disable SQL query logging.
    pub fn with_query_logging(mut self, enable: bool) -> Self {
        self.log_queries = enable;
        self
    }

    /// Enable or disable performance tracking.
    pub fn with_performance_tracking(mut self, enable: bool) -> Self {
        self.track_performance = enable;
        self
    }

    /// Log a SQL query execution.
    pub fn log_query(&self, query: &str, table_context: &str) {
        if self.log_queries && self.level != DebugLevel::None {
            let mut collector = self.collector.lock().unwrap();
            collector.add_query(query.to_string(), table_context.to_string());
            trace!("SQL Query for {}: {}", table_context, query);
        }
    }

    /// Start tracking a constraint execution.
    pub fn start_constraint(&self, constraint_name: &str) -> Option<ConstraintTracker> {
        if self.track_performance && self.level != DebugLevel::None {
            Some(ConstraintTracker {
                name: constraint_name.to_string(),
                start: Instant::now(),
                context: self.clone(),
            })
        } else {
            None
        }
    }

    /// Record a constraint result.
    pub fn record_result(&self, constraint_name: &str, result: &ConstraintResult) {
        if self.level != DebugLevel::None {
            let mut collector = self.collector.lock().unwrap();
            collector.add_result(constraint_name.to_string(), result.clone());
        }
    }

    /// Get the collected debug information.
    pub fn get_debug_info(&self) -> DebugInfo {
        let collector = self.collector.lock().unwrap();
        collector.to_debug_info()
    }
}

/// Tracker for constraint execution timing.
pub struct ConstraintTracker {
    name: String,
    start: Instant,
    context: DebugContext,
}

impl Drop for ConstraintTracker {
    fn drop(&mut self) {
        let duration = self.start.elapsed();
        let mut collector = self.context.collector.lock().unwrap();
        collector.add_timing(self.name.clone(), duration);
        debug!("Constraint '{}' executed in {:?}", self.name, duration);
    }
}

/// Collector for debug information during execution.
#[derive(Debug)]
struct DebugCollector {
    queries: Vec<QueryExecution>,
    timings: Vec<ConstraintTiming>,
    results: HashMap<String, ConstraintResult>,
    timeline: Vec<TimelineEvent>,
}

impl DebugCollector {
    fn new() -> Self {
        Self {
            queries: Vec::new(),
            timings: Vec::new(),
            results: HashMap::new(),
            timeline: Vec::new(),
        }
    }

    fn add_query(&mut self, query: String, context: String) {
        let event = QueryExecution {
            query: query.clone(),
            context,
            timestamp: Some(Instant::now()),
        };
        self.queries.push(event.clone());
        self.timeline.push(TimelineEvent::QueryExecuted(event));
    }

    fn add_timing(&mut self, constraint: String, duration: Duration) {
        let timing = ConstraintTiming {
            constraint: constraint.clone(),
            duration,
        };
        self.timings.push(timing.clone());
        self.timeline
            .push(TimelineEvent::ConstraintCompleted(timing));
    }

    fn add_result(&mut self, constraint: String, result: ConstraintResult) {
        self.results.insert(constraint.clone(), result.clone());
        self.timeline.push(TimelineEvent::ResultRecorded {
            constraint,
            success: matches!(result.status, crate::core::ConstraintStatus::Success),
        });
    }

    fn to_debug_info(&self) -> DebugInfo {
        DebugInfo {
            queries: self.queries.clone(),
            timings: self.timings.clone(),
            results: self.results.clone(),
            timeline: self.timeline.clone(),
            summary: self.generate_summary(),
        }
    }

    fn generate_summary(&self) -> DebugSummary {
        let total_queries = self.queries.len();
        let total_constraints = self.timings.len();
        let total_duration: Duration = self.timings.iter().map(|t| t.duration).sum();
        let failed_constraints = self
            .results
            .values()
            .filter(|r| matches!(r.status, crate::core::ConstraintStatus::Failure))
            .count();

        DebugSummary {
            total_queries,
            total_constraints,
            total_duration,
            failed_constraints,
            avg_constraint_time: if total_constraints > 0 {
                total_duration / total_constraints as u32
            } else {
                Duration::from_secs(0)
            },
        }
    }
}

/// Debug information collected during validation execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebugInfo {
    /// SQL queries executed
    pub queries: Vec<QueryExecution>,
    /// Constraint execution timings
    pub timings: Vec<ConstraintTiming>,
    /// Constraint results
    pub results: HashMap<String, ConstraintResult>,
    /// Execution timeline
    pub timeline: Vec<TimelineEvent>,
    /// Summary statistics
    pub summary: DebugSummary,
}

impl DebugInfo {
    /// Generate a detailed error report for failed constraints.
    pub fn generate_error_report(&self) -> ErrorReport {
        let mut failed_constraints = Vec::new();

        for (name, result) in &self.results {
            if matches!(result.status, crate::core::ConstraintStatus::Failure) {
                let related_queries = self
                    .queries
                    .iter()
                    .filter(|q| q.context.contains(name))
                    .cloned()
                    .collect();

                let timing = self.timings.iter().find(|t| t.constraint == *name).cloned();

                failed_constraints.push(FailedConstraintDetail {
                    name: name.clone(),
                    result: result.clone(),
                    related_queries,
                    timing,
                    suggestions: self.generate_suggestions_for(name, result),
                });
            }
        }

        let total_failures = failed_constraints.len();
        ErrorReport {
            failed_constraints,
            total_failures,
            execution_summary: self.summary.clone(),
        }
    }

    /// Generate debugging suggestions for a failed constraint.
    fn generate_suggestions_for(
        &self,
        constraint_name: &str,
        result: &ConstraintResult,
    ) -> Vec<String> {
        let mut suggestions = Vec::new();

        // Analyze the constraint type and provide specific suggestions
        if constraint_name.contains("foreign_key") {
            suggestions.push("Check that both tables are properly registered".to_string());
            suggestions.push(
                "Verify that the referenced columns exist and have compatible types".to_string(),
            );
            suggestions.push("Consider allowing nulls if the relationship is optional".to_string());
        }

        if constraint_name.contains("cross_table_sum") {
            suggestions.push("Verify that numeric columns have the same precision".to_string());
            suggestions.push(
                "Check for floating-point precision issues - consider using tolerance".to_string(),
            );
            suggestions.push("Ensure GROUP BY columns exist in both tables".to_string());
        }

        if constraint_name.contains("join_coverage") {
            suggestions
                .push("Review the expected coverage rate - it might be too high".to_string());
            suggestions.push("Check for data quality issues in join keys".to_string());
            suggestions
                .push("Consider using distinct counts if duplicates are expected".to_string());
        }

        if constraint_name.contains("temporal") {
            suggestions.push("Verify timestamp formats are consistent".to_string());
            suggestions.push("Check timezone handling".to_string());
            suggestions.push("Consider allowing small time differences with tolerance".to_string());
        }

        // Add generic suggestions
        if result.message.is_some() {
            suggestions.push("Review the error message for specific details".to_string());
        }
        suggestions.push("Enable verbose debug logging for more details".to_string());

        suggestions
    }

    /// Generate a visual representation of table relationships.
    pub fn visualize_relationships(&self) -> String {
        let mut output = String::new();
        output.push_str("Table Relationships Detected:\n");
        output.push_str("============================\n\n");

        // Extract table relationships from queries
        let mut relationships: HashMap<String, Vec<String>> = HashMap::new();

        for query in &self.queries {
            if query.query.contains("JOIN") {
                // Simple extraction of table names from JOIN clauses
                // In production, this would use proper SQL parsing
                if let Some(tables) = self.extract_join_tables(&query.query) {
                    relationships
                        .entry(tables.0.clone())
                        .or_default()
                        .push(tables.1.clone());
                }
            }
        }

        // Generate ASCII art representation
        for (left_table, right_tables) in relationships {
            for right_table in right_tables {
                output.push_str(&format!("{left_table} ──────> {right_table}\n"));
            }
        }

        output
    }

    /// Extract table names from a JOIN query (simplified).
    fn extract_join_tables(&self, query: &str) -> Option<(String, String)> {
        // This is a simplified extraction - in production, use a proper SQL parser
        if query.contains("JOIN") {
            // Extract patterns like "FROM table1 JOIN table2"
            // This is a placeholder implementation
            None
        } else {
            None
        }
    }
}

/// Query execution record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryExecution {
    /// The SQL query executed
    pub query: String,
    /// Context (e.g., constraint name)
    pub context: String,
    /// When the query was executed (not serialized)
    #[serde(skip_deserializing, skip_serializing)]
    pub timestamp: Option<Instant>,
}

/// Constraint timing information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstraintTiming {
    /// Constraint name
    pub constraint: String,
    /// Execution duration
    pub duration: Duration,
}

/// Timeline event during execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TimelineEvent {
    /// A SQL query was executed
    QueryExecuted(QueryExecution),
    /// A constraint completed execution
    ConstraintCompleted(ConstraintTiming),
    /// A constraint result was recorded
    ResultRecorded { constraint: String, success: bool },
}

/// Summary of debug information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebugSummary {
    /// Total number of SQL queries executed
    pub total_queries: usize,
    /// Total number of constraints evaluated
    pub total_constraints: usize,
    /// Total execution time
    pub total_duration: Duration,
    /// Number of failed constraints
    pub failed_constraints: usize,
    /// Average time per constraint
    pub avg_constraint_time: Duration,
}

/// Detailed error report for failed validations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorReport {
    /// Details of each failed constraint
    pub failed_constraints: Vec<FailedConstraintDetail>,
    /// Total number of failures
    pub total_failures: usize,
    /// Execution summary
    pub execution_summary: DebugSummary,
}

impl fmt::Display for ErrorReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "═══════════════════════════════════════")?;
        writeln!(f, "  Validation Error Report")?;
        writeln!(f, "═══════════════════════════════════════")?;
        writeln!(f)?;
        writeln!(f, "Summary:")?;
        writeln!(f, "  Total Failures: {}", self.total_failures)?;
        writeln!(
            f,
            "  Total Constraints: {}",
            self.execution_summary.total_constraints
        )?;
        writeln!(
            f,
            "  Total Duration: {:?}",
            self.execution_summary.total_duration
        )?;
        writeln!(f)?;

        for (i, failed) in self.failed_constraints.iter().enumerate() {
            writeln!(f, "Failure #{}: {}", i + 1, failed.name)?;
            writeln!(f, "───────────────────────────────────────")?;

            if let Some(ref message) = failed.result.message {
                writeln!(f, "  Error: {message}")?;
            }

            if let Some(ref timing) = failed.timing {
                writeln!(f, "  Duration: {:?}", timing.duration)?;
            }

            if !failed.suggestions.is_empty() {
                writeln!(f, "  Suggestions:")?;
                for suggestion in &failed.suggestions {
                    writeln!(f, "    • {suggestion}")?;
                }
            }

            if !failed.related_queries.is_empty() {
                writeln!(f, "  Related Queries:")?;
                for query in &failed.related_queries {
                    writeln!(f, "    {}", query.query)?;
                }
            }

            writeln!(f)?;
        }

        Ok(())
    }
}

/// Detailed information about a failed constraint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailedConstraintDetail {
    /// Constraint name
    pub name: String,
    /// The failure result
    pub result: ConstraintResult,
    /// Related SQL queries
    pub related_queries: Vec<QueryExecution>,
    /// Execution timing
    pub timing: Option<ConstraintTiming>,
    /// Debugging suggestions
    pub suggestions: Vec<String>,
}

/// Extension trait for ValidationResult to add debug information.
pub trait ValidationResultDebugExt {
    /// Get debug information if available.
    fn debug_info(&self) -> Option<&DebugInfo>;

    /// Generate an error report for failures.
    fn error_report(&self) -> Option<ErrorReport>;
}

// Note: In production, this would be implemented on ValidationResult
// For now, we'll leave it as a trait definition

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_debug_context_creation() {
        let ctx = DebugContext::new()
            .with_level(DebugLevel::Detailed)
            .with_query_logging(true);

        assert!(ctx.log_queries);
        assert!(ctx.track_performance);
    }

    #[test]
    fn test_debug_collector() {
        let mut collector = DebugCollector::new();

        collector.add_query(
            "SELECT * FROM users".to_string(),
            "test_constraint".to_string(),
        );
        collector.add_timing("test_constraint".to_string(), Duration::from_millis(100));

        let info = collector.to_debug_info();

        assert_eq!(info.queries.len(), 1);
        assert_eq!(info.timings.len(), 1);
        assert_eq!(info.summary.total_queries, 1);
        assert_eq!(info.summary.total_constraints, 1);
    }

    #[test]
    fn test_error_report_generation() {
        let mut collector = DebugCollector::new();

        collector.add_result(
            "foreign_key_check".to_string(),
            ConstraintResult {
                status: crate::core::ConstraintStatus::Failure,
                message: Some("Foreign key violation found".to_string()),
                metric: None,
            },
        );

        let info = collector.to_debug_info();
        let report = info.generate_error_report();

        assert_eq!(report.total_failures, 1);
        assert!(!report.failed_constraints[0].suggestions.is_empty());
    }
}
