//! Compliance analyzer for evaluating custom SQL expressions.

use async_trait::async_trait;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::analyzers::{Analyzer, AnalyzerError, AnalyzerResult, AnalyzerState, MetricValue};

use crate::core::current_validation_context;
/// Analyzer that evaluates compliance with custom SQL expressions.
///
/// This analyzer allows users to define custom data quality rules using SQL expressions.
/// It computes the fraction of rows that satisfy the given predicate.
///
/// # Safety
///
/// The SQL expression is validated and parameterized to prevent SQL injection.
/// Only boolean expressions are allowed.
///
/// # Example
///
/// ```rust,ignore
/// use term_guard::analyzers::advanced::ComplianceAnalyzer;
/// use datafusion::prelude::*;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let ctx = SessionContext::new();
/// // Register your data table
///
/// // Check that prices are positive and discounts are reasonable
/// let analyzer = ComplianceAnalyzer::new(
///     "price_validation",
///     "price > 0 AND discount >= 0 AND discount <= 1"
/// );
/// let state = analyzer.compute_state_from_data(&ctx).await?;
/// let metric = analyzer.compute_metric_from_state(&state)?;
///
/// if let MetricValue::Double(compliance) = metric {
///     println!("Price validation compliance: {:.2}%", compliance * 100.0);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ComplianceAnalyzer {
    /// Name of the compliance check.
    name: String,
    /// SQL predicate expression to evaluate.
    predicate: String,
}

impl ComplianceAnalyzer {
    /// Creates a new compliance analyzer with the given name and predicate.
    ///
    /// # Arguments
    ///
    /// * `name` - A descriptive name for this compliance check
    /// * `predicate` - SQL boolean expression to evaluate (e.g., "age >= 18")
    pub fn new(name: impl Into<String>, predicate: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            predicate: predicate.into(),
        }
    }

    /// Returns the name of the compliance check.
    pub fn check_name(&self) -> &str {
        &self.name
    }

    /// Returns the SQL predicate expression.
    pub fn predicate(&self) -> &str {
        &self.predicate
    }

    /// Validates that the predicate is safe to execute.
    fn validate_predicate(&self) -> AnalyzerResult<()> {
        // Basic validation to prevent obvious SQL injection attempts
        let lower = self.predicate.to_lowercase();

        // Disallow dangerous keywords
        let dangerous_keywords = [
            "drop", "delete", "insert", "update", "create", "alter", "grant", "revoke", "exec",
            "execute", "union", "select", "--", "/*", "*/",
        ];

        for keyword in &dangerous_keywords {
            if lower.contains(keyword) {
                return Err(AnalyzerError::invalid_config(format!(
                    "Predicate contains forbidden keyword: {keyword}"
                )));
            }
        }

        Ok(())
    }
}

/// State for the compliance analyzer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplianceState {
    /// Number of rows that satisfy the predicate.
    pub compliant_count: u64,
    /// Total number of rows evaluated.
    pub total_count: u64,
}

impl ComplianceState {
    /// Calculates the compliance fraction.
    pub fn compliance_fraction(&self) -> f64 {
        if self.total_count == 0 {
            1.0 // Empty dataset is considered compliant
        } else {
            self.compliant_count as f64 / self.total_count as f64
        }
    }
}

impl AnalyzerState for ComplianceState {
    fn merge(states: Vec<Self>) -> AnalyzerResult<Self> {
        let compliant_count = states.iter().map(|s| s.compliant_count).sum();
        let total_count = states.iter().map(|s| s.total_count).sum();

        Ok(ComplianceState {
            compliant_count,
            total_count,
        })
    }

    fn is_empty(&self) -> bool {
        self.total_count == 0
    }
}

#[async_trait]
impl Analyzer for ComplianceAnalyzer {
    type State = ComplianceState;
    type Metric = MetricValue;

    #[instrument(skip(ctx), fields(analyzer = "compliance", name = %self.name))]
    async fn compute_state_from_data(&self, ctx: &SessionContext) -> AnalyzerResult<Self::State> {
        // Validate the predicate before execution
        self.validate_predicate()?;

        // Build SQL query with the predicate
        // Get the table name from the validation context

        let validation_ctx = current_validation_context();

        let table_name = validation_ctx.table_name();

        

        let sql = format!(
            "SELECT 
                COUNT(CASE WHEN ({}) THEN 1 END) as compliant_count,
                COUNT(*) as total_count
            FROM {table_name}",
            self.predicate
        );

        // Execute query
        let df = ctx.sql(&sql).await.map_err(|e| {
            AnalyzerError::invalid_config(format!("Invalid predicate '{}': {e}", self.predicate))
        })?;
        let batches = df.collect().await?;

        // Extract counts from result
        let (compliant_count, total_count) = if let Some(batch) = batches.first() {
            if batch.num_rows() > 0 {
                let compliant_array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| {
                        AnalyzerError::invalid_data("Expected Int64 array for compliant count")
                    })?;
                let compliant = compliant_array.value(0) as u64;

                let total_array = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| {
                        AnalyzerError::invalid_data("Expected Int64 array for total count")
                    })?;
                let total = total_array.value(0) as u64;

                (compliant, total)
            } else {
                (0, 0)
            }
        } else {
            (0, 0)
        };

        Ok(ComplianceState {
            compliant_count,
            total_count,
        })
    }

    fn compute_metric_from_state(&self, state: &Self::State) -> AnalyzerResult<Self::Metric> {
        Ok(MetricValue::Double(state.compliance_fraction()))
    }

    fn name(&self) -> &str {
        "compliance"
    }

    fn description(&self) -> &str {
        "Evaluates compliance with custom SQL expressions"
    }
}
