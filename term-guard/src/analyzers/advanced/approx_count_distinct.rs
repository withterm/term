//! Approximate count distinct analyzer using HyperLogLog algorithm.

use async_trait::async_trait;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::analyzers::{Analyzer, AnalyzerError, AnalyzerResult, AnalyzerState, MetricValue};

use crate::core::current_validation_context;
/// Analyzer that computes approximate count distinct using HyperLogLog.
///
/// This analyzer provides memory-efficient cardinality estimation for high-cardinality
/// columns with configurable precision. It uses DataFusion's built-in APPROX_DISTINCT
/// function which implements the HyperLogLog algorithm.
///
/// # Example
///
/// ```rust,ignore
/// use term_guard::analyzers::advanced::ApproxCountDistinctAnalyzer;
/// use datafusion::prelude::*;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let ctx = SessionContext::new();
/// // Register your data table
///
/// let analyzer = ApproxCountDistinctAnalyzer::new("user_id");
/// let state = analyzer.compute_state_from_data(&ctx).await?;
/// let metric = analyzer.compute_metric_from_state(&state)?;
///
/// if let MetricValue::Long(approx_distinct) = metric {
///     println!("Approximate distinct users: {}", approx_distinct);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ApproxCountDistinctAnalyzer {
    /// The column to analyze.
    column: String,
}

impl ApproxCountDistinctAnalyzer {
    /// Creates a new approximate count distinct analyzer for the specified column.
    pub fn new(column: impl Into<String>) -> Self {
        Self {
            column: column.into(),
        }
    }

    /// Returns the column being analyzed.
    pub fn column(&self) -> &str {
        &self.column
    }
}

/// State for the approximate count distinct analyzer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApproxCountDistinctState {
    /// Approximate count of distinct values.
    pub approx_distinct_count: u64,
    /// Total count of non-null values for calculating distinctness ratio.
    pub total_count: u64,
}

impl ApproxCountDistinctState {
    /// Calculates the approximate distinctness ratio.
    pub fn distinctness_ratio(&self) -> f64 {
        if self.total_count == 0 {
            1.0
        } else {
            self.approx_distinct_count as f64 / self.total_count as f64
        }
    }
}

impl AnalyzerState for ApproxCountDistinctState {
    fn merge(states: Vec<Self>) -> AnalyzerResult<Self> {
        // Note: Merging HyperLogLog states would require access to the actual HLL sketches
        // Since we only have the final counts, we take the max as an approximation
        // In a production system, we'd store and merge the actual HLL data structures
        let approx_distinct_count = states
            .iter()
            .map(|s| s.approx_distinct_count)
            .max()
            .unwrap_or(0);
        let total_count = states.iter().map(|s| s.total_count).sum();

        Ok(ApproxCountDistinctState {
            approx_distinct_count,
            total_count,
        })
    }

    fn is_empty(&self) -> bool {
        self.total_count == 0
    }
}

#[async_trait]
impl Analyzer for ApproxCountDistinctAnalyzer {
    type State = ApproxCountDistinctState;
    type Metric = MetricValue;

    #[instrument(skip(ctx), fields(analyzer = "approx_count_distinct", column = %self.column))]
    async fn compute_state_from_data(&self, ctx: &SessionContext) -> AnalyzerResult<Self::State> {
        // Build SQL query using APPROX_DISTINCT function
        // Get the table name from the validation context

        let validation_ctx = current_validation_context();

        let table_name = validation_ctx.table_name();

        let sql = format!(
            "SELECT APPROX_DISTINCT({0}) as approx_distinct, COUNT({0}) as total FROM {table_name}",
            self.column
        );

        // Execute query
        let df = ctx.sql(&sql).await?;
        let batches = df.collect().await?;

        // Extract counts from result
        let (approx_distinct_count, total_count) = if let Some(batch) = batches.first() {
            if batch.num_rows() > 0 {
                // APPROX_DISTINCT returns UInt64
                let approx_distinct_array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::UInt64Array>()
                    .ok_or_else(|| {
                        AnalyzerError::invalid_data("Expected UInt64 array for approx_distinct")
                    })?;
                let approx_distinct = approx_distinct_array.value(0);

                let total_array = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| {
                        AnalyzerError::invalid_data("Expected Int64 array for total count")
                    })?;
                let total = total_array.value(0) as u64;

                (approx_distinct, total)
            } else {
                (0, 0)
            }
        } else {
            (0, 0)
        };

        Ok(ApproxCountDistinctState {
            approx_distinct_count,
            total_count,
        })
    }

    fn compute_metric_from_state(&self, state: &Self::State) -> AnalyzerResult<Self::Metric> {
        Ok(MetricValue::Long(state.approx_distinct_count as i64))
    }

    fn name(&self) -> &str {
        "approx_count_distinct"
    }

    fn description(&self) -> &str {
        "Computes approximate count of distinct values using HyperLogLog"
    }

    fn metric_key(&self) -> String {
        format!("{}.{}", self.name(), self.column)
    }

    fn columns(&self) -> Vec<&str> {
        vec![&self.column]
    }
}
