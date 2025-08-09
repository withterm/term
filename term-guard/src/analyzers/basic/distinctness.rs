//! Distinctness analyzer for measuring the fraction of distinct values.

use async_trait::async_trait;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::analyzers::{Analyzer, AnalyzerError, AnalyzerResult, AnalyzerState, MetricValue};

use crate::core::current_validation_context;
/// Analyzer that computes the fraction of distinct values for a column.
///
/// Distinctness measures uniqueness in data and is useful for identifying
/// columns that might be identifiers or have high cardinality.
///
/// # Example
///
/// ```rust,ignore
/// use term_guard::analyzers::basic::DistinctnessAnalyzer;
/// use datafusion::prelude::*;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let ctx = SessionContext::new();
/// // Register your data table
///
/// let analyzer = DistinctnessAnalyzer::new("user_id");
/// let state = analyzer.compute_state_from_data(&ctx).await?;
/// let metric = analyzer.compute_metric_from_state(&state)?;
///
/// if let MetricValue::Double(distinctness) = metric {
///     println!("Column distinctness: {:.2}%", distinctness * 100.0);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct DistinctnessAnalyzer {
    /// The column to analyze.
    column: String,
}

impl DistinctnessAnalyzer {
    /// Creates a new distinctness analyzer for the specified column.
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

/// State for the distinctness analyzer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistinctnessState {
    /// Total number of non-null values.
    pub total_count: u64,
    /// Number of distinct values.
    pub distinct_count: u64,
}

impl DistinctnessState {
    /// Calculates the distinctness fraction.
    pub fn distinctness(&self) -> f64 {
        if self.total_count == 0 {
            1.0 // Empty dataset is considered fully distinct
        } else {
            self.distinct_count as f64 / self.total_count as f64
        }
    }
}

impl AnalyzerState for DistinctnessState {
    fn merge(states: Vec<Self>) -> AnalyzerResult<Self> {
        // Note: Merging distinct counts is approximate when done across partitions
        // For exact results, we would need to track actual distinct values
        // This implementation provides an upper bound
        let total_count = states.iter().map(|s| s.total_count).sum();
        let distinct_count = states
            .iter()
            .map(|s| s.distinct_count)
            .sum::<u64>()
            .min(total_count);

        Ok(DistinctnessState {
            total_count,
            distinct_count,
        })
    }

    fn is_empty(&self) -> bool {
        self.total_count == 0
    }
}

#[async_trait]
impl Analyzer for DistinctnessAnalyzer {
    type State = DistinctnessState;
    type Metric = MetricValue;

    #[instrument(skip(ctx), fields(analyzer = "distinctness", column = %self.column))]
    async fn compute_state_from_data(&self, ctx: &SessionContext) -> AnalyzerResult<Self::State> {
        // Build SQL query to count total non-null values and distinct values
        // Get the table name from the validation context

        let validation_ctx = current_validation_context();

        let table_name = validation_ctx.table_name();

        let sql = format!(
            "SELECT COUNT({0}) as total_count, COUNT(DISTINCT {0}) as distinct_count FROM {table_name}",
            self.column
        );

        // Execute query
        let df = ctx.sql(&sql).await?;
        let batches = df.collect().await?;

        // Extract counts from result
        let (total_count, distinct_count) = if let Some(batch) = batches.first() {
            if batch.num_rows() > 0 {
                let total_array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| {
                        AnalyzerError::invalid_data("Expected Int64 array for total count")
                    })?;

                let distinct_array = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| {
                        AnalyzerError::invalid_data("Expected Int64 array for distinct count")
                    })?;

                (total_array.value(0) as u64, distinct_array.value(0) as u64)
            } else {
                (0, 0)
            }
        } else {
            (0, 0)
        };

        Ok(DistinctnessState {
            total_count,
            distinct_count,
        })
    }

    fn compute_metric_from_state(&self, state: &Self::State) -> AnalyzerResult<Self::Metric> {
        Ok(MetricValue::Double(state.distinctness()))
    }

    fn name(&self) -> &str {
        "distinctness"
    }

    fn description(&self) -> &str {
        "Computes the fraction of distinct values in a column"
    }

    fn metric_key(&self) -> String {
        format!("{}.{}", self.name(), self.column)
    }

    fn columns(&self) -> Vec<&str> {
        vec![&self.column]
    }
}
