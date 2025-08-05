//! Completeness analyzer for measuring the fraction of non-null values.

use async_trait::async_trait;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::analyzers::{Analyzer, AnalyzerError, AnalyzerResult, AnalyzerState, MetricValue};

use crate::core::current_validation_context;
/// Analyzer that computes the fraction of non-null values for a column.
///
/// Completeness is a fundamental data quality metric that measures
/// how much of the expected data is actually present.
///
/// # Example
///
/// ```rust,ignore
/// use term_guard::analyzers::basic::CompletenessAnalyzer;
/// use datafusion::prelude::*;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let ctx = SessionContext::new();
/// // Register your data table
///
/// let analyzer = CompletenessAnalyzer::new("user_id");
/// let state = analyzer.compute_state_from_data(&ctx).await?;
/// let metric = analyzer.compute_metric_from_state(&state)?;
///
/// if let MetricValue::Double(completeness) = metric {
///     println!("Column completeness: {:.2}%", completeness * 100.0);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct CompletenessAnalyzer {
    /// The column to analyze.
    column: String,
}

impl CompletenessAnalyzer {
    /// Creates a new completeness analyzer for the specified column.
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

/// State for the completeness analyzer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletenessState {
    /// Total number of rows.
    pub total_count: u64,
    /// Number of non-null values.
    pub non_null_count: u64,
}

impl CompletenessState {
    /// Calculates the completeness fraction.
    pub fn completeness(&self) -> f64 {
        if self.total_count == 0 {
            1.0 // Empty dataset is considered complete
        } else {
            self.non_null_count as f64 / self.total_count as f64
        }
    }
}

impl AnalyzerState for CompletenessState {
    fn merge(states: Vec<Self>) -> AnalyzerResult<Self> {
        let total_count = states.iter().map(|s| s.total_count).sum();
        let non_null_count = states.iter().map(|s| s.non_null_count).sum();

        Ok(CompletenessState {
            total_count,
            non_null_count,
        })
    }

    fn is_empty(&self) -> bool {
        self.total_count == 0
    }
}

#[async_trait]
impl Analyzer for CompletenessAnalyzer {
    type State = CompletenessState;
    type Metric = MetricValue;

    #[instrument(skip(ctx), fields(analyzer = "completeness", column = %self.column))]
    async fn compute_state_from_data(&self, ctx: &SessionContext) -> AnalyzerResult<Self::State> {
        // Build SQL query to count total rows and non-null values
        // Get the table name from the validation context

        let validation_ctx = current_validation_context();

        let table_name = validation_ctx.table_name();

        

        let sql = format!(
            "SELECT COUNT(*) as total_count, COUNT({}) as non_null_count FROM {table_name}",
            self.column
        );

        // Execute query
        let df = ctx.sql(&sql).await?;
        let batches = df.collect().await?;

        // Extract counts from result
        let (total_count, non_null_count) = if let Some(batch) = batches.first() {
            if batch.num_rows() > 0 {
                let total_array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| {
                        AnalyzerError::invalid_data("Expected Int64 array for total count")
                    })?;

                let non_null_array = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| {
                        AnalyzerError::invalid_data("Expected Int64 array for non-null count")
                    })?;

                (total_array.value(0) as u64, non_null_array.value(0) as u64)
            } else {
                (0, 0)
            }
        } else {
            (0, 0)
        };

        Ok(CompletenessState {
            total_count,
            non_null_count,
        })
    }

    fn compute_metric_from_state(&self, state: &Self::State) -> AnalyzerResult<Self::Metric> {
        Ok(MetricValue::Double(state.completeness()))
    }

    fn name(&self) -> &str {
        "completeness"
    }

    fn description(&self) -> &str {
        "Computes the fraction of non-null values in a column"
    }

    fn metric_key(&self) -> String {
        format!("{}.{}", self.name(), self.column)
    }

    fn columns(&self) -> Vec<&str> {
        vec![&self.column]
    }
}
