//! Mean analyzer for computing average values.

use async_trait::async_trait;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::analyzers::{Analyzer, AnalyzerError, AnalyzerResult, AnalyzerState, MetricValue};

/// Analyzer that computes the mean (average) value of a numeric column.
///
/// The mean is calculated using incremental computation to support
/// distributed processing and efficient state merging.
///
/// # Example
///
/// ```rust,ignore
/// use term_guard::analyzers::basic::MeanAnalyzer;
/// use datafusion::prelude::*;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let ctx = SessionContext::new();
/// // Register your data table
///
/// let analyzer = MeanAnalyzer::new("age");
/// let state = analyzer.compute_state_from_data(&ctx).await?;
/// let metric = analyzer.compute_metric_from_state(&state)?;
///
/// if let MetricValue::Double(mean) = metric {
///     println!("Average age: {:.2}", mean);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct MeanAnalyzer {
    /// The column to analyze.
    column: String,
}

impl MeanAnalyzer {
    /// Creates a new mean analyzer for the specified column.
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

/// State for the mean analyzer supporting incremental computation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeanState {
    /// Sum of all values.
    pub sum: f64,
    /// Count of non-null values.
    pub count: u64,
}

impl MeanState {
    /// Calculates the mean value.
    pub fn mean(&self) -> Option<f64> {
        if self.count == 0 {
            None
        } else {
            Some(self.sum / self.count as f64)
        }
    }
}

impl AnalyzerState for MeanState {
    fn merge(states: Vec<Self>) -> AnalyzerResult<Self> {
        let sum = states.iter().map(|s| s.sum).sum();
        let count = states.iter().map(|s| s.count).sum();

        Ok(MeanState { sum, count })
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }
}

#[async_trait]
impl Analyzer for MeanAnalyzer {
    type State = MeanState;
    type Metric = MetricValue;

    #[instrument(skip(ctx), fields(analyzer = "mean", column = %self.column))]
    async fn compute_state_from_data(&self, ctx: &SessionContext) -> AnalyzerResult<Self::State> {
        // Build SQL query to compute sum and count for incremental mean calculation
        let sql = format!(
            "SELECT SUM({0}) as sum, COUNT({0}) as count FROM data",
            self.column
        );

        // Execute query
        let df = ctx.sql(&sql).await?;
        let batches = df.collect().await?;

        // Extract sum and count from result
        let (sum, count) = if let Some(batch) = batches.first() {
            if batch.num_rows() > 0 {
                // Sum can be Float64 or null
                let sum = if batch.column(0).is_null(0) {
                    0.0
                } else {
                    let sum_array = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::Float64Array>()
                        .ok_or_else(|| {
                            AnalyzerError::invalid_data("Expected Float64 array for sum")
                        })?;
                    sum_array.value(0)
                };

                let count_array = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| AnalyzerError::invalid_data("Expected Int64 array for count"))?;
                let count = count_array.value(0) as u64;

                (sum, count)
            } else {
                (0.0, 0)
            }
        } else {
            (0.0, 0)
        };

        Ok(MeanState { sum, count })
    }

    fn compute_metric_from_state(&self, state: &Self::State) -> AnalyzerResult<Self::Metric> {
        match state.mean() {
            Some(mean) => Ok(MetricValue::Double(mean)),
            None => Err(AnalyzerError::NoData),
        }
    }

    fn name(&self) -> &str {
        "mean"
    }

    fn description(&self) -> &str {
        "Computes the average value of a numeric column"
    }

    fn metric_key(&self) -> String {
        format!("{}.{}", self.name(), self.column)
    }

    fn columns(&self) -> Vec<&str> {
        vec![&self.column]
    }
}
