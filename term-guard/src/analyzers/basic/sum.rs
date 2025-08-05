//! Sum analyzer for computing the total of numeric values.

use async_trait::async_trait;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::analyzers::{Analyzer, AnalyzerError, AnalyzerResult, AnalyzerState, MetricValue};

use crate::core::current_validation_context;
/// Analyzer that computes the sum of values in a numeric column.
///
/// # Example
///
/// ```rust,ignore
/// use term_guard::analyzers::basic::SumAnalyzer;
/// use datafusion::prelude::*;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let ctx = SessionContext::new();
/// // Register your data table
///
/// let analyzer = SumAnalyzer::new("amount");
/// let state = analyzer.compute_state_from_data(&ctx).await?;
/// let metric = analyzer.compute_metric_from_state(&state)?;
///
/// if let MetricValue::Double(sum) = metric {
///     println!("Total amount: ${:.2}", sum);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct SumAnalyzer {
    /// The column to analyze.
    column: String,
}

impl SumAnalyzer {
    /// Creates a new sum analyzer for the specified column.
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

/// State for the sum analyzer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SumState {
    /// Sum of all non-null values.
    pub sum: f64,
    /// Whether any non-null values were found.
    pub has_values: bool,
}

impl AnalyzerState for SumState {
    fn merge(states: Vec<Self>) -> AnalyzerResult<Self> {
        let sum = states.iter().map(|s| s.sum).sum();
        let has_values = states.iter().any(|s| s.has_values);

        Ok(SumState { sum, has_values })
    }

    fn is_empty(&self) -> bool {
        !self.has_values
    }
}

#[async_trait]
impl Analyzer for SumAnalyzer {
    type State = SumState;
    type Metric = MetricValue;

    #[instrument(skip(ctx), fields(analyzer = "sum", column = %self.column))]
    async fn compute_state_from_data(&self, ctx: &SessionContext) -> AnalyzerResult<Self::State> {
        // Build SQL query to compute sum and check for non-null values
        // Get the table name from the validation context

        let validation_ctx = current_validation_context();

        let table_name = validation_ctx.table_name();

        let sql = format!(
            "SELECT SUM({0}) as sum, COUNT({0}) as count FROM {table_name}",
            self.column
        );

        // Execute query
        let df = ctx.sql(&sql).await?;
        let batches = df.collect().await?;

        // Extract sum from result
        let (sum, has_values) = if let Some(batch) = batches.first() {
            if batch.num_rows() > 0 {
                // Sum can be null if all values are null
                let sum = if batch.column(0).is_null(0) {
                    0.0
                } else {
                    // Try Float64 first, then Int64
                    if let Some(arr) = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::Float64Array>()
                    {
                        arr.value(0)
                    } else if let Some(arr) = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                    {
                        arr.value(0) as f64
                    } else {
                        return Err(AnalyzerError::invalid_data(format!(
                            "Expected numeric array for sum, got {:?}",
                            batch.column(0).data_type()
                        )));
                    }
                };

                // Check if we had any non-null values
                let count_array = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| AnalyzerError::invalid_data("Expected Int64 array for count"))?;
                let has_values = count_array.value(0) > 0;

                (sum, has_values)
            } else {
                (0.0, false)
            }
        } else {
            (0.0, false)
        };

        Ok(SumState { sum, has_values })
    }

    fn compute_metric_from_state(&self, state: &Self::State) -> AnalyzerResult<Self::Metric> {
        if state.has_values {
            Ok(MetricValue::Double(state.sum))
        } else {
            Err(AnalyzerError::NoData)
        }
    }

    fn name(&self) -> &str {
        "sum"
    }

    fn description(&self) -> &str {
        "Computes the sum of values in a numeric column"
    }

    fn metric_key(&self) -> String {
        format!("{}.{}", self.name(), self.column)
    }

    fn columns(&self) -> Vec<&str> {
        vec![&self.column]
    }
}
