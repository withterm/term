//! Min and Max analyzers for finding extreme values.

use async_trait::async_trait;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::analyzers::{Analyzer, AnalyzerError, AnalyzerResult, AnalyzerState, MetricValue};

use crate::core::current_validation_context;
/// Shared state for min/max analyzers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MinMaxState {
    /// Minimum value found.
    pub min: Option<f64>,
    /// Maximum value found.
    pub max: Option<f64>,
}

impl AnalyzerState for MinMaxState {
    fn merge(states: Vec<Self>) -> AnalyzerResult<Self> {
        let min = states
            .iter()
            .filter_map(|s| s.min)
            .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let max = states
            .iter()
            .filter_map(|s| s.max)
            .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        Ok(MinMaxState { min, max })
    }

    fn is_empty(&self) -> bool {
        self.min.is_none() && self.max.is_none()
    }
}

/// Analyzer that computes the minimum value of a numeric column.
///
/// # Example
///
/// ```rust,ignore
/// use term_guard::analyzers::basic::MinAnalyzer;
/// use datafusion::prelude::*;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let ctx = SessionContext::new();
/// // Register your data table
///
/// let analyzer = MinAnalyzer::new("price");
/// let state = analyzer.compute_state_from_data(&ctx).await?;
/// let metric = analyzer.compute_metric_from_state(&state)?;
///
/// if let MetricValue::Double(min) = metric {
///     println!("Minimum price: ${:.2}", min);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct MinAnalyzer {
    /// The column to analyze.
    column: String,
}

impl MinAnalyzer {
    /// Creates a new min analyzer for the specified column.
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

#[async_trait]
impl Analyzer for MinAnalyzer {
    type State = MinMaxState;
    type Metric = MetricValue;

    #[instrument(skip(ctx), fields(analyzer = "min", column = %self.column))]
    async fn compute_state_from_data(&self, ctx: &SessionContext) -> AnalyzerResult<Self::State> {
        // Build SQL query to compute min
        // Get the table name from the validation context

        let validation_ctx = current_validation_context();

        let table_name = validation_ctx.table_name();

        let sql = format!(
            "SELECT MIN({0}) as min, MAX({0}) as max FROM {table_name}",
            self.column
        );

        // Execute query
        let df = ctx.sql(&sql).await?;
        let batches = df.collect().await?;

        // Extract min/max from result
        let (min, max) = if let Some(batch) = batches.first() {
            if batch.num_rows() > 0 {
                let min = if batch.column(0).is_null(0) {
                    None
                } else {
                    // Try Float64 first, then Int64
                    if let Some(arr) = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::Float64Array>()
                    {
                        Some(arr.value(0))
                    } else if let Some(arr) = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                    {
                        Some(arr.value(0) as f64)
                    } else {
                        return Err(AnalyzerError::invalid_data(format!(
                            "Expected numeric array for min, got {:?}",
                            batch.column(0).data_type()
                        )));
                    }
                };

                let max = if batch.column(1).is_null(0) {
                    None
                } else {
                    // Try Float64 first, then Int64
                    if let Some(arr) = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<arrow::array::Float64Array>()
                    {
                        Some(arr.value(0))
                    } else if let Some(arr) = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                    {
                        Some(arr.value(0) as f64)
                    } else {
                        return Err(AnalyzerError::invalid_data(format!(
                            "Expected numeric array for max, got {:?}",
                            batch.column(1).data_type()
                        )));
                    }
                };

                (min, max)
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        Ok(MinMaxState { min, max })
    }

    fn compute_metric_from_state(&self, state: &Self::State) -> AnalyzerResult<Self::Metric> {
        match state.min {
            Some(min) => Ok(MetricValue::Double(min)),
            None => Err(AnalyzerError::NoData),
        }
    }

    fn name(&self) -> &str {
        "min"
    }

    fn description(&self) -> &str {
        "Computes the minimum value of a numeric column"
    }

    fn metric_key(&self) -> String {
        format!("{}.{}", self.name(), self.column)
    }

    fn columns(&self) -> Vec<&str> {
        vec![&self.column]
    }
}

/// Analyzer that computes the maximum value of a numeric column.
///
/// # Example
///
/// ```rust,ignore
/// use term_guard::analyzers::basic::MaxAnalyzer;
/// use datafusion::prelude::*;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let ctx = SessionContext::new();
/// // Register your data table
///
/// let analyzer = MaxAnalyzer::new("price");
/// let state = analyzer.compute_state_from_data(&ctx).await?;
/// let metric = analyzer.compute_metric_from_state(&state)?;
///
/// if let MetricValue::Double(max) = metric {
///     println!("Maximum price: ${:.2}", max);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct MaxAnalyzer {
    /// The column to analyze.
    column: String,
}

impl MaxAnalyzer {
    /// Creates a new max analyzer for the specified column.
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

#[async_trait]
impl Analyzer for MaxAnalyzer {
    type State = MinMaxState;
    type Metric = MetricValue;

    #[instrument(skip(ctx), fields(analyzer = "max", column = %self.column))]
    async fn compute_state_from_data(&self, ctx: &SessionContext) -> AnalyzerResult<Self::State> {
        // Get the table name from the validation context
        let validation_ctx = current_validation_context();
        let table_name = validation_ctx.table_name();

        // Build SQL query to compute max (we compute both for efficiency)
        let sql = format!(
            "SELECT MIN({0}) as min, MAX({0}) as max FROM {table_name}",
            self.column
        );

        // Execute query
        let df = ctx.sql(&sql).await?;
        let batches = df.collect().await?;

        // Extract min/max from result
        let (min, max) = if let Some(batch) = batches.first() {
            if batch.num_rows() > 0 {
                let min = if batch.column(0).is_null(0) {
                    None
                } else {
                    // Try Float64 first, then Int64
                    if let Some(arr) = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::Float64Array>()
                    {
                        Some(arr.value(0))
                    } else if let Some(arr) = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                    {
                        Some(arr.value(0) as f64)
                    } else {
                        return Err(AnalyzerError::invalid_data(format!(
                            "Expected numeric array for min, got {:?}",
                            batch.column(0).data_type()
                        )));
                    }
                };

                let max = if batch.column(1).is_null(0) {
                    None
                } else {
                    // Try Float64 first, then Int64
                    if let Some(arr) = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<arrow::array::Float64Array>()
                    {
                        Some(arr.value(0))
                    } else if let Some(arr) = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                    {
                        Some(arr.value(0) as f64)
                    } else {
                        return Err(AnalyzerError::invalid_data(format!(
                            "Expected numeric array for max, got {:?}",
                            batch.column(1).data_type()
                        )));
                    }
                };

                (min, max)
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        Ok(MinMaxState { min, max })
    }

    fn compute_metric_from_state(&self, state: &Self::State) -> AnalyzerResult<Self::Metric> {
        match state.max {
            Some(max) => Ok(MetricValue::Double(max)),
            None => Err(AnalyzerError::NoData),
        }
    }

    fn name(&self) -> &str {
        "max"
    }

    fn description(&self) -> &str {
        "Computes the maximum value of a numeric column"
    }

    fn metric_key(&self) -> String {
        format!("{}.{}", self.name(), self.column)
    }

    fn columns(&self) -> Vec<&str> {
        vec![&self.column]
    }
}
