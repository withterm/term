//! Size analyzer for counting rows in a dataset.

use async_trait::async_trait;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::analyzers::{Analyzer, AnalyzerResult, AnalyzerState, MetricValue};
use crate::core::current_validation_context;

/// Analyzer that computes the number of rows in a dataset.
///
/// This is one of the most basic analyzers and serves as a foundation
/// for other metrics that need row counts.
///
/// # Example
///
/// ```rust,ignore
/// use term_guard::analyzers::basic::SizeAnalyzer;
/// use datafusion::prelude::*;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let ctx = SessionContext::new();
/// // Register your data table
///
/// let analyzer = SizeAnalyzer::new();
/// let state = analyzer.compute_state_from_data(&ctx).await?;
/// let metric = analyzer.compute_metric_from_state(&state)?;
///
/// if let MetricValue::Long(count) = metric {
///     println!("Dataset has {} rows", count);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct SizeAnalyzer;

impl SizeAnalyzer {
    /// Creates a new size analyzer.
    pub fn new() -> Self {
        Self
    }
}

impl Default for SizeAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

/// State for the size analyzer containing the row count.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SizeState {
    /// Number of rows counted.
    pub count: u64,
}

impl AnalyzerState for SizeState {
    fn merge(states: Vec<Self>) -> AnalyzerResult<Self> {
        let total_count = states.iter().map(|s| s.count).sum();
        Ok(SizeState { count: total_count })
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }
}

#[async_trait]
impl Analyzer for SizeAnalyzer {
    type State = SizeState;
    type Metric = MetricValue;

    #[instrument(skip(ctx), fields(analyzer = "size"))]
    async fn compute_state_from_data(&self, ctx: &SessionContext) -> AnalyzerResult<Self::State> {
        // Get the table name from the validation context
        let validation_ctx = current_validation_context();
        let table_name = validation_ctx.table_name();
        
        // Execute count query
        let sql = format!("SELECT COUNT(*) as count FROM {table_name}");
        let df = ctx.sql(&sql).await?;
        let batches = df.collect().await?;

        // Extract count from result
        let count = if let Some(batch) = batches.first() {
            if batch.num_rows() > 0 {
                if let Some(array) = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                {
                    array.value(0) as u64
                } else {
                    0
                }
            } else {
                0
            }
        } else {
            0
        };

        Ok(SizeState { count })
    }

    fn compute_metric_from_state(&self, state: &Self::State) -> AnalyzerResult<Self::Metric> {
        Ok(MetricValue::Long(state.count as i64))
    }

    fn name(&self) -> &str {
        "size"
    }

    fn description(&self) -> &str {
        "Computes the number of rows in the dataset"
    }
}
