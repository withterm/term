//! Core analyzer traits for the Term framework.

use async_trait::async_trait;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

use super::errors::AnalyzerResult;
use super::types::MetricValue;

/// Core trait for analyzers that compute metrics from data.
///
/// Analyzers support incremental computation through state management,
/// enabling efficient processing of large datasets and parallel execution.
///
/// # Type Parameters
///
/// * `State` - The state type that holds intermediate computation results
/// * `Metric` - The final metric type produced by this analyzer
///
/// # Example
///
/// ```rust,ignore
/// use term_guard::analyzers::{Analyzer, AnalyzerState, MetricValue};
/// use async_trait::async_trait;
/// use datafusion::prelude::*;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// struct SizeState {
///     count: u64,
/// }
///
/// impl AnalyzerState for SizeState {
///     fn merge(states: Vec<Self>) -> Result<Self, Box<dyn std::error::Error>> {
///         let total_count = states.iter().map(|s| s.count).sum();
///         Ok(SizeState { count: total_count })
///     }
/// }
///
/// struct SizeAnalyzer;
///
/// #[async_trait]
/// impl Analyzer for SizeAnalyzer {
///     type State = SizeState;
///     type Metric = MetricValue;
///
///     async fn compute_state_from_data(&self, ctx: &SessionContext) -> Result<Self::State> {
///         let df = ctx.sql("SELECT COUNT(*) as count FROM data").await?;
///         let batches = df.collect().await?;
///         // Extract count from batches...
///         Ok(SizeState { count: 42 })
///     }
///
///     fn compute_metric_from_state(&self, state: &Self::State) -> Result<Self::Metric> {
///         Ok(MetricValue::Long(state.count as i64))
///     }
///
///     fn name(&self) -> &str {
///         "size"
///     }
/// }
/// ```
#[async_trait]
pub trait Analyzer: Send + Sync + Debug {
    /// The state type for incremental computation.
    type State: AnalyzerState;

    /// The metric type produced by this analyzer.
    type Metric: Into<MetricValue> + Send + Sync + Debug;

    /// Computes the state from the input data.
    ///
    /// This method performs the main computation, extracting intermediate
    /// results that can be merged with other states for parallel processing.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The DataFusion session context with registered data tables
    ///
    /// # Returns
    ///
    /// The computed state or an error if computation fails
    async fn compute_state_from_data(&self, ctx: &SessionContext) -> AnalyzerResult<Self::State>;

    /// Computes the final metric from the accumulated state.
    ///
    /// This method transforms the intermediate state into the final metric value.
    ///
    /// # Arguments
    ///
    /// * `state` - The accumulated state from one or more data computations
    ///
    /// # Returns
    ///
    /// The final metric value or an error if transformation fails
    fn compute_metric_from_state(&self, state: &Self::State) -> AnalyzerResult<Self::Metric>;

    /// Merges multiple states into a single state.
    ///
    /// This method enables parallel computation by allowing states computed
    /// from different data partitions to be combined.
    ///
    /// # Arguments
    ///
    /// * `states` - A vector of states to merge
    ///
    /// # Returns
    ///
    /// The merged state or an error if merging fails
    fn merge_states(&self, states: Vec<Self::State>) -> AnalyzerResult<Self::State> {
        Self::State::merge(states)
    }

    /// Returns the name of this analyzer.
    ///
    /// Used for identification in results and debugging.
    fn name(&self) -> &str;

    /// Returns a description of what this analyzer computes.
    ///
    /// Used for documentation and error messages.
    fn description(&self) -> &str {
        ""
    }

    /// Returns the metric key for storing results.
    ///
    /// By default, this returns the analyzer name, but column-based
    /// analyzers should override this to include the column name.
    fn metric_key(&self) -> String {
        self.name().to_string()
    }

    /// Returns the column(s) this analyzer operates on, if any.
    ///
    /// Used for optimization and dependency analysis.
    fn columns(&self) -> Vec<&str> {
        vec![]
    }

    /// Indicates whether this analyzer can be combined with others.
    ///
    /// Some analyzers may have complex logic that prevents efficient combination.
    fn is_combinable(&self) -> bool {
        true
    }
}

/// Trait for analyzer state that supports incremental computation.
///
/// States must be serializable to support distributed computation
/// and caching of intermediate results.
pub trait AnalyzerState:
    Clone + Send + Sync + Debug + Serialize + for<'de> Deserialize<'de>
{
    /// Merges multiple states into a single state.
    ///
    /// This enables parallel computation where states are computed
    /// independently on data partitions and then combined.
    ///
    /// # Arguments
    ///
    /// * `states` - States to merge together
    ///
    /// # Returns
    ///
    /// The merged state or an error if states cannot be merged
    fn merge(states: Vec<Self>) -> AnalyzerResult<Self>
    where
        Self: Sized;

    /// Returns whether this state represents an empty computation.
    ///
    /// Used to optimize away empty states during merging.
    fn is_empty(&self) -> bool {
        false
    }
}
