//! Orchestration layer for efficient analyzer execution.

use datafusion::prelude::*;
use std::sync::Arc;
use tracing::{debug, error, info, instrument};

use super::{AnalyzerContext, AnalyzerError, AnalyzerResult, MetricValue};

/// Type alias for progress callback function.
pub type ProgressCallback = Arc<dyn Fn(f64) + Send + Sync>;

/// Type alias for a boxed analyzer execution function.
pub type AnalyzerExecution = Box<
    dyn Fn(&SessionContext) -> futures::future::BoxFuture<'_, AnalyzerResult<(String, MetricValue)>>
        + Send
        + Sync,
>;

/// Orchestrates the execution of multiple analyzers on a dataset.
///
/// The AnalysisRunner optimizes execution by grouping compatible analyzers
/// that can share computation, minimizing the number of DataFrame scans required.
///
/// # Example
///
/// ```rust,ignore
/// use term_guard::analyzers::{AnalysisRunner, basic::*};
/// use datafusion::prelude::*;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let ctx = SessionContext::new();
/// // Register your data table
///
/// let runner = AnalysisRunner::new()
///     .add(SizeAnalyzer::new())
///     .add(CompletenessAnalyzer::new("user_id"))
///     .add(DistinctnessAnalyzer::new("user_id"))
///     .on_progress(|progress| {
///         println!("Analysis progress: {:.1}%", progress * 100.0);
///     });
///
/// let context = runner.run(&ctx).await?;
/// println!("Computed {} metrics", context.all_metrics().len());
/// # Ok(())
/// # }
/// ```
pub struct AnalysisRunner {
    /// Analyzer executions to run.
    executions: Vec<AnalyzerExecution>,
    /// Names of the analyzers for debugging.
    analyzer_names: Vec<String>,
    /// Optional progress callback.
    on_progress: Option<ProgressCallback>,
    /// Whether to continue on analyzer failures.
    continue_on_error: bool,
}

impl Default for AnalysisRunner {
    fn default() -> Self {
        Self::new()
    }
}

impl AnalysisRunner {
    /// Creates a new empty AnalysisRunner.
    pub fn new() -> Self {
        Self {
            executions: Vec::new(),
            analyzer_names: Vec::new(),
            on_progress: None,
            continue_on_error: true,
        }
    }

    /// Adds an analyzer to the runner.
    ///
    /// # Arguments
    ///
    /// * `analyzer` - The analyzer to add
    ///
    /// # Type Parameters
    ///
    /// * `A` - The analyzer type that implements the Analyzer trait
    #[allow(clippy::should_implement_trait)]
    pub fn add<A>(mut self, analyzer: A) -> Self
    where
        A: crate::analyzers::Analyzer + 'static,
        A::Metric: Into<MetricValue> + 'static,
    {
        use futures::FutureExt;

        let name = analyzer.name().to_string();
        self.analyzer_names.push(name.clone());

        // Wrap analyzer in Arc to allow sharing
        let analyzer = Arc::new(analyzer);

        // Create an execution closure that captures the analyzer
        let execution: AnalyzerExecution = Box::new(move |ctx| {
            let analyzer = analyzer.clone();
            async move {
                // Compute state from data
                let state = analyzer.compute_state_from_data(ctx).await?;

                // Compute metric from state
                let metric = analyzer.compute_metric_from_state(&state)?;

                Ok((analyzer.metric_key(), metric.into()))
            }
            .boxed()
        });

        self.executions.push(execution);
        self
    }

    /// Sets a progress callback that will be called during execution.
    ///
    /// The callback receives a float between 0.0 and 1.0 indicating progress.
    pub fn on_progress<F>(mut self, callback: F) -> Self
    where
        F: Fn(f64) + Send + Sync + 'static,
    {
        self.on_progress = Some(Arc::new(callback));
        self
    }

    /// Sets whether to continue execution when individual analyzers fail.
    ///
    /// Default is true (continue on error).
    pub fn continue_on_error(mut self, continue_on_error: bool) -> Self {
        self.continue_on_error = continue_on_error;
        self
    }

    /// Executes all analyzers on the given data context.
    ///
    /// This method optimizes execution by grouping compatible analyzers
    /// and executing them together when possible.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The DataFusion session context with registered data
    ///
    /// # Returns
    ///
    /// An AnalyzerContext containing all computed metrics and any errors
    #[instrument(skip(self, ctx), fields(analyzer_count = self.executions.len()))]
    pub async fn run(&self, ctx: &SessionContext) -> AnalyzerResult<AnalyzerContext> {
        info!("Starting analysis with {} analyzers", self.executions.len());

        let mut context = AnalyzerContext::new();
        context.metadata_mut().record_start();

        let total_analyzers = self.executions.len() as f64;
        let mut completed = 0.0;

        // Execute each analyzer
        // TODO: In the future, group compatible analyzers for shared execution
        for (idx, execution) in self.executions.iter().enumerate() {
            let analyzer_name = &self.analyzer_names[idx];
            debug!("Executing analyzer: {}", analyzer_name);

            // Execute the analyzer
            let result = execution(ctx).await;

            match result {
                Ok((name, metric)) => {
                    // Store the metric in the context
                    context.store_metric(&name, metric);
                    debug!("Stored metric for analyzer: {}", name);
                }
                Err(e) => {
                    error!("Analyzer {} failed: {}", analyzer_name, e);
                    context.record_error(analyzer_name, e);

                    if !self.continue_on_error {
                        return Err(AnalyzerError::execution(format!(
                            "Analyzer {analyzer_name} failed"
                        )));
                    }
                }
            }

            // Update progress
            completed += 1.0;
            if let Some(ref callback) = self.on_progress {
                callback(completed / total_analyzers);
            }
        }

        context.metadata_mut().record_end();

        if let Some(duration) = context.metadata().duration() {
            info!(
                "Analysis completed in {:.2}s",
                duration.num_milliseconds() as f64 / 1000.0
            );
        }

        Ok(context)
    }

    /// Returns the number of analyzers configured.
    pub fn analyzer_count(&self) -> usize {
        self.executions.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyzers::basic::{CompletenessAnalyzer, SizeAnalyzer};
    use crate::analyzers::MetricValue;
    use datafusion::arrow::array::{Float64Array, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    async fn create_test_context() -> SessionContext {
        let ctx = SessionContext::new();

        // Create test data
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(Float64Array::from(vec![
                    Some(10.0),
                    None,
                    Some(30.0),
                    Some(40.0),
                    Some(50.0),
                ])),
            ],
        )
        .unwrap();

        ctx.register_batch("data", batch).unwrap();
        ctx
    }

    #[tokio::test]
    async fn test_analysis_runner_basic() {
        let ctx = create_test_context().await;

        let runner = AnalysisRunner::new().add(SizeAnalyzer::new());

        let context = runner.run(&ctx).await.unwrap();

        // Check that we got the size metric
        let size_metric = context.get_metric("size").expect("Size metric not found");
        if let MetricValue::Long(size) = size_metric {
            assert_eq!(*size, 5);
        } else {
            panic!("Expected Long metric for size");
        }
    }

    #[tokio::test]
    async fn test_analysis_runner_multiple_analyzers() {
        let ctx = create_test_context().await;

        let runner = AnalysisRunner::new()
            .add(SizeAnalyzer::new())
            .add(CompletenessAnalyzer::new("value"));

        let context = runner.run(&ctx).await.unwrap();

        // Check size metric
        let size_metric = context.get_metric("size").expect("Size metric not found");
        if let MetricValue::Long(size) = size_metric {
            assert_eq!(*size, 5);
        }

        // Check completeness metric
        let completeness_metric = context
            .get_metric("completeness.value")
            .expect("Completeness metric not found");
        if let MetricValue::Double(completeness) = completeness_metric {
            assert!((completeness - 0.8).abs() < 0.001); // 4/5 = 0.8
        }
    }

    #[tokio::test]
    async fn test_progress_callback() {
        let ctx = create_test_context().await;

        let progress_values = Arc::new(std::sync::Mutex::new(Vec::new()));
        let progress_clone = progress_values.clone();

        let runner = AnalysisRunner::new()
            .add(SizeAnalyzer::new())
            .add(CompletenessAnalyzer::new("value"))
            .on_progress(move |progress| {
                progress_clone.lock().unwrap().push(progress);
            });

        let _context = runner.run(&ctx).await.unwrap();

        let progress = progress_values.lock().unwrap();
        assert!(!progress.is_empty());
        assert_eq!(*progress.last().unwrap(), 1.0);
    }

    #[tokio::test]
    async fn test_error_handling() {
        let ctx = SessionContext::new(); // No data registered

        let runner = AnalysisRunner::new()
            .add(SizeAnalyzer::new())
            .continue_on_error(true);

        let context = runner.run(&ctx).await.unwrap();

        // Should have recorded an error
        assert!(context.has_errors());
        assert_eq!(context.errors().len(), 1);
    }

    #[tokio::test]
    async fn test_fail_fast() {
        let ctx = SessionContext::new(); // No data registered

        let runner = AnalysisRunner::new()
            .add(SizeAnalyzer::new())
            .continue_on_error(false);

        let result = runner.run(&ctx).await;

        // Should fail immediately
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_many_analyzers() {
        use crate::analyzers::basic::*;

        let ctx = create_test_context().await;

        // Add 10+ analyzers
        let runner = AnalysisRunner::new()
            .add(SizeAnalyzer::new())
            .add(CompletenessAnalyzer::new("id"))
            .add(CompletenessAnalyzer::new("value"))
            .add(DistinctnessAnalyzer::new("id"))
            .add(DistinctnessAnalyzer::new("value"))
            .add(MeanAnalyzer::new("value"))
            .add(MinAnalyzer::new("value"))
            .add(MaxAnalyzer::new("value"))
            .add(SumAnalyzer::new("value"))
            .add(MinAnalyzer::new("id"))
            .add(MaxAnalyzer::new("id"))
            .add(SumAnalyzer::new("id"));

        assert_eq!(runner.analyzer_count(), 12);

        let context = runner.run(&ctx).await.unwrap();

        // Verify we got metrics from all analyzers
        assert!(context.get_metric("size").is_some());
        assert!(context.get_metric("completeness.id").is_some());
        assert!(context.get_metric("completeness.value").is_some());
        assert!(context.get_metric("distinctness.id").is_some());
        assert!(context.get_metric("distinctness.value").is_some());
        assert!(context.get_metric("mean.value").is_some());
        assert!(context.get_metric("min.value").is_some());
        assert!(context.get_metric("max.value").is_some());
        assert!(context.get_metric("sum.value").is_some());
        assert!(context.get_metric("min.id").is_some());
        assert!(context.get_metric("max.id").is_some());
        assert!(context.get_metric("sum.id").is_some());

        // Verify specific metric values
        if let MetricValue::Long(size) = context.get_metric("size").unwrap() {
            assert_eq!(*size, 5);
        }

        if let MetricValue::Double(completeness) = context.get_metric("completeness.id").unwrap() {
            assert_eq!(*completeness, 1.0); // All IDs are non-null
        }

        if let MetricValue::Double(completeness) = context.get_metric("completeness.value").unwrap()
        {
            assert!((completeness - 0.8).abs() < 0.001); // 4/5 values are non-null
        }

        // Check that analysis completed without errors
        assert!(!context.has_errors());
    }
}
