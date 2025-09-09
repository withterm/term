//! Incremental analysis runner for efficient partition-based computation.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json;
use tracing::{debug, info, instrument, warn};

use crate::analyzers::{Analyzer, AnalyzerContext, AnalyzerError, AnalyzerResult, MetricValue};

use super::state_store::{StateMap, StateStore};

/// Configuration for incremental analysis
#[derive(Debug, Clone)]
pub struct IncrementalConfig {
    /// Whether to fail fast on first error
    pub fail_fast: bool,
    /// Whether to save empty states
    pub save_empty_states: bool,
    /// Maximum number of partitions to merge at once
    pub max_merge_batch_size: usize,
}

impl Default for IncrementalConfig {
    fn default() -> Self {
        Self {
            fail_fast: true,
            save_empty_states: false,
            max_merge_batch_size: 100,
        }
    }
}

/// Type-erased analyzer wrapper for dynamic dispatch
trait ErasedAnalyzer: Send + Sync {
    /// Computes state from data
    fn compute_state<'a>(
        &'a self,
        ctx: &'a SessionContext,
    ) -> futures::future::BoxFuture<'a, AnalyzerResult<Vec<u8>>>;

    /// Merges serialized states
    fn merge_states(&self, states: Vec<Vec<u8>>) -> AnalyzerResult<Vec<u8>>;

    /// Computes metric from serialized state
    fn compute_metric(&self, state: &[u8]) -> AnalyzerResult<(String, MetricValue)>;

    /// Returns the analyzer name
    fn name(&self) -> &str;

    /// Returns the metric key
    fn metric_key(&self) -> String;
}

/// Concrete implementation of ErasedAnalyzer for any Analyzer type
struct ErasedAnalyzerImpl<A>
where
    A: Analyzer + 'static,
{
    analyzer: Arc<A>,
}

impl<A> ErasedAnalyzer for ErasedAnalyzerImpl<A>
where
    A: Analyzer + 'static,
    A::State: Serialize + for<'de> Deserialize<'de>,
    A::Metric: Into<MetricValue>,
{
    fn compute_state<'a>(
        &'a self,
        ctx: &'a SessionContext,
    ) -> futures::future::BoxFuture<'a, AnalyzerResult<Vec<u8>>> {
        Box::pin(async move {
            let state = self.analyzer.compute_state_from_data(ctx).await?;
            let serialized = serde_json::to_vec(&state)
                .map_err(|e| AnalyzerError::Custom(format!("Failed to serialize state: {e}")))?;
            Ok(serialized)
        })
    }

    fn merge_states(&self, states: Vec<Vec<u8>>) -> AnalyzerResult<Vec<u8>> {
        let mut deserialized_states = Vec::new();
        for state_data in states {
            let state: A::State = serde_json::from_slice(&state_data)
                .map_err(|e| AnalyzerError::Custom(format!("Failed to deserialize state: {e}")))?;
            deserialized_states.push(state);
        }

        let merged = self.analyzer.merge_states(deserialized_states)?;
        let serialized = serde_json::to_vec(&merged)
            .map_err(|e| AnalyzerError::Custom(format!("Failed to serialize merged state: {e}")))?;
        Ok(serialized)
    }

    fn compute_metric(&self, state: &[u8]) -> AnalyzerResult<(String, MetricValue)> {
        let state: A::State = serde_json::from_slice(state)
            .map_err(|e| AnalyzerError::Custom(format!("Failed to deserialize state: {e}")))?;
        let metric = self.analyzer.compute_metric_from_state(&state)?;
        Ok((self.analyzer.metric_key(), metric.into()))
    }

    fn name(&self) -> &str {
        self.analyzer.name()
    }

    fn metric_key(&self) -> String {
        self.analyzer.metric_key()
    }
}

/// Orchestrates incremental analysis across partitions.
///
/// The runner maintains a collection of analyzers and manages their state
/// across data partitions, enabling efficient incremental computation.
pub struct IncrementalAnalysisRunner {
    state_store: Box<dyn StateStore>,
    analyzers: Vec<Box<dyn ErasedAnalyzer>>,
    config: IncrementalConfig,
}

impl IncrementalAnalysisRunner {
    /// Creates a new incremental analysis runner
    pub fn new(state_store: Box<dyn StateStore>) -> Self {
        Self {
            state_store,
            analyzers: Vec::new(),
            config: IncrementalConfig::default(),
        }
    }

    /// Creates a new incremental analysis runner with custom config
    pub fn with_config(state_store: Box<dyn StateStore>, config: IncrementalConfig) -> Self {
        Self {
            state_store,
            analyzers: Vec::new(),
            config,
        }
    }

    /// Adds an analyzer to the runner
    pub fn add_analyzer<A>(mut self, analyzer: A) -> Self
    where
        A: Analyzer + 'static,
        A::State: Serialize + for<'de> Deserialize<'de>,
        A::Metric: Into<MetricValue>,
    {
        let erased = Box::new(ErasedAnalyzerImpl {
            analyzer: Arc::new(analyzer),
        });
        self.analyzers.push(erased);
        self
    }

    /// Analyzes a single partition, computing and storing its state
    ///
    /// # Arguments
    /// * `ctx` - DataFusion context with registered data  
    /// * `partition` - Partition identifier
    ///
    /// # Returns
    /// Analysis context with computed metrics
    #[instrument(skip(self, ctx))]
    pub async fn analyze_partition(
        &self,
        ctx: &SessionContext,
        partition: &str,
    ) -> AnalyzerResult<AnalyzerContext> {
        info!(
            partition = %partition,
            analyzers = self.analyzers.len(),
            "Starting partition analysis"
        );

        let mut state_map = StateMap::new();
        let mut context = AnalyzerContext::new();
        context.metadata_mut().record_start();

        // Compute state for each analyzer
        for analyzer in &self.analyzers {
            debug!(
                analyzer = analyzer.name(),
                partition = %partition,
                "Computing analyzer state"
            );

            match analyzer.compute_state(ctx).await {
                Ok(state) => {
                    // Only save non-empty states or if configured to save empty
                    if !state.is_empty() || self.config.save_empty_states {
                        state_map.insert(analyzer.metric_key(), state.clone());
                    }

                    // Compute metric from state
                    match analyzer.compute_metric(&state) {
                        Ok((key, metric)) => {
                            context.store_metric(&key, metric);
                        }
                        Err(e) => {
                            warn!(
                                analyzer = analyzer.name(),
                                error = %e,
                                "Failed to compute metric from state"
                            );
                            if self.config.fail_fast {
                                return Err(e);
                            }
                            context.record_error(analyzer.name(), e);
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        analyzer = analyzer.name(),
                        partition = %partition,
                        error = %e,
                        "Failed to compute state"
                    );
                    if self.config.fail_fast {
                        return Err(e);
                    }
                    context.record_error(analyzer.name(), e);
                }
            }
        }

        // Save state to store
        self.state_store.save_state(partition, state_map).await?;

        context.metadata_mut().record_end();
        info!(
            partition = %partition,
            metrics = context.all_metrics().len(),
            "Completed partition analysis"
        );

        Ok(context)
    }

    /// Analyzes new data and merges with existing partition state
    ///
    /// This method loads the existing state for a partition, computes new state
    /// from the provided data, merges them, and saves the updated state.
    ///
    /// # Arguments
    /// * `ctx` - DataFusion context with new data
    /// * `partition` - Partition identifier
    #[instrument(skip(self, ctx))]
    pub async fn analyze_incremental(
        &self,
        ctx: &SessionContext,
        partition: &str,
    ) -> AnalyzerResult<AnalyzerContext> {
        info!(
            partition = %partition,
            "Starting incremental analysis"
        );

        // Load existing state
        let existing_state = self.state_store.load_state(partition).await?;

        let mut merged_state_map = StateMap::new();
        let mut context = AnalyzerContext::new();
        context.metadata_mut().record_start();

        // Process each analyzer
        for analyzer in &self.analyzers {
            let key = analyzer.metric_key();
            debug!(
                analyzer = analyzer.name(),
                partition = %partition,
                "Processing incremental update"
            );

            // Compute new state from data
            let new_state = match analyzer.compute_state(ctx).await {
                Ok(state) => state,
                Err(e) => {
                    warn!(
                        analyzer = analyzer.name(),
                        error = %e,
                        "Failed to compute new state"
                    );
                    if self.config.fail_fast {
                        return Err(e);
                    }
                    context.record_error(analyzer.name(), e);
                    continue;
                }
            };

            // Merge with existing state if present
            let final_state = if let Some(existing) = existing_state.get(&key) {
                match analyzer.merge_states(vec![existing.clone(), new_state]) {
                    Ok(merged) => merged,
                    Err(e) => {
                        warn!(
                            analyzer = analyzer.name(),
                            error = %e,
                            "Failed to merge states"
                        );
                        if self.config.fail_fast {
                            return Err(e);
                        }
                        context.record_error(analyzer.name(), e);
                        continue;
                    }
                }
            } else {
                new_state
            };

            // Store merged state
            if !final_state.is_empty() || self.config.save_empty_states {
                merged_state_map.insert(key.clone(), final_state.clone());
            }

            // Compute metric from merged state
            match analyzer.compute_metric(&final_state) {
                Ok((metric_key, metric)) => {
                    context.store_metric(&metric_key, metric);
                }
                Err(e) => {
                    warn!(
                        analyzer = analyzer.name(),
                        error = %e,
                        "Failed to compute metric"
                    );
                    if self.config.fail_fast {
                        return Err(e);
                    }
                    context.record_error(analyzer.name(), e);
                }
            }
        }

        // Save updated state
        self.state_store
            .save_state(partition, merged_state_map)
            .await?;

        context.metadata_mut().record_end();
        info!(
            partition = %partition,
            metrics = context.all_metrics().len(),
            "Completed incremental analysis"
        );

        Ok(context)
    }

    /// Computes metrics over a range of partitions by merging their states.
    ///
    /// # Arguments
    /// * `partitions` - List of partition identifiers to analyze
    ///
    /// # Returns
    /// The merged analysis context with aggregate metrics
    #[instrument(skip(self))]
    pub async fn analyze_partitions(
        &self,
        partitions: &[String],
    ) -> AnalyzerResult<AnalyzerContext> {
        info!(
            partitions = partitions.len(),
            "Analyzing multiple partitions"
        );

        if partitions.is_empty() {
            return Ok(AnalyzerContext::new());
        }

        let mut context = AnalyzerContext::new();
        context.metadata_mut().record_start();

        // Collect all states across all batches first
        let mut all_analyzer_states: HashMap<String, Vec<Vec<u8>>> = HashMap::new();

        // Process in batches to avoid memory issues
        for batch in partitions.chunks(self.config.max_merge_batch_size) {
            debug!(batch_size = batch.len(), "Processing partition batch");

            // Load states for all partitions in batch
            let partition_states = self.state_store.load_states_batch(batch).await?;

            // Group states by analyzer
            for (_partition, state_map) in partition_states {
                for (analyzer_key, state_data) in state_map {
                    all_analyzer_states
                        .entry(analyzer_key)
                        .or_default()
                        .push(state_data);
                }
            }
        }

        // Now merge all collected states for each analyzer
        for analyzer in &self.analyzers {
            let key = analyzer.metric_key();

            if let Some(states) = all_analyzer_states.get(&key) {
                if states.is_empty() {
                    continue;
                }

                debug!(
                    analyzer = analyzer.name(),
                    states = states.len(),
                    "Merging all analyzer states"
                );

                match analyzer.merge_states(states.clone()) {
                    Ok(merged_state) => {
                        // Compute metric from merged state
                        match analyzer.compute_metric(&merged_state) {
                            Ok((metric_key, metric)) => {
                                context.store_metric(&metric_key, metric);
                            }
                            Err(e) => {
                                warn!(
                                    analyzer = analyzer.name(),
                                    error = %e,
                                    "Failed to compute metric from merged state"
                                );
                                if self.config.fail_fast {
                                    return Err(e);
                                }
                                context.record_error(analyzer.name(), e);
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            analyzer = analyzer.name(),
                            error = %e,
                            "Failed to merge states"
                        );
                        if self.config.fail_fast {
                            return Err(e);
                        }
                        context.record_error(analyzer.name(), e);
                    }
                }
            }
        }

        context.metadata_mut().record_end();
        info!(
            partitions = partitions.len(),
            metrics = context.all_metrics().len(),
            "Completed multi-partition analysis"
        );

        Ok(context)
    }

    /// Returns the number of analyzers configured
    pub fn analyzer_count(&self) -> usize {
        self.analyzers.len()
    }

    /// Lists all stored partitions
    pub async fn list_partitions(&self) -> AnalyzerResult<Vec<String>> {
        self.state_store.list_partitions().await
    }

    /// Deletes a partition's stored state
    pub async fn delete_partition(&self, partition: &str) -> AnalyzerResult<()> {
        self.state_store.delete_partition(partition).await
    }
}
