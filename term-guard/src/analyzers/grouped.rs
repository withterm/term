//! Grouped metrics computation for segment-level data quality monitoring.
//!
//! This module provides infrastructure for computing metrics separately for each
//! distinct value in grouping columns, similar to Deequ's grouped analysis capabilities.

use async_trait::async_trait;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use tracing::{debug, instrument};

use super::{Analyzer, AnalyzerResult, AnalyzerState, MetricValue};

/// Configuration for grouped metric analysis.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupingConfig {
    /// Columns to group by.
    pub columns: Vec<String>,

    /// Maximum number of groups to track (for memory management).
    pub max_groups: Option<usize>,

    /// Whether to include overall (ungrouped) metric.
    pub include_overall: bool,

    /// Strategy for handling high-cardinality groups.
    pub overflow_strategy: OverflowStrategy,
}

impl GroupingConfig {
    /// Creates a new grouping configuration with default settings.
    pub fn new(columns: Vec<String>) -> Self {
        Self {
            columns,
            max_groups: Some(10000),
            include_overall: true,
            overflow_strategy: OverflowStrategy::TopK,
        }
    }

    /// Sets the maximum number of groups to track.
    pub fn with_max_groups(mut self, max: usize) -> Self {
        self.max_groups = Some(max);
        self
    }

    /// Sets whether to include the overall metric.
    pub fn with_overall(mut self, include: bool) -> Self {
        self.include_overall = include;
        self
    }

    /// Sets the overflow strategy for high-cardinality groups.
    pub fn with_overflow_strategy(mut self, strategy: OverflowStrategy) -> Self {
        self.overflow_strategy = strategy;
        self
    }

    /// Returns SQL column list for GROUP BY clause.
    pub fn group_by_sql(&self) -> String {
        self.columns.join(", ")
    }

    /// Returns SQL column selection for grouping columns.
    pub fn select_group_columns_sql(&self) -> String {
        self.columns
            .iter()
            .map(|col| format!("{col} as group_{col}"))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Strategy for handling high-cardinality grouping columns.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum OverflowStrategy {
    /// Keep only the top-K groups by metric value.
    TopK,

    /// Keep only the bottom-K groups by metric value.
    BottomK,

    /// Sample randomly from groups.
    Sample,

    /// Fail if limit is exceeded.
    Fail,
}

/// Result of grouped metric computation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupedMetrics {
    /// Metrics per group value.
    /// Key is a vector of group values (one per grouping column).
    pub groups: BTreeMap<Vec<String>, MetricValue>,

    /// Overall metric across all groups (if requested).
    pub overall: Option<MetricValue>,

    /// Metadata about the grouped computation.
    pub metadata: GroupedMetadata,
}

impl GroupedMetrics {
    /// Creates a new grouped metrics result.
    pub fn new(
        groups: BTreeMap<Vec<String>, MetricValue>,
        overall: Option<MetricValue>,
        metadata: GroupedMetadata,
    ) -> Self {
        Self {
            groups,
            overall,
            metadata,
        }
    }

    /// Returns the number of groups.
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Gets the metric for a specific group.
    pub fn get_group(&self, key: &[String]) -> Option<&MetricValue> {
        self.groups.get(key)
    }

    /// Checks if grouping was truncated due to cardinality limits.
    pub fn is_truncated(&self) -> bool {
        self.metadata.truncated
    }

    /// Converts to a standard MetricValue Map.
    pub fn to_metric_value(&self) -> MetricValue {
        let mut map = HashMap::new();

        // Add grouped metrics
        for (key, value) in &self.groups {
            let key_str = key.join("_");
            map.insert(key_str, value.clone());
        }

        // Add overall if present
        if let Some(ref overall) = self.overall {
            map.insert("__overall__".to_string(), overall.clone());
        }

        // Add metadata
        map.insert(
            "__metadata__".to_string(),
            MetricValue::String(serde_json::to_string(&self.metadata).unwrap_or_default()),
        );

        MetricValue::Map(map)
    }
}

/// Metadata about grouped computation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupedMetadata {
    /// Grouping columns used.
    pub group_columns: Vec<String>,

    /// Total number of groups found.
    pub total_groups: usize,

    /// Number of groups included in results.
    pub included_groups: usize,

    /// Whether results were truncated.
    pub truncated: bool,

    /// Strategy used for overflow handling.
    pub overflow_strategy: Option<OverflowStrategy>,
}

impl GroupedMetadata {
    /// Creates new metadata for grouped computation.
    pub fn new(group_columns: Vec<String>, total_groups: usize, included_groups: usize) -> Self {
        Self {
            group_columns,
            total_groups,
            included_groups,
            truncated: total_groups > included_groups,
            overflow_strategy: None,
        }
    }
}

/// Extension trait for analyzers that support grouped computation.
#[async_trait]
pub trait GroupedAnalyzer: Analyzer {
    /// The grouped state type for this analyzer.
    type GroupedState: GroupedAnalyzerState;

    /// Configures grouping for this analyzer.
    fn with_grouping(self, config: GroupingConfig) -> GroupedAnalyzerWrapper<Self>
    where
        Self: Sized + 'static,
    {
        GroupedAnalyzerWrapper::new(self, config)
    }

    /// Computes grouped state from data.
    ///
    /// Implementers should generate SQL with GROUP BY clauses and
    /// return states per group value.
    async fn compute_grouped_state_from_data(
        &self,
        ctx: &SessionContext,
        config: &GroupingConfig,
    ) -> AnalyzerResult<Self::GroupedState>;

    /// Computes metrics from grouped state.
    fn compute_grouped_metrics_from_state(
        &self,
        state: &Self::GroupedState,
    ) -> AnalyzerResult<GroupedMetrics>;
}

/// State trait for grouped analyzers.
///
/// This trait extends the base AnalyzerState trait to provide grouped computation support.
pub trait GroupedAnalyzerState: AnalyzerState {}

/// Wrapper that adds grouping capability to any analyzer that implements GroupedAnalyzer.
pub struct GroupedAnalyzerWrapper<A: GroupedAnalyzer> {
    /// The underlying analyzer.
    analyzer: A,

    /// Grouping configuration.
    config: GroupingConfig,
}

impl<A: GroupedAnalyzer> GroupedAnalyzerWrapper<A> {
    /// Creates a new grouped analyzer wrapper.
    pub fn new(analyzer: A, config: GroupingConfig) -> Self {
        Self { analyzer, config }
    }
}

impl<A: GroupedAnalyzer> fmt::Debug for GroupedAnalyzerWrapper<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupedAnalyzerWrapper")
            .field("analyzer", &self.analyzer.name())
            .field("group_columns", &self.config.columns)
            .finish()
    }
}

#[async_trait]
impl<A> Analyzer for GroupedAnalyzerWrapper<A>
where
    A: GroupedAnalyzer + Send + Sync + 'static,
    A::GroupedState: AnalyzerState + 'static,
{
    type State = A::GroupedState;
    type Metric = MetricValue;

    #[instrument(skip(ctx), fields(
        analyzer = %self.analyzer.name(),
        group_columns = ?self.config.columns
    ))]
    async fn compute_state_from_data(&self, ctx: &SessionContext) -> AnalyzerResult<Self::State> {
        debug!(
            "Computing grouped state for {} analyzer",
            self.analyzer.name()
        );
        self.analyzer
            .compute_grouped_state_from_data(ctx, &self.config)
            .await
    }

    fn compute_metric_from_state(&self, state: &Self::State) -> AnalyzerResult<Self::Metric> {
        let grouped_metrics = self.analyzer.compute_grouped_metrics_from_state(state)?;
        Ok(grouped_metrics.to_metric_value())
    }

    fn name(&self) -> &str {
        self.analyzer.name()
    }

    fn description(&self) -> &str {
        self.analyzer.description()
    }

    fn metric_key(&self) -> String {
        format!(
            "{}_grouped_by_{}",
            self.analyzer.metric_key(),
            self.config.columns.join("_")
        )
    }

    fn columns(&self) -> Vec<&str> {
        let mut cols = self.analyzer.columns();
        for col in &self.config.columns {
            cols.push(col);
        }
        cols
    }
}

/// Helper functions for building grouped SQL queries.
pub mod sql_helpers {
    use super::GroupingConfig;

    /// Builds a GROUP BY clause from configuration.
    pub fn build_group_by_clause(config: &GroupingConfig) -> String {
        if config.columns.is_empty() {
            String::new()
        } else {
            format!(" GROUP BY {}", config.group_by_sql())
        }
    }

    /// Builds column selection for grouped queries.
    pub fn build_group_select(config: &GroupingConfig, metric_sql: &str) -> String {
        if config.columns.is_empty() {
            metric_sql.to_string()
        } else {
            format!("{}, {metric_sql}", config.select_group_columns_sql())
        }
    }

    /// Builds LIMIT clause based on max_groups setting.
    pub fn build_limit_clause(config: &GroupingConfig) -> String {
        if let Some(max) = config.max_groups {
            format!(" LIMIT {max}")
        } else {
            String::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grouping_config() {
        let config = GroupingConfig::new(vec!["country".to_string(), "city".to_string()])
            .with_max_groups(1000)
            .with_overall(false);

        assert_eq!(config.group_by_sql(), "country, city");
        assert_eq!(
            config.select_group_columns_sql(),
            "country as group_country, city as group_city"
        );
        assert_eq!(config.max_groups, Some(1000));
        assert!(!config.include_overall);
    }

    #[test]
    fn test_grouped_metrics() {
        let mut groups = BTreeMap::new();
        groups.insert(
            vec!["US".to_string(), "NYC".to_string()],
            MetricValue::Double(0.95),
        );
        groups.insert(
            vec!["US".to_string(), "LA".to_string()],
            MetricValue::Double(0.92),
        );

        let metadata = GroupedMetadata::new(vec!["country".to_string(), "city".to_string()], 2, 2);

        let grouped = GroupedMetrics::new(groups, Some(MetricValue::Double(0.935)), metadata);

        assert_eq!(grouped.group_count(), 2);
        assert!(!grouped.is_truncated());

        let us_nyc = grouped.get_group(&["US".to_string(), "NYC".to_string()]);
        assert_eq!(us_nyc, Some(&MetricValue::Double(0.95)));
    }
}
