//! Entropy analyzer for information theory metrics.

use arrow::array::{Array, StringViewArray};
use async_trait::async_trait;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::instrument;

use crate::analyzers::{Analyzer, AnalyzerError, AnalyzerResult, AnalyzerState, MetricValue};

use crate::core::current_validation_context;
/// Analyzer that computes Shannon entropy and related information theory metrics.
///
/// Entropy measures the average information content or uncertainty in a dataset.
/// Higher entropy indicates more randomness/diversity, while lower entropy
/// indicates more predictability/uniformity.
///
/// # Metrics Computed
///
/// - **Shannon Entropy**: -Σ(p_i * log2(p_i)) where p_i is the probability of each value
/// - **Normalized Entropy**: Entropy divided by log2(n) where n is the number of unique values
/// - **Gini Impurity**: 1 - Σ(p_i²), another measure of diversity
/// - **Effective Number of Values**: 2^entropy, interpretable as the effective cardinality
///
/// # Example
///
/// ```rust,ignore
/// use term_guard::analyzers::advanced::EntropyAnalyzer;
/// use datafusion::prelude::*;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let ctx = SessionContext::new();
/// // Register your data table
///
/// let analyzer = EntropyAnalyzer::new("category");
/// let state = analyzer.compute_state_from_data(&ctx).await?;
/// let metric = analyzer.compute_metric_from_state(&state)?;
///
/// if let MetricValue::Map(metrics) = metric {
///     println!("Category entropy: {:?} bits", metrics.get("entropy"));
///     println!("Normalized entropy: {:?}", metrics.get("normalized_entropy"));
///     println!("Effective categories: {:?}", metrics.get("effective_values"));
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct EntropyAnalyzer {
    /// The column to analyze.
    column: String,
    /// Maximum number of unique values to track (for memory efficiency).
    max_unique_values: usize,
}

impl EntropyAnalyzer {
    /// Creates a new entropy analyzer for the specified column.
    pub fn new(column: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            max_unique_values: 10_000,
        }
    }

    /// Creates a new entropy analyzer with a custom maximum unique values limit.
    pub fn with_max_unique_values(column: impl Into<String>, max_unique_values: usize) -> Self {
        Self {
            column: column.into(),
            max_unique_values: max_unique_values.max(10),
        }
    }

    /// Returns the column being analyzed.
    pub fn column(&self) -> &str {
        &self.column
    }

    /// Returns the maximum unique values limit.
    pub fn max_unique_values(&self) -> usize {
        self.max_unique_values
    }
}

/// State for the entropy analyzer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntropyState {
    /// Count of occurrences for each unique value.
    pub value_counts: HashMap<String, u64>,
    /// Total count of non-null values.
    pub total_count: u64,
    /// Whether the unique value limit was exceeded.
    pub truncated: bool,
}

impl EntropyState {
    /// Calculates Shannon entropy in bits.
    pub fn entropy(&self) -> f64 {
        if self.total_count == 0 {
            return 0.0;
        }

        let total = self.total_count as f64;
        self.value_counts
            .values()
            .map(|&count| {
                let p = count as f64 / total;
                if p > 0.0 {
                    -p * p.log2()
                } else {
                    0.0
                }
            })
            .sum()
    }

    /// Calculates normalized entropy (0 to 1).
    pub fn normalized_entropy(&self) -> f64 {
        let num_unique = self.value_counts.len();
        if num_unique <= 1 {
            0.0
        } else {
            let max_entropy = (num_unique as f64).log2();
            if max_entropy > 0.0 {
                self.entropy() / max_entropy
            } else {
                0.0
            }
        }
    }

    /// Calculates Gini impurity.
    pub fn gini_impurity(&self) -> f64 {
        if self.total_count == 0 {
            return 0.0;
        }

        let total = self.total_count as f64;
        let sum_squared_probs: f64 = self
            .value_counts
            .values()
            .map(|&count| {
                let p = count as f64 / total;
                p * p
            })
            .sum();

        1.0 - sum_squared_probs
    }

    /// Calculates the effective number of values (perplexity).
    pub fn effective_values(&self) -> f64 {
        2.0_f64.powf(self.entropy())
    }

    /// Returns the probability distribution.
    pub fn probability_distribution(&self) -> HashMap<String, f64> {
        let total = self.total_count as f64;
        self.value_counts
            .iter()
            .map(|(value, &count)| (value.clone(), count as f64 / total))
            .collect()
    }
}

impl AnalyzerState for EntropyState {
    fn merge(states: Vec<Self>) -> AnalyzerResult<Self> {
        let mut merged_counts = HashMap::new();
        let mut total_count = 0;
        let mut truncated = false;

        for state in states {
            total_count += state.total_count;
            truncated |= state.truncated;

            for (value, count) in state.value_counts {
                *merged_counts.entry(value).or_insert(0) += count;
            }
        }

        Ok(EntropyState {
            value_counts: merged_counts,
            total_count,
            truncated,
        })
    }

    fn is_empty(&self) -> bool {
        self.total_count == 0
    }
}

#[async_trait]
impl Analyzer for EntropyAnalyzer {
    type State = EntropyState;
    type Metric = MetricValue;

    #[instrument(skip(ctx), fields(analyzer = "entropy", column = %self.column, max_unique = %self.max_unique_values))]
    async fn compute_state_from_data(&self, ctx: &SessionContext) -> AnalyzerResult<Self::State> {
        // Get the table name from the validation context
        let validation_ctx = current_validation_context();
        let table_name = validation_ctx.table_name();
        
        // First check if we have too many unique values
        let count_distinct_sql = format!(
            "SELECT COUNT(DISTINCT {0}) as unique_count FROM {table_name} WHERE {0} IS NOT NULL",
            self.column
        );

        let count_df = ctx.sql(&count_distinct_sql).await?;
        let count_batches = count_df.collect().await?;

        let unique_count = if let Some(batch) = count_batches.first() {
            if batch.num_rows() > 0 {
                let count_array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| {
                        AnalyzerError::invalid_data("Expected Int64 for unique count")
                    })?;
                count_array.value(0) as usize
            } else {
                0
            }
        } else {
            0
        };

        // If too many unique values, use sampling or approximation
        let (sql, truncated) = if unique_count > self.max_unique_values {
            // Sample top N most frequent values
            // Get the table name from the validation context

            let validation_ctx = current_validation_context();

            let table_name = validation_ctx.table_name();

            

            let sql = format!(
                "SELECT 
                    CAST({0} AS VARCHAR) as value, 
                    COUNT(*) as count
                FROM {table_name}
                WHERE {0} IS NOT NULL
                GROUP BY CAST({0} AS VARCHAR)
                ORDER BY count DESC
                LIMIT {1}",
                self.column, self.max_unique_values
            );
            (sql, true)
        } else {
            // Get all values
            let sql = format!(
                "SELECT 
                    CAST({0} AS VARCHAR) as value, 
                    COUNT(*) as count
                FROM {table_name}
                WHERE {0} IS NOT NULL
                GROUP BY CAST({0} AS VARCHAR)",
                self.column
            );
            (sql, false)
        };

        // Execute query
        let df = ctx.sql(&sql).await?;
        let batches = df.collect().await?;

        // Build value counts map
        let mut value_counts = HashMap::new();
        let mut total_count = 0;

        for batch in &batches {
            let value_array = batch.column(0).as_any();

            // Try to handle different string array types
            let values: Vec<(String, bool)> =
                if let Some(arr) = value_array.downcast_ref::<arrow::array::StringArray>() {
                    (0..arr.len())
                        .map(|i| (arr.value(i).to_string(), arr.is_null(i)))
                        .collect()
                } else if let Some(arr) = value_array.downcast_ref::<StringViewArray>() {
                    (0..arr.len())
                        .map(|i| (arr.value(i).to_string(), arr.is_null(i)))
                        .collect()
                } else {
                    return Err(AnalyzerError::invalid_data(format!(
                        "Expected String array for values, got {:?}",
                        batch.column(0).data_type()
                    )));
                };

            let count_array = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| AnalyzerError::invalid_data("Expected Int64 array for counts"))?;

            for (i, (value, is_null)) in values.iter().enumerate() {
                if !is_null {
                    let count = count_array.value(i) as u64;
                    value_counts.insert(value.clone(), count);
                    total_count += count;
                }
            }
        }

        // If truncated, we need to get the true total count
        if truncated {
            let total_sql = format!(
                "SELECT COUNT({0}) as total FROM {table_name} WHERE {0} IS NOT NULL",
                self.column
            );
            let total_df = ctx.sql(&total_sql).await?;
            let total_batches = total_df.collect().await?;

            if let Some(batch) = total_batches.first() {
                if batch.num_rows() > 0 {
                    let total_array = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .ok_or_else(|| {
                            AnalyzerError::invalid_data("Expected Int64 for total count")
                        })?;
                    total_count = total_array.value(0) as u64;
                }
            }
        }

        Ok(EntropyState {
            value_counts,
            total_count,
            truncated,
        })
    }

    fn compute_metric_from_state(&self, state: &Self::State) -> AnalyzerResult<Self::Metric> {
        use std::collections::HashMap;

        let mut metrics = HashMap::new();

        // Core entropy metrics
        metrics.insert("entropy".to_string(), MetricValue::Double(state.entropy()));
        metrics.insert(
            "normalized_entropy".to_string(),
            MetricValue::Double(state.normalized_entropy()),
        );
        metrics.insert(
            "gini_impurity".to_string(),
            MetricValue::Double(state.gini_impurity()),
        );
        metrics.insert(
            "effective_values".to_string(),
            MetricValue::Double(state.effective_values()),
        );

        // Additional statistics
        metrics.insert(
            "unique_values".to_string(),
            MetricValue::Long(state.value_counts.len() as i64),
        );
        metrics.insert(
            "total_count".to_string(),
            MetricValue::Long(state.total_count as i64),
        );
        metrics.insert(
            "truncated".to_string(),
            MetricValue::Boolean(state.truncated),
        );

        // Add top 10 most frequent values if not too many
        if state.value_counts.len() <= 100 {
            let mut sorted_values: Vec<_> = state.value_counts.iter().collect();
            sorted_values.sort_by(|a, b| b.1.cmp(a.1));

            let top_values: HashMap<String, MetricValue> = sorted_values
                .iter()
                .take(10)
                .map(|(value, &count)| {
                    let prob = count as f64 / state.total_count as f64;
                    (
                        value.to_string(),
                        MetricValue::Map(HashMap::from([
                            ("count".to_string(), MetricValue::Long(count as i64)),
                            ("probability".to_string(), MetricValue::Double(prob)),
                        ])),
                    )
                })
                .collect();

            metrics.insert("top_values".to_string(), MetricValue::Map(top_values));
        }

        Ok(MetricValue::Map(metrics))
    }

    fn name(&self) -> &str {
        "entropy"
    }

    fn description(&self) -> &str {
        "Computes Shannon entropy and information theory metrics"
    }

    fn columns(&self) -> Vec<&str> {
        vec![&self.column]
    }
}
