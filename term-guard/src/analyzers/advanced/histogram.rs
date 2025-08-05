//! Histogram analyzer for computing value distributions.

use arrow::array::Array;
use async_trait::async_trait;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::analyzers::{
    types::HistogramBucket, Analyzer, AnalyzerError, AnalyzerResult, AnalyzerState,
    MetricDistribution, MetricValue,
};
use crate::core::current_validation_context;

/// Analyzer that computes histogram distributions for numeric columns.
///
/// This analyzer creates a histogram with configurable number of buckets,
/// providing insights into data distribution patterns. It's memory-efficient
/// even for high-cardinality columns by using fixed-size buckets.
///
/// # Example
///
/// ```rust,ignore
/// use term_guard::analyzers::advanced::HistogramAnalyzer;
/// use datafusion::prelude::*;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let ctx = SessionContext::new();
/// // Register your data table
///
/// let analyzer = HistogramAnalyzer::new("price", 10); // 10 buckets
/// let state = analyzer.compute_state_from_data(&ctx).await?;
/// let metric = analyzer.compute_metric_from_state(&state)?;
///
/// if let MetricValue::Histogram(distribution) = metric {
///     println!("Price distribution: {} buckets", distribution.buckets.len());
///     for bucket in &distribution.buckets {
///         println!("[{:.2}, {:.2}): {} items",
///             bucket.lower_bound, bucket.upper_bound, bucket.count);
///     }
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct HistogramAnalyzer {
    /// The column to analyze.
    column: String,
    /// Number of histogram buckets.
    num_buckets: usize,
}

impl HistogramAnalyzer {
    /// Creates a new histogram analyzer with the specified number of buckets.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to analyze
    /// * `num_buckets` - Number of histogram buckets (clamped between 1 and 1000)
    pub fn new(column: impl Into<String>, num_buckets: usize) -> Self {
        Self {
            column: column.into(),
            num_buckets: num_buckets.clamp(1, 1000),
        }
    }

    /// Returns the column being analyzed.
    pub fn column(&self) -> &str {
        &self.column
    }

    /// Returns the number of buckets.
    pub fn num_buckets(&self) -> usize {
        self.num_buckets
    }
}

/// State for the histogram analyzer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramState {
    /// The histogram buckets.
    pub buckets: Vec<HistogramBucket>,
    /// Minimum value in the dataset.
    pub min_value: f64,
    /// Maximum value in the dataset.
    pub max_value: f64,
    /// Total count of non-null values.
    pub total_count: u64,
    /// Sum of all values (for mean calculation).
    pub sum: f64,
    /// Sum of squared values (for std dev calculation).
    pub sum_squared: f64,
}

impl HistogramState {
    /// Calculates the mean value.
    pub fn mean(&self) -> Option<f64> {
        if self.total_count > 0 {
            Some(self.sum / self.total_count as f64)
        } else {
            None
        }
    }

    /// Calculates the standard deviation.
    pub fn std_dev(&self) -> Option<f64> {
        if self.total_count > 1 {
            let mean = self.mean()?;
            let variance = (self.sum_squared / self.total_count as f64) - (mean * mean);
            Some(variance.sqrt())
        } else {
            None
        }
    }
}

impl AnalyzerState for HistogramState {
    fn merge(states: Vec<Self>) -> AnalyzerResult<Self> {
        if states.is_empty() {
            return Err(AnalyzerError::state_merge("No states to merge"));
        }

        // For simplicity, we take the first state's bucket structure
        // In production, we'd re-bucket based on global min/max
        let first = &states[0];
        let mut merged_buckets = first.buckets.clone();

        // Merge bucket counts
        for state in &states[1..] {
            if state.buckets.len() == merged_buckets.len() {
                for (i, bucket) in state.buckets.iter().enumerate() {
                    merged_buckets[i] = HistogramBucket::new(
                        merged_buckets[i].lower_bound,
                        merged_buckets[i].upper_bound,
                        merged_buckets[i].count + bucket.count,
                    );
                }
            }
        }

        let min_value = states
            .iter()
            .map(|s| s.min_value)
            .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .unwrap_or(0.0);

        let max_value = states
            .iter()
            .map(|s| s.max_value)
            .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .unwrap_or(0.0);

        let total_count = states.iter().map(|s| s.total_count).sum();
        let sum = states.iter().map(|s| s.sum).sum();
        let sum_squared = states.iter().map(|s| s.sum_squared).sum();

        Ok(HistogramState {
            buckets: merged_buckets,
            min_value,
            max_value,
            total_count,
            sum,
            sum_squared,
        })
    }

    fn is_empty(&self) -> bool {
        self.total_count == 0
    }
}

#[async_trait]
impl Analyzer for HistogramAnalyzer {
    type State = HistogramState;
    type Metric = MetricValue;

    #[instrument(skip(ctx), fields(analyzer = "histogram", column = %self.column, buckets = %self.num_buckets))]
    async fn compute_state_from_data(&self, ctx: &SessionContext) -> AnalyzerResult<Self::State> {
        // Get the table name from the validation context
        let validation_ctx = current_validation_context();
        let table_name = validation_ctx.table_name();
        
        // First, get min/max values to determine bucket boundaries
        let stats_sql = format!(
            "SELECT 
                MIN({0}) as min_val, 
                MAX({0}) as max_val,
                COUNT({0}) as count,
                SUM({0}) as sum,
                SUM({0} * {0}) as sum_squared
            FROM {table_name} 
            WHERE {0} IS NOT NULL",
            self.column
        );

        let stats_df = ctx.sql(&stats_sql).await?;
        let stats_batches = stats_df.collect().await?;

        let (min_value, max_value, total_count, sum, sum_squared) = if let Some(batch) =
            stats_batches.first()
        {
            if batch.num_rows() > 0 && !batch.column(0).is_null(0) {
                let min_val = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .ok_or_else(|| AnalyzerError::invalid_data("Expected Float64 for min"))?
                    .value(0);

                let max_val = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .ok_or_else(|| AnalyzerError::invalid_data("Expected Float64 for max"))?
                    .value(0);

                let count = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| AnalyzerError::invalid_data("Expected Int64 for count"))?
                    .value(0) as u64;

                let sum_val = batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .ok_or_else(|| AnalyzerError::invalid_data("Expected Float64 for sum"))?
                    .value(0);

                let sum_sq = batch
                    .column(4)
                    .as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .ok_or_else(|| AnalyzerError::invalid_data("Expected Float64 for sum_squared"))?
                    .value(0);

                (min_val, max_val, count, sum_val, sum_sq)
            } else {
                // No data
                return Ok(HistogramState {
                    buckets: vec![],
                    min_value: 0.0,
                    max_value: 0.0,
                    total_count: 0,
                    sum: 0.0,
                    sum_squared: 0.0,
                });
            }
        } else {
            return Err(AnalyzerError::NoData);
        };

        // Calculate bucket width
        let range = max_value - min_value;
        let bucket_width = if range > 0.0 && self.num_buckets > 1 {
            range / self.num_buckets as f64
        } else {
            1.0
        };

        // Build histogram query using CASE statement since WIDTH_BUCKET is not available
        let mut case_clauses = Vec::new();
        for i in 0..self.num_buckets {
            let lower = min_value + (i as f64 * bucket_width);
            let upper = if i == self.num_buckets - 1 {
                max_value + bucket_width * 0.001
            } else {
                min_value + ((i + 1) as f64 * bucket_width)
            };
            case_clauses.push(format!(
                "WHEN {0} >= {1} AND {0} < {2} THEN {3}",
                self.column,
                lower,
                upper,
                i + 1
            ));
        }

        let histogram_sql = format!(
            "SELECT 
                CASE 
                    {}
                    ELSE {}
                END as bucket_num,
                COUNT(*) as count
            FROM {table_name}
            WHERE {} IS NOT NULL
            GROUP BY bucket_num
            ORDER BY bucket_num",
            case_clauses.join(" "),
            self.num_buckets,
            self.column
        );

        let hist_df = ctx.sql(&histogram_sql).await?;
        let hist_batches = hist_df.collect().await?;

        // Build buckets array
        let mut buckets = vec![HistogramBucket::new(0.0, 0.0, 0); self.num_buckets];

        // Initialize bucket boundaries
        for (i, bucket) in buckets.iter_mut().enumerate() {
            let lower = min_value + (i as f64 * bucket_width);
            let upper = if i == self.num_buckets - 1 {
                max_value + bucket_width * 0.001
            } else {
                min_value + ((i + 1) as f64 * bucket_width)
            };
            *bucket = HistogramBucket::new(lower, upper, 0);
        }

        // Fill in counts from query results
        for batch in &hist_batches {
            let bucket_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| AnalyzerError::invalid_data("Expected Int64 for bucket_num"))?;

            let count_array = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| AnalyzerError::invalid_data("Expected Int64 for count"))?;

            for i in 0..batch.num_rows() {
                let bucket_idx = (bucket_array.value(i) - 1) as usize;
                let count = count_array.value(i) as u64;

                if bucket_idx < buckets.len() {
                    buckets[bucket_idx] = HistogramBucket::new(
                        buckets[bucket_idx].lower_bound,
                        buckets[bucket_idx].upper_bound,
                        count,
                    );
                }
            }
        }

        Ok(HistogramState {
            buckets,
            min_value,
            max_value,
            total_count,
            sum,
            sum_squared,
        })
    }

    fn compute_metric_from_state(&self, state: &Self::State) -> AnalyzerResult<Self::Metric> {
        let distribution = MetricDistribution::from_buckets(state.buckets.clone()).with_stats(
            state.min_value,
            state.max_value,
            state.mean().unwrap_or(0.0),
            state.std_dev().unwrap_or(0.0),
        );

        Ok(MetricValue::Histogram(distribution))
    }

    fn name(&self) -> &str {
        "histogram"
    }

    fn description(&self) -> &str {
        "Computes value distribution histogram"
    }

    fn columns(&self) -> Vec<&str> {
        vec![&self.column]
    }
}
