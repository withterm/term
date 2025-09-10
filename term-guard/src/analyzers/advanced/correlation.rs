//! Correlation analyzer for computing relationships between numeric columns.
//!
//! This module provides analyzers for computing various types of correlations
//! including Pearson, Spearman, and Kendall's tau correlations between pairs
//! of numeric columns.

use crate::analyzers::{Analyzer, AnalyzerError, AnalyzerResult, AnalyzerState, MetricValue};
use crate::security::SqlSecurity;
use arrow::array::{Array, ArrayRef};
use async_trait::async_trait;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tracing::instrument;

/// Types of correlation that can be computed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CorrelationType {
    /// Pearson correlation coefficient (-1 to 1)
    Pearson,
    /// Spearman rank correlation coefficient
    Spearman,
    /// Kendall's tau correlation
    KendallTau,
    /// Covariance
    Covariance,
}

impl CorrelationType {
    /// Returns a human-readable name for this correlation type.
    pub fn name(&self) -> &str {
        match self {
            CorrelationType::Pearson => "Pearson",
            CorrelationType::Spearman => "Spearman",
            CorrelationType::KendallTau => "Kendall's tau",
            CorrelationType::Covariance => "Covariance",
        }
    }
}

/// State for correlation computation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorrelationState {
    /// Number of valid pairs (both values non-null)
    pub n: u64,
    /// Sum of x values
    pub sum_x: f64,
    /// Sum of y values
    pub sum_y: f64,
    /// Sum of x squared
    pub sum_x2: f64,
    /// Sum of y squared
    pub sum_y2: f64,
    /// Sum of x*y
    pub sum_xy: f64,
    /// For Spearman: ranks of x values
    pub x_ranks: Option<Vec<f64>>,
    /// For Spearman: ranks of y values
    pub y_ranks: Option<Vec<f64>>,
    /// The correlation type being computed
    pub correlation_type: CorrelationType,
}

impl AnalyzerState for CorrelationState {
    fn merge(states: Vec<Self>) -> AnalyzerResult<Self>
    where
        Self: Sized,
    {
        if states.is_empty() {
            return Err(AnalyzerError::state_merge("Cannot merge empty states"));
        }

        let first = &states[0];
        let correlation_type = first.correlation_type.clone();

        // For Pearson and Covariance, we can merge by summing
        if matches!(
            correlation_type,
            CorrelationType::Pearson | CorrelationType::Covariance
        ) {
            let mut merged = CorrelationState {
                n: 0,
                sum_x: 0.0,
                sum_y: 0.0,
                sum_x2: 0.0,
                sum_y2: 0.0,
                sum_xy: 0.0,
                x_ranks: None,
                y_ranks: None,
                correlation_type,
            };

            for state in states {
                merged.n += state.n;
                merged.sum_x += state.sum_x;
                merged.sum_y += state.sum_y;
                merged.sum_x2 += state.sum_x2;
                merged.sum_y2 += state.sum_y2;
                merged.sum_xy += state.sum_xy;
            }

            Ok(merged)
        } else {
            // For rank-based correlations, merging is more complex
            // and would require re-ranking combined data
            Err(AnalyzerError::state_merge(
                "Cannot merge rank-based correlation states",
            ))
        }
    }

    fn is_empty(&self) -> bool {
        self.n == 0
    }
}

/// Analyzer for computing correlation between two numeric columns.
///
/// # Example
///
/// ```rust,ignore
/// use term_guard::analyzers::advanced::{CorrelationAnalyzer, CorrelationType};
///
/// let analyzer = CorrelationAnalyzer::new(
///     "height",
///     "weight",
///     CorrelationType::Pearson
/// );
/// ```
#[derive(Debug, Clone)]
pub struct CorrelationAnalyzer {
    /// First column name
    column1: String,
    /// Second column name
    column2: String,
    /// Type of correlation to compute
    correlation_type: CorrelationType,
}

impl CorrelationAnalyzer {
    /// Creates a new correlation analyzer.
    pub fn new(
        column1: impl Into<String>,
        column2: impl Into<String>,
        correlation_type: CorrelationType,
    ) -> Self {
        Self {
            column1: column1.into(),
            column2: column2.into(),
            correlation_type,
        }
    }

    /// Creates a Pearson correlation analyzer.
    pub fn pearson(column1: impl Into<String>, column2: impl Into<String>) -> Self {
        Self::new(column1, column2, CorrelationType::Pearson)
    }

    /// Creates a Spearman correlation analyzer.
    pub fn spearman(column1: impl Into<String>, column2: impl Into<String>) -> Self {
        Self::new(column1, column2, CorrelationType::Spearman)
    }

    /// Creates a covariance analyzer.
    pub fn covariance(column1: impl Into<String>, column2: impl Into<String>) -> Self {
        Self::new(column1, column2, CorrelationType::Covariance)
    }

    /// Computes ranks for Spearman correlation (used in tests).
    #[allow(dead_code)]
    fn compute_ranks(values: &[f64]) -> Vec<f64> {
        let mut indexed: Vec<(usize, f64)> =
            values.iter().enumerate().map(|(i, &v)| (i, v)).collect();
        indexed.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        let mut ranks = vec![0.0; values.len()];
        let mut i = 0;
        while i < indexed.len() {
            let mut j = i;
            // Find all equal values
            while j < indexed.len() && indexed[j].1 == indexed[i].1 {
                j += 1;
            }
            // Assign average rank to all equal values
            let avg_rank = (i + j) as f64 / 2.0 + 0.5;
            for k in i..j {
                ranks[indexed[k].0] = avg_rank;
            }
            i = j;
        }
        ranks
    }

    /// Extracts a numeric value from an Arrow array column, supporting various numeric types.
    fn extract_numeric_value(column: &ArrayRef, field_name: &str) -> AnalyzerResult<f64> {
        // Try different numeric array types that DataFusion might return
        if let Some(arr) = column.as_any().downcast_ref::<arrow::array::Float64Array>() {
            Ok(arr.value(0))
        } else if let Some(arr) = column.as_any().downcast_ref::<arrow::array::Int64Array>() {
            Ok(arr.value(0) as f64)
        } else if let Some(arr) = column.as_any().downcast_ref::<arrow::array::UInt64Array>() {
            Ok(arr.value(0) as f64)
        } else if let Some(arr) = column.as_any().downcast_ref::<arrow::array::Int32Array>() {
            Ok(arr.value(0) as f64)
        } else if let Some(arr) = column.as_any().downcast_ref::<arrow::array::UInt32Array>() {
            Ok(arr.value(0) as f64)
        } else if let Some(arr) = column.as_any().downcast_ref::<arrow::array::Float32Array>() {
            Ok(arr.value(0) as f64)
        } else {
            Err(AnalyzerError::state_computation(format!(
                "Failed to get {field_name}: unsupported array type"
            )))
        }
    }
}

#[async_trait]
impl Analyzer for CorrelationAnalyzer {
    type State = CorrelationState;
    type Metric = MetricValue;

    #[instrument(skip(self, ctx), fields(
        column1 = %self.column1,
        column2 = %self.column2,
        correlation_type = ?self.correlation_type
    ))]
    async fn compute_state_from_data(&self, ctx: &SessionContext) -> AnalyzerResult<Self::State> {
        match self.correlation_type {
            CorrelationType::Pearson | CorrelationType::Covariance => {
                // Validate and escape column identifiers to prevent SQL injection
                let col1_escaped = SqlSecurity::escape_identifier(&self.column1).map_err(|e| {
                    AnalyzerError::state_computation(format!("Invalid column1 name: {e}"))
                })?;
                let col2_escaped = SqlSecurity::escape_identifier(&self.column2).map_err(|e| {
                    AnalyzerError::state_computation(format!("Invalid column2 name: {e}"))
                })?;

                // Compute sums for Pearson correlation or covariance using escaped identifiers
                let sql = format!(
                    "SELECT 
                        COUNT(*) as n,
                        SUM(CAST({col1_escaped} AS DOUBLE)) as sum_x,
                        SUM(CAST({col2_escaped} AS DOUBLE)) as sum_y,
                        SUM(CAST({col1_escaped} AS DOUBLE) * CAST({col1_escaped} AS DOUBLE)) as sum_x2,
                        SUM(CAST({col2_escaped} AS DOUBLE) * CAST({col2_escaped} AS DOUBLE)) as sum_y2,
                        SUM(CAST({col1_escaped} AS DOUBLE) * CAST({col2_escaped} AS DOUBLE)) as sum_xy
                    FROM data
                    WHERE {col1_escaped} IS NOT NULL AND {col2_escaped} IS NOT NULL"
                );

                let df = ctx.sql(&sql).await?;
                let batches = df.collect().await?;

                if batches.is_empty() || batches[0].num_rows() == 0 {
                    return Ok(CorrelationState {
                        n: 0,
                        sum_x: 0.0,
                        sum_y: 0.0,
                        sum_x2: 0.0,
                        sum_y2: 0.0,
                        sum_xy: 0.0,
                        x_ranks: None,
                        y_ranks: None,
                        correlation_type: self.correlation_type.clone(),
                    });
                }

                let batch = &batches[0];
                let n = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| AnalyzerError::state_computation("Failed to get count"))?
                    .value(0) as u64;

                let sum_x = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .ok_or_else(|| AnalyzerError::state_computation("Failed to get sum_x"))?
                    .value(0);

                let sum_y = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .ok_or_else(|| AnalyzerError::state_computation("Failed to get sum_y"))?
                    .value(0);

                let sum_x2 = batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .ok_or_else(|| AnalyzerError::state_computation("Failed to get sum_x2"))?
                    .value(0);

                let sum_y2 = batch
                    .column(4)
                    .as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .ok_or_else(|| AnalyzerError::state_computation("Failed to get sum_y2"))?
                    .value(0);

                let sum_xy = batch
                    .column(5)
                    .as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .ok_or_else(|| AnalyzerError::state_computation("Failed to get sum_xy"))?
                    .value(0);

                Ok(CorrelationState {
                    n,
                    sum_x,
                    sum_y,
                    sum_x2,
                    sum_y2,
                    sum_xy,
                    x_ranks: None,
                    y_ranks: None,
                    correlation_type: self.correlation_type.clone(),
                })
            }
            CorrelationType::Spearman => {
                // Validate and escape column identifiers to prevent SQL injection
                let col1_escaped = SqlSecurity::escape_identifier(&self.column1).map_err(|e| {
                    AnalyzerError::state_computation(format!("Invalid column1 name: {e}"))
                })?;
                let col2_escaped = SqlSecurity::escape_identifier(&self.column2).map_err(|e| {
                    AnalyzerError::state_computation(format!("Invalid column2 name: {e}"))
                })?;

                // For Spearman, compute ranks directly in SQL for memory efficiency
                // This avoids loading all data into memory and is much faster for large datasets
                let sql = format!(
                    "WITH ranked AS (
                        SELECT 
                            RANK() OVER (ORDER BY CAST({col1_escaped} AS DOUBLE)) as rank_x,
                            RANK() OVER (ORDER BY CAST({col2_escaped} AS DOUBLE)) as rank_y
                        FROM data
                        WHERE {col1_escaped} IS NOT NULL AND {col2_escaped} IS NOT NULL
                    )
                    SELECT 
                        COUNT(*) as n,
                        SUM(rank_x) as sum_x,
                        SUM(rank_y) as sum_y,
                        SUM(rank_x * rank_x) as sum_x2,
                        SUM(rank_y * rank_y) as sum_y2,
                        SUM(rank_x * rank_y) as sum_xy
                    FROM ranked"
                );

                let df = ctx.sql(&sql).await?;
                let batches = df.collect().await?;

                if batches.is_empty() || batches[0].num_rows() == 0 {
                    return Ok(CorrelationState {
                        n: 0,
                        sum_x: 0.0,
                        sum_y: 0.0,
                        sum_x2: 0.0,
                        sum_y2: 0.0,
                        sum_xy: 0.0,
                        x_ranks: None, // No need to store ranks when computed in SQL
                        y_ranks: None,
                        correlation_type: self.correlation_type.clone(),
                    });
                }

                let batch = &batches[0];

                // Handle count which should be Int64
                let n = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| AnalyzerError::state_computation("Failed to get count"))?
                    .value(0) as u64;

                // Extract numeric values with support for various Arrow types that DataFusion might return
                let sum_x = Self::extract_numeric_value(batch.column(1), "sum_x")?;

                let sum_y = Self::extract_numeric_value(batch.column(2), "sum_y")?;
                let sum_x2 = Self::extract_numeric_value(batch.column(3), "sum_x2")?;
                let sum_y2 = Self::extract_numeric_value(batch.column(4), "sum_y2")?;
                let sum_xy = Self::extract_numeric_value(batch.column(5), "sum_xy")?;

                Ok(CorrelationState {
                    n,
                    sum_x,
                    sum_y,
                    sum_x2,
                    sum_y2,
                    sum_xy,
                    x_ranks: None, // No need to store individual ranks with SQL-based computation
                    y_ranks: None,
                    correlation_type: self.correlation_type.clone(),
                })
            }
            CorrelationType::KendallTau => {
                // Kendall's tau requires pairwise comparisons
                // This is a simplified implementation
                Err(AnalyzerError::custom("Kendall's tau not yet implemented"))
            }
        }
    }

    fn compute_metric_from_state(&self, state: &Self::State) -> AnalyzerResult<Self::Metric> {
        if state.n < 2 {
            return Ok(MetricValue::Double(f64::NAN));
        }

        let n = state.n as f64;

        match state.correlation_type {
            CorrelationType::Pearson | CorrelationType::Spearman => {
                // Pearson correlation formula (same for Spearman on ranks)
                let numerator = n * state.sum_xy - state.sum_x * state.sum_y;
                let denominator = ((n * state.sum_x2 - state.sum_x * state.sum_x)
                    * (n * state.sum_y2 - state.sum_y * state.sum_y))
                    .sqrt();

                if denominator == 0.0 {
                    Ok(MetricValue::Double(0.0))
                } else {
                    Ok(MetricValue::Double(numerator / denominator))
                }
            }
            CorrelationType::Covariance => {
                // Sample covariance
                let covariance = (state.sum_xy - (state.sum_x * state.sum_y) / n) / (n - 1.0);
                Ok(MetricValue::Double(covariance))
            }
            CorrelationType::KendallTau => Ok(MetricValue::Double(f64::NAN)),
        }
    }

    fn name(&self) -> &str {
        "correlation"
    }

    fn description(&self) -> &str {
        "Computes correlation between two numeric columns"
    }

    fn metric_key(&self) -> String {
        format!(
            "correlation_{}_{}_{}",
            self.correlation_type.name().to_lowercase(),
            self.column1,
            self.column2
        )
    }

    fn columns(&self) -> Vec<&str> {
        vec![&self.column1, &self.column2]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Float64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use std::sync::Arc;

    async fn create_test_context() -> SessionContext {
        let ctx = SessionContext::new();

        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Float64, true),
            Field::new("y", DataType::Float64, true),
        ]));

        // Create perfectly correlated data: y = 2x + 1
        let x_values: Vec<Option<f64>> = (0..100).map(|i| Some(i as f64)).collect();
        let y_values: Vec<Option<f64>> =
            x_values.iter().map(|x| x.map(|v| 2.0 * v + 1.0)).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(x_values)),
                Arc::new(Float64Array::from(y_values)),
            ],
        )
        .unwrap();

        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("data", Arc::new(provider)).unwrap();

        ctx
    }

    #[tokio::test]
    async fn test_pearson_correlation_perfect() {
        let ctx = create_test_context().await;
        let analyzer = CorrelationAnalyzer::pearson("x", "y");

        let state = analyzer.compute_state_from_data(&ctx).await.unwrap();
        let metric = analyzer.compute_metric_from_state(&state).unwrap();

        if let MetricValue::Double(corr) = metric {
            assert!((corr - 1.0).abs() < 0.0001, "Expected perfect correlation");
        } else {
            panic!("Expected Double metric");
        }
    }

    #[tokio::test]
    async fn test_covariance() {
        let ctx = create_test_context().await;
        let analyzer = CorrelationAnalyzer::covariance("x", "y");

        let state = analyzer.compute_state_from_data(&ctx).await.unwrap();
        let metric = analyzer.compute_metric_from_state(&state).unwrap();

        if let MetricValue::Double(cov) = metric {
            // For y = 2x + 1, covariance should be 2 * Var(x)
            // Var(x) for 0..99 is approximately 833.25
            assert!(
                cov > 1600.0 && cov < 1700.0,
                "Expected covariance around 1666"
            );
        } else {
            panic!("Expected Double metric");
        }
    }

    #[tokio::test]
    async fn test_spearman_correlation() {
        let ctx = create_test_context().await;
        let analyzer = CorrelationAnalyzer::spearman("x", "y");

        let state = analyzer.compute_state_from_data(&ctx).await.unwrap();
        let metric = analyzer.compute_metric_from_state(&state).unwrap();

        if let MetricValue::Double(corr) = metric {
            // For monotonic relationship, Spearman should be 1.0
            assert!(
                (corr - 1.0).abs() < 0.0001,
                "Expected perfect rank correlation"
            );
        } else {
            panic!("Expected Double metric");
        }
    }

    #[test]
    fn test_compute_ranks() {
        let values = vec![3.0, 1.0, 4.0, 1.0, 5.0];
        let ranks = CorrelationAnalyzer::compute_ranks(&values);

        // Expected ranks: [3.0, 1.5, 4.0, 1.5, 5.0]
        assert_eq!(ranks[0], 3.0);
        assert_eq!(ranks[1], 1.5); // Tied for rank 1 and 2
        assert_eq!(ranks[2], 4.0);
        assert_eq!(ranks[3], 1.5); // Tied for rank 1 and 2
        assert_eq!(ranks[4], 5.0);
    }
}
