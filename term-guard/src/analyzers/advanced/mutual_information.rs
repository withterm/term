//! Mutual information analyzer for measuring statistical dependence between columns.
//!
//! This module provides an analyzer for computing mutual information between pairs
//! of columns, which measures the amount of information obtained about one variable
//! through observing another variable.

use crate::analyzers::{Analyzer, AnalyzerError, AnalyzerResult, AnalyzerState, MetricValue};
use arrow::array::{Array, StringViewArray};
use async_trait::async_trait;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use tracing::instrument;

/// State for mutual information computation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MutualInformationState {
    /// Total number of observations
    pub n: u64,
    /// Joint frequency distribution (binned values)
    pub joint_counts: HashMap<(String, String), u64>,
    /// Marginal counts for first variable
    pub x_counts: HashMap<String, u64>,
    /// Marginal counts for second variable
    pub y_counts: HashMap<String, u64>,
    /// Number of bins used for discretization
    pub bins: usize,
}

impl AnalyzerState for MutualInformationState {
    fn merge(states: Vec<Self>) -> AnalyzerResult<Self>
    where
        Self: Sized,
    {
        if states.is_empty() {
            return Err(AnalyzerError::state_merge("Cannot merge empty states"));
        }

        let bins = states[0].bins;
        let mut merged = MutualInformationState {
            n: 0,
            joint_counts: HashMap::new(),
            x_counts: HashMap::new(),
            y_counts: HashMap::new(),
            bins,
        };

        for state in states {
            if state.bins != bins {
                return Err(AnalyzerError::state_merge(
                    "Cannot merge states with different bin counts",
                ));
            }

            merged.n += state.n;

            // Merge joint counts
            for ((x, y), count) in state.joint_counts {
                *merged.joint_counts.entry((x, y)).or_insert(0) += count;
            }

            // Merge x marginal counts
            for (x, count) in state.x_counts {
                *merged.x_counts.entry(x).or_insert(0) += count;
            }

            // Merge y marginal counts
            for (y, count) in state.y_counts {
                *merged.y_counts.entry(y).or_insert(0) += count;
            }
        }

        Ok(merged)
    }

    fn is_empty(&self) -> bool {
        self.n == 0
    }
}

/// Analyzer for computing mutual information between two columns.
///
/// Mutual information measures the statistical dependence between two variables
/// and is zero if and only if the variables are independent.
///
/// # Example
///
/// ```rust,ignore
/// use term_guard::analyzers::advanced::MutualInformationAnalyzer;
///
/// let analyzer = MutualInformationAnalyzer::new(
///     "category",
///     "price",
///     10  // number of bins for continuous variables
/// );
/// ```
#[derive(Debug, Clone)]
pub struct MutualInformationAnalyzer {
    /// First column name
    column1: String,
    /// Second column name
    column2: String,
    /// Number of bins for discretization (for numeric columns)
    bins: usize,
}

impl MutualInformationAnalyzer {
    /// Creates a new mutual information analyzer.
    ///
    /// # Arguments
    ///
    /// * `column1` - First column name
    /// * `column2` - Second column name
    /// * `bins` - Number of bins for discretizing continuous variables
    pub fn new(column1: impl Into<String>, column2: impl Into<String>, bins: usize) -> Self {
        Self {
            column1: column1.into(),
            column2: column2.into(),
            bins: bins.max(2), // Ensure at least 2 bins
        }
    }

    /// Creates a mutual information analyzer with default bin count (10).
    pub fn with_default_bins(column1: impl Into<String>, column2: impl Into<String>) -> Self {
        Self::new(column1, column2, 10)
    }
}

#[async_trait]
impl Analyzer for MutualInformationAnalyzer {
    type State = MutualInformationState;
    type Metric = MetricValue;

    #[instrument(skip(self, ctx), fields(
        column1 = %self.column1,
        column2 = %self.column2,
        bins = %self.bins
    ))]
    async fn compute_state_from_data(&self, ctx: &SessionContext) -> AnalyzerResult<Self::State> {
        // First, try to get min/max for binning numeric columns
        // Use TRY_CAST to handle non-numeric columns gracefully
        let stats_sql = format!(
            "SELECT 
                MIN(TRY_CAST({} AS DOUBLE)) as x_min,
                MAX(TRY_CAST({} AS DOUBLE)) as x_max,
                MIN(TRY_CAST({} AS DOUBLE)) as y_min,
                MAX(TRY_CAST({} AS DOUBLE)) as y_max
            FROM data
            WHERE {} IS NOT NULL AND {} IS NOT NULL",
            self.column1, self.column1, self.column2, self.column2, self.column1, self.column2
        );

        let stats_df = ctx.sql(&stats_sql).await?;
        let stats_batches = stats_df.collect().await?;

        if stats_batches.is_empty() || stats_batches[0].num_rows() == 0 {
            return Ok(MutualInformationState {
                n: 0,
                joint_counts: HashMap::new(),
                x_counts: HashMap::new(),
                y_counts: HashMap::new(),
                bins: self.bins,
            });
        }

        let stats_batch = &stats_batches[0];

        // Extract min/max values
        let x_min = stats_batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .and_then(|arr| {
                if arr.is_null(0) {
                    None
                } else {
                    Some(arr.value(0))
                }
            });

        let x_max = stats_batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .and_then(|arr| {
                if arr.is_null(0) {
                    None
                } else {
                    Some(arr.value(0))
                }
            });

        let y_min = stats_batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .and_then(|arr| {
                if arr.is_null(0) {
                    None
                } else {
                    Some(arr.value(0))
                }
            });

        let y_max = stats_batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .and_then(|arr| {
                if arr.is_null(0) {
                    None
                } else {
                    Some(arr.value(0))
                }
            });

        // Build SQL with binning logic
        let binned_sql = if let (Some(x_min), Some(x_max)) = (x_min, x_max) {
            let x_range = x_max - x_min;
            let x_bin_width = if x_range > 0.0 {
                x_range / self.bins as f64
            } else {
                1.0
            };

            if let (Some(y_min), Some(y_max)) = (y_min, y_max) {
                let y_range = y_max - y_min;
                let y_bin_width = if y_range > 0.0 {
                    y_range / self.bins as f64
                } else {
                    1.0
                };

                // Both columns are numeric - bin them
                format!(
                    "SELECT 
                        CAST(FLOOR((TRY_CAST({} AS DOUBLE) - {x_min}) / {x_bin_width}) AS VARCHAR) as x_bin,
                        CAST(FLOOR((TRY_CAST({} AS DOUBLE) - {y_min}) / {y_bin_width}) AS VARCHAR) as y_bin,
                        COUNT(*) as count
                    FROM data
                    WHERE {} IS NOT NULL AND {} IS NOT NULL
                    GROUP BY x_bin, y_bin",
                    self.column1,
                    self.column2,
                    self.column1,
                    self.column2
                )
            } else {
                // X is numeric, Y is categorical
                format!(
                    "SELECT 
                        CAST(FLOOR((TRY_CAST({} AS DOUBLE) - {x_min}) / {x_bin_width}) AS VARCHAR) as x_bin,
                        CAST({} AS VARCHAR) as y_bin,
                        COUNT(*) as count
                    FROM data
                    WHERE {} IS NOT NULL AND {} IS NOT NULL
                    GROUP BY x_bin, y_bin",
                    self.column1, self.column2, self.column1, self.column2
                )
            }
        } else if let (Some(y_min), Some(y_max)) = (y_min, y_max) {
            let y_range = y_max - y_min;
            let y_bin_width = if y_range > 0.0 {
                y_range / self.bins as f64
            } else {
                1.0
            };

            // X is categorical, Y is numeric
            format!(
                "SELECT 
                    CAST({} AS VARCHAR) as x_bin,
                    CAST(FLOOR((TRY_CAST({} AS DOUBLE) - {y_min}) / {y_bin_width}) AS VARCHAR) as y_bin,
                    COUNT(*) as count
                FROM data
                WHERE {} IS NOT NULL AND {} IS NOT NULL
                GROUP BY x_bin, y_bin",
                self.column1, self.column2, self.column1, self.column2
            )
        } else {
            // Both columns are categorical
            format!(
                "SELECT 
                    CAST({} AS VARCHAR) as x_bin,
                    CAST({} AS VARCHAR) as y_bin,
                    COUNT(*) as count
                FROM data
                WHERE {} IS NOT NULL AND {} IS NOT NULL
                GROUP BY x_bin, y_bin",
                self.column1, self.column2, self.column1, self.column2
            )
        };

        let df = ctx.sql(&binned_sql).await?;
        let batches = df.collect().await?;

        let mut joint_counts = HashMap::new();
        let mut x_counts = HashMap::new();
        let mut y_counts = HashMap::new();
        let mut n = 0u64;

        for batch in &batches {
            // Extract string values from columns - handle both String and LargeString types
            let x_column = batch.column(0);
            let y_column = batch.column(1);

            let count_array = batch
                .column(2)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| AnalyzerError::state_computation("Failed to get counts"))?;

            for i in 0..batch.num_rows() {
                if !x_column.is_null(i) && !y_column.is_null(i) {
                    // Extract string values based on the actual type
                    let x_bin = if let Some(x_array) = x_column
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                    {
                        x_array.value(i).to_string()
                    } else if let Some(x_array) = x_column
                        .as_any()
                        .downcast_ref::<arrow::array::LargeStringArray>()
                    {
                        x_array.value(i).to_string()
                    } else if let Some(x_array) =
                        x_column.as_any().downcast_ref::<StringViewArray>()
                    {
                        x_array.value(i).to_string()
                    } else {
                        return Err(AnalyzerError::state_computation(format!(
                            "Unsupported x column type: {:?}",
                            x_column.data_type()
                        )));
                    };

                    let y_bin = if let Some(y_array) = y_column
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                    {
                        y_array.value(i).to_string()
                    } else if let Some(y_array) = y_column
                        .as_any()
                        .downcast_ref::<arrow::array::LargeStringArray>()
                    {
                        y_array.value(i).to_string()
                    } else if let Some(y_array) =
                        y_column.as_any().downcast_ref::<StringViewArray>()
                    {
                        y_array.value(i).to_string()
                    } else {
                        return Err(AnalyzerError::state_computation(format!(
                            "Unsupported y column type: {:?}",
                            y_column.data_type()
                        )));
                    };

                    let count = count_array.value(i) as u64;

                    n += count;
                    joint_counts.insert((x_bin.clone(), y_bin.clone()), count);
                    *x_counts.entry(x_bin).or_insert(0) += count;
                    *y_counts.entry(y_bin).or_insert(0) += count;
                }
            }
        }

        Ok(MutualInformationState {
            n,
            joint_counts,
            x_counts,
            y_counts,
            bins: self.bins,
        })
    }

    fn compute_metric_from_state(&self, state: &Self::State) -> AnalyzerResult<Self::Metric> {
        if state.n == 0 {
            return Ok(MetricValue::Double(0.0));
        }

        let n = state.n as f64;
        let mut mutual_information = 0.0;

        // Compute mutual information: I(X;Y) = Σ p(x,y) * log(p(x,y) / (p(x) * p(y)))
        for ((x, y), joint_count) in &state.joint_counts {
            let p_xy = *joint_count as f64 / n;

            let x_count = state.x_counts.get(x).unwrap_or(&0);
            let y_count = state.y_counts.get(y).unwrap_or(&0);

            if *x_count > 0 && *y_count > 0 {
                let p_x = *x_count as f64 / n;
                let p_y = *y_count as f64 / n;

                if p_xy > 0.0 {
                    mutual_information += p_xy * (p_xy / (p_x * p_y)).ln();
                }
            }
        }

        // Convert from natural log to bits (divide by ln(2))
        mutual_information /= std::f64::consts::LN_2;

        Ok(MetricValue::Double(mutual_information))
    }

    fn name(&self) -> &str {
        "mutual_information"
    }

    fn description(&self) -> &str {
        "Computes mutual information between two columns"
    }

    fn metric_key(&self) -> String {
        format!("mutual_information_{}_{}", self.column1, self.column2)
    }

    fn columns(&self) -> Vec<&str> {
        vec![&self.column1, &self.column2]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use std::sync::Arc;

    async fn create_test_context_independent() -> SessionContext {
        let ctx = SessionContext::new();

        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Float64, true),
            Field::new("y", DataType::Float64, true),
        ]));

        // Create independent random-like data
        let mut x_values = Vec::new();
        let mut y_values = Vec::new();

        for i in 0..100 {
            x_values.push(Some(i as f64));
            // Use a pseudo-random pattern that's uncorrelated with x
            y_values.push(Some(((i * 37 + 13) % 100) as f64));
        }

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

    async fn create_test_context_dependent() -> SessionContext {
        let ctx = SessionContext::new();

        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Float64, true),
            Field::new("y", DataType::Float64, true),
        ]));

        // Create perfectly dependent data: y = f(x)
        let mut x_values = Vec::new();
        let mut y_values = Vec::new();

        for i in 0..100 {
            let x = i as f64;
            x_values.push(Some(x));
            // Make y a deterministic function of x
            y_values.push(Some(x * 2.0));
        }

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

    async fn create_test_context_categorical() -> SessionContext {
        let ctx = SessionContext::new();

        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, true),
            Field::new("value", DataType::Utf8, true),
        ]));

        // Create categorical data with some dependence
        let mut categories = Vec::new();
        let mut values = Vec::new();

        for i in 0..100 {
            let category = if i < 50 { "A" } else { "B" };
            let value = if i < 25 || (50..75).contains(&i) {
                "High"
            } else {
                "Low"
            };
            categories.push(Some(category));
            values.push(Some(value));
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(categories)),
                Arc::new(StringArray::from(values)),
            ],
        )
        .unwrap();

        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("data", Arc::new(provider)).unwrap();

        ctx
    }

    #[tokio::test]
    async fn test_mutual_information_independent() {
        let ctx = create_test_context_independent().await;
        let analyzer = MutualInformationAnalyzer::new("x", "y", 5);

        let state = analyzer.compute_state_from_data(&ctx).await.unwrap();
        let metric = analyzer.compute_metric_from_state(&state).unwrap();

        if let MetricValue::Double(mi) = metric {
            // Independent variables should have low mutual information
            assert!(
                mi < 0.5,
                "Expected low mutual information for independent variables, got {mi}"
            );
        } else {
            panic!("Expected Double metric");
        }
    }

    #[tokio::test]
    async fn test_mutual_information_dependent() {
        let ctx = create_test_context_dependent().await;
        let analyzer = MutualInformationAnalyzer::new("x", "y", 5);

        let state = analyzer.compute_state_from_data(&ctx).await.unwrap();
        let metric = analyzer.compute_metric_from_state(&state).unwrap();

        if let MetricValue::Double(mi) = metric {
            // Perfectly dependent variables should have high mutual information
            // With 5 bins, we expect MI close to log2(5) ≈ 2.32
            assert!(
                mi > 1.5,
                "Expected high mutual information for dependent variables, got {mi}"
            );
        } else {
            panic!("Expected Double metric");
        }
    }

    #[tokio::test]
    async fn test_mutual_information_categorical() {
        let ctx = create_test_context_categorical().await;
        let analyzer = MutualInformationAnalyzer::new("category", "value", 10);

        let state = analyzer.compute_state_from_data(&ctx).await.unwrap();
        let metric = analyzer.compute_metric_from_state(&state).unwrap();

        if let MetricValue::Double(mi) = metric {
            // Should have zero MI since distribution is same for both categories
            assert!(
                mi < 0.01,
                "Expected near-zero MI for independent categorical variables, got {mi}"
            );
        } else {
            panic!("Expected Double metric");
        }
    }

    #[tokio::test]
    async fn test_state_merge() {
        let state1 = MutualInformationState {
            n: 50,
            joint_counts: vec![(("0".to_string(), "0".to_string()), 10)]
                .into_iter()
                .collect(),
            x_counts: vec![("0".to_string(), 10)].into_iter().collect(),
            y_counts: vec![("0".to_string(), 10)].into_iter().collect(),
            bins: 5,
        };

        let state2 = MutualInformationState {
            n: 50,
            joint_counts: vec![(("0".to_string(), "0".to_string()), 15)]
                .into_iter()
                .collect(),
            x_counts: vec![("0".to_string(), 15)].into_iter().collect(),
            y_counts: vec![("0".to_string(), 15)].into_iter().collect(),
            bins: 5,
        };

        let merged = MutualInformationState::merge(vec![state1, state2]).unwrap();

        assert_eq!(merged.n, 100);
        assert_eq!(
            merged.joint_counts.get(&("0".to_string(), "0".to_string())),
            Some(&25)
        );
        assert_eq!(merged.x_counts.get("0"), Some(&25));
        assert_eq!(merged.y_counts.get("0"), Some(&25));
    }
}
