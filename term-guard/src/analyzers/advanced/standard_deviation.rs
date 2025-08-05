//! Standard deviation analyzer for measuring data spread.

use arrow::array::Array;
use async_trait::async_trait;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::analyzers::{Analyzer, AnalyzerError, AnalyzerResult, AnalyzerState, MetricValue};

use crate::core::current_validation_context;
/// Analyzer that computes standard deviation and variance.
///
/// This analyzer calculates both population and sample standard deviation,
/// providing insights into data variability and spread. It uses numerically
/// stable algorithms to avoid precision loss with large datasets.
///
/// # Example
///
/// ```rust,ignore
/// use term_guard::analyzers::advanced::StandardDeviationAnalyzer;
/// use datafusion::prelude::*;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let ctx = SessionContext::new();
/// // Register your data table
///
/// let analyzer = StandardDeviationAnalyzer::new("temperature");
/// let state = analyzer.compute_state_from_data(&ctx).await?;
/// let metric = analyzer.compute_metric_from_state(&state)?;
///
/// if let MetricValue::Map(stats) = metric {
///     println!("Temperature statistics:");
///     println!("  Standard Deviation: {:?}", stats.get("std_dev"));
///     println!("  Variance: {:?}", stats.get("variance"));
///     println!("  Sample Std Dev: {:?}", stats.get("sample_std_dev"));
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct StandardDeviationAnalyzer {
    /// The column to analyze.
    column: String,
}

impl StandardDeviationAnalyzer {
    /// Creates a new standard deviation analyzer for the specified column.
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

/// State for the standard deviation analyzer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StandardDeviationState {
    /// Count of non-null values.
    pub count: u64,
    /// Sum of values.
    pub sum: f64,
    /// Sum of squared values.
    pub sum_squared: f64,
    /// Mean value.
    pub mean: f64,
}

impl StandardDeviationState {
    /// Calculates the population standard deviation.
    pub fn population_std_dev(&self) -> Option<f64> {
        if self.count == 0 {
            None
        } else {
            let variance = self.population_variance()?;
            Some(variance.sqrt())
        }
    }

    /// Calculates the sample standard deviation.
    pub fn sample_std_dev(&self) -> Option<f64> {
        if self.count <= 1 {
            None
        } else {
            let variance = self.sample_variance()?;
            Some(variance.sqrt())
        }
    }

    /// Calculates the population variance.
    pub fn population_variance(&self) -> Option<f64> {
        if self.count == 0 {
            None
        } else {
            // Var(X) = E[X²] - E[X]²
            let mean_of_squares = self.sum_squared / self.count as f64;
            let variance = mean_of_squares - (self.mean * self.mean);
            // Ensure non-negative due to floating point precision
            Some(variance.max(0.0))
        }
    }

    /// Calculates the sample variance.
    pub fn sample_variance(&self) -> Option<f64> {
        if self.count <= 1 {
            None
        } else {
            // Sample variance uses n-1 in denominator (Bessel's correction)
            let sum_of_squared_deviations =
                self.sum_squared - (self.sum * self.sum / self.count as f64);
            let variance = sum_of_squared_deviations / (self.count - 1) as f64;
            Some(variance.max(0.0))
        }
    }

    /// Calculates the coefficient of variation (CV).
    pub fn coefficient_of_variation(&self) -> Option<f64> {
        let std_dev = self.population_std_dev()?;
        if self.mean.abs() < f64::EPSILON {
            None
        } else {
            Some(std_dev / self.mean.abs())
        }
    }
}

impl AnalyzerState for StandardDeviationState {
    fn merge(states: Vec<Self>) -> AnalyzerResult<Self> {
        if states.is_empty() {
            return Err(AnalyzerError::state_merge("No states to merge"));
        }

        let count: u64 = states.iter().map(|s| s.count).sum();
        let sum: f64 = states.iter().map(|s| s.sum).sum();
        let sum_squared: f64 = states.iter().map(|s| s.sum_squared).sum();

        let mean = if count > 0 { sum / count as f64 } else { 0.0 };

        Ok(StandardDeviationState {
            count,
            sum,
            sum_squared,
            mean,
        })
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }
}

#[async_trait]
impl Analyzer for StandardDeviationAnalyzer {
    type State = StandardDeviationState;
    type Metric = MetricValue;

    #[instrument(skip(ctx), fields(analyzer = "standard_deviation", column = %self.column))]
    async fn compute_state_from_data(&self, ctx: &SessionContext) -> AnalyzerResult<Self::State> {
        // Build SQL query to compute statistics
        // Get the table name from the validation context

        let validation_ctx = current_validation_context();

        let table_name = validation_ctx.table_name();

        

        let sql = format!(
            "SELECT 
                COUNT({0}) as count,
                AVG({0}) as mean,
                SUM({0}) as sum,
                SUM({0} * {0}) as sum_squared
            FROM {table_name} 
            WHERE {0} IS NOT NULL",
            self.column
        );

        // Execute query
        let df = ctx.sql(&sql).await?;
        let batches = df.collect().await?;

        // Extract statistics from result
        let (count, mean, sum, sum_squared) = if let Some(batch) = batches.first() {
            if batch.num_rows() > 0 && !batch.column(0).is_null(0) {
                let count_array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| AnalyzerError::invalid_data("Expected Int64 for count"))?;
                let count = count_array.value(0) as u64;

                if count == 0 {
                    (0, 0.0, 0.0, 0.0)
                } else {
                    let mean_array = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<arrow::array::Float64Array>()
                        .ok_or_else(|| AnalyzerError::invalid_data("Expected Float64 for mean"))?;
                    let mean = mean_array.value(0);

                    let sum_array = batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<arrow::array::Float64Array>()
                        .ok_or_else(|| AnalyzerError::invalid_data("Expected Float64 for sum"))?;
                    let sum = sum_array.value(0);

                    let sum_squared_array = batch
                        .column(3)
                        .as_any()
                        .downcast_ref::<arrow::array::Float64Array>()
                        .ok_or_else(|| {
                            AnalyzerError::invalid_data("Expected Float64 for sum_squared")
                        })?;
                    let sum_squared = sum_squared_array.value(0);

                    (count, mean, sum, sum_squared)
                }
            } else {
                (0, 0.0, 0.0, 0.0)
            }
        } else {
            return Err(AnalyzerError::NoData);
        };

        Ok(StandardDeviationState {
            count,
            sum,
            sum_squared,
            mean,
        })
    }

    fn compute_metric_from_state(&self, state: &Self::State) -> AnalyzerResult<Self::Metric> {
        use std::collections::HashMap;

        let mut stats = HashMap::new();

        // Add basic statistics
        stats.insert("count".to_string(), MetricValue::Long(state.count as i64));
        stats.insert("mean".to_string(), MetricValue::Double(state.mean));

        // Add standard deviations and variances
        if let Some(pop_std_dev) = state.population_std_dev() {
            stats.insert("std_dev".to_string(), MetricValue::Double(pop_std_dev));
        }

        if let Some(sample_std_dev) = state.sample_std_dev() {
            stats.insert(
                "sample_std_dev".to_string(),
                MetricValue::Double(sample_std_dev),
            );
        }

        if let Some(pop_variance) = state.population_variance() {
            stats.insert("variance".to_string(), MetricValue::Double(pop_variance));
        }

        if let Some(sample_variance) = state.sample_variance() {
            stats.insert(
                "sample_variance".to_string(),
                MetricValue::Double(sample_variance),
            );
        }

        if let Some(cv) = state.coefficient_of_variation() {
            stats.insert(
                "coefficient_of_variation".to_string(),
                MetricValue::Double(cv),
            );
        }

        Ok(MetricValue::Map(stats))
    }

    fn name(&self) -> &str {
        "standard_deviation"
    }

    fn description(&self) -> &str {
        "Computes standard deviation and variance metrics"
    }

    fn columns(&self) -> Vec<&str> {
        vec![&self.column]
    }
}
