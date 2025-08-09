//! Data type analyzer for inferring and validating column data types.

use arrow::array::Array;
use async_trait::async_trait;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::instrument;

use crate::analyzers::{Analyzer, AnalyzerError, AnalyzerResult, AnalyzerState, MetricValue};
use crate::core::current_validation_context;

/// Analyzer that infers data types and detects type inconsistencies.
///
/// This analyzer examines column values to determine their actual data types,
/// which is useful for detecting schema drift or validating data type assumptions.
///
/// # Example
///
/// ```rust,ignore
/// use term_guard::analyzers::advanced::DataTypeAnalyzer;
/// use datafusion::prelude::*;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let ctx = SessionContext::new();
/// // Register your data table
///
/// let analyzer = DataTypeAnalyzer::new("user_input");
/// let state = analyzer.compute_state_from_data(&ctx).await?;
/// let metric = analyzer.compute_metric_from_state(&state)?;
///
/// if let MetricValue::Map(type_distribution) = metric {
///     for (data_type, count) in type_distribution {
///         println!("{}: {}", data_type, count);
///     }
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct DataTypeAnalyzer {
    /// The column to analyze.
    column: String,
}

impl DataTypeAnalyzer {
    /// Creates a new data type analyzer for the specified column.
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

/// State for the data type analyzer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataTypeState {
    /// Count of values by inferred data type.
    pub type_counts: HashMap<String, u64>,
    /// Total number of non-null values analyzed.
    pub total_count: u64,
}

impl DataTypeState {
    /// Returns the most common data type.
    pub fn dominant_type(&self) -> Option<(&str, f64)> {
        self.type_counts
            .iter()
            .max_by_key(|(_, count)| *count)
            .map(|(dtype, count)| {
                let fraction = *count as f64 / self.total_count as f64;
                (dtype.as_str(), fraction)
            })
    }

    /// Calculates the type consistency (fraction of values matching dominant type).
    pub fn type_consistency(&self) -> f64 {
        if self.total_count == 0 {
            1.0
        } else if let Some((_, fraction)) = self.dominant_type() {
            fraction
        } else {
            0.0
        }
    }
}

impl AnalyzerState for DataTypeState {
    fn merge(states: Vec<Self>) -> AnalyzerResult<Self> {
        let mut merged_counts = HashMap::new();
        let mut total_count = 0;

        for state in states {
            total_count += state.total_count;
            for (dtype, count) in state.type_counts {
                *merged_counts.entry(dtype).or_insert(0) += count;
            }
        }

        Ok(DataTypeState {
            type_counts: merged_counts,
            total_count,
        })
    }

    fn is_empty(&self) -> bool {
        self.total_count == 0
    }
}

#[async_trait]
impl Analyzer for DataTypeAnalyzer {
    type State = DataTypeState;
    type Metric = MetricValue;

    #[instrument(skip(ctx), fields(analyzer = "data_type", column = %self.column))]
    async fn compute_state_from_data(&self, ctx: &SessionContext) -> AnalyzerResult<Self::State> {
        // Get the table name from the validation context
        let validation_ctx = current_validation_context();
        let table_name = validation_ctx.table_name();

        // Build SQL query to categorize values by their inferred type
        // This uses SQL type checking functions to infer types
        let sql = format!(
            r#"
            SELECT 
                CASE
                    WHEN {0} IS NULL THEN 'null'
                    WHEN TRY_CAST({0} AS BOOLEAN) IS NOT NULL AND 
                         (UPPER(CAST({0} AS VARCHAR)) = 'TRUE' OR 
                          UPPER(CAST({0} AS VARCHAR)) = 'FALSE' OR
                          CAST({0} AS VARCHAR) = '0' OR 
                          CAST({0} AS VARCHAR) = '1') THEN 'boolean'
                    WHEN TRY_CAST({0} AS INT) IS NOT NULL AND 
                         CAST(TRY_CAST({0} AS INT) AS VARCHAR) = CAST({0} AS VARCHAR) THEN 'integer'
                    WHEN TRY_CAST({0} AS DOUBLE) IS NOT NULL THEN 'double'
                    WHEN TRY_CAST({0} AS DATE) IS NOT NULL THEN 'date'
                    WHEN TRY_CAST({0} AS TIMESTAMP) IS NOT NULL THEN 'timestamp'
                    ELSE 'string'
                END as inferred_type,
                COUNT(*) as count
            FROM {table_name}
            WHERE {0} IS NOT NULL
            GROUP BY inferred_type
            "#,
            self.column
        );

        // Execute query
        let df = ctx.sql(&sql).await?;
        let batches = df.collect().await?;

        // Extract type counts from result
        let mut type_counts = HashMap::new();
        let mut total_count = 0;

        for batch in &batches {
            let type_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| {
                    AnalyzerError::invalid_data("Expected String array for inferred types")
                })?;

            let count_array = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| AnalyzerError::invalid_data("Expected Int64 array for counts"))?;

            for i in 0..batch.num_rows() {
                if !type_array.is_null(i) {
                    let dtype = type_array.value(i).to_string();
                    let count = count_array.value(i) as u64;
                    *type_counts.entry(dtype).or_insert(0) += count;
                    total_count += count;
                }
            }
        }

        Ok(DataTypeState {
            type_counts,
            total_count,
        })
    }

    fn compute_metric_from_state(&self, state: &Self::State) -> AnalyzerResult<Self::Metric> {
        // Convert type counts to MetricValue::Map
        let type_map: HashMap<String, MetricValue> = state
            .type_counts
            .iter()
            .map(|(dtype, count)| (dtype.clone(), MetricValue::Long(*count as i64)))
            .collect();

        Ok(MetricValue::Map(type_map))
    }

    fn name(&self) -> &str {
        "data_type"
    }

    fn description(&self) -> &str {
        "Infers data types and detects type inconsistencies"
    }

    fn columns(&self) -> Vec<&str> {
        vec![&self.column]
    }
}
