//! Histogram analysis constraint for value distribution analysis.

use crate::core::{Constraint, ConstraintMetadata, ConstraintResult, ConstraintStatus};
use crate::prelude::*;
use arrow::array::{Array, LargeStringArray, StringViewArray};
use async_trait::async_trait;
use datafusion::prelude::*;
use std::fmt;
use std::sync::Arc;
use tracing::instrument;

/// A bucket in a histogram representing a value and its frequency information.
#[derive(Debug, Clone, PartialEq)]
pub struct HistogramBucket {
    /// The value in this bucket
    pub value: String,
    /// The count of occurrences
    pub count: i64,
    /// The ratio of this value to the total count
    pub ratio: f64,
}

/// A histogram representing the distribution of values in a column.
#[derive(Debug, Clone)]
pub struct Histogram {
    /// The buckets in the histogram, ordered by frequency (descending)
    pub buckets: Vec<HistogramBucket>,
    /// Total number of values (including nulls if present)
    pub total_count: i64,
    /// Number of distinct values
    pub distinct_count: usize,
    /// Number of null values
    pub null_count: i64,
}

impl Histogram {
    /// Creates a new histogram from buckets.
    pub fn new(buckets: Vec<HistogramBucket>, total_count: i64, null_count: i64) -> Self {
        let distinct_count = buckets.len();
        Self {
            buckets,
            total_count,
            distinct_count,
            null_count,
        }
    }

    /// Returns the ratio of the most common value.
    pub fn most_common_ratio(&self) -> f64 {
        self.buckets.first().map(|b| b.ratio).unwrap_or(0.0)
    }

    /// Returns the ratio of the least common value.
    pub fn least_common_ratio(&self) -> f64 {
        self.buckets.last().map(|b| b.ratio).unwrap_or(0.0)
    }

    /// Returns the number of buckets (distinct values).
    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    /// Returns the top N most common values and their ratios.
    pub fn top_n(&self, n: usize) -> Vec<(&str, f64)> {
        self.buckets
            .iter()
            .take(n)
            .map(|b| (b.value.as_str(), b.ratio))
            .collect()
    }

    /// Checks if the distribution is roughly uniform (all values have similar frequencies).
    ///
    /// A distribution is considered roughly uniform if the ratio between the most common
    /// and least common values is less than the threshold (default 1.5).
    pub fn is_roughly_uniform(&self, threshold: f64) -> bool {
        if self.buckets.is_empty() {
            return true;
        }

        let max_ratio = self.most_common_ratio();
        let min_ratio = self.least_common_ratio();

        if min_ratio == 0.0 {
            return false;
        }

        max_ratio / min_ratio <= threshold
    }

    /// Gets the ratio for a specific value, if it exists in the histogram.
    pub fn get_value_ratio(&self, value: &str) -> Option<f64> {
        self.buckets
            .iter()
            .find(|b| b.value == value)
            .map(|b| b.ratio)
    }

    /// Returns the entropy of the distribution.
    ///
    /// Higher entropy indicates more uniform distribution.
    pub fn entropy(&self) -> f64 {
        self.buckets
            .iter()
            .filter(|b| b.ratio > 0.0)
            .map(|b| -b.ratio * b.ratio.ln())
            .sum()
    }

    /// Checks if the distribution follows a power law (few values dominate).
    ///
    /// Returns true if the top `n` values account for more than `threshold` of the distribution.
    pub fn follows_power_law(&self, top_n: usize, threshold: f64) -> bool {
        let top_sum: f64 = self.buckets.iter().take(top_n).map(|b| b.ratio).sum();
        top_sum >= threshold
    }

    /// Returns the null ratio in the data.
    pub fn null_ratio(&self) -> f64 {
        if self.total_count == 0 {
            0.0
        } else {
            self.null_count as f64 / self.total_count as f64
        }
    }
}

/// Type alias for histogram assertion function.
pub type HistogramAssertion = Arc<dyn Fn(&Histogram) -> bool + Send + Sync>;

/// A constraint that analyzes value distribution in a column and applies custom assertions.
///
/// This constraint computes a histogram of value frequencies and allows custom assertion
/// functions to validate the distribution characteristics.
///
/// # Examples
///
/// ```rust
/// use term_core::constraints::{HistogramConstraint, Histogram};
/// use term_core::core::Constraint;
/// use std::sync::Arc;
///
/// // Check that no single value dominates
/// let constraint = HistogramConstraint::new("status", Arc::new(|hist: &Histogram| {
///     hist.most_common_ratio() < 0.5
/// }));
///
/// // Verify distribution has expected number of categories
/// let constraint = HistogramConstraint::new("category", Arc::new(|hist| {
///     hist.bucket_count() >= 5 && hist.bucket_count() <= 10
/// }));
/// ```
#[derive(Clone)]
pub struct HistogramConstraint {
    column: String,
    assertion: HistogramAssertion,
    assertion_description: String,
}

impl fmt::Debug for HistogramConstraint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HistogramConstraint")
            .field("column", &self.column)
            .field("assertion_description", &self.assertion_description)
            .finish()
    }
}

impl HistogramConstraint {
    /// Creates a new histogram constraint.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to analyze
    /// * `assertion` - The assertion function to apply to the histogram
    pub fn new(column: impl Into<String>, assertion: HistogramAssertion) -> Self {
        Self {
            column: column.into(),
            assertion,
            assertion_description: "custom assertion".to_string(),
        }
    }

    /// Creates a new histogram constraint with a description.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to analyze
    /// * `assertion` - The assertion function to apply to the histogram
    /// * `description` - A description of what the assertion checks
    pub fn new_with_description(
        column: impl Into<String>,
        assertion: HistogramAssertion,
        description: impl Into<String>,
    ) -> Self {
        Self {
            column: column.into(),
            assertion,
            assertion_description: description.into(),
        }
    }
}

#[async_trait]
impl Constraint for HistogramConstraint {
    #[instrument(skip(self, ctx), fields(column = %self.column))]
    async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult> {
        // SQL query to compute value frequencies
        let sql = format!(
            r#"
            WITH value_counts AS (
                SELECT 
                    CAST({} AS VARCHAR) as value,
                    COUNT(*) as count
                FROM data
                WHERE {} IS NOT NULL
                GROUP BY {}
            ),
            totals AS (
                SELECT 
                    COUNT(*) as total_cnt,
                    SUM(CASE WHEN {} IS NULL THEN 1 ELSE 0 END) as null_cnt
                FROM data
            )
            SELECT 
                vc.value,
                vc.count,
                vc.count * 1.0 / (t.total_cnt - t.null_cnt) as ratio,
                t.total_cnt as total_count,
                t.null_cnt as null_count
            FROM value_counts vc
            CROSS JOIN totals t
            ORDER BY vc.count DESC, vc.value
            "#,
            self.column, self.column, self.column, self.column
        );

        let df = ctx.sql(&sql).await.map_err(|e| {
            TermError::constraint_evaluation(
                self.name(),
                format!("Failed to execute histogram query: {}", e),
            )
        })?;

        let batches = df.collect().await?;

        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Ok(ConstraintResult::skipped("No data to analyze"));
        }

        // Extract histogram data from results
        let mut buckets = Vec::new();
        let mut total_count = 0i64;
        let mut null_count = 0i64;

        for batch in &batches {
            // DataFusion might return various string types
            let values_col = batch.column(0);
            let value_strings: Vec<String> = match values_col.data_type() {
                arrow::datatypes::DataType::Utf8 => {
                    let arr = values_col
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .ok_or_else(|| {
                            TermError::constraint_evaluation(
                                self.name(),
                                "Failed to extract string values",
                            )
                        })?;
                    (0..arr.len()).map(|i| arr.value(i).to_string()).collect()
                }
                arrow::datatypes::DataType::Utf8View => {
                    let arr = values_col
                        .as_any()
                        .downcast_ref::<StringViewArray>()
                        .ok_or_else(|| {
                            TermError::constraint_evaluation(
                                self.name(),
                                "Failed to extract string view values",
                            )
                        })?;
                    (0..arr.len()).map(|i| arr.value(i).to_string()).collect()
                }
                arrow::datatypes::DataType::LargeUtf8 => {
                    let arr = values_col
                        .as_any()
                        .downcast_ref::<LargeStringArray>()
                        .ok_or_else(|| {
                            TermError::constraint_evaluation(
                                self.name(),
                                "Failed to extract large string values",
                            )
                        })?;
                    (0..arr.len()).map(|i| arr.value(i).to_string()).collect()
                }
                _ => {
                    return Err(TermError::constraint_evaluation(
                        self.name(),
                        format!("Unexpected value column type: {:?}", values_col.data_type()),
                    ));
                }
            };

            let count_array = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| {
                    TermError::constraint_evaluation(self.name(), "Failed to extract counts")
                })?;

            let ratio_array = batch
                .column(2)
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .ok_or_else(|| {
                    TermError::constraint_evaluation(self.name(), "Failed to extract ratios")
                })?;

            let total_array = batch
                .column(3)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| {
                    TermError::constraint_evaluation(self.name(), "Failed to extract total count")
                })?;

            let null_array = batch
                .column(4)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| {
                    TermError::constraint_evaluation(self.name(), "Failed to extract null count")
                })?;

            // Get total and null counts from first row
            if batch.num_rows() > 0 {
                total_count = total_array.value(0);
                null_count = null_array.value(0);
            }

            // Collect buckets
            for (i, value) in value_strings.into_iter().enumerate() {
                let count = count_array.value(i);
                let ratio = ratio_array.value(i);

                buckets.push(HistogramBucket {
                    value,
                    count,
                    ratio,
                });
            }
        }

        // Create histogram
        let histogram = Histogram::new(buckets, total_count, null_count);

        // Apply assertion
        let assertion_result = (self.assertion)(&histogram);

        let status = if assertion_result {
            ConstraintStatus::Success
        } else {
            ConstraintStatus::Failure
        };

        let message = if status == ConstraintStatus::Failure {
            Some(format!(
                "Histogram assertion '{}' failed for column '{}'. Distribution: {} distinct values, most common ratio: {:.2}%, null ratio: {:.2}%",
                self.assertion_description,
                self.column,
                histogram.distinct_count,
                histogram.most_common_ratio() * 100.0,
                histogram.null_ratio() * 100.0
            ))
        } else {
            None
        };

        // Store histogram entropy as metric
        Ok(ConstraintResult {
            status,
            metric: Some(histogram.entropy()),
            message,
        })
    }

    fn name(&self) -> &str {
        "histogram"
    }

    fn column(&self) -> Option<&str> {
        Some(&self.column)
    }

    fn metadata(&self) -> ConstraintMetadata {
        ConstraintMetadata::for_column(&self.column)
            .with_description(format!(
                "Analyzes value distribution in column '{}' and applies assertion: {}",
                self.column, self.assertion_description
            ))
            .with_custom("assertion", &self.assertion_description)
            .with_custom("constraint_type", "histogram")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ConstraintStatus;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use std::sync::Arc;

    async fn create_test_context_with_data(values: Vec<Option<&str>>) -> SessionContext {
        let ctx = SessionContext::new();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "test_col",
            DataType::Utf8,
            true,
        )]));

        let array = StringArray::from(values);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();

        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("data", Arc::new(provider)).unwrap();

        ctx
    }

    #[test]
    fn test_histogram_basic() {
        let buckets = vec![
            HistogramBucket {
                value: "A".to_string(),
                count: 50,
                ratio: 0.5,
            },
            HistogramBucket {
                value: "B".to_string(),
                count: 30,
                ratio: 0.3,
            },
            HistogramBucket {
                value: "C".to_string(),
                count: 20,
                ratio: 0.2,
            },
        ];

        let histogram = Histogram::new(buckets, 100, 0);

        assert_eq!(histogram.most_common_ratio(), 0.5);
        assert_eq!(histogram.least_common_ratio(), 0.2);
        assert_eq!(histogram.bucket_count(), 3);
        assert_eq!(histogram.null_ratio(), 0.0);
    }

    #[test]
    fn test_histogram_entropy() {
        // Uniform distribution should have higher entropy
        let uniform_buckets = vec![
            HistogramBucket {
                value: "A".to_string(),
                count: 25,
                ratio: 0.25,
            },
            HistogramBucket {
                value: "B".to_string(),
                count: 25,
                ratio: 0.25,
            },
            HistogramBucket {
                value: "C".to_string(),
                count: 25,
                ratio: 0.25,
            },
            HistogramBucket {
                value: "D".to_string(),
                count: 25,
                ratio: 0.25,
            },
        ];

        let uniform_hist = Histogram::new(uniform_buckets, 100, 0);

        // Skewed distribution should have lower entropy
        let skewed_buckets = vec![
            HistogramBucket {
                value: "A".to_string(),
                count: 90,
                ratio: 0.9,
            },
            HistogramBucket {
                value: "B".to_string(),
                count: 10,
                ratio: 0.1,
            },
        ];

        let skewed_hist = Histogram::new(skewed_buckets, 100, 0);

        assert!(uniform_hist.entropy() > skewed_hist.entropy());
    }

    #[tokio::test]
    async fn test_most_common_ratio_constraint() {
        // Create data where "A" appears 60% of the time
        let values = vec![
            Some("A"),
            Some("A"),
            Some("A"),
            Some("A"),
            Some("A"),
            Some("A"),
            Some("B"),
            Some("B"),
            Some("C"),
            Some("C"),
        ];
        let ctx = create_test_context_with_data(values).await;

        // Constraint that fails: most common should be < 50%
        let constraint = HistogramConstraint::new_with_description(
            "test_col",
            Arc::new(|hist| hist.most_common_ratio() < 0.5),
            "most common value appears less than 50%",
        );

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Failure);
        assert!(result.message.is_some());

        // Constraint that passes: most common should be < 70%
        let constraint =
            HistogramConstraint::new("test_col", Arc::new(|hist| hist.most_common_ratio() < 0.7));

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
    }

    #[tokio::test]
    async fn test_bucket_count_constraint() {
        // Create data with 4 distinct values
        let values = vec![
            Some("RED"),
            Some("BLUE"),
            Some("GREEN"),
            Some("YELLOW"),
            Some("RED"),
            Some("BLUE"),
        ];
        let ctx = create_test_context_with_data(values).await;

        let constraint = HistogramConstraint::new_with_description(
            "test_col",
            Arc::new(|hist| hist.bucket_count() >= 3 && hist.bucket_count() <= 5),
            "has between 3 and 5 distinct values",
        );

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
    }

    #[tokio::test]
    async fn test_uniform_distribution_check() {
        // Create roughly uniform distribution
        let values = vec![
            Some("A"),
            Some("A"),
            Some("B"),
            Some("B"),
            Some("C"),
            Some("C"),
            Some("D"),
            Some("D"),
        ];
        let ctx = create_test_context_with_data(values).await;

        let constraint =
            HistogramConstraint::new("test_col", Arc::new(|hist| hist.is_roughly_uniform(1.5)));

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
    }

    #[tokio::test]
    async fn test_power_law_distribution() {
        // Create power law distribution where top 2 values dominate
        let values = vec![
            Some("Popular1"),
            Some("Popular1"),
            Some("Popular1"),
            Some("Popular1"),
            Some("Popular2"),
            Some("Popular2"),
            Some("Popular2"),
            Some("Rare1"),
            Some("Rare2"),
            Some("Rare3"),
        ];
        let ctx = create_test_context_with_data(values).await;

        let constraint = HistogramConstraint::new_with_description(
            "test_col",
            Arc::new(|hist| hist.follows_power_law(2, 0.7)),
            "top 2 values account for 70% of distribution",
        );

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
    }

    #[tokio::test]
    async fn test_with_nulls() {
        let values = vec![
            Some("A"),
            Some("A"),
            None,
            None,
            None,
            Some("B"),
            Some("B"),
            Some("C"),
        ];
        let ctx = create_test_context_with_data(values).await;

        let constraint = HistogramConstraint::new(
            "test_col",
            Arc::new(|hist| hist.null_ratio() > 0.3 && hist.null_ratio() < 0.4),
        );

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
    }

    #[tokio::test]
    async fn test_empty_data() {
        let ctx = create_test_context_with_data(vec![]).await;

        let constraint = HistogramConstraint::new("test_col", Arc::new(|_| true));

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Skipped);
    }

    #[tokio::test]
    async fn test_specific_value_check() {
        let values = vec![
            Some("PENDING"),
            Some("PENDING"),
            Some("APPROVED"),
            Some("APPROVED"),
            Some("APPROVED"),
            Some("REJECTED"),
        ];
        let ctx = create_test_context_with_data(values).await;

        let constraint = HistogramConstraint::new_with_description(
            "test_col",
            Arc::new(|hist| {
                // Check that APPROVED is the most common status
                hist.get_value_ratio("APPROVED").unwrap_or(0.0) > 0.4
            }),
            "APPROVED status is most common",
        );

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
    }

    #[tokio::test]
    async fn test_top_n_values() {
        let values = vec![
            Some("A"),
            Some("A"),
            Some("A"),
            Some("A"), // 40%
            Some("B"),
            Some("B"),
            Some("B"), // 30%
            Some("C"),
            Some("C"), // 20%
            Some("D"), // 10%
        ];
        let ctx = create_test_context_with_data(values).await;

        let constraint = HistogramConstraint::new(
            "test_col",
            Arc::new(|hist| {
                let top_2 = hist.top_n(2);
                top_2.len() == 2 && top_2[0].1 == 0.4 && top_2[1].1 == 0.3
            }),
        );

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
    }

    #[tokio::test]
    async fn test_numeric_data_histogram() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let ctx = SessionContext::new();

        let schema = Arc::new(Schema::new(vec![Field::new("age", DataType::Int64, true)]));

        let values = vec![
            Some(25),
            Some(25),
            Some(30),
            Some(30),
            Some(30),
            Some(35),
            Some(35),
            Some(40),
            Some(45),
            Some(50),
        ];
        let array = Int64Array::from(values);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();

        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("data", Arc::new(provider)).unwrap();

        let constraint = HistogramConstraint::new_with_description(
            "age",
            Arc::new(|hist| {
                // Check we have reasonable age distribution
                hist.bucket_count() >= 5 && hist.most_common_ratio() < 0.4
            }),
            "age distribution is reasonable",
        );

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
    }
}
