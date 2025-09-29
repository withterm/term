//! Grouped completeness analyzer implementation.

use async_trait::async_trait;
use datafusion::arrow::array::{StringArray, UInt64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tracing::{debug, instrument};

use crate::analyzers::{
    grouped::{
        GroupedAnalyzer, GroupedAnalyzerState, GroupedMetadata, GroupedMetrics, GroupingConfig,
    },
    AnalyzerError, AnalyzerResult, AnalyzerState, MetricValue,
};
use crate::core::current_validation_context;

use super::completeness::{CompletenessAnalyzer, CompletenessState};

/// State for grouped completeness analysis.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupedCompletenessState {
    /// Map of group keys to completeness states.
    pub groups: BTreeMap<Vec<String>, CompletenessState>,

    /// Overall state across all groups (if requested).
    pub overall: Option<CompletenessState>,

    /// Metadata about the computation.
    pub metadata: GroupedMetadata,
}

impl GroupedAnalyzerState for GroupedCompletenessState {}

impl AnalyzerState for GroupedCompletenessState {
    fn merge(states: Vec<Self>) -> AnalyzerResult<Self> {
        if states.is_empty() {
            return Err(AnalyzerError::InvalidConfiguration(
                "Cannot merge empty states".to_string(),
            ));
        }

        let mut merged_groups: BTreeMap<Vec<String>, Vec<CompletenessState>> = BTreeMap::new();
        let mut overall_states = Vec::new();
        let mut metadata = states[0].metadata.clone();

        // Collect states by group
        for state in states {
            for (key, group_state) in state.groups {
                merged_groups.entry(key).or_default().push(group_state);
            }

            if let Some(overall) = state.overall {
                overall_states.push(overall);
            }

            // Update metadata
            metadata.total_groups = metadata.total_groups.max(state.metadata.total_groups);
        }

        // Merge states for each group
        let mut final_groups = BTreeMap::new();
        for (key, states) in merged_groups {
            let merged = CompletenessState::merge(states)?;
            final_groups.insert(key, merged);
        }

        // Merge overall states
        let overall = if !overall_states.is_empty() {
            Some(CompletenessState::merge(overall_states)?)
        } else {
            None
        };

        metadata.included_groups = final_groups.len();
        metadata.truncated = metadata.total_groups > metadata.included_groups;

        Ok(GroupedCompletenessState {
            groups: final_groups,
            overall,
            metadata,
        })
    }

    fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }
}

#[async_trait]
impl GroupedAnalyzer for CompletenessAnalyzer {
    type GroupedState = GroupedCompletenessState;

    #[instrument(skip(ctx), fields(
        analyzer = "grouped_completeness",
        column = %self.column()
    ))]
    async fn compute_grouped_state_from_data(
        &self,
        ctx: &SessionContext,
        config: &GroupingConfig,
    ) -> AnalyzerResult<Self::GroupedState> {
        let table_name = current_validation_context().table_name().to_string();

        // Build grouped SQL query
        let group_columns = if config.columns.is_empty() {
            String::new()
        } else {
            format!("{}, ", config.columns.join(", "))
        };

        let column = self.column();
        let group_by = if config.columns.is_empty() {
            String::new()
        } else {
            format!("GROUP BY {}", config.columns.join(", "))
        };
        let order_by = if config.columns.is_empty() {
            String::new()
        } else {
            // Order by completeness descending to get worst groups first if truncated
            format!("ORDER BY COUNT({column}) * 1.0 / COUNT(*) DESC")
        };
        let limit = if let Some(max) = config.max_groups {
            format!("LIMIT {}", max + 1) // +1 to detect truncation
        } else {
            String::new()
        };

        let sql = format!(
            "SELECT {group_columns}
                COUNT(*) as total_count,
                COUNT({column}) as non_null_count
             FROM {table_name}
             {group_by}
             {order_by}
             {limit}"
        );

        debug!("Executing grouped completeness query: {}", sql);

        let df = ctx
            .sql(&sql)
            .await
            .map_err(|e| AnalyzerError::Custom(format!("Failed to execute grouped query: {e}")))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| AnalyzerError::Custom(format!("Failed to collect results: {e}")))?;

        let mut groups = BTreeMap::new();
        let mut total_groups = 0;
        let mut overall_total = 0u64;
        let mut overall_non_null = 0u64;

        for batch in &batches {
            total_groups += batch.num_rows();

            if let Some(max_groups) = config.max_groups {
                if groups.len() >= max_groups {
                    break;
                }
            }

            // Extract group keys and metrics
            let group_values = extract_group_values(batch, &config.columns)?;
            let totals = extract_counts(batch, "total_count")?;
            let non_nulls = extract_counts(batch, "non_null_count")?;

            for i in 0..batch.num_rows() {
                if let Some(max_groups) = config.max_groups {
                    if groups.len() >= max_groups {
                        break;
                    }
                }

                let key = group_values
                    .get(i)
                    .cloned()
                    .unwrap_or_else(|| vec!["NULL".to_string(); config.columns.len()]);

                let state = CompletenessState {
                    total_count: totals[i],
                    non_null_count: non_nulls[i],
                };

                groups.insert(key, state);

                if config.include_overall {
                    overall_total += totals[i];
                    overall_non_null += non_nulls[i];
                }
            }
        }

        let overall = if config.include_overall {
            Some(CompletenessState {
                total_count: overall_total,
                non_null_count: overall_non_null,
            })
        } else {
            None
        };

        let metadata = GroupedMetadata::new(config.columns.clone(), total_groups, groups.len());

        Ok(GroupedCompletenessState {
            groups,
            overall,
            metadata,
        })
    }

    fn compute_grouped_metrics_from_state(
        &self,
        state: &Self::GroupedState,
    ) -> AnalyzerResult<GroupedMetrics> {
        let mut metric_groups = BTreeMap::new();

        // Convert each group's state to a metric
        for (key, group_state) in &state.groups {
            let completeness = group_state.completeness();
            metric_groups.insert(key.clone(), MetricValue::Double(completeness));
        }

        // Convert overall state to metric
        let overall_metric = state
            .overall
            .as_ref()
            .map(|s| MetricValue::Double(s.completeness()));

        Ok(GroupedMetrics::new(
            metric_groups,
            overall_metric,
            state.metadata.clone(),
        ))
    }
}

/// Helper function to extract group values from a record batch.
fn extract_group_values(
    batch: &RecordBatch,
    group_columns: &[String],
) -> AnalyzerResult<Vec<Vec<String>>> {
    let mut result = vec![vec![]; batch.num_rows()];

    for col_name in group_columns {
        let col_idx = batch
            .schema()
            .index_of(col_name)
            .map_err(|_| AnalyzerError::Custom(format!("Column {col_name} not found")))?;

        let array = batch.column(col_idx);

        // Convert to string representation
        if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
            for (i, row) in result.iter_mut().enumerate().take(batch.num_rows()) {
                let value = string_array.value(i).to_string();
                row.push(value);
            }
        } else {
            // Try to convert other types to string
            for row in result.iter_mut().take(batch.num_rows()) {
                row.push(format!("{array:?}"));
            }
        }
    }

    Ok(result)
}

/// Helper function to extract count values from a record batch.
fn extract_counts(batch: &RecordBatch, column_name: &str) -> AnalyzerResult<Vec<u64>> {
    let col_idx = batch
        .schema()
        .index_of(column_name)
        .map_err(|_| AnalyzerError::Custom(format!("Column {column_name} not found")))?;

    let array = batch.column(col_idx);

    // DataFusion's COUNT(*) can return either Int64 or UInt64, so handle both
    if let Some(uint_array) = array.as_any().downcast_ref::<UInt64Array>() {
        Ok((0..batch.num_rows()).map(|i| uint_array.value(i)).collect())
    } else if let Some(int_array) = array
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
    {
        Ok((0..batch.num_rows())
            .map(|i| int_array.value(i) as u64)
            .collect())
    } else {
        Err(AnalyzerError::Custom(format!(
            "Expected Int64Array or UInt64Array for {column_name}, got {:?}",
            array.data_type()
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_grouped_completeness() {
        // Create test data with groups
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("product", DataType::Utf8, false),
            Field::new("sales", DataType::Int32, true),
        ]));

        let regions = StringArray::from(vec!["US", "US", "EU", "EU", "US", "EU"]);
        let products = StringArray::from(vec!["A", "B", "A", "B", "A", "A"]);
        let sales = Int32Array::from(vec![
            Some(100),
            Some(200),
            None, // EU-A has null
            Some(150),
            Some(250),
            Some(300),
        ]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(regions), Arc::new(products), Arc::new(sales)],
        )
        .unwrap();

        // Create context and register data
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("data", Arc::new(table)).unwrap();

        // Test grouped completeness
        let analyzer = CompletenessAnalyzer::new("sales");
        let config = GroupingConfig::new(vec!["region".to_string(), "product".to_string()]);

        let state = analyzer
            .compute_grouped_state_from_data(&ctx, &config)
            .await
            .unwrap();

        // Check results
        assert_eq!(state.groups.len(), 4); // US-A, US-B, EU-A, EU-B

        // US-A should have 100% completeness (2 non-null out of 2)
        let us_a = state
            .groups
            .get(&vec!["US".to_string(), "A".to_string()])
            .unwrap();
        assert_eq!(us_a.completeness(), 1.0);

        // EU-A should have 50% completeness (1 non-null out of 2)
        let eu_a = state
            .groups
            .get(&vec!["EU".to_string(), "A".to_string()])
            .unwrap();
        assert_eq!(eu_a.completeness(), 0.5);
    }
}
