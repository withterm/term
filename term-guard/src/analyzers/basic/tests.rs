//! Tests for basic analyzers.

use super::*;
use crate::analyzers::{Analyzer, AnalyzerState, MetricValue};
use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use std::sync::Arc;

/// Creates a test context with sample data.
async fn create_test_context() -> Result<SessionContext, datafusion::error::DataFusionError> {
    let ctx = SessionContext::new();

    // Create test data
    let id_array = Int64Array::from(vec![Some(1), Some(2), Some(3), Some(4), None]);
    let value_array =
        Float64Array::from(vec![Some(10.0), Some(20.0), None, Some(30.0), Some(40.0)]);
    let name_array = StringArray::from(vec![Some("a"), Some("b"), Some("a"), None, Some("c")]);

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("value", DataType::Float64, true),
            Field::new("name", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(id_array) as ArrayRef,
            Arc::new(value_array) as ArrayRef,
            Arc::new(name_array) as ArrayRef,
        ],
    )?;

    ctx.register_batch("data", batch)?;
    Ok(ctx)
}

#[cfg(test)]
mod size_tests {
    use super::*;

    #[tokio::test]
    async fn test_size_analyzer() {
        let ctx = create_test_context().await.unwrap();
        let analyzer = SizeAnalyzer::new();

        let state = analyzer.compute_state_from_data(&ctx).await.unwrap();
        assert_eq!(state.count, 5);

        let metric = analyzer.compute_metric_from_state(&state).unwrap();
        assert_eq!(metric, MetricValue::Long(5));
    }

    #[test]
    fn test_size_state_merge() {
        let states = vec![
            SizeState { count: 10 },
            SizeState { count: 20 },
            SizeState { count: 15 },
        ];

        let merged = SizeState::merge(states).unwrap();
        assert_eq!(merged.count, 45);
    }
}

#[cfg(test)]
mod completeness_tests {
    use super::*;

    #[tokio::test]
    async fn test_completeness_analyzer() {
        let ctx = create_test_context().await.unwrap();

        // Test with "id" column (4 non-null out of 5)
        let analyzer = CompletenessAnalyzer::new("id");
        let state = analyzer.compute_state_from_data(&ctx).await.unwrap();
        assert_eq!(state.total_count, 5);
        assert_eq!(state.non_null_count, 4);

        let metric = analyzer.compute_metric_from_state(&state).unwrap();
        assert_eq!(metric, MetricValue::Double(0.8)); // 4/5 = 0.8

        // Test with "value" column (4 non-null out of 5)
        let analyzer = CompletenessAnalyzer::new("value");
        let state = analyzer.compute_state_from_data(&ctx).await.unwrap();
        assert_eq!(state.total_count, 5);
        assert_eq!(state.non_null_count, 4);
    }

    #[test]
    fn test_completeness_state_merge() {
        let states = vec![
            CompletenessState {
                total_count: 10,
                non_null_count: 8,
            },
            CompletenessState {
                total_count: 20,
                non_null_count: 18,
            },
        ];

        let merged = CompletenessState::merge(states).unwrap();
        assert_eq!(merged.total_count, 30);
        assert_eq!(merged.non_null_count, 26);
        assert_eq!(merged.completeness(), 26.0 / 30.0);
    }
}

#[cfg(test)]
mod distinctness_tests {
    use super::*;

    #[tokio::test]
    async fn test_distinctness_analyzer() {
        let ctx = create_test_context().await.unwrap();

        // Test with "name" column (3 distinct non-null values out of 4 non-null values)
        let analyzer = DistinctnessAnalyzer::new("name");
        let state = analyzer.compute_state_from_data(&ctx).await.unwrap();
        assert_eq!(state.total_count, 4); // Only counts non-null
        assert_eq!(state.distinct_count, 3); // "a", "b", "c"

        let metric = analyzer.compute_metric_from_state(&state).unwrap();
        assert_eq!(metric, MetricValue::Double(0.75)); // 3/4 = 0.75
    }
}

#[cfg(test)]
mod mean_tests {
    use super::*;

    #[tokio::test]
    async fn test_mean_analyzer() {
        let ctx = create_test_context().await.unwrap();
        let analyzer = MeanAnalyzer::new("value");

        let state = analyzer.compute_state_from_data(&ctx).await.unwrap();
        assert_eq!(state.sum, 100.0); // 10 + 20 + 30 + 40
        assert_eq!(state.count, 4);

        let metric = analyzer.compute_metric_from_state(&state).unwrap();
        assert_eq!(metric, MetricValue::Double(25.0)); // 100/4 = 25
    }

    #[test]
    fn test_mean_state_merge() {
        let states = vec![
            MeanState {
                sum: 100.0,
                count: 4,
            },
            MeanState {
                sum: 50.0,
                count: 2,
            },
        ];

        let merged = MeanState::merge(states).unwrap();
        assert_eq!(merged.sum, 150.0);
        assert_eq!(merged.count, 6);
        assert_eq!(merged.mean(), Some(25.0)); // 150/6 = 25
    }
}

#[cfg(test)]
mod min_max_tests {
    use super::*;

    #[tokio::test]
    async fn test_min_analyzer() {
        let ctx = create_test_context().await.unwrap();
        let analyzer = MinAnalyzer::new("value");

        let state = analyzer.compute_state_from_data(&ctx).await.unwrap();
        assert_eq!(state.min, Some(10.0));

        let metric = analyzer.compute_metric_from_state(&state).unwrap();
        assert_eq!(metric, MetricValue::Double(10.0));
    }

    #[tokio::test]
    async fn test_max_analyzer() {
        let ctx = create_test_context().await.unwrap();
        let analyzer = MaxAnalyzer::new("value");

        let state = analyzer.compute_state_from_data(&ctx).await.unwrap();
        assert_eq!(state.max, Some(40.0));

        let metric = analyzer.compute_metric_from_state(&state).unwrap();
        assert_eq!(metric, MetricValue::Double(40.0));
    }

    #[test]
    fn test_min_max_state_merge() {
        let states = vec![
            MinMaxState {
                min: Some(10.0),
                max: Some(30.0),
            },
            MinMaxState {
                min: Some(5.0),
                max: Some(40.0),
            },
            MinMaxState {
                min: Some(15.0),
                max: Some(25.0),
            },
        ];

        let merged = MinMaxState::merge(states).unwrap();
        assert_eq!(merged.min, Some(5.0));
        assert_eq!(merged.max, Some(40.0));
    }
}

#[cfg(test)]
mod sum_tests {
    use super::*;

    #[tokio::test]
    async fn test_sum_analyzer() {
        let ctx = create_test_context().await.unwrap();
        let analyzer = SumAnalyzer::new("value");

        let state = analyzer.compute_state_from_data(&ctx).await.unwrap();
        assert_eq!(state.sum, 100.0); // 10 + 20 + 30 + 40
        assert!(state.has_values);

        let metric = analyzer.compute_metric_from_state(&state).unwrap();
        assert_eq!(metric, MetricValue::Double(100.0));
    }

    #[test]
    fn test_sum_state_merge() {
        let states = vec![
            SumState {
                sum: 100.0,
                has_values: true,
            },
            SumState {
                sum: 50.0,
                has_values: true,
            },
            SumState {
                sum: 0.0,
                has_values: false,
            }, // No values state
        ];

        let merged = SumState::merge(states).unwrap();
        assert_eq!(merged.sum, 150.0);
        assert!(merged.has_values);
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[tokio::test]
    async fn test_all_analyzers_empty_data() {
        let ctx = SessionContext::new();

        // Create empty table
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Float64,
                true,
            )])),
            vec![Arc::new(Float64Array::from(vec![] as Vec<Option<f64>>)) as ArrayRef],
        )
        .unwrap();

        ctx.register_batch("data", batch).unwrap();

        // Size analyzer should return 0
        let size = SizeAnalyzer::new();
        let state = size.compute_state_from_data(&ctx).await.unwrap();
        assert_eq!(state.count, 0);

        // Completeness should return 1.0 for empty data
        let completeness = CompletenessAnalyzer::new("value");
        let state = completeness.compute_state_from_data(&ctx).await.unwrap();
        assert_eq!(state.completeness(), 1.0);

        // Mean should error with NoData
        let mean = MeanAnalyzer::new("value");
        let state = mean.compute_state_from_data(&ctx).await.unwrap();
        assert!(mean.compute_metric_from_state(&state).is_err());
    }

    #[tokio::test]
    async fn test_all_analyzers_null_only_data() {
        let ctx = SessionContext::new();

        // Create table with only null values
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Float64,
                true,
            )])),
            vec![Arc::new(Float64Array::from(vec![None, None, None])) as ArrayRef],
        )
        .unwrap();

        ctx.register_batch("data", batch).unwrap();

        // Size analyzer should return 3
        let size = SizeAnalyzer::new();
        let state = size.compute_state_from_data(&ctx).await.unwrap();
        assert_eq!(state.count, 3);

        // Completeness should return 0.0
        let completeness = CompletenessAnalyzer::new("value");
        let state = completeness.compute_state_from_data(&ctx).await.unwrap();
        assert_eq!(state.completeness(), 0.0);

        // All numeric analyzers should error with NoData
        let sum = SumAnalyzer::new("value");
        let state = sum.compute_state_from_data(&ctx).await.unwrap();
        assert!(sum.compute_metric_from_state(&state).is_err());
    }
}
