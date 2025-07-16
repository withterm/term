//! Tests for the analyzer framework.

use super::*;
use async_trait::async_trait;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};

/// Test state for a simple size analyzer.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestSizeState {
    count: u64,
}

impl AnalyzerState for TestSizeState {
    fn merge(states: Vec<Self>) -> AnalyzerResult<Self> {
        let total_count = states.iter().map(|s| s.count).sum();
        Ok(TestSizeState { count: total_count })
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }
}

/// Test analyzer that computes dataset size.
#[derive(Debug)]
struct TestSizeAnalyzer;

#[async_trait]
impl Analyzer for TestSizeAnalyzer {
    type State = TestSizeState;
    type Metric = MetricValue;

    async fn compute_state_from_data(&self, ctx: &SessionContext) -> AnalyzerResult<Self::State> {
        let df = ctx.sql("SELECT COUNT(*) as count FROM data").await?;
        let batches = df.collect().await?;

        let count = if let Some(batch) = batches.first() {
            if let Some(array) = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
            {
                array.value(0) as u64
            } else {
                0
            }
        } else {
            0
        };

        Ok(TestSizeState { count })
    }

    fn compute_metric_from_state(&self, state: &Self::State) -> AnalyzerResult<Self::Metric> {
        Ok(MetricValue::Long(state.count as i64))
    }

    fn name(&self) -> &str {
        "size"
    }

    fn description(&self) -> &str {
        "Computes the number of rows in the dataset"
    }
}

#[cfg(test)]
mod metric_value_tests {
    use super::*;

    #[test]
    fn test_metric_value_numeric_checks() {
        let double = MetricValue::Double(42.5);
        let long = MetricValue::Long(42);
        let string = MetricValue::String("test".to_string());

        assert!(double.is_numeric());
        assert!(long.is_numeric());
        assert!(!string.is_numeric());
    }

    #[test]
    fn test_metric_value_conversions() {
        let double = MetricValue::Double(42.0);
        let long = MetricValue::Long(42);
        let fractional = MetricValue::Double(42.5);

        assert_eq!(double.as_f64(), Some(42.0));
        assert_eq!(double.as_i64(), Some(42));
        assert_eq!(long.as_f64(), Some(42.0));
        assert_eq!(long.as_i64(), Some(42));
        assert_eq!(fractional.as_f64(), Some(42.5));
        assert_eq!(fractional.as_i64(), None); // Fractional part prevents conversion
    }

    #[test]
    fn test_metric_value_display() {
        assert_eq!(MetricValue::Double(42.0).to_string_pretty(), "42");
        assert_eq!(MetricValue::Double(42.5678).to_string_pretty(), "42.5678");
        assert_eq!(MetricValue::Long(1000).to_string_pretty(), "1000");
        assert_eq!(
            MetricValue::String("test".to_string()).to_string_pretty(),
            "test"
        );
        assert_eq!(MetricValue::Boolean(true).to_string_pretty(), "true");
    }

    #[test]
    fn test_metric_value_from_primitives() {
        let from_f64: MetricValue = 42.5.into();
        let from_i64: MetricValue = 42i64.into();
        let from_bool: MetricValue = true.into();
        let from_string: MetricValue = "test".into();

        assert_eq!(from_f64, MetricValue::Double(42.5));
        assert_eq!(from_i64, MetricValue::Long(42));
        assert_eq!(from_bool, MetricValue::Boolean(true));
        assert_eq!(from_string, MetricValue::String("test".to_string()));
    }
}

#[cfg(test)]
mod distribution_tests {
    #[allow(unused_imports)]
    use super::*;
    use crate::analyzers::types::{HistogramBucket, MetricDistribution};

    #[test]
    fn test_histogram_bucket() {
        let bucket = HistogramBucket::new(0.0, 10.0, 5);
        assert_eq!(bucket.width(), 10.0);
        assert_eq!(bucket.midpoint(), 5.0);
    }

    #[test]
    fn test_metric_distribution() {
        let buckets = vec![
            HistogramBucket::new(0.0, 10.0, 5),
            HistogramBucket::new(10.0, 20.0, 10),
            HistogramBucket::new(20.0, 30.0, 3),
        ];

        let dist = MetricDistribution::from_buckets(buckets.clone());
        assert_eq!(dist.total_count, 18);
        assert_eq!(dist.buckets.len(), 3);

        let dist_with_stats = dist.with_stats(0.0, 30.0, 15.0, 5.5);
        assert_eq!(dist_with_stats.min, Some(0.0));
        assert_eq!(dist_with_stats.max, Some(30.0));
        assert_eq!(dist_with_stats.mean, Some(15.0));
        assert_eq!(dist_with_stats.std_dev, Some(5.5));
    }
}

#[cfg(test)]
mod context_tests {
    #[allow(unused_imports)]
    use super::*;
    #[allow(unused_imports)]
    use crate::analyzers::context::AnalysisMetadata;

    #[test]
    fn test_analyzer_context_basic() {
        let mut context = AnalyzerContext::new();

        context.store_metric("test_metric", MetricValue::Long(42));
        context.store_analyzer_metric("size", "rows", MetricValue::Long(1000));

        assert_eq!(
            context.get_metric("test_metric"),
            Some(&MetricValue::Long(42))
        );
        assert_eq!(
            context.get_metric("size.rows"),
            Some(&MetricValue::Long(1000))
        );

        let size_metrics = context.get_analyzer_metrics("size");
        assert_eq!(size_metrics.len(), 1);
        assert_eq!(size_metrics.get("rows"), Some(&&MetricValue::Long(1000)));
    }

    #[test]
    fn test_analyzer_context_errors() {
        let mut context = AnalyzerContext::new();

        context.record_error("test_analyzer", AnalyzerError::NoData);
        assert!(context.has_errors());
        assert_eq!(context.errors().len(), 1);
        assert_eq!(context.errors()[0].analyzer_name, "test_analyzer");
    }

    #[test]
    fn test_analyzer_context_merge() {
        let mut context1 = AnalyzerContext::new();
        context1.store_metric("metric1", MetricValue::Long(1));

        let mut context2 = AnalyzerContext::new();
        context2.store_metric("metric2", MetricValue::Long(2));
        context2.record_error("analyzer2", AnalyzerError::NoData);

        context1.merge(context2);

        assert_eq!(context1.get_metric("metric1"), Some(&MetricValue::Long(1)));
        assert_eq!(context1.get_metric("metric2"), Some(&MetricValue::Long(2)));
        assert_eq!(context1.errors().len(), 1);
    }

    #[test]
    fn test_analyzer_context_summary() {
        let mut context = AnalyzerContext::with_dataset("test_data");
        context.store_analyzer_metric("size", "rows", MetricValue::Long(1000));
        context.store_analyzer_metric("completeness", "user_id", MetricValue::Double(0.98));
        context.store_analyzer_metric("completeness", "email", MetricValue::Double(0.95));
        context.record_error("invalid", AnalyzerError::NoData);

        let summary = context.summary();
        assert_eq!(summary.total_metrics, 3);
        assert_eq!(summary.total_errors, 1);
        assert_eq!(summary.analyzer_count, 2); // size and completeness
        assert_eq!(summary.dataset_name, Some("test_data".to_string()));
    }
}

#[cfg(test)]
mod metadata_tests {
    use crate::analyzers::context::AnalysisMetadata;

    #[test]
    fn test_analysis_metadata() {
        let mut metadata = AnalysisMetadata::with_dataset("test_data");

        metadata.record_start();
        std::thread::sleep(std::time::Duration::from_millis(10));
        metadata.record_end();

        assert!(metadata.duration().is_some());
        assert!(metadata.duration().unwrap().num_milliseconds() >= 10);

        metadata.add_custom("version", "1.0");
        assert_eq!(metadata.custom.get("version"), Some(&"1.0".to_string()));
    }
}

#[cfg(test)]
mod serialization_tests {
    use super::*;

    #[test]
    fn test_metric_value_serialization() {
        let values = vec![
            MetricValue::Double(42.5),
            MetricValue::Long(1000),
            MetricValue::String("test".to_string()),
            MetricValue::Boolean(true),
            MetricValue::Vector(vec![1.0, 2.0, 3.0]),
        ];

        for value in values {
            let serialized = serde_json::to_string(&value).unwrap();
            let deserialized: MetricValue = serde_json::from_str(&serialized).unwrap();
            assert_eq!(value, deserialized);
        }
    }

    #[test]
    fn test_analyzer_context_serialization() {
        let mut context = AnalyzerContext::with_dataset("test");
        context.store_metric("test", MetricValue::Long(42));
        context.record_error("analyzer", AnalyzerError::NoData);

        let serialized = serde_json::to_string(&context).unwrap();
        let deserialized: AnalyzerContext = serde_json::from_str(&serialized).unwrap();

        assert_eq!(
            deserialized.get_metric("test"),
            Some(&MetricValue::Long(42))
        );
        assert_eq!(deserialized.errors().len(), 1);
    }

    #[test]
    fn test_test_size_state_serialization() {
        let state = TestSizeState { count: 42 };

        let serialized = serde_json::to_string(&state).unwrap();
        let deserialized: TestSizeState = serde_json::from_str(&serialized).unwrap();

        assert_eq!(state.count, deserialized.count);
    }
}

#[cfg(test)]
mod trait_bound_tests {
    use super::*;
    use crate::analyzers::context::{AnalysisError, AnalysisMetadata, AnalysisSummary};
    use crate::analyzers::types::{HistogramBucket, MetricDistribution};

    // These tests verify that our types satisfy the required trait bounds

    #[allow(dead_code)]
    fn assert_send<T: Send>() {}
    #[allow(dead_code)]
    fn assert_sync<T: Sync>() {}
    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn test_types_are_send_sync() {
        // MetricValue types
        assert_send_sync::<MetricValue>();
        assert_send_sync::<MetricDistribution>();
        assert_send_sync::<HistogramBucket>();

        // Context types
        assert_send_sync::<AnalyzerContext>();
        assert_send_sync::<AnalysisMetadata>();
        assert_send_sync::<AnalysisError>();
        assert_send_sync::<AnalysisSummary>();

        // Error types
        assert_send_sync::<AnalyzerError>();

        // Test implementations
        assert_send_sync::<TestSizeState>();
        assert_send_sync::<TestSizeAnalyzer>();
    }
}
