//! Tests for incremental computation framework.

use super::*;
use crate::analyzers::basic::{CompletenessAnalyzer, MeanAnalyzer, SizeAnalyzer};
use crate::analyzers::MetricValue;
use crate::core::ValidationContext;
use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use std::sync::Arc;
use tempfile::TempDir;

/// Creates a test session context with sample data.
async fn create_test_context(rows: Vec<(i64, Option<f64>, Option<String>)>) -> SessionContext {
    let ctx = SessionContext::new();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, true),
        Field::new("category", DataType::Utf8, true),
    ]));

    let mut id_values = Vec::new();
    let mut value_values = Vec::new();
    let mut category_values = Vec::new();

    for (id, value, category) in rows {
        id_values.push(id);
        value_values.push(value);
        category_values.push(category);
    }

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(id_values)),
            Arc::new(Float64Array::from(value_values)),
            Arc::new(StringArray::from(category_values)),
        ],
    )
    .unwrap();

    ctx.register_batch("data", batch).unwrap();
    ctx
}

#[tokio::test]
async fn test_incremental_runner_single_partition() {
    use crate::core::CURRENT_CONTEXT;

    // Set up validation context
    let validation_ctx = ValidationContext::new("data");

    CURRENT_CONTEXT
        .scope(validation_ctx, async {
            // Create temporary directory for state storage
            let temp_dir = TempDir::new().unwrap();
            let state_store = FileSystemStateStore::new(temp_dir.path()).unwrap();

            // Create runner with size analyzer
            let runner = IncrementalAnalysisRunner::new(Box::new(state_store))
                .add_analyzer(SizeAnalyzer::new());

            // Create context with initial data
            let ctx = create_test_context(vec![
                (1, Some(10.0), Some("A".to_string())),
                (2, Some(20.0), Some("B".to_string())),
                (3, None, Some("A".to_string())),
            ])
            .await;

            // Analyze first partition
            let result = runner.analyze_partition(&ctx, "2024-01-01").await.unwrap();

            // Verify size metric
            let size_metric = result.get_metric("size").expect("Size metric not found");
            if let MetricValue::Long(size) = size_metric {
                assert_eq!(*size, 3);
            } else {
                panic!("Expected Long metric for size");
            }

            // Verify state was persisted
            let partitions = runner.list_partitions().await.unwrap();
            assert_eq!(partitions.len(), 1);
            assert_eq!(partitions[0], "2024-01-01");
        })
        .await;
}

#[tokio::test]
async fn test_incremental_runner_merge_states() {
    use crate::core::CURRENT_CONTEXT;

    // Set up validation context
    let validation_ctx = ValidationContext::new("data");

    CURRENT_CONTEXT
        .scope(validation_ctx, async {
            // Create temporary directory for state storage
            let temp_dir = TempDir::new().unwrap();
            let state_store = FileSystemStateStore::new(temp_dir.path()).unwrap();

            // Create runner with multiple analyzers
            let runner = IncrementalAnalysisRunner::new(Box::new(state_store))
                .add_analyzer(SizeAnalyzer::new())
                .add_analyzer(MeanAnalyzer::new("value"));

            // Analyze first partition
            let ctx1 = create_test_context(vec![
                (1, Some(10.0), Some("A".to_string())),
                (2, Some(20.0), Some("B".to_string())),
            ])
            .await;
            runner.analyze_partition(&ctx1, "2024-01-01").await.unwrap();

            // Analyze second partition
            let ctx2 = create_test_context(vec![
                (3, Some(30.0), Some("A".to_string())),
                (4, Some(40.0), Some("C".to_string())),
            ])
            .await;
            runner.analyze_partition(&ctx2, "2024-01-02").await.unwrap();

            // Merge partitions
            let partitions = vec!["2024-01-01".to_string(), "2024-01-02".to_string()];
            let merged_result = runner.analyze_partitions(&partitions).await.unwrap();

            // Verify merged size
            let size_metric = merged_result
                .get_metric("size")
                .expect("Size metric not found");
            if let MetricValue::Long(size) = size_metric {
                assert_eq!(*size, 4); // Total of 4 rows across both partitions
            } else {
                panic!("Expected Long metric for size");
            }

            // Verify merged mean
            let mean_metric = merged_result
                .get_metric("mean.value")
                .expect("Mean metric not found");
            if let MetricValue::Double(mean) = mean_metric {
                assert!((mean - 25.0).abs() < 0.001); // (10+20+30+40)/4 = 25
            } else {
                panic!("Expected Double metric for mean");
            }
        })
        .await;
}

#[tokio::test]
async fn test_incremental_update() {
    use crate::core::CURRENT_CONTEXT;

    // Set up validation context
    let validation_ctx = ValidationContext::new("data");

    CURRENT_CONTEXT
        .scope(validation_ctx, async {
            // Create temporary directory for state storage
            let temp_dir = TempDir::new().unwrap();
            let state_store = FileSystemStateStore::new(temp_dir.path()).unwrap();

            // Create runner with size analyzer
            let runner = IncrementalAnalysisRunner::new(Box::new(state_store))
                .add_analyzer(SizeAnalyzer::new());

            // Initial analysis
            let ctx1 = create_test_context(vec![
                (1, Some(10.0), Some("A".to_string())),
                (2, Some(20.0), Some("B".to_string())),
            ])
            .await;
            runner.analyze_partition(&ctx1, "daily").await.unwrap();

            // Incremental update with new data
            let ctx2 = create_test_context(vec![
                (3, Some(30.0), Some("C".to_string())),
                (4, Some(40.0), Some("D".to_string())),
                (5, None, Some("E".to_string())),
            ])
            .await;
            let updated_result = runner.analyze_incremental(&ctx2, "daily").await.unwrap();

            // Verify updated size includes both old and new data
            let size_metric = updated_result
                .get_metric("size")
                .expect("Size metric not found");
            if let MetricValue::Long(size) = size_metric {
                assert_eq!(*size, 5); // 2 original + 3 new = 5 total
            } else {
                panic!("Expected Long metric for size");
            }
        })
        .await;
}

#[tokio::test]
async fn test_error_handling_with_config() {
    use crate::core::CURRENT_CONTEXT;

    // Set up validation context
    let validation_ctx = ValidationContext::new("data");

    CURRENT_CONTEXT
        .scope(validation_ctx, async {
            // Create temporary directory for state storage
            let temp_dir = TempDir::new().unwrap();
            let state_store = FileSystemStateStore::new(temp_dir.path()).unwrap();

            // Create runner with fail_fast = false
            let config = IncrementalConfig {
                fail_fast: false,
                save_empty_states: false,
                max_merge_batch_size: 100,
            };

            let runner = IncrementalAnalysisRunner::with_config(Box::new(state_store), config)
                .add_analyzer(SizeAnalyzer::new())
                .add_analyzer(CompletenessAnalyzer::new("nonexistent_column")); // This will fail

            let ctx = create_test_context(vec![(1, Some(10.0), Some("A".to_string()))]).await;

            // Should not fail completely due to fail_fast = false
            let result = runner.analyze_partition(&ctx, "test").await.unwrap();

            // Should have size metric
            assert!(result.get_metric("size").is_some());

            // Should have recorded an error for completeness
            assert!(result.has_errors());
            assert_eq!(result.errors().len(), 1);
        })
        .await;
}

#[tokio::test]
async fn test_empty_state_handling() {
    use crate::core::CURRENT_CONTEXT;

    // Set up validation context
    let validation_ctx = ValidationContext::new("data");

    CURRENT_CONTEXT
        .scope(validation_ctx, async {
            // Create temporary directory for state storage
            let temp_dir = TempDir::new().unwrap();
            let state_store = FileSystemStateStore::new(temp_dir.path()).unwrap();

            // Create runner with save_empty_states = true
            let config = IncrementalConfig {
                fail_fast: true,
                save_empty_states: true,
                max_merge_batch_size: 100,
            };

            let runner = IncrementalAnalysisRunner::with_config(Box::new(state_store), config)
                .add_analyzer(SizeAnalyzer::new());

            // Analyze empty dataset
            let ctx = create_test_context(vec![]).await;
            let result = runner.analyze_partition(&ctx, "empty").await.unwrap();

            // Should have size metric of 0
            let size_metric = result.get_metric("size").expect("Size metric not found");
            if let MetricValue::Long(size) = size_metric {
                assert_eq!(*size, 0);
            }

            // State should be saved even though it's empty
            let partitions = runner.list_partitions().await.unwrap();
            assert_eq!(partitions.len(), 1);
        })
        .await;
}

#[tokio::test]
async fn test_batch_processing() {
    use crate::core::CURRENT_CONTEXT;

    // Set up validation context
    let validation_ctx = ValidationContext::new("data");

    CURRENT_CONTEXT
        .scope(validation_ctx, async {
            // Create temporary directory for state storage
            let temp_dir = TempDir::new().unwrap();
            let state_store = FileSystemStateStore::new(temp_dir.path()).unwrap();

            // Create runner with small batch size
            let config = IncrementalConfig {
                fail_fast: true,
                save_empty_states: false,
                max_merge_batch_size: 2, // Process only 2 partitions at a time
            };

            let runner = IncrementalAnalysisRunner::with_config(Box::new(state_store), config)
                .add_analyzer(SizeAnalyzer::new());

            // Create multiple partitions
            for i in 1..=5 {
                let ctx =
                    create_test_context(vec![(i, Some(i as f64 * 10.0), Some("A".to_string()))])
                        .await;
                runner
                    .analyze_partition(&ctx, &format!("partition-{i}"))
                    .await
                    .unwrap();
            }

            // Merge all partitions (should be processed in batches of 2)
            let partitions: Vec<String> = (1..=5).map(|i| format!("partition-{i}")).collect();
            let merged_result = runner.analyze_partitions(&partitions).await.unwrap();

            // Verify total size
            let size_metric = merged_result
                .get_metric("size")
                .expect("Size metric not found");
            if let MetricValue::Long(size) = size_metric {
                assert_eq!(*size, 5); // Total of 5 rows (one per partition)
            }
        })
        .await;
}

#[tokio::test]
async fn test_partition_management() {
    use crate::core::CURRENT_CONTEXT;

    // Set up validation context
    let validation_ctx = ValidationContext::new("data");

    CURRENT_CONTEXT
        .scope(validation_ctx, async {
            // Create temporary directory for state storage
            let temp_dir = TempDir::new().unwrap();
            let state_store = FileSystemStateStore::new(temp_dir.path()).unwrap();

            let runner = IncrementalAnalysisRunner::new(Box::new(state_store))
                .add_analyzer(SizeAnalyzer::new());

            // Create partitions
            for i in 1..=3 {
                let ctx =
                    create_test_context(vec![(i, Some(i as f64), Some("A".to_string()))]).await;
                runner
                    .analyze_partition(&ctx, &format!("2024-01-{i:02}"))
                    .await
                    .unwrap();
            }

            // List partitions
            let partitions = runner.list_partitions().await.unwrap();
            assert_eq!(partitions.len(), 3);
            assert_eq!(partitions[0], "2024-01-01");
            assert_eq!(partitions[1], "2024-01-02");
            assert_eq!(partitions[2], "2024-01-03");

            // Delete a partition
            runner.delete_partition("2024-01-02").await.unwrap();

            // Verify deletion
            let remaining = runner.list_partitions().await.unwrap();
            assert_eq!(remaining.len(), 2);
            assert!(!remaining.contains(&"2024-01-02".to_string()));
        })
        .await;
}

#[tokio::test]
async fn test_analyzer_count() {
    let temp_dir = TempDir::new().unwrap();
    let state_store = FileSystemStateStore::new(temp_dir.path()).unwrap();

    let runner = IncrementalAnalysisRunner::new(Box::new(state_store))
        .add_analyzer(SizeAnalyzer::new())
        .add_analyzer(MeanAnalyzer::new("value"))
        .add_analyzer(CompletenessAnalyzer::new("value"));

    assert_eq!(runner.analyzer_count(), 3);
}

#[cfg(test)]
mod state_store_tests {
    use super::*;

    #[tokio::test]
    async fn test_filesystem_state_store_basic() {
        let temp_dir = TempDir::new().unwrap();
        let store = FileSystemStateStore::new(temp_dir.path()).unwrap();

        // Create test state
        let mut state_map = StateMap::new();
        state_map.insert("analyzer1".to_string(), vec![1, 2, 3]);
        state_map.insert("analyzer2".to_string(), vec![4, 5, 6]);

        // Save state
        store
            .save_state("partition1", state_map.clone())
            .await
            .unwrap();

        // Load state
        let loaded = store.load_state("partition1").await.unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded.get("analyzer1").unwrap(), &vec![1, 2, 3]);
        assert_eq!(loaded.get("analyzer2").unwrap(), &vec![4, 5, 6]);
    }

    #[tokio::test]
    async fn test_filesystem_state_store_batch_load() {
        let temp_dir = TempDir::new().unwrap();
        let store = FileSystemStateStore::new(temp_dir.path()).unwrap();

        // Save states for multiple partitions
        for i in 1..=3 {
            let mut state_map = StateMap::new();
            state_map.insert("analyzer".to_string(), vec![i]);
            store
                .save_state(&format!("partition{i}"), state_map)
                .await
                .unwrap();
        }

        // Batch load
        let partitions = vec![
            "partition1".to_string(),
            "partition2".to_string(),
            "partition3".to_string(),
        ];
        let batch_result = store.load_states_batch(&partitions).await.unwrap();

        assert_eq!(batch_result.len(), 3);
        assert_eq!(
            batch_result
                .get("partition1")
                .unwrap()
                .get("analyzer")
                .unwrap(),
            &vec![1]
        );
        assert_eq!(
            batch_result
                .get("partition2")
                .unwrap()
                .get("analyzer")
                .unwrap(),
            &vec![2]
        );
        assert_eq!(
            batch_result
                .get("partition3")
                .unwrap()
                .get("analyzer")
                .unwrap(),
            &vec![3]
        );
    }

    #[tokio::test]
    async fn test_filesystem_state_store_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let store = FileSystemStateStore::new(temp_dir.path()).unwrap();

        // Load nonexistent partition
        let result = store.load_state("nonexistent").await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_filesystem_state_store_delete() {
        let temp_dir = TempDir::new().unwrap();
        let store = FileSystemStateStore::new(temp_dir.path()).unwrap();

        // Save state
        let mut state_map = StateMap::new();
        state_map.insert("analyzer".to_string(), vec![1, 2, 3]);
        store.save_state("to_delete", state_map).await.unwrap();

        // Verify it exists
        let partitions = store.list_partitions().await.unwrap();
        assert!(partitions.contains(&"to_delete".to_string()));

        // Delete it
        store.delete_partition("to_delete").await.unwrap();

        // Verify deletion
        let partitions = store.list_partitions().await.unwrap();
        assert!(!partitions.contains(&"to_delete".to_string()));
    }
}
