//! Integration tests for the metrics repository framework.

use std::collections::HashMap;

use datafusion::prelude::*;
use term_guard::analyzers::context::AnalyzerContext;
use term_guard::analyzers::types::MetricValue;
use term_guard::repository::{InMemoryRepository, MetricsRepository, ResultKey, SortOrder};

/// Helper function to create a test context with sample data.
#[allow(dead_code)]
async fn create_test_context() -> SessionContext {
    let ctx = SessionContext::new();

    // Create a simple test table
    let sql = r#"
        CREATE TABLE test_data (
            id INT,
            name VARCHAR,
            value FLOAT,
            status VARCHAR
        ) AS VALUES
        (1, 'Alice', 100.5, 'active'),
        (2, 'Bob', 200.0, 'active'),
        (3, 'Charlie', NULL, 'inactive'),
        (4, 'David', 150.75, 'active'),
        (5, NULL, 300.0, 'pending')
    "#;

    ctx.sql(sql).await.unwrap();
    ctx
}

/// Tests basic repository operations with analyzer results.
#[tokio::test]
async fn test_repository_with_analyzer_results() {
    let repo = InMemoryRepository::new();

    // Create multiple contexts with different metrics
    for i in 0..3 {
        let mut context = AnalyzerContext::with_dataset(format!("dataset_{i}"));

        // Add various metrics
        context.store_metric("row_count", MetricValue::Long((i + 1) * 100));
        context.store_metric("completeness", MetricValue::Double(0.95 - (i as f64 * 0.1)));
        context.store_analyzer_metric("size", "bytes", MetricValue::Long((i + 1) * 1024));

        // Save with tags
        let key = ResultKey::new((i + 1) * 1000)
            .with_tag("dataset", format!("dataset_{i}"))
            .with_tag("environment", "test")
            .with_tag("version", "1.0.0");

        repo.save(key, context).await.unwrap();
    }

    // Verify all data was saved
    assert_eq!(repo.size().await, 3);

    // Query by dataset tag
    let results = repo
        .load()
        .await
        .with_tag("dataset", "dataset_0")
        .execute()
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    let (key, context) = &results[0];
    assert_eq!(key.get_tag("dataset"), Some("dataset_0"));

    // Verify metrics are present
    assert!(context.get_metric("row_count").is_some());
    assert_eq!(
        context.get_metric("row_count"),
        Some(&MetricValue::Long(100))
    );
}

/// Tests time-based queries with repository.
#[tokio::test]
async fn test_repository_time_queries() {
    let repo = InMemoryRepository::new();

    // Generate metrics at different timestamps
    let base_time = chrono::Utc::now().timestamp_millis();

    for i in 0..5 {
        let mut context = AnalyzerContext::new();
        context.store_metric("iteration", MetricValue::Long(i));
        context.store_metric("timestamp", MetricValue::Long(base_time + (i * 3600000)));

        let key = ResultKey::new(base_time + (i * 3600000)) // 1 hour apart
            .with_tag("run_id", format!("run_{i}"));

        repo.save(key, context).await.unwrap();
    }

    // Query for metrics in the last 3 hours
    let three_hours_ago = base_time + (2 * 3600000);
    let results = repo
        .load()
        .await
        .after(three_hours_ago)
        .execute()
        .await
        .unwrap();

    assert_eq!(results.len(), 3); // Should get runs 2, 3, 4

    // Query for metrics between specific times
    let start = base_time + 3600000; // After first hour
    let end = base_time + (4 * 3600000); // Before last hour

    let results = repo
        .load()
        .await
        .between(start, end)
        .execute()
        .await
        .unwrap();

    assert_eq!(results.len(), 3); // Should get runs 1, 2, 3
}

/// Tests analyzer-specific filtering in queries.
#[tokio::test]
async fn test_repository_analyzer_filtering() {
    let repo = InMemoryRepository::new();

    // Create contexts with different analyzer metrics
    let analyzer_types = ["size", "completeness", "mean", "uniqueness"];

    for (i, analyzer_type) in analyzer_types.iter().enumerate() {
        let mut context = AnalyzerContext::new();

        // Store metrics with analyzer-specific keys
        context.store_analyzer_metric(analyzer_type, "value", MetricValue::Double(i as f64));
        context.store_analyzer_metric(analyzer_type, "confidence", MetricValue::Double(0.99));

        let key = ResultKey::new((i as i64 + 1) * 1000).with_tag("analyzer_type", *analyzer_type);

        repo.save(key, context).await.unwrap();
    }

    // Query for specific analyzer results
    let results = repo
        .load()
        .await
        .for_analyzers(vec!["size", "mean"])
        .execute()
        .await
        .unwrap();

    // Should return results that have size or mean metrics
    assert_eq!(results.len(), 2);
}

/// Tests pagination and sorting.
#[tokio::test]
async fn test_repository_pagination_and_sorting() {
    let repo = InMemoryRepository::new();

    // Create 10 metrics with different timestamps
    for i in 0..10 {
        let mut context = AnalyzerContext::new();
        context.store_metric("index", MetricValue::Long(i));

        let key =
            ResultKey::new(i * 1000).with_tag("batch", if i < 5 { "first" } else { "second" });

        repo.save(key, context).await.unwrap();
    }

    // Test ascending sort
    let results = repo
        .load()
        .await
        .sort(SortOrder::Ascending)
        .limit(3)
        .execute()
        .await
        .unwrap();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0.timestamp, 0);
    assert_eq!(results[1].0.timestamp, 1000);
    assert_eq!(results[2].0.timestamp, 2000);

    // Test descending sort with offset
    let results = repo
        .load()
        .await
        .sort(SortOrder::Descending)
        .offset(2)
        .limit(3)
        .execute()
        .await
        .unwrap();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0.timestamp, 7000);
    assert_eq!(results[1].0.timestamp, 6000);
    assert_eq!(results[2].0.timestamp, 5000);
}

/// Tests complex queries with multiple filters.
#[tokio::test]
async fn test_repository_complex_queries() {
    let repo = InMemoryRepository::new();

    // Generate diverse test data
    let environments = ["dev", "staging", "prod"];
    let datasets = ["users", "orders", "products"];
    let base_time = chrono::Utc::now().timestamp_millis();

    for (i, env) in environments.iter().enumerate() {
        for (j, dataset) in datasets.iter().enumerate() {
            let mut context = AnalyzerContext::with_dataset(dataset.to_string());
            context.store_metric(
                "record_count",
                MetricValue::Long(((i + 1) * (j + 1) * 100) as i64),
            );
            context.store_metric("processing_time", MetricValue::Double((i + j) as f64 * 0.5));

            let key = ResultKey::new(base_time + ((i * datasets.len() + j) as i64 * 1000))
                .with_tag("environment", *env)
                .with_tag("dataset", *dataset)
                .with_tag("version", format!("v1.{i}.{j}"));

            repo.save(key, context).await.unwrap();
        }
    }

    // Complex query: prod environment, users dataset, recent data
    let one_minute_ago = base_time - 60000;
    let results = repo
        .load()
        .await
        .after(one_minute_ago)
        .with_tag("environment", "prod")
        .with_tag("dataset", "users")
        .execute()
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    let (key, _) = &results[0];
    assert_eq!(key.get_tag("environment"), Some("prod"));
    assert_eq!(key.get_tag("dataset"), Some("users"));
}

/// Tests the exists and count query methods.
#[tokio::test]
async fn test_repository_exists_and_count() {
    let repo = InMemoryRepository::new();

    // Add test data
    for i in 0..5 {
        let context = AnalyzerContext::new();
        let key =
            ResultKey::new(i * 1000).with_tag("type", if i % 2 == 0 { "even" } else { "odd" });

        repo.save(key, context).await.unwrap();
    }

    // Test exists
    let exists = repo
        .load()
        .await
        .with_tag("type", "even")
        .exists()
        .await
        .unwrap();
    assert!(exists);

    let not_exists = repo
        .load()
        .await
        .with_tag("type", "nonexistent")
        .exists()
        .await
        .unwrap();
    assert!(!not_exists);

    // Test count
    let count = repo
        .load()
        .await
        .with_tag("type", "odd")
        .count()
        .await
        .unwrap();
    assert_eq!(count, 2);

    let total_count = repo.load().await.count().await.unwrap();
    assert_eq!(total_count, 5);
}

/// Tests edge cases and error handling.
#[tokio::test]
async fn test_repository_edge_cases() {
    let repo = InMemoryRepository::new();

    // Test empty repository queries
    let results = repo.load().await.execute().await.unwrap();
    assert!(results.is_empty());

    // Test deleting non-existent key
    let key = ResultKey::now().with_tag("test", "nonexistent");
    let delete_result = repo.delete(key.clone()).await;
    assert!(delete_result.is_err());

    // Test exists on non-existent key
    let exists = repo.exists(&key).await.unwrap();
    assert!(!exists);

    // Test invalid time range (before < after) - should return an error
    let result = repo.load().await.after(2000).before(1000).execute().await;
    assert!(result.is_err());

    // Test with empty tag filter
    let empty_tags: HashMap<String, String> = HashMap::new();
    let results = repo
        .load()
        .await
        .with_tags(empty_tags)
        .execute()
        .await
        .unwrap();
    assert!(results.is_empty());
}

/// Tests repository metadata tracking.
#[tokio::test]
async fn test_repository_metadata_tracking() {
    let repo = InMemoryRepository::new();

    // Check initial metadata
    let metadata = repo.metadata().await.unwrap();
    assert_eq!(metadata.backend_type, Some("in_memory".to_string()));
    assert_eq!(metadata.total_metrics, Some(0));

    // Add metrics and verify metadata updates
    for i in 0..3 {
        let context = AnalyzerContext::new();
        let key = ResultKey::new(i * 1000);
        repo.save(key, context).await.unwrap();
    }

    let metadata = repo.metadata().await.unwrap();
    assert_eq!(metadata.total_metrics, Some(3));
    assert!(metadata.last_modified.is_some());
    assert!(metadata.storage_size_bytes.unwrap() > 0);

    // Delete a metric and verify update
    let key = ResultKey::new(1000);
    repo.delete(key).await.unwrap();

    let metadata = repo.metadata().await.unwrap();
    assert_eq!(metadata.total_metrics, Some(2));
}

/// Tests ResultKey functionality in repository context.
#[tokio::test]
async fn test_result_key_features() {
    let repo = InMemoryRepository::new();

    // Test storage key conversion
    let key = ResultKey::new(1234567890)
        .with_tag("env", "prod")
        .with_tag("region", "us-west-2");

    let storage_key = key.to_storage_key();
    assert!(storage_key.starts_with("1234567890"));

    // Save and retrieve using the key
    let context = AnalyzerContext::new();
    repo.save(key.clone(), context).await.unwrap();

    // Verify the key can be found
    assert!(repo.exists(&key).await.unwrap());

    // Test key matching with tags
    let results = repo
        .load()
        .await
        .with_tag("env", "prod")
        .with_tag("region", "us-west-2")
        .execute()
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, key);
}

/// Tests concurrent access to the repository.
#[tokio::test]
async fn test_repository_concurrent_access() {
    use std::sync::Arc;
    use tokio::task::JoinHandle;

    let repo = Arc::new(InMemoryRepository::new());

    // Spawn multiple tasks to write concurrently
    let mut handles: Vec<JoinHandle<()>> = vec![];

    for i in 0..10 {
        let repo_clone = repo.clone();
        let handle = tokio::spawn(async move {
            let mut context = AnalyzerContext::new();
            context.store_metric("task_id", MetricValue::Long(i));

            let key = ResultKey::new(i * 1000).with_tag("task", format!("task_{i}"));

            repo_clone.save(key, context).await.unwrap();
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify all data was saved
    assert_eq!(repo.size().await, 10);

    // Verify each task's data is present
    for i in 0..10 {
        let results = repo
            .load()
            .await
            .with_tag("task", format!("task_{i}"))
            .execute()
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].1.get_metric("task_id"),
            Some(&MetricValue::Long(i))
        );
    }
}
