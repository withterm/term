#![cfg(feature = "cloud")]

use std::collections::HashMap;
use std::time::Duration;

use term_guard::analyzers::{AnalyzerContext, MetricValue};
use term_guard::cloud::{
    AlertPayload, AlertSeverity, CloudConfig, CloudMetadata, CloudMetric, CloudMetricValue,
    CloudResultKey, CloudValidationIssue, CloudValidationResult, TermCloudRepository,
};
use term_guard::repository::{MetricsRepository, ResultKey};

#[tokio::test]
async fn test_full_cloud_flow() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let cache_path = temp_dir.path().join("test_cache.db");

    let config = CloudConfig::new("test-api-key-12345")
        .with_endpoint("http://localhost:1")
        .with_buffer_size(100)
        .with_flush_interval(Duration::from_millis(100));

    let mut repository = TermCloudRepository::new(config).expect("Failed to create repository");
    repository
        .setup_cache(Some(&cache_path))
        .expect("Failed to setup cache");

    let mut context = AnalyzerContext::with_dataset("test_dataset");
    context.store_metric("completeness.user_id", MetricValue::Double(0.98));
    context.store_metric("size", MetricValue::Long(1000));
    context.store_metric("is_valid", MetricValue::Boolean(true));

    let key = ResultKey::now()
        .with_tag("env", "test")
        .with_tag("version", "1.0.0");

    repository
        .save(key, context)
        .await
        .expect("Failed to save metrics");

    assert_eq!(repository.pending_count().await, 1);

    let stats = repository.shutdown().await.expect("Failed to shutdown");
    assert!(stats.is_some());
}

#[test]
fn test_cloud_metric_wire_format() {
    let metric = CloudMetric {
        result_key: CloudResultKey {
            dataset_date: 1704931200000,
            tags: vec![
                ("env".to_string(), "production".to_string()),
                ("region".to_string(), "us-east-1".to_string()),
            ]
            .into_iter()
            .collect(),
        },
        metrics: vec![
            (
                "completeness.user_id".to_string(),
                CloudMetricValue::Double(0.98),
            ),
            ("size".to_string(), CloudMetricValue::Long(5000)),
            ("is_valid".to_string(), CloudMetricValue::Boolean(true)),
        ]
        .into_iter()
        .collect(),
        metadata: CloudMetadata {
            dataset_name: Some("orders_table".to_string()),
            start_time: Some("2024-01-10T12:00:00Z".to_string()),
            end_time: Some("2024-01-10T12:05:00Z".to_string()),
            term_version: "0.0.2".to_string(),
            custom: HashMap::new(),
        },
        validation_result: None,
    };

    let json = serde_json::to_string(&metric).expect("Failed to serialize metric");

    assert!(json.contains("result_key"));
    assert!(json.contains("dataset_date"));
    assert!(json.contains("1704931200000"));
    assert!(json.contains("metrics"));
    assert!(json.contains("completeness.user_id"));
    assert!(json.contains("metadata"));
    assert!(json.contains("dataset_name"));
    assert!(json.contains("orders_table"));
    assert!(json.contains("term_version"));

    let parsed: serde_json::Value = serde_json::from_str(&json).expect("Failed to parse JSON");

    assert!(parsed["result_key"]["dataset_date"].is_number());
    assert!(parsed["result_key"]["tags"].is_object());
    assert!(parsed["metrics"].is_object());
    assert!(parsed["metadata"]["dataset_name"].is_string());
}

#[test]
fn test_webhook_alert_generation() {
    let validation_result = CloudValidationResult {
        status: "error".to_string(),
        total_checks: 10,
        passed_checks: 3,
        failed_checks: 7,
        issues: vec![
            CloudValidationIssue {
                check_name: "DataQuality".to_string(),
                constraint_name: "Completeness".to_string(),
                level: "error".to_string(),
                message: "Column 'user_id' has 15% null values".to_string(),
                metric: Some(0.85),
            },
            CloudValidationIssue {
                check_name: "DataQuality".to_string(),
                constraint_name: "Uniqueness".to_string(),
                level: "error".to_string(),
                message: "Column 'email' has duplicate values".to_string(),
                metric: Some(0.92),
            },
        ],
    };

    let payload =
        AlertPayload::from_validation_result(&validation_result, "orders_table", "production");

    assert_eq!(payload.severity, AlertSeverity::Critical);
    assert!(payload.title.contains("Critical") || payload.title.contains("Failed"));
    assert_eq!(payload.dataset, "orders_table");
    assert_eq!(payload.environment, "production");
    assert_eq!(payload.summary.total_checks, 10);
    assert_eq!(payload.summary.passed, 3);
    assert_eq!(payload.summary.failed, 7);

    assert!(payload.details.is_some());
    let details = payload.details.unwrap();
    assert_eq!(details.len(), 2);
    assert_eq!(details[0].check, "DataQuality");
    assert_eq!(details[0].constraint, "Completeness");
    assert_eq!(details[0].metric, Some(0.85));
}

#[test]
fn test_config_builder() {
    let config = CloudConfig::new("my-api-key")
        .with_endpoint("https://custom.endpoint.com")
        .with_timeout(Duration::from_secs(60))
        .with_max_retries(5)
        .with_buffer_size(5000)
        .with_batch_size(200)
        .with_flush_interval(Duration::from_secs(10))
        .with_offline_cache_path("/tmp/test_cache.db");

    assert_eq!(config.api_key().expose(), "my-api-key");
    assert_eq!(config.endpoint(), "https://custom.endpoint.com");
    assert_eq!(config.timeout(), Duration::from_secs(60));
    assert_eq!(config.max_retries(), 5);
    assert_eq!(config.buffer_size(), 5000);
    assert_eq!(config.batch_size(), 200);
    assert_eq!(config.flush_interval(), Duration::from_secs(10));
    assert_eq!(
        config.offline_cache_path(),
        Some(std::path::Path::new("/tmp/test_cache.db"))
    );
}

#[tokio::test]
async fn test_repository_with_multiple_metrics() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let cache_path = temp_dir.path().join("multi_metrics_cache.db");

    let config = CloudConfig::new("test-key")
        .with_endpoint("http://localhost:1")
        .with_buffer_size(50);

    let mut repository = TermCloudRepository::new(config).expect("Failed to create repository");
    repository
        .setup_cache(Some(&cache_path))
        .expect("Failed to setup cache");

    for i in 0..5 {
        let mut context = AnalyzerContext::with_dataset(format!("dataset_{}", i));
        context.store_metric(
            "completeness.col1",
            MetricValue::Double(0.9 + (i as f64) * 0.01),
        );
        context.store_metric("row_count", MetricValue::Long((i + 1) * 1000));

        let key = ResultKey::new(1704931200000 + i)
            .with_tag("batch", i.to_string())
            .with_tag("env", "test");

        repository
            .save(key, context)
            .await
            .expect("Failed to save metrics");
    }

    assert_eq!(repository.pending_count().await, 5);

    let _ = repository.shutdown().await;
}

#[test]
fn test_cloud_validation_result_serialization() {
    let result = CloudValidationResult {
        status: "warning".to_string(),
        total_checks: 5,
        passed_checks: 4,
        failed_checks: 1,
        issues: vec![CloudValidationIssue {
            check_name: "QualityCheck".to_string(),
            constraint_name: "PatternMatch".to_string(),
            level: "warning".to_string(),
            message: "Pattern mismatch in 2% of rows".to_string(),
            metric: Some(0.98),
        }],
    };

    let json = serde_json::to_string(&result).expect("Failed to serialize");

    assert!(json.contains("warning"));
    assert!(json.contains("total_checks"));
    assert!(json.contains("issues"));

    let deserialized: CloudValidationResult =
        serde_json::from_str(&json).expect("Failed to deserialize");

    assert_eq!(deserialized.status, "warning");
    assert_eq!(deserialized.total_checks, 5);
    assert_eq!(deserialized.failed_checks, 1);
    assert_eq!(deserialized.issues.len(), 1);
}

#[test]
fn test_alert_severity_levels() {
    let info_result = CloudValidationResult {
        status: "success".to_string(),
        total_checks: 10,
        passed_checks: 10,
        failed_checks: 0,
        issues: vec![],
    };
    let info_payload = AlertPayload::from_validation_result(&info_result, "test", "dev");
    assert_eq!(info_payload.severity, AlertSeverity::Info);

    let warning_result = CloudValidationResult {
        status: "warning".to_string(),
        total_checks: 10,
        passed_checks: 8,
        failed_checks: 2,
        issues: vec![],
    };
    let warning_payload = AlertPayload::from_validation_result(&warning_result, "test", "dev");
    assert_eq!(warning_payload.severity, AlertSeverity::Warning);

    let critical_result = CloudValidationResult {
        status: "error".to_string(),
        total_checks: 10,
        passed_checks: 3,
        failed_checks: 7,
        issues: vec![],
    };
    let critical_payload = AlertPayload::from_validation_result(&critical_result, "test", "dev");
    assert_eq!(critical_payload.severity, AlertSeverity::Critical);
}
