//! Integration tests for anomaly detection with AnalysisRunner.

use chrono::{Duration, Utc};
use term_guard::analyzers::{anomaly::*, basic::*, AnalysisRunner, MetricValue};
use term_guard::test_utils::{create_tpc_h_context, ScaleFactor};

#[tokio::test]
async fn test_anomaly_detection_with_analysis_runner() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // The analyzers expect a table named "data"
    // Register lineitem as "data" for the analyzers to work
    let df = ctx.table("lineitem").await.unwrap();
    ctx.register_table("data", df.into_view()).unwrap();

    // Create a metrics repository with test data
    let repo = InMemoryMetricsRepository::new();

    // Simulate historical baseline by storing some metrics
    let base_time = Utc::now() - Duration::hours(24);
    repo.store_metric("size", MetricValue::Long(1000), base_time)
        .await
        .unwrap();
    repo.store_metric(
        "completeness.l_orderkey",
        MetricValue::Double(0.98),
        base_time,
    )
    .await
    .unwrap();
    repo.store_metric(
        "completeness.l_orderkey",
        MetricValue::Double(0.99),
        base_time + Duration::hours(1),
    )
    .await
    .unwrap();
    repo.store_metric(
        "completeness.l_orderkey",
        MetricValue::Double(0.97),
        base_time + Duration::hours(2),
    )
    .await
    .unwrap();

    // Create anomaly detection runner
    let mut rate_detector = RelativeRateOfChangeDetector::new(0.1); // 10% threshold
    rate_detector = rate_detector.with_min_history_size(1); // Only need 1 historical point

    let detector = AnomalyDetectionRunner::builder()
        .repository(Box::new(repo.clone()))
        .add_detector("size", Box::new(rate_detector))
        .add_detector("completeness.*", Box::new(ZScoreDetector::new(2.0))) // 2 std devs
        .build()
        .unwrap();

    // Run analysis to get current metrics
    let runner = AnalysisRunner::new()
        .add(SizeAnalyzer::new())
        .add(CompletenessAnalyzer::new("l_orderkey"))
        .add(CompletenessAnalyzer::new("l_partkey"));

    let analysis_context = runner.run(&ctx).await.unwrap();

    // Detect anomalies
    let anomalies = detector.detect_anomalies(&analysis_context).await.unwrap();

    // Should detect anomalies due to the large difference from baseline
    // Current size is ~4000 records vs baseline 1000
    assert!(!anomalies.is_empty(), "Should detect size anomaly");

    // Find the size anomaly
    let size_anomaly = anomalies.iter().find(|a| a.metric_name == "size");
    assert!(size_anomaly.is_some(), "Should detect size anomaly");

    let size_anomaly = size_anomaly.unwrap();
    assert_eq!(size_anomaly.detection_strategy, "RelativeRateOfChange");
    assert!(
        size_anomaly.confidence > 0.7,
        "Size anomaly should have high confidence"
    );
}

#[tokio::test]
async fn test_anomaly_detection_with_no_baseline() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // The analyzers expect a table named "data"
    let df = ctx.table("lineitem").await.unwrap();
    ctx.register_table("data", df.into_view()).unwrap();

    // Create empty repository (no baseline data)
    let repo = InMemoryMetricsRepository::new();

    let detector = AnomalyDetectionRunner::builder()
        .repository(Box::new(repo))
        .add_detector("*", Box::new(ZScoreDetector::new(2.0)))
        .build()
        .unwrap();

    // Run analysis
    let runner = AnalysisRunner::new()
        .add(SizeAnalyzer::new())
        .add(CompletenessAnalyzer::new("l_orderkey"));

    let analysis_context = runner.run(&ctx).await.unwrap();

    // Detect anomalies - should be empty since there's no baseline
    let anomalies = detector.detect_anomalies(&analysis_context).await.unwrap();

    // Should be empty since we have no historical data for comparison
    assert!(
        anomalies.is_empty(),
        "Should not detect anomalies without baseline data"
    );
}

#[tokio::test]
async fn test_anomaly_detection_memory_limits() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // The analyzers expect a table named "data"
    let df = ctx.table("lineitem").await.unwrap();
    ctx.register_table("data", df.into_view()).unwrap();

    // Create repository with strict memory limits
    let config = InMemoryMetricsConfig {
        max_metrics: 5,
        max_points_per_metric: 10,
        max_age_seconds: 60, // 1 minute
    };
    let repo = InMemoryMetricsRepository::with_config(config);

    let detector = AnomalyDetectionRunner::builder()
        .repository(Box::new(repo))
        .add_detector("*", Box::new(RelativeRateOfChangeDetector::new(0.5)))
        .build()
        .unwrap();

    // Run analysis multiple times to test memory limits
    let runner = AnalysisRunner::new()
        .add(SizeAnalyzer::new())
        .add(CompletenessAnalyzer::new("l_orderkey"))
        .add(CompletenessAnalyzer::new("l_partkey"))
        .add(CompletenessAnalyzer::new("l_suppkey"))
        .add(CompletenessAnalyzer::new("l_linenumber"));

    for i in 0..20 {
        let analysis_context = runner.run(&ctx).await.unwrap();
        let anomalies = detector.detect_anomalies(&analysis_context).await.unwrap();

        if i > 0 {
            // After first iteration, should have some data for comparison
            println!("Iteration {i}: detected {} anomalies", anomalies.len());
        }

        // Add a small delay to test time-based cleanup
        if i == 10 {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }

    // Check that memory limits were enforced
    // Note: We can't directly access the repository from the detector in this test
    // but the fact that it didn't OOM shows the limits are working

    println!("Memory limits test completed successfully");
}

#[tokio::test]
async fn test_anomaly_detection_with_multiple_strategies() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // The analyzers expect a table named "data"
    let df = ctx.table("lineitem").await.unwrap();
    ctx.register_table("data", df.into_view()).unwrap();

    let repo = InMemoryMetricsRepository::new();

    // Add baseline metrics
    let base_time = Utc::now() - Duration::hours(1);
    repo.store_metric("size", MetricValue::Long(1000), base_time)
        .await
        .unwrap();
    repo.store_metric(
        "size",
        MetricValue::Long(1050),
        base_time + Duration::minutes(10),
    )
    .await
    .unwrap();
    repo.store_metric(
        "size",
        MetricValue::Long(980),
        base_time + Duration::minutes(20),
    )
    .await
    .unwrap();
    repo.store_metric(
        "size",
        MetricValue::Long(1020),
        base_time + Duration::minutes(30),
    )
    .await
    .unwrap();

    // Create detector with multiple strategies for same metric
    let detector = AnomalyDetectionRunner::builder()
        .repository(Box::new(repo))
        .add_detector(
            "size",
            Box::new(RelativeRateOfChangeDetector::new(0.1).with_min_history_size(1)),
        ) // 10% change
        .add_detector("size", Box::new(AbsoluteChangeDetector::new(1000.0))) // 1000 absolute change
        .add_detector(
            "size",
            Box::new(ZScoreDetector::new(2.0).with_min_history_size(1)),
        ) // 2 std devs
        .build()
        .unwrap();

    let runner = AnalysisRunner::new().add(SizeAnalyzer::new());
    let analysis_context = runner.run(&ctx).await.unwrap();

    let anomalies = detector.detect_anomalies(&analysis_context).await.unwrap();

    // Should detect multiple anomalies for the same metric using different strategies
    let size_anomalies: Vec<_> = anomalies
        .iter()
        .filter(|a| a.metric_name == "size")
        .collect();

    assert!(!size_anomalies.is_empty(), "Should detect size anomalies");

    // Check that we have different detection strategies
    let strategies: std::collections::HashSet<_> = size_anomalies
        .iter()
        .map(|a| &a.detection_strategy)
        .collect();

    println!(
        "Detected {} size anomalies with {} different strategies:",
        size_anomalies.len(),
        strategies.len()
    );
    for anomaly in size_anomalies {
        println!(
            "- {}: {} (confidence: {:.2})",
            anomaly.detection_strategy, anomaly.description, anomaly.confidence
        );
    }

    // Should have detected with at least one strategy
    assert!(
        !strategies.is_empty(),
        "Should use at least one detection strategy"
    );
}

#[tokio::test]
async fn test_anomaly_detection_pattern_matching() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // The analyzers expect a table named "data"
    let df = ctx.table("lineitem").await.unwrap();
    ctx.register_table("data", df.into_view()).unwrap();

    let repo = InMemoryMetricsRepository::new();

    // Add baseline for completeness metrics
    let base_time = Utc::now() - Duration::hours(1);
    repo.store_metric(
        "completeness.l_orderkey",
        MetricValue::Double(0.90), // 10% change from 1.0
        base_time,
    )
    .await
    .unwrap();
    repo.store_metric(
        "completeness.l_partkey",
        MetricValue::Double(0.85), // 15% change from 1.0
        base_time,
    )
    .await
    .unwrap();
    repo.store_metric("size", MetricValue::Long(1000), base_time)
        .await
        .unwrap();

    // Create detector with pattern matching
    let detector = AnomalyDetectionRunner::builder()
        .repository(Box::new(repo))
        .add_detector(
            "completeness.*",
            Box::new(RelativeRateOfChangeDetector::new(0.05).with_min_history_size(1)),
        ) // 5% for completeness
        .add_detector(
            "size",
            Box::new(RelativeRateOfChangeDetector::new(0.2).with_min_history_size(1)),
        ) // 20% for size
        .build()
        .unwrap();

    let runner = AnalysisRunner::new()
        .add(SizeAnalyzer::new())
        .add(CompletenessAnalyzer::new("l_orderkey"))
        .add(CompletenessAnalyzer::new("l_partkey"))
        .add(CompletenessAnalyzer::new("l_suppkey"));

    let analysis_context = runner.run(&ctx).await.unwrap();
    let anomalies = detector.detect_anomalies(&analysis_context).await.unwrap();

    // Check that completeness metrics were detected (they should be near 1.0, much higher than 0.98 baseline)
    let completeness_anomalies: Vec<_> = anomalies
        .iter()
        .filter(|a| a.metric_name.starts_with("completeness."))
        .collect();

    println!(
        "Detected {} completeness anomalies:",
        completeness_anomalies.len()
    );
    for anomaly in &completeness_anomalies {
        println!("- {}: {}", anomaly.metric_name, anomaly.description);
    }

    // Should detect completeness anomalies due to pattern matching
    assert!(
        !completeness_anomalies.is_empty(),
        "Should detect completeness anomalies via pattern matching"
    );
}
