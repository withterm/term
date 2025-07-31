//! Integration tests for analyzers using TPC-H data.

use term_guard::analyzers::basic::*;
use term_guard::analyzers::{Analyzer, MetricValue};
use term_guard::test_fixtures::create_minimal_tpc_h_context;

#[tokio::test]
async fn test_analyzers_with_tpc_h_lineitem() {
    let ctx = create_minimal_tpc_h_context().await.unwrap();

    // Create a view named "data" pointing to the lineitem table
    ctx.sql("CREATE VIEW data AS SELECT * FROM lineitem")
        .await
        .unwrap();

    // Test SizeAnalyzer on lineitem table
    let size_analyzer = SizeAnalyzer::new();
    let state = size_analyzer.compute_state_from_data(&ctx).await.unwrap();
    let metric = size_analyzer.compute_metric_from_state(&state).unwrap();

    if let MetricValue::Long(count) = metric {
        println!("Lineitem table has {count} rows");
        // TPC-H test data should have some reasonable number of rows
        assert!(count > 0);
        assert!(count < 1_000_000); // Reasonable upper bound
    } else {
        panic!("Expected Long metric from SizeAnalyzer");
    }

    // Test CompletenessAnalyzer on l_quantity column
    let completeness_analyzer = CompletenessAnalyzer::new("l_quantity");
    let state = completeness_analyzer
        .compute_state_from_data(&ctx)
        .await
        .unwrap();
    let metric = completeness_analyzer
        .compute_metric_from_state(&state)
        .unwrap();

    if let MetricValue::Double(completeness) = metric {
        // l_quantity should be fully complete in TPC-H data
        assert_eq!(completeness, 1.0);
        println!("l_quantity completeness: {:.2}%", completeness * 100.0);
    }

    // Test DistinctnessAnalyzer on l_orderkey
    let distinctness_analyzer = DistinctnessAnalyzer::new("l_orderkey");
    let state = distinctness_analyzer
        .compute_state_from_data(&ctx)
        .await
        .unwrap();
    let metric = distinctness_analyzer
        .compute_metric_from_state(&state)
        .unwrap();

    if let MetricValue::Double(distinctness) = metric {
        // l_orderkey has some repetition (multiple line items per order)
        assert!(distinctness > 0.0);
        assert!(distinctness < 1.0);
        println!("l_orderkey distinctness: {:.2}%", distinctness * 100.0);
    }

    // Test numeric analyzers on l_extendedprice
    let mean_analyzer = MeanAnalyzer::new("l_extendedprice");
    let state = mean_analyzer.compute_state_from_data(&ctx).await.unwrap();
    let metric = mean_analyzer.compute_metric_from_state(&state).unwrap();

    if let MetricValue::Double(mean) = metric {
        // Extended price should be reasonable
        assert!(mean > 0.0);
        assert!(mean < 100000.0);
        println!("Average extended price: ${mean:.2}");
    }

    let min_analyzer = MinAnalyzer::new("l_extendedprice");
    let state = min_analyzer.compute_state_from_data(&ctx).await.unwrap();
    let metric = min_analyzer.compute_metric_from_state(&state).unwrap();

    if let MetricValue::Double(min) = metric {
        assert!(min >= 0.0);
        println!("Minimum extended price: ${min:.2}");
    }

    let max_analyzer = MaxAnalyzer::new("l_extendedprice");
    let state = max_analyzer.compute_state_from_data(&ctx).await.unwrap();
    let metric = max_analyzer.compute_metric_from_state(&state).unwrap();

    if let MetricValue::Double(max) = metric {
        assert!(max > 0.0);
        println!("Maximum extended price: ${max:.2}");
    }

    let sum_analyzer = SumAnalyzer::new("l_extendedprice");
    let state = sum_analyzer.compute_state_from_data(&ctx).await.unwrap();
    let metric = sum_analyzer.compute_metric_from_state(&state).unwrap();

    if let MetricValue::Double(sum) = metric {
        assert!(sum > 0.0);
        println!("Total extended price: ${sum:.2}");
    }
}

#[tokio::test]
async fn test_analyzer_performance_characteristics() {
    let ctx = create_minimal_tpc_h_context().await.unwrap();

    // Create a view named "data" pointing to the lineitem table
    ctx.sql("CREATE VIEW data AS SELECT * FROM lineitem")
        .await
        .unwrap();

    // Measure time for different analyzers
    use std::time::Instant;

    // Size should be very fast (simple COUNT(*))
    let start = Instant::now();
    let size_analyzer = SizeAnalyzer::new();
    let _ = size_analyzer.compute_state_from_data(&ctx).await.unwrap();
    let size_duration = start.elapsed();

    // Distinctness is more expensive (COUNT DISTINCT)
    let start = Instant::now();
    let distinct_analyzer = DistinctnessAnalyzer::new("l_orderkey");
    let _ = distinct_analyzer
        .compute_state_from_data(&ctx)
        .await
        .unwrap();
    let distinct_duration = start.elapsed();

    // Print timing info for manual verification
    // Note: Performance characteristics can vary by system
    println!("Performance comparison: size={size_duration:?}, distinct={distinct_duration:?}");

    println!("Size analyzer took: {size_duration:?}");
    println!("Distinctness analyzer took: {distinct_duration:?}");
}
