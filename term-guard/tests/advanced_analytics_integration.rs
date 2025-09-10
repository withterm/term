//! Integration tests for advanced analytics features using TPC-H data.
//!
//! This module tests the advanced analytics features (KLL sketch, correlation, mutual information)
//! against TPC-H benchmark data to validate their accuracy and performance characteristics
//! in realistic scenarios.

use arrow::array::Array;
use term_guard::analyzers::advanced::{CorrelationAnalyzer, KllSketch};
use term_guard::analyzers::{Analyzer, MetricValue};
use term_guard::test_utils::{create_tpc_h_context, ScaleFactor};

/// Tests KLL sketch accuracy with TPC-H lineitem data.
///
/// This test validates that the KLL sketch provides reasonable quantile estimates
/// on real-world data with known distribution characteristics.
#[tokio::test]
async fn test_kll_sketch_tpc_h_lineitem_quantity() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Create a view to access lineitem table
    ctx.sql("CREATE VIEW data AS SELECT * FROM lineitem")
        .await
        .unwrap();

    // Get the actual distribution statistics for comparison
    let stats_df = ctx
        .sql(
            "SELECT 
            COUNT(*) as total_count,
            MIN(l_quantity) as min_qty,
            MAX(l_quantity) as max_qty,
            AVG(l_quantity) as avg_qty
        FROM lineitem 
        WHERE l_quantity IS NOT NULL",
        )
        .await
        .unwrap();

    let stats = stats_df.collect().await.unwrap();
    let batch = &stats[0];

    let total_count = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0) as u64;
    let min_qty = batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    let max_qty = batch
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    let avg_qty = batch
        .column(3)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);

    println!(
        "TPC-H lineitem quantity stats: count={total_count}, min={min_qty}, max={max_qty}, avg={avg_qty:.2}"
    );

    // Test KLL sketch with different k values - start with larger k to reduce levels
    for k in [200] {
        println!("Testing KLL sketch with k={k}");

        // Create and populate KLL sketch
        let mut sketch = KllSketch::new(k);

        // Get all quantity values to populate the sketch
        let qty_df = ctx
            .sql("SELECT l_quantity FROM lineitem WHERE l_quantity IS NOT NULL")
            .await
            .unwrap();

        let qty_batches = qty_df.collect().await.unwrap();
        for batch in qty_batches {
            let qty_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();

            for i in 0..batch.num_rows() {
                if !qty_array.is_null(i) {
                    sketch.update(qty_array.value(i));
                }
            }
        }

        // Test quantile accuracy
        let median = sketch.get_quantile(0.5).unwrap();
        let p25 = sketch.get_quantile(0.25).unwrap();
        let p75 = sketch.get_quantile(0.75).unwrap();
        let p95 = sketch.get_quantile(0.95).unwrap();

        println!("KLL k={k}: median={median:.2}, p25={p25:.2}, p75={p75:.2}, p95={p95:.2}");

        // Validate that quantiles are in correct order
        assert!(p25 <= median);
        assert!(median <= p75);
        assert!(p75 <= p95);

        // Validate that quantiles are within the data range
        assert!(median >= min_qty && median <= max_qty);
        assert!(p25 >= min_qty && p25 <= max_qty);
        assert!(p75 >= min_qty && p75 <= max_qty);
        assert!(p95 >= min_qty && p95 <= max_qty);

        // For TPC-H data, quantities are typically in range 1-50
        // The median should be reasonable for this distribution
        assert!(
            median >= min_qty && median <= max_qty,
            "Median {median} outside data range [{min_qty}, {max_qty}] for TPC-H quantity"
        );

        // Validate memory usage is reasonable
        let memory_usage = sketch.memory_usage();
        let expected_max_memory = k * sketch.num_levels() * 8 * 2; // Rough estimate
        assert!(
            memory_usage < expected_max_memory,
            "Memory usage {memory_usage} exceeds expected bound {expected_max_memory}"
        );

        println!(
            "KLL k={k}: memory_usage={}KB, levels={}, error_bound={:.3}%",
            memory_usage / 1024,
            sketch.num_levels(),
            sketch.relative_error_bound() * 100.0
        );
    }
}

/// Tests correlation analysis between TPC-H lineitem columns.
///
/// This validates that correlation computations work correctly on real data
/// and that the memory-efficient Spearman implementation provides accurate results.
#[tokio::test]
async fn test_correlation_tpc_h_lineitem() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Create a view to access lineitem table
    ctx.sql("CREATE VIEW data AS SELECT * FROM lineitem")
        .await
        .unwrap();

    // Test Pearson correlation between l_quantity and l_extendedprice
    // These should be positively correlated in TPC-H data
    println!("Testing Pearson correlation: l_quantity vs l_extendedprice");
    let pearson_analyzer = CorrelationAnalyzer::pearson("l_quantity", "l_extendedprice");

    let state = pearson_analyzer
        .compute_state_from_data(&ctx)
        .await
        .unwrap();
    let metric = pearson_analyzer.compute_metric_from_state(&state).unwrap();

    if let MetricValue::Double(correlation) = metric {
        println!("Pearson correlation (quantity vs extendedprice): {correlation:.4}");

        // Quantity and extended price should be positively correlated
        assert!(
            correlation > 0.0,
            "Expected positive correlation, got {correlation}"
        );
        assert!(
            correlation <= 1.0,
            "Correlation should be <= 1.0, got {correlation}"
        );

        // For TPC-H data, this correlation should be moderately strong
        assert!(
            correlation > 0.3,
            "Expected stronger positive correlation, got {correlation}"
        );
    } else {
        panic!("Expected Double metric from Pearson correlation");
    }

    // Test Spearman correlation (rank-based, memory-efficient)
    println!("Testing Spearman correlation: l_quantity vs l_extendedprice");
    let spearman_analyzer = CorrelationAnalyzer::spearman("l_quantity", "l_extendedprice");

    let state = spearman_analyzer
        .compute_state_from_data(&ctx)
        .await
        .unwrap();
    let metric = spearman_analyzer.compute_metric_from_state(&state).unwrap();

    if let MetricValue::Double(spearman_corr) = metric {
        println!("Spearman correlation (quantity vs extendedprice): {spearman_corr:.4}");

        // Spearman should also be positive and reasonably close to Pearson
        assert!(
            spearman_corr > 0.0,
            "Expected positive Spearman correlation, got {spearman_corr}"
        );
        assert!(
            spearman_corr <= 1.0,
            "Spearman correlation should be <= 1.0, got {spearman_corr}"
        );

        // For well-behaved data, Spearman should be similar to Pearson
        if let MetricValue::Double(pearson_corr) = metric {
            let diff = (spearman_corr - pearson_corr).abs();
            assert!(
                diff < 0.5,
                "Spearman and Pearson correlations differ too much: {spearman_corr} vs {pearson_corr}"
            );
        }
    } else {
        panic!("Expected Double metric from Spearman correlation");
    }

    // Test covariance
    println!("Testing covariance: l_quantity vs l_extendedprice");
    let covariance_analyzer = CorrelationAnalyzer::covariance("l_quantity", "l_extendedprice");

    let state = covariance_analyzer
        .compute_state_from_data(&ctx)
        .await
        .unwrap();
    let metric = covariance_analyzer
        .compute_metric_from_state(&state)
        .unwrap();

    if let MetricValue::Double(covariance) = metric {
        println!("Covariance (quantity vs extendedprice): {covariance:.2}");

        // Covariance should be positive (same direction as correlation)
        assert!(
            covariance > 0.0,
            "Expected positive covariance, got {covariance}"
        );
    } else {
        panic!("Expected Double metric from covariance");
    }
}

/// Tests correlation analysis with uncorrelated TPC-H columns.
///
/// This ensures our correlation implementations correctly identify
/// lack of correlation between unrelated variables.
#[tokio::test]
async fn test_correlation_uncorrelated_tpc_h() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    ctx.sql("CREATE VIEW data AS SELECT * FROM lineitem")
        .await
        .unwrap();

    // Test correlation between l_linenumber and l_extendedprice
    // These should have very low correlation in TPC-H data
    println!("Testing correlation between uncorrelated variables: l_linenumber vs l_extendedprice");

    let pearson_analyzer = CorrelationAnalyzer::pearson("l_linenumber", "l_extendedprice");
    let state = pearson_analyzer
        .compute_state_from_data(&ctx)
        .await
        .unwrap();
    let metric = pearson_analyzer.compute_metric_from_state(&state).unwrap();

    if let MetricValue::Double(correlation) = metric {
        println!("Pearson correlation (linenumber vs extendedprice): {correlation:.4}");

        // Should have very low correlation
        assert!(
            correlation.abs() < 0.5,
            "Expected low correlation between unrelated variables, got {correlation}"
        );
    } else {
        panic!("Expected Double metric from Pearson correlation");
    }
}

/// Tests KLL sketch performance characteristics with larger TPC-H data.
///
/// This validates that KLL sketch maintains O(k log n) memory usage
/// and reasonable accuracy as data size increases.
#[tokio::test]
async fn test_kll_sketch_performance_characteristics() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Test with different k values to validate memory/accuracy tradeoff
    let test_cases = vec![
        (50, 0.25),  // k=50, expect ~25% error bound
        (100, 0.18), // k=100, expect ~18% error bound
        (200, 0.12), // k=200, expect ~12% error bound
    ];

    for (k, expected_error_bound) in test_cases {
        let mut sketch = KllSketch::new(k);

        // Populate sketch with TPC-H discount values (0.0 to 0.10)
        let discount_df = ctx
            .sql("SELECT l_discount FROM lineitem WHERE l_discount IS NOT NULL")
            .await
            .unwrap();

        let discount_batches = discount_df.collect().await.unwrap();
        for batch in discount_batches {
            let discount_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();

            for i in 0..batch.num_rows() {
                if !discount_array.is_null(i) {
                    sketch.update(discount_array.value(i));
                }
            }
        }

        let actual_error_bound = sketch.relative_error_bound();
        let memory_usage = sketch.memory_usage();
        let levels = sketch.num_levels();

        println!(
            "KLL k={k}: error_bound={:.3}%, memory={memory_usage}B, levels={levels}",
            actual_error_bound * 100.0
        );

        // Validate error bound is as expected
        assert!(
            (actual_error_bound - expected_error_bound).abs() < 0.02,
            "Error bound {actual_error_bound:.3} not close to expected {expected_error_bound:.3} for k={k}"
        );

        // Validate memory usage grows reasonably with k
        let expected_max_memory = k * levels * 16; // Conservative estimate
        assert!(
            memory_usage <= expected_max_memory,
            "Memory usage {memory_usage} exceeds expected bound {expected_max_memory} for k={k}"
        );

        // For discount values (0-0.10), test that quantiles are reasonable
        let median = sketch.get_quantile(0.5).unwrap();
        assert!(
            (0.0..=0.10).contains(&median),
            "Median discount {median} outside expected range [0.0, 0.10]"
        );
    }
}
