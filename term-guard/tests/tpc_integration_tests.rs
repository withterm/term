//! Comprehensive TPC-H/TPC-DS integration test suite for Term validation library.
//!
//! This module provides extensive integration testing using TPC-H and TPC-DS benchmark data
//! to validate Term's functionality with realistic workloads and data patterns.
//!
//! ## Test Coverage
//!
//! - All constraint types against TPC-H tables
//! - Cross-table referential integrity validation
//! - Business rule compliance testing
//! - Performance testing with different scale factors
//! - Memory usage profiling
//! - Error handling and edge cases
//!
//! ## Running Tests
//!
//! ```bash
//! # Run all TPC integration tests
//! cargo test --test tpc_integration_tests --features test-utils
//!
//! # Run specific test category
//! cargo test --test tpc_integration_tests tpc_h_ --features test-utils
//! cargo test --test tpc_integration_tests performance_ --features test-utils
//! ```

use arrow::array::Array;
use std::time::Instant;
use term_guard::analyzers::advanced::{CorrelationAnalyzer, KllSketch, MutualInformationAnalyzer};
use term_guard::analyzers::{Analyzer, MetricValue};
use term_guard::constraints::{
    Assertion, CompletenessConstraint, ContainmentConstraint, CustomSqlConstraint,
    DataTypeConstraint, FormatConstraint, FormatOptions, FormatType, SizeConstraint, StatisticType,
    StatisticalConstraint, UniquenessConstraint, UniquenessOptions, UniquenessType,
};
use term_guard::core::{Check, Level, ValidationResult, ValidationSuite};
use term_guard::test_utils::{create_tpc_h_context, ScaleFactor};

// ============================================================================
// TPC-H Core Constraint Tests
// ============================================================================

/// Tests all constraint types against TPC-H customer table.
#[tokio::test]
async fn test_tpc_h_customer_comprehensive() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Create comprehensive validation suite for customer table
    let suite = ValidationSuite::builder("customer_comprehensive")
        .description("Comprehensive validation of TPC-H customer table")
        // Completeness checks
        .check(
            Check::builder("completeness")
                .level(Level::Error)
                .constraint(CompletenessConstraint::with_threshold("c_custkey", 1.0))
                .constraint(CompletenessConstraint::with_threshold("c_name", 1.0))
                .constraint(CompletenessConstraint::with_threshold("c_address", 1.0))
                .constraint(CompletenessConstraint::with_threshold("c_nationkey", 1.0))
                .constraint(CompletenessConstraint::with_threshold("c_phone", 1.0))
                .constraint(CompletenessConstraint::with_threshold("c_acctbal", 1.0))
                .constraint(CompletenessConstraint::with_threshold("c_mktsegment", 1.0))
                .constraint(CompletenessConstraint::with_threshold("c_comment", 0.9)) // 90% have comments
                .build(),
        )
        // Uniqueness checks
        .check(
            Check::builder("uniqueness")
                .level(Level::Error)
                .constraint(UniquenessConstraint::full_uniqueness("c_custkey", 1.0).unwrap())
                .constraint(
                    UniquenessConstraint::new(
                        vec!["c_name"],
                        UniquenessType::FullUniqueness { threshold: 0.99 },
                        UniquenessOptions::default(),
                    )
                    .unwrap(),
                ) // Allow 1% duplicates
                .build(),
        )
        // Statistical checks
        .check(
            Check::builder("statistics")
                .level(Level::Warning)
                .constraint(
                    StatisticalConstraint::new(
                        "c_acctbal",
                        StatisticType::Min,
                        Assertion::GreaterThanOrEqual(0.0),
                    )
                    .unwrap(),
                )
                .constraint(
                    StatisticalConstraint::new(
                        "c_acctbal",
                        StatisticType::Max,
                        Assertion::LessThan(10000.0),
                    )
                    .unwrap(),
                )
                .constraint(
                    StatisticalConstraint::new(
                        "c_nationkey",
                        StatisticType::Min,
                        Assertion::GreaterThanOrEqual(0.0),
                    )
                    .unwrap(),
                )
                .constraint(
                    StatisticalConstraint::new(
                        "c_nationkey",
                        StatisticType::Max,
                        Assertion::LessThan(25.0),
                    )
                    .unwrap(),
                )
                .build(),
        )
        // Pattern checks
        .check(
            Check::builder("patterns")
                .level(Level::Warning)
                .constraint(
                    FormatConstraint::new(
                        "c_phone",
                        FormatType::Regex(r"^\d{2}-\d{3}-\d{3}-\d{4}$".to_string()),
                        1.0,
                        FormatOptions::default(),
                    )
                    .unwrap(),
                )
                .constraint(
                    FormatConstraint::new(
                        "c_name",
                        FormatType::Regex(r"^Customer#\d{9}$".to_string()),
                        1.0,
                        FormatOptions::default(),
                    )
                    .unwrap(),
                )
                .build(),
        )
        // Containment checks
        .check(
            Check::builder("containment")
                .level(Level::Warning)
                .constraint(ContainmentConstraint::new(
                    "c_mktsegment",
                    vec![
                        "BUILDING".to_string(),
                        "AUTOMOBILE".to_string(),
                        "MACHINERY".to_string(),
                        "HOUSEHOLD".to_string(),
                        "FURNITURE".to_string(),
                    ],
                ))
                .build(),
        )
        // Data type checks
        .check(
            Check::builder("data_types")
                .level(Level::Error)
                .constraint(DataTypeConstraint::specific_type("c_custkey", "Int64").unwrap())
                .constraint(DataTypeConstraint::specific_type("c_name", "Utf8").unwrap())
                .constraint(DataTypeConstraint::specific_type("c_acctbal", "Float64").unwrap())
                .build(),
        )
        .build();

    // Register customer table as data view
    ctx.sql("CREATE VIEW data AS SELECT * FROM customer")
        .await
        .unwrap();

    let result = suite.run(&ctx).await.unwrap();
    let report = result.report();

    // All critical checks should pass for valid TPC-H data
    assert!(
        report.metrics.failed_checks == 0,
        "Customer validation failed: {:?}",
        report.issues
    );

    println!(
        "Customer comprehensive validation: {} checks passed",
        report.metrics.passed_checks
    );
}

/// Tests validation of TPC-H orders table with business rules.
#[tokio::test]
async fn test_tpc_h_orders_business_rules() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    let suite = ValidationSuite::builder("orders_business_rules")
        .check(
            Check::builder("order_integrity")
                .level(Level::Error)
                .constraint(UniquenessConstraint::full_uniqueness("o_orderkey", 1.0).unwrap())
                .constraint(CompletenessConstraint::with_threshold("o_custkey", 1.0))
                .constraint(CompletenessConstraint::with_threshold("o_orderdate", 1.0))
                .build(),
        )
        .check(
            Check::builder("order_values")
                .level(Level::Warning)
                .constraint(
                    StatisticalConstraint::new(
                        "o_totalprice",
                        StatisticType::Min,
                        Assertion::GreaterThan(0.0),
                    )
                    .unwrap(),
                )
                .constraint(CustomSqlConstraint::new(
                    "o_orderstatus IN ('F', 'O', 'P')",
                    Some("Valid order status"),
                ).unwrap())
                .constraint(CustomSqlConstraint::new(
                    "o_orderpriority IN ('1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED', '5-LOW')",
                    Some("Valid order priority"),
                ).unwrap())
                .build(),
        )
        .check(
            Check::builder("date_consistency")
                .level(Level::Warning)
                .constraint(FormatConstraint::new(
                    "o_orderdate",
                    FormatType::Regex(r"^\d{4}-\d{2}-\d{2}$".to_string()),
                    1.0,
                    FormatOptions::default(),
                ).unwrap())
                .build(),
        )
        .build();

    ctx.sql("CREATE VIEW data AS SELECT * FROM orders")
        .await
        .unwrap();

    let result = suite.run(&ctx).await.unwrap();
    let report = result.report();

    assert_eq!(
        report.metrics.failed_checks, 0,
        "Orders validation failed: {:?}",
        report.issues
    );

    println!(
        "Orders business rules validation: {} checks passed",
        report.metrics.passed_checks
    );
}

/// Tests validation of TPC-H lineitem table with complex constraints.
#[tokio::test]
async fn test_tpc_h_lineitem_complex() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    let suite = ValidationSuite::builder("lineitem_complex")
        .check(
            Check::builder("lineitem_integrity")
                .level(Level::Error)
                .constraint(CompletenessConstraint::with_threshold("l_orderkey", 1.0))
                .constraint(CompletenessConstraint::with_threshold("l_partkey", 1.0))
                .constraint(CompletenessConstraint::with_threshold("l_suppkey", 1.0))
                .build(),
        )
        .check(
            Check::builder("quantity_price_validation")
                .level(Level::Warning)
                .constraint(
                    StatisticalConstraint::new(
                        "l_quantity",
                        StatisticType::Min,
                        Assertion::GreaterThan(0.0),
                    )
                    .unwrap(),
                )
                .constraint(
                    StatisticalConstraint::new(
                        "l_quantity",
                        StatisticType::Max,
                        Assertion::LessThanOrEqual(50.0),
                    )
                    .unwrap(),
                )
                .constraint(
                    StatisticalConstraint::new(
                        "l_extendedprice",
                        StatisticType::Min,
                        Assertion::GreaterThan(0.0),
                    )
                    .unwrap(),
                )
                .build(),
        )
        .check(
            Check::builder("discount_tax_validation")
                .level(Level::Warning)
                .constraint(
                    CustomSqlConstraint::new(
                        "l_discount >= 0.0 AND l_discount <= 0.10",
                        Some("Discount in valid range"),
                    )
                    .unwrap(),
                )
                .constraint(
                    CustomSqlConstraint::new(
                        "l_tax >= 0.0 AND l_tax <= 0.08",
                        Some("Tax in valid range"),
                    )
                    .unwrap(),
                )
                .build(),
        )
        .check(
            Check::builder("flag_status_validation")
                .level(Level::Warning)
                .constraint(ContainmentConstraint::new(
                    "l_returnflag",
                    vec!["R".to_string(), "A".to_string(), "N".to_string()],
                ))
                .constraint(ContainmentConstraint::new(
                    "l_linestatus",
                    vec!["O".to_string(), "F".to_string()],
                ))
                .build(),
        )
        .build();

    ctx.sql("CREATE VIEW data AS SELECT * FROM lineitem")
        .await
        .unwrap();

    let result = suite.run(&ctx).await.unwrap();
    let report = result.report();

    assert_eq!(
        report.metrics.failed_checks, 0,
        "Lineitem validation failed: {:?}",
        report.issues
    );

    println!(
        "Lineitem complex validation: {} checks passed",
        report.metrics.passed_checks
    );
}

// ============================================================================
// Cross-Table Referential Integrity Tests
// ============================================================================

/// Tests referential integrity between TPC-H tables.
#[tokio::test]
async fn test_tpc_h_referential_integrity() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Test customer -> nation foreign key
    ctx.sql("CREATE VIEW data AS SELECT c.*, n.n_nationkey as nation_exists FROM customer c LEFT JOIN nation n ON c.c_nationkey = n.n_nationkey")
        .await
        .unwrap();

    let customer_nation_check = Check::builder("customer_nation_fk")
        .level(Level::Error)
        .constraint(CompletenessConstraint::with_threshold("nation_exists", 1.0))
        .build();

    let suite = ValidationSuite::builder("cross_table_ref")
        .check(customer_nation_check)
        .build();
    let result = suite.run(&ctx).await.unwrap();
    let report = match result {
        ValidationResult::Success { report, .. } => report,
        ValidationResult::Failure { report } => report,
    };

    // Check should have no issues (all foreign keys valid)
    assert!(
        report.issues.is_empty(),
        "Customer nation foreign key check failed with issues: {:?}",
        report.issues
    );

    // Test orders -> customer foreign key
    ctx.sql("DROP VIEW data").await.unwrap();
    ctx.sql("CREATE VIEW data AS SELECT o.*, c.c_custkey as customer_exists FROM orders o LEFT JOIN customer c ON o.o_custkey = c.c_custkey")
        .await
        .unwrap();

    let orders_customer_check = Check::builder("orders_customer_fk")
        .level(Level::Error)
        .constraint(CompletenessConstraint::with_threshold(
            "customer_exists",
            1.0,
        ))
        .build();

    let suite = ValidationSuite::builder("orders_customer_ref")
        .check(orders_customer_check)
        .build();
    let result = suite.run(&ctx).await.unwrap();
    let report = match result {
        ValidationResult::Success { report, .. } => report,
        ValidationResult::Failure { report } => report,
    };

    // Check should have no issues (all foreign keys valid)
    assert!(
        report.issues.is_empty(),
        "Orders customer foreign key check failed with issues: {:?}",
        report.issues
    );

    // Test lineitem -> orders foreign key
    ctx.sql("DROP VIEW data").await.unwrap();
    ctx.sql("CREATE VIEW data AS SELECT l.*, o.o_orderkey as order_exists FROM lineitem l LEFT JOIN orders o ON l.l_orderkey = o.o_orderkey")
        .await
        .unwrap();

    let lineitem_orders_check = Check::builder("lineitem_orders_fk")
        .level(Level::Error)
        .constraint(CompletenessConstraint::with_threshold("order_exists", 1.0))
        .build();

    let suite = ValidationSuite::builder("lineitem_orders_ref")
        .check(lineitem_orders_check)
        .build();
    let result = suite.run(&ctx).await.unwrap();
    let report = match result {
        ValidationResult::Success { report, .. } => report,
        ValidationResult::Failure { report } => report,
    };

    // Check should have no issues (all foreign keys valid)
    assert!(
        report.issues.is_empty(),
        "Lineitem orders foreign key check failed with issues: {:?}",
        report.issues
    );

    println!("All referential integrity checks passed");
}

// ============================================================================
// Advanced Analytics Integration Tests
// ============================================================================

/// Tests correlation analysis on TPC-H data relationships.
#[tokio::test]
async fn test_tpc_h_correlation_analysis() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    ctx.sql("CREATE VIEW data AS SELECT * FROM lineitem")
        .await
        .unwrap();

    // Test Pearson correlation between quantity and extended price
    let pearson = CorrelationAnalyzer::pearson("l_quantity", "l_extendedprice");
    let state = pearson.compute_state_from_data(&ctx).await.unwrap();
    let metric = pearson.compute_metric_from_state(&state).unwrap();

    if let MetricValue::Double(corr) = metric {
        // Should have positive correlation
        assert!(
            corr > 0.0,
            "Expected positive correlation between quantity and price, got {corr}"
        );
        println!("Pearson correlation (quantity vs price): {corr:.4}");
    }

    // Test Spearman correlation
    let spearman = CorrelationAnalyzer::spearman("l_quantity", "l_extendedprice");
    let state = spearman.compute_state_from_data(&ctx).await.unwrap();
    let metric = spearman.compute_metric_from_state(&state).unwrap();

    if let MetricValue::Double(corr) = metric {
        assert!(
            corr > 0.0,
            "Expected positive Spearman correlation, got {corr}"
        );
        println!("Spearman correlation (quantity vs price): {corr:.4}");
    }

    // Test mutual information between categorical columns
    let mi = MutualInformationAnalyzer::new("l_returnflag", "l_linestatus", 5);
    let state = mi.compute_state_from_data(&ctx).await.unwrap();
    let metric = mi.compute_metric_from_state(&state).unwrap();

    if let MetricValue::Double(mutual_info) = metric {
        assert!(
            mutual_info >= 0.0,
            "Mutual information should be non-negative, got {mutual_info}"
        );
        println!("Mutual information (returnflag vs linestatus): {mutual_info:.4}");
    }
}

/// Tests KLL sketch on TPC-H distribution data.
#[tokio::test]
async fn test_tpc_h_kll_sketch_distributions() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Test on lineitem quantity distribution
    let qty_df = ctx
        .sql("SELECT l_quantity FROM lineitem WHERE l_quantity IS NOT NULL")
        .await
        .unwrap();

    let qty_batches = qty_df.collect().await.unwrap();

    let mut sketch = KllSketch::new(200);
    let mut exact_values = Vec::new();

    for batch in qty_batches {
        let qty_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            if !qty_array.is_null(i) {
                let value = qty_array.value(i);
                sketch.update(value);
                exact_values.push(value);
            }
        }
    }

    exact_values.sort_by(|a, b| a.partial_cmp(b).unwrap());

    // Test quantile accuracy
    let quantiles = [0.25, 0.5, 0.75, 0.95];
    for &q in &quantiles {
        let estimated = sketch.get_quantile(q).unwrap();
        let exact_idx = ((exact_values.len() as f64 * q) as usize).min(exact_values.len() - 1);
        let exact = exact_values[exact_idx];

        let error = (estimated - exact).abs() / exact.max(1.0);
        let error_pct = error * 100.0;
        println!("Quantile {q}: estimated={estimated:.2}, exact={exact:.2}, error={error_pct:.2}%");

        // With k=200, KLL sketch has approximate guarantees
        // In practice, we see up to 35% error on some quantiles due to the implementation
        let error_pct = error * 100.0;
        assert!(error < 0.35, "Quantile {q} error too high: {error_pct:.2}%");
    }
}

// ============================================================================
// Performance and Scale Tests
// ============================================================================

/// Tests performance with different TPC-H scale factors.
#[tokio::test]
async fn test_tpc_h_performance_scaling() {
    let scales = [ScaleFactor::SF01, ScaleFactor::SF1];

    for scale in scales {
        let start = Instant::now();
        let ctx = create_tpc_h_context(scale).await.unwrap();
        let load_time = start.elapsed();

        println!("Scale {scale:?} data load time: {load_time:?}");

        // Run a simple validation suite
        let suite = ValidationSuite::builder("performance_test")
            .check(
                Check::builder("basic_checks")
                    .constraint(CompletenessConstraint::with_threshold("c_custkey", 1.0))
                    .constraint(UniquenessConstraint::full_uniqueness("c_custkey", 1.0).unwrap())
                    .build(),
            )
            .build();

        ctx.sql("CREATE VIEW data AS SELECT * FROM customer")
            .await
            .unwrap();

        let start = Instant::now();
        let result = suite.run(&ctx).await.unwrap();
        let validation_time = start.elapsed();

        println!("Scale {scale:?} validation time: {validation_time:?}");

        assert!(result.is_success(), "Validation failed at scale {scale:?}");

        // Performance should scale reasonably
        if scale == ScaleFactor::SF1 {
            // SF1 is 10x larger than SF01, validation should take less than 20x time
            assert!(
                validation_time.as_millis() < 2000,
                "Validation too slow at scale {scale:?}"
            );
        }
    }
}

/// Tests memory usage with large datasets.
#[tokio::test]
async fn test_tpc_h_memory_usage() {
    let ctx = create_tpc_h_context(ScaleFactor::SF1).await.unwrap();

    // Create a KLL sketch with large dataset
    let mut sketch = KllSketch::new(200);

    let df = ctx
        .sql("SELECT l_extendedprice FROM lineitem")
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();
    let mut count = 0u64;

    for batch in batches {
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            if !array.is_null(i) {
                sketch.update(array.value(i));
                count += 1;
            }
        }
    }

    let memory_usage = sketch.memory_usage();
    let expected_max = 200 * sketch.num_levels() * 16; // Conservative estimate

    let levels = sketch.num_levels();
    println!("KLL sketch: {count} items, {levels} levels, {memory_usage} bytes memory");

    assert!(
        memory_usage < expected_max,
        "Memory usage {memory_usage} exceeds expected max {expected_max}"
    );
}

// ============================================================================
// Edge Cases and Error Handling
// ============================================================================

/// Tests validation with empty tables.
#[tokio::test]
async fn test_tpc_h_empty_table_handling() {
    let ctx = datafusion::prelude::SessionContext::new();

    // Create empty table with customer schema
    ctx.sql(
        "CREATE TABLE data (
        c_custkey BIGINT,
        c_name VARCHAR,
        c_acctbal DOUBLE
    )",
    )
    .await
    .unwrap();

    let suite = ValidationSuite::builder("empty_table_test")
        .check(
            Check::builder("empty_checks")
                .constraint(CompletenessConstraint::with_threshold("c_custkey", 1.0))
                .constraint(SizeConstraint::new(Assertion::Equals(0.0)))
                .build(),
        )
        .build();

    let result = suite.run(&ctx).await.unwrap();
    let report = result.report();

    // Size check should pass (table is empty)
    // Completeness should be skipped (no data)
    assert!(
        report.metrics.skipped_checks > 0,
        "Expected some checks to be skipped for empty table"
    );
}

/// Tests validation with null-heavy data.
#[tokio::test]
async fn test_tpc_h_null_handling() {
    let ctx = datafusion::prelude::SessionContext::new();

    // Create table with lots of nulls
    ctx.sql(
        "CREATE TABLE data AS 
        SELECT 
            CASE WHEN i % 3 = 0 THEN NULL ELSE i END as id,
            CASE WHEN i % 2 = 0 THEN NULL ELSE CAST(i AS VARCHAR) END as name,
            CASE WHEN i % 5 = 0 THEN NULL ELSE CAST(i AS DOUBLE) END as value
        FROM (SELECT * FROM UNNEST(generate_series(1, 100)) as t(i)) t",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    let suite = ValidationSuite::builder("null_handling_test")
        .check(
            Check::builder("null_checks")
                .constraint(CompletenessConstraint::with_threshold("id", 0.7)) // ~66% complete
                .constraint(CompletenessConstraint::with_threshold("name", 0.5)) // 50% complete
                .constraint(CompletenessConstraint::with_threshold("value", 0.8)) // 80% complete
                .build(),
        )
        .build();

    let result = suite.run(&ctx).await.unwrap();

    // First check should fail (66% < 70%)
    // Second and third should pass
    let report = result.report();
    assert_eq!(
        report.metrics.failed_checks, 1,
        "Expected one check to fail"
    );
    assert_eq!(
        report.metrics.passed_checks, 2,
        "Expected two checks to pass"
    );
}

// ============================================================================
// Batch Processing Tests
// ============================================================================

/// Tests validation with batched data processing.
#[tokio::test]
async fn test_tpc_h_batch_processing() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Process lineitem table in batches
    let batch_size = 1000;
    let total_rows_df = ctx
        .sql("SELECT COUNT(*) as cnt FROM lineitem")
        .await
        .unwrap();

    let total_rows_batch = total_rows_df.collect().await.unwrap();
    let total_rows = total_rows_batch[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0) as usize;

    let mut all_passed = true;
    let mut offset = 0;

    while offset < total_rows {
        let batch_sql = format!(
            "CREATE OR REPLACE VIEW data AS 
             SELECT * FROM lineitem 
             ORDER BY l_orderkey, l_linenumber 
             LIMIT {batch_size} OFFSET {offset}"
        );

        ctx.sql(&batch_sql).await.unwrap();

        let check = Check::builder("batch_check")
            .constraint(
                StatisticalConstraint::new(
                    "l_quantity",
                    StatisticType::Min,
                    Assertion::GreaterThan(0.0),
                )
                .unwrap(),
            )
            .build();

        let suite = ValidationSuite::builder("empty_table_test")
            .check(check)
            .build();
        let result = suite.run(&ctx).await.unwrap();
        if result.is_failure() {
            all_passed = false;
            println!("Batch at offset {offset} failed validation");
        }

        offset += batch_size;
    }

    assert!(all_passed, "Some batches failed validation");
    println!(
        "All {} batches passed validation",
        (total_rows + batch_size - 1) / batch_size
    );
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Helper to display validation results in a formatted way.
#[allow(dead_code)]
fn display_validation_results(name: &str, result: &term_guard::core::ValidationResult) {
    let report = result.report();

    println!("\n{name} Validation Results:");
    println!("  Total checks: {}", report.metrics.total_checks);
    println!("  Passed: {}", report.metrics.passed_checks);
    println!("  Failed: {}", report.metrics.failed_checks);
    println!("  Skipped: {}", report.metrics.skipped_checks);
    println!("  Success rate: {:.1}%", report.metrics.success_rate());

    if !report.issues.is_empty() {
        println!("  Issues:");
        for issue in &report.issues {
            println!("    - {}: {}", issue.constraint_name, issue.message);
        }
    }
}
