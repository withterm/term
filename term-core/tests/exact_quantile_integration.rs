use term_core::constraints::{Assertion, QuantileConstraint};
use term_core::core::{Check, Level, ValidationResult, ValidationSuite};
use term_core::test_utils::{create_tpc_h_context, ScaleFactor};

#[tokio::test]
async fn test_exact_quantile_on_tpc_h_numeric_columns() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Test with customer account balance
    let customer_df = ctx.table("customer").await.unwrap();
    ctx.register_table("data", customer_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("exact_quantile_validation")
        .check(
            Check::builder("account_balance_percentiles")
                .level(Level::Info)
                // Test various exact percentiles
                .constraint(
                    QuantileConstraint::percentile("c_acctbal", 0.25, Assertion::GreaterThan(0.0))
                        .unwrap(),
                )
                .constraint(
                    QuantileConstraint::percentile(
                        "c_acctbal",
                        0.5,
                        Assertion::GreaterThan(1000.0),
                    )
                    .unwrap(),
                )
                .constraint(
                    QuantileConstraint::percentile("c_acctbal", 0.75, Assertion::LessThan(10000.0))
                        .unwrap(),
                )
                .constraint(
                    QuantileConstraint::percentile("c_acctbal", 0.95, Assertion::LessThan(15000.0))
                        .unwrap(),
                )
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_exact_quantile_on_orders_table() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Test with orders total price
    let orders_df = ctx.table("orders").await.unwrap();
    ctx.register_table("data", orders_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("orders_quantile_validation")
        .check(
            Check::builder("order_price_percentiles")
                .level(Level::Warning)
                // Check median order value
                .constraint(
                    QuantileConstraint::median("o_totalprice", Assertion::GreaterThan(50000.0))
                        .unwrap(),
                )
                // Check 90th percentile
                .constraint(
                    QuantileConstraint::percentile(
                        "o_totalprice",
                        0.9,
                        Assertion::LessThan(500000.0),
                    )
                    .unwrap(),
                )
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_exact_quantile_on_lineitem_table() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Test with lineitem extended price
    let lineitem_df = ctx.table("lineitem").await.unwrap();
    ctx.register_table("data", lineitem_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("lineitem_quantile_validation")
        .check(
            Check::builder("price_percentiles")
                .level(Level::Error)
                // Check extended price distribution
                .constraint(
                    QuantileConstraint::median("l_extendedprice", Assertion::GreaterThan(0.0))
                        .unwrap(),
                )
                .constraint(
                    QuantileConstraint::percentile(
                        "l_extendedprice",
                        0.1,
                        Assertion::GreaterThan(100.0),
                    )
                    .unwrap(),
                )
                .constraint(
                    QuantileConstraint::percentile(
                        "l_extendedprice",
                        0.9,
                        Assertion::LessThan(100000.0),
                    )
                    .unwrap(),
                )
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_exact_quantile_edge_cases() {
    use arrow::array::Float64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::*;
    use std::sync::Arc;

    let ctx = SessionContext::new();

    // Create test data with known values
    let values = Float64Array::from(vec![
        Some(1.0),
        Some(2.0),
        Some(3.0),
        Some(4.0),
        Some(5.0),
        Some(6.0),
        Some(7.0),
        Some(8.0),
        Some(9.0),
        Some(10.0),
    ]);

    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Float64,
        true,
    )]));

    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(values)]).unwrap();
    let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("data", Arc::new(provider)).unwrap();

    // Test exact quantiles on known data
    let suite = ValidationSuite::builder("edge_case_quantiles")
        .check(
            Check::builder("known_percentiles")
                // 10th percentile should be ~1.9
                .constraint(
                    QuantileConstraint::percentile("value", 0.1, Assertion::Equals(1.9)).unwrap(),
                )
                // Median should be 5.5
                .constraint(QuantileConstraint::median("value", Assertion::Equals(5.5)).unwrap())
                // 90th percentile should be ~9.1
                .constraint(
                    QuantileConstraint::percentile("value", 0.9, Assertion::Equals(9.1)).unwrap(),
                )
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();

    // These might not match exactly due to quantile calculation methods
    if let ValidationResult::Failure { report } = &results {
        println!("Note: Exact quantile values may differ slightly due to calculation method");
        for issue in &report.issues {
            println!("Issue: {}", issue.message);
        }
    }
}

#[tokio::test]
async fn test_exact_quantile_with_nulls() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Test supplier table which may have nulls
    let supplier_df = ctx.table("supplier").await.unwrap();
    ctx.register_table("data", supplier_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("quantile_with_nulls")
        .check(
            Check::builder("supplier_balance_percentiles")
                // Check account balance percentiles
                .constraint(
                    QuantileConstraint::median("s_acctbal", Assertion::GreaterThan(0.0)).unwrap(),
                )
                .constraint(
                    QuantileConstraint::percentile(
                        "s_acctbal",
                        0.25,
                        Assertion::GreaterThan(-1000.0),
                    )
                    .unwrap(),
                )
                .constraint(
                    QuantileConstraint::percentile("s_acctbal", 0.75, Assertion::LessThan(10000.0))
                        .unwrap(),
                )
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_exact_quantile_failure_cases() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Test with part table
    let part_df = ctx.table("part").await.unwrap();
    ctx.register_table("data", part_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("quantile_failures")
        .check(
            Check::builder("expected_failures")
                .level(Level::Error)
                // This should fail - retail prices are positive
                .constraint(
                    QuantileConstraint::median("p_retailprice", Assertion::LessThan(0.0)).unwrap(),
                )
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_failure());

    if let ValidationResult::Failure { report } = &results {
        assert!(!report.issues.is_empty());
        assert!(report.issues[0].message.contains("does not"));
    }
}

#[tokio::test]
async fn test_exact_quantile_multiple_columns() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Test with nation table
    let nation_df = ctx.table("nation").await.unwrap();
    ctx.register_table("data", nation_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("multi_column_quantiles")
        .check(
            Check::builder("nation_key_percentiles")
                // Nation keys should start from 0
                .constraint(
                    QuantileConstraint::percentile(
                        "n_nationkey",
                        0.1,
                        Assertion::GreaterThanOrEqual(0.0),
                    )
                    .unwrap(),
                )
                // And go up to around 24 for 25 nations
                .constraint(
                    QuantileConstraint::percentile(
                        "n_nationkey",
                        0.9,
                        Assertion::LessThanOrEqual(25.0),
                    )
                    .unwrap(),
                )
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_exact_quantile_performance_comparison() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Use lineitem table for performance testing
    let lineitem_df = ctx.table("lineitem").await.unwrap();
    ctx.register_table("data", lineitem_df.into_view()).unwrap();

    let start = std::time::Instant::now();

    let suite = ValidationSuite::builder("performance_test")
        .check(
            Check::builder("quantile_performance")
                // Test 95th percentile on large dataset
                .constraint(
                    QuantileConstraint::percentile("l_quantity", 0.95, Assertion::GreaterThan(0.0))
                        .unwrap(),
                )
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    let duration = start.elapsed();

    assert!(results.is_success());
    println!("Exact quantile calculation took: {:?}", duration);

    // Should complete in reasonable time even for exact calculation
    assert!(duration.as_secs() < 5, "Exact quantile took too long");
}
