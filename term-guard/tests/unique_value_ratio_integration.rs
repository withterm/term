use term_guard::constraints::Assertion;
use term_guard::core::{Check, Level, ValidationResult, ValidationSuite};
use term_guard::test_utils::{create_tpc_h_context, ScaleFactor};

#[tokio::test]
async fn test_unique_value_ratio_on_tpc_h_customer() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Register customer table as "data" for constraint evaluation
    let customer_df = ctx.table("customer").await.unwrap();
    ctx.register_table("data", customer_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("customer_unique_value_ratio")
        .check(
            Check::builder("customer_key_unique_ratio")
                .level(Level::Error)
                // c_custkey should have very high unique value ratio (all values unique)
                .validates_unique_value_ratio(vec!["c_custkey"], Assertion::GreaterThan(0.99))
                .build(),
        )
        .check(
            Check::builder("customer_segment_unique_ratio")
                .level(Level::Warning)
                // c_mktsegment has only 5 distinct values with many repetitions
                // so unique value ratio should be very low
                .validates_unique_value_ratio(vec!["c_mktsegment"], Assertion::Equals(0.0))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());

    if let ValidationResult::Success { report, .. } = &results {
        // Verify we have expected metrics
        assert_eq!(report.metrics.total_checks, 2);
        assert_eq!(report.metrics.passed_checks, 2);
    }
}

#[tokio::test]
async fn test_unique_value_ratio_on_tpc_h_orders() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    let orders_df = ctx.table("orders").await.unwrap();
    ctx.register_table("data", orders_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("orders_unique_value_ratio")
        .check(
            Check::builder("order_status_unique_ratio")
                .level(Level::Info)
                // o_orderstatus has only 3 values ('F', 'O', 'P') with many repetitions
                .validates_unique_value_ratio(vec!["o_orderstatus"], Assertion::Equals(0.0))
                .build(),
        )
        .check(
            Check::builder("order_key_unique_ratio")
                .level(Level::Error)
                // o_orderkey should be unique (each value appears once)
                .validates_unique_value_ratio(vec!["o_orderkey"], Assertion::Equals(1.0))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_multi_column_unique_value_ratio() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    let lineitem_df = ctx.table("lineitem").await.unwrap();
    ctx.register_table("data", lineitem_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("lineitem_multi_column_unique_ratio")
        .check(
            Check::builder("order_line_unique_ratio")
                // Combination of orderkey and linenumber should be unique
                .validates_unique_value_ratio(
                    vec!["l_orderkey", "l_linenumber"],
                    Assertion::Equals(1.0),
                )
                .build(),
        )
        .check(
            Check::builder("ship_mode_unique_ratio")
                // l_shipmode has limited values with many repetitions
                .validates_unique_value_ratio(vec!["l_shipmode"], Assertion::Equals(0.0))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_unique_value_ratio_vs_distinctness_difference() {
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::*;
    use std::sync::Arc;

    let ctx = SessionContext::new();

    // Create test data where:
    // - Distinctness is 0.6 (3 distinct values out of 5 rows)
    // - Unique value ratio is 0.4 (2 values appear exactly once: "B" and "C")
    let values = StringArray::from(vec![Some("A"), Some("A"), Some("B"), Some("C"), Some("A")]);

    let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap();
    ctx.register_batch("data", batch).unwrap();

    let suite = ValidationSuite::builder("unique_ratio_vs_distinctness")
        .check(
            Check::builder("test_distinctness")
                // Distinctness should be 0.6 (3 distinct values / 5 total)
                .validates_distinctness(vec!["col"], Assertion::Equals(0.6))
                .build(),
        )
        .check(
            Check::builder("test_unique_value_ratio")
                // Unique value ratio should be 0.4 (2 values appear once / 5 total)
                .validates_unique_value_ratio(vec!["col"], Assertion::Equals(0.4))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();

    // Both checks should pass, demonstrating the difference
    assert!(results.is_success());
}

#[tokio::test]
async fn test_unique_value_ratio_with_high_cardinality() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Part table has high cardinality columns
    let part_df = ctx.table("part").await.unwrap();
    ctx.register_table("data", part_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("high_cardinality_unique_ratio")
        .check(
            Check::builder("part_key_unique_ratio")
                // p_partkey should be unique (all values appear once)
                .validates_unique_value_ratio(vec!["p_partkey"], Assertion::Equals(1.0))
                .build(),
        )
        .check(
            Check::builder("part_name_unique_ratio")
                // p_name should have very high unique value ratio
                .validates_unique_value_ratio(vec!["p_name"], Assertion::GreaterThan(0.95))
                .build(),
        )
        .check(
            Check::builder("part_type_unique_ratio")
                // p_type has moderate cardinality but many repeated values
                // so unique value ratio should be lower than distinctness
                .validates_unique_value_ratio(vec!["p_type"], Assertion::LessThan(0.1))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_unique_value_ratio_failures() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    let nation_df = ctx.table("nation").await.unwrap();
    ctx.register_table("data", nation_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("unique_ratio_failures")
        .check(
            Check::builder("unrealistic_expectation")
                .level(Level::Error)
                // n_regionkey has only 5 distinct values with many repetitions
                // but we expect high unique value ratio
                .validates_unique_value_ratio(vec!["n_regionkey"], Assertion::GreaterThan(0.8))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_failure());

    if let ValidationResult::Failure { report } = &results {
        assert!(!report.issues.is_empty());
        let error_issues = report
            .issues
            .iter()
            .filter(|issue| issue.level == Level::Error)
            .count();
        assert_eq!(error_issues, 1);

        // Check that the message mentions unique value ratio (with underscores or spaces)
        let issue = &report.issues[0];
        assert!(
            issue.message.contains("unique_value_ratio")
                || issue.message.contains("unique value ratio")
                || issue.message.contains("Unique value ratio")
                || issue.message.to_lowercase().contains("uniqueness")
        );
    }
}

#[tokio::test]
async fn test_unique_value_ratio_edge_cases() {
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::*;
    use std::sync::Arc;

    // Test with empty dataset
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, true)]));
    let empty_batch = RecordBatch::new_empty(schema.clone());
    ctx.register_batch("data", empty_batch).unwrap();

    let suite = ValidationSuite::builder("empty_data")
        .check(
            Check::builder("empty_unique_ratio")
                .validates_unique_value_ratio(vec!["col"], Assertion::Equals(1.0))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    // Should be skipped due to no data
    assert!(results.is_success());

    // Test with all NULL values
    let ctx = SessionContext::new();
    let null_values = StringArray::from(vec![Option::<&str>::None, None, None, None]);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(null_values)]).unwrap();
    ctx.register_batch("data", batch).unwrap();

    let suite = ValidationSuite::builder("all_nulls")
        .check(
            Check::builder("null_unique_ratio")
                // All NULLs are grouped together, so no unique values
                .validates_unique_value_ratio(vec!["col"], Assertion::Equals(0.0))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());

    // Test with single value
    let ctx = SessionContext::new();
    let single_value = StringArray::from(vec![Some("only_one")]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(single_value)]).unwrap();
    ctx.register_batch("data", batch).unwrap();

    let suite = ValidationSuite::builder("single_value")
        .check(
            Check::builder("single_unique_ratio")
                // Single value appears once, so unique value ratio is 1.0
                .validates_unique_value_ratio(vec!["col"], Assertion::Equals(1.0))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_unique_value_ratio_with_supplier_data() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    let supplier_df = ctx.table("supplier").await.unwrap();
    ctx.register_table("data", supplier_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("supplier_unique_ratio")
        .check(
            Check::builder("supplier_key_unique_ratio")
                .level(Level::Error)
                // s_suppkey should be unique
                .validates_unique_value_ratio(vec!["s_suppkey"], Assertion::Equals(1.0))
                .build(),
        )
        .check(
            Check::builder("supplier_nation_unique_ratio")
                .level(Level::Info)
                // s_nationkey has limited values with repetitions
                .validates_unique_value_ratio(vec!["s_nationkey"], Assertion::LessThan(0.1))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_unique_value_ratio_specific_scenario() {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::*;
    use std::sync::Arc;

    let ctx = SessionContext::new();

    // Create specific test data:
    // Values: [1, 2, 2, 3, 3, 3, 4, 4, 4, 4]
    // Distinct values: 4 (distinctness = 0.4)
    // Values appearing exactly once: 1 (unique value ratio = 0.1)
    let values = Int64Array::from(vec![
        Some(1), // appears once
        Some(2),
        Some(2), // appears twice
        Some(3),
        Some(3),
        Some(3), // appears three times
        Some(4),
        Some(4),
        Some(4),
        Some(4), // appears four times
    ]);

    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap();
    ctx.register_batch("data", batch).unwrap();

    let suite = ValidationSuite::builder("specific_scenario")
        .check(
            Check::builder("test_unique_value_ratio")
                // Only value "1" appears exactly once, so ratio = 1/10 = 0.1
                .validates_unique_value_ratio(vec!["value"], Assertion::Equals(0.1))
                .build(),
        )
        .check(
            Check::builder("test_distinctness_comparison")
                // Distinctness is 4/10 = 0.4
                .validates_distinctness(vec!["value"], Assertion::Equals(0.4))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}
