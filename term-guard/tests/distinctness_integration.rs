use term_guard::constraints::{Assertion, UniquenessConstraint};
use term_guard::core::{Check, Level, ValidationResult, ValidationSuite};
use term_guard::test_utils::{create_tpc_h_context, ScaleFactor};

#[tokio::test]
async fn test_distinctness_on_tpc_h_customer() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Register customer table as "data" for constraint evaluation
    let customer_df = ctx.table("customer").await.unwrap();
    ctx.register_table("data", customer_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("customer_distinctness")
        .check(
            Check::builder("customer_key_distinctness")
                .level(Level::Error)
                // c_custkey should have very high distinctness (ideally 1.0)
                .validates_distinctness(vec!["c_custkey"], Assertion::GreaterThan(0.99))
                .build(),
        )
        .check(
            Check::builder("customer_segment_distinctness")
                .level(Level::Warning)
                // c_mktsegment has only 5 distinct values
                .validates_distinctness(vec!["c_mktsegment"], Assertion::LessThan(0.1))
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
async fn test_distinctness_on_tpc_h_orders() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    let orders_df = ctx.table("orders").await.unwrap();
    ctx.register_table("data", orders_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("orders_distinctness")
        .check(
            Check::builder("order_status_distinctness")
                .level(Level::Info)
                // o_orderstatus has only 3 values: 'F', 'O', 'P'
                .validates_distinctness(vec!["o_orderstatus"], Assertion::LessThan(0.01))
                .build(),
        )
        .check(
            Check::builder("order_priority_distinctness")
                .level(Level::Info)
                // o_orderpriority has 5 distinct values
                .validates_distinctness(vec!["o_orderpriority"], Assertion::LessThan(0.01))
                .build(),
        )
        .check(
            Check::builder("order_key_distinctness")
                .level(Level::Error)
                // o_orderkey should be unique
                .validates_distinctness(vec!["o_orderkey"], Assertion::Equals(1.0))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_multi_column_distinctness() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    let lineitem_df = ctx.table("lineitem").await.unwrap();
    ctx.register_table("data", lineitem_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("lineitem_multi_column_distinctness")
        .check(
            Check::builder("order_line_distinctness")
                // Combination of orderkey and linenumber should be unique
                .validates_distinctness(vec!["l_orderkey", "l_linenumber"], Assertion::Equals(1.0))
                .build(),
        )
        .check(
            Check::builder("ship_mode_distinctness")
                // l_shipmode has limited values
                .validates_distinctness(vec!["l_shipmode"], Assertion::LessThan(0.01))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_distinctness_vs_uniqueness_difference() {
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::*;
    use std::sync::Arc;

    let ctx = SessionContext::new();

    // Create test data where:
    // - Distinctness is 0.4 (2 distinct values out of 5 rows)
    // - Uniqueness is 0.0 (no value appears exactly once)
    let values = StringArray::from(vec![Some("A"), Some("A"), Some("B"), Some("B"), Some("A")]);

    let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap();
    ctx.register_batch("data", batch).unwrap();

    let suite = ValidationSuite::builder("distinctness_vs_uniqueness")
        .check(
            Check::builder("test_distinctness")
                // Distinctness should be 0.4 (2 distinct values / 5 total)
                .validates_distinctness(vec!["col"], Assertion::Equals(0.4))
                .build(),
        )
        .check(
            Check::builder("test_uniqueness")
                // Uniqueness should be 0.0 (no unique values)
                .constraint(UniquenessConstraint::full_uniqueness("col", 0.0).unwrap())
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();

    // Both checks should pass, demonstrating the difference
    assert!(results.is_success());
}

#[tokio::test]
async fn test_distinctness_with_high_cardinality() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Part table has high cardinality columns
    let part_df = ctx.table("part").await.unwrap();
    ctx.register_table("data", part_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("high_cardinality_distinctness")
        .check(
            Check::builder("part_key_distinctness")
                // p_partkey should be unique
                .validates_distinctness(vec!["p_partkey"], Assertion::Equals(1.0))
                .build(),
        )
        .check(
            Check::builder("part_name_distinctness")
                // p_name should have very high distinctness
                .validates_distinctness(vec!["p_name"], Assertion::GreaterThan(0.95))
                .build(),
        )
        .check(
            Check::builder("part_type_distinctness")
                // p_type has moderate distinctness
                .validates_distinctness(vec!["p_type"], Assertion::Between(0.1, 0.5))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_distinctness_failures() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    let nation_df = ctx.table("nation").await.unwrap();
    ctx.register_table("data", nation_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("distinctness_failures")
        .check(
            Check::builder("unrealistic_expectation")
                .level(Level::Error)
                // n_regionkey has only 5 distinct values but we expect high distinctness
                .validates_distinctness(vec!["n_regionkey"], Assertion::GreaterThan(0.8))
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

        // Check that the message mentions distinctness
        let issue = &report.issues[0];
        assert!(issue.message.contains("distinctness") || issue.message.contains("Distinctness"));
    }
}

#[tokio::test]
async fn test_distinctness_edge_cases() {
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
            Check::builder("empty_distinctness")
                .validates_distinctness(vec!["col"], Assertion::Equals(1.0))
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
            Check::builder("null_distinctness")
                // All NULLs count as 1 distinct value
                .validates_distinctness(vec!["col"], Assertion::Equals(0.25))
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
            Check::builder("single_distinctness")
                .validates_distinctness(vec!["col"], Assertion::Equals(1.0))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}
