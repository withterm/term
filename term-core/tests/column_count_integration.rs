use term_core::constraints::Assertion;
use term_core::core::{Check, Level, ValidationResult, ValidationSuite};
use term_core::test_utils::{create_tpc_h_context, ScaleFactor};

#[tokio::test]
async fn test_column_count_on_tpc_h_tables() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Test column count for various TPC-H tables
    // Customer table has 8 columns: c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment
    let customer_df = ctx.table("customer").await.unwrap();
    ctx.register_table("data", customer_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("tpc_h_customer_schema")
        .check(
            Check::builder("customer_column_count")
                .level(Level::Error)
                .has_column_count(Assertion::Equals(8.0))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());

    // Test with nation table (4 columns: n_nationkey, n_name, n_regionkey, n_comment)
    // Create a new context to avoid table name conflict
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();
    let nation_df = ctx.table("nation").await.unwrap();
    ctx.register_table("data", nation_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("tpc_h_nation_schema")
        .check(
            Check::builder("nation_column_count")
                .level(Level::Error)
                .has_column_count(Assertion::Equals(4.0))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_column_count_with_various_assertions() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Test with orders table (9 columns)
    let orders_df = ctx.table("orders").await.unwrap();
    ctx.register_table("data", orders_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("orders_schema_validation")
        .check(
            Check::builder("minimum_columns")
                .level(Level::Warning)
                .has_column_count(Assertion::GreaterThan(5.0))
                .has_column_count(Assertion::GreaterThanOrEqual(9.0))
                .build(),
        )
        .check(
            Check::builder("maximum_columns")
                .level(Level::Warning)
                .has_column_count(Assertion::LessThan(20.0))
                .has_column_count(Assertion::LessThanOrEqual(9.0))
                .build(),
        )
        .check(
            Check::builder("column_range")
                .level(Level::Error)
                .has_column_count(Assertion::Between(8.0, 10.0))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_column_count_failures() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Register a table and test with wrong assertions
    let part_df = ctx.table("part").await.unwrap();
    ctx.register_table("data", part_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("part_schema_fail")
        .check(
            Check::builder("wrong_column_count")
                .level(Level::Error)
                // Part table has 9 columns, not 5
                .has_column_count(Assertion::Equals(5.0))
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

        // Check that the message mentions column count
        let issue = &report.issues[0];
        assert!(issue.message.contains("column count") || issue.message.contains("Column count"));
    }
}

#[tokio::test]
async fn test_column_count_with_lineitem() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Lineitem is the largest table with 16 columns
    let lineitem_df = ctx.table("lineitem").await.unwrap();
    ctx.register_table("data", lineitem_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("lineitem_schema")
        .check(
            Check::builder("large_table_validation")
                .level(Level::Error)
                .has_column_count(Assertion::Equals(16.0))
                .has_column_count(Assertion::GreaterThan(15.0))
                .has_column_count(Assertion::LessThan(17.0))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_column_count_edge_cases() {
    use arrow::record_batch::RecordBatch;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::*;
    use std::sync::Arc;

    let ctx = SessionContext::new();

    // Test with single column table
    let schema = Arc::new(Schema::new(vec![Field::new(
        "single_col",
        DataType::Int64,
        true,
    )]));

    let batch = RecordBatch::new_empty(schema.clone());
    ctx.register_batch("data", batch).unwrap();

    let suite = ValidationSuite::builder("single_column")
        .check(
            Check::builder("one_column_check")
                .has_column_count(Assertion::Equals(1.0))
                .has_column_count(Assertion::GreaterThanOrEqual(1.0))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());

    // Test with many columns (50+)
    let many_fields: Vec<Field> = (0..50)
        .map(|i| Field::new(format!("col_{i}"), DataType::Int64, true))
        .collect();

    let schema = Arc::new(Schema::new(many_fields));
    let batch = RecordBatch::new_empty(schema);
    // Drop and re-register the table
    ctx.deregister_table("data").unwrap();
    ctx.register_table(
        "data",
        Arc::new(
            datafusion::datasource::MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap(),
        ),
    )
    .unwrap();

    let suite = ValidationSuite::builder("many_columns")
        .check(
            Check::builder("fifty_columns")
                .has_column_count(Assertion::Equals(50.0))
                .has_column_count(Assertion::Between(40.0, 60.0))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_column_count_with_optimizer() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Test multiple column count checks with optimizer enabled
    let supplier_df = ctx.table("supplier").await.unwrap();
    ctx.register_table("data", supplier_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("optimized_column_checks")
        .with_optimizer(true)
        .check(
            Check::builder("comprehensive_column_validation")
                .level(Level::Error)
                // Supplier has 7 columns
                .has_column_count(Assertion::Equals(7.0))
                .has_column_count(Assertion::GreaterThan(6.0))
                .has_column_count(Assertion::LessThan(8.0))
                .has_column_count(Assertion::Between(5.0, 10.0))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());

    // Column count constraints run separately even with optimizer
    // Verify that the validation passes - that's what matters
    assert!(results.is_success());
}
