use term_core::constraints::Assertion;
use term_core::core::{Check, Level, ValidationResult, ValidationSuite};
use term_core::test_utils::{create_tpc_h_context, ScaleFactor};

#[tokio::test]
async fn test_approx_count_distinct_on_tpc_h_tables() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Test with customer table - c_custkey should have high cardinality
    let customer_df = ctx.table("customer").await.unwrap();
    ctx.register_table("data", customer_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("customer_cardinality")
        .check(
            Check::builder("customer_key_cardinality")
                .level(Level::Error)
                // Customer keys should be unique, so approx count should be high
                .has_approx_count_distinct("c_custkey", Assertion::GreaterThan(900.0))
                .build(),
        )
        .check(
            Check::builder("market_segment_cardinality")
                .level(Level::Warning)
                // Market segments have low cardinality (only a few distinct values)
                .has_approx_count_distinct("c_mktsegment", Assertion::LessThan(10.0))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    if let ValidationResult::Failure { report } = &results {
        for issue in &report.issues {
            eprintln!("Issue: {} - {}", issue.check_name, issue.message);
        }
    }
    assert!(results.is_success());
}

#[tokio::test]
async fn test_approx_vs_exact_accuracy() {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::*;
    use std::sync::Arc;

    let ctx = SessionContext::new();

    // Create a dataset with known cardinality
    let mut values = Vec::new();
    for i in 0..10000 {
        // 1000 distinct values repeated 10 times each
        values.push(Some(i % 1000));
    }

    let schema = Arc::new(Schema::new(vec![Field::new(
        "test_col",
        DataType::Int64,
        true,
    )]));

    let array = Int64Array::from(values);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
    ctx.register_batch("data", batch).unwrap();

    // Test that APPROX_DISTINCT is within 3% of exact count (1000)
    let suite = ValidationSuite::builder("accuracy_test")
        .check(
            Check::builder("approx_accuracy")
                .level(Level::Error)
                // Allow 3% error margin: 970 to 1030
                .has_approx_count_distinct("test_col", Assertion::Between(970.0, 1030.0))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    if let ValidationResult::Failure { report } = &results {
        for issue in &report.issues {
            eprintln!("Issue: {} - {}", issue.check_name, issue.message);
        }
    }
    assert!(results.is_success());

    // Verify the actual value is close to 1000
    // The metrics verification is optional since we already tested the constraint
}

#[tokio::test]
async fn test_high_cardinality_columns() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Test with orders table - o_orderkey should have very high cardinality
    let orders_df = ctx.table("orders").await.unwrap();
    ctx.register_table("data", orders_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("high_cardinality_test")
        .check(
            Check::builder("order_key_cardinality")
                .level(Level::Error)
                // Order keys are unique, expect high cardinality
                .has_approx_count_distinct("o_orderkey", Assertion::GreaterThan(900.0))
                .build(),
        )
        .check(
            Check::builder("order_priority_cardinality")
                .level(Level::Info)
                // Order priority has only a few values
                .has_approx_count_distinct("o_orderpriority", Assertion::LessThan(10.0))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    if let ValidationResult::Failure { report } = &results {
        for issue in &report.issues {
            eprintln!("Issue: {} - {}", issue.check_name, issue.message);
        }
    }
    assert!(results.is_success());
}

#[tokio::test]
async fn test_null_handling() {
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::*;
    use std::sync::Arc;

    let ctx = SessionContext::new();

    // Create data with nulls
    let values = vec![
        Some("A"),
        None,
        Some("B"),
        None,
        Some("C"),
        Some("A"),
        None,
        Some("B"),
        Some("D"),
        None,
    ];

    let schema = Arc::new(Schema::new(vec![Field::new(
        "test_col",
        DataType::Utf8,
        true,
    )]));

    let array = StringArray::from(values);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
    ctx.register_batch("data", batch).unwrap();

    // Should count approximately 4 distinct non-null values (A, B, C, D)
    let suite = ValidationSuite::builder("null_handling_test")
        .check(
            Check::builder("distinct_with_nulls")
                .has_approx_count_distinct("test_col", Assertion::Between(3.0, 5.0))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    if let ValidationResult::Failure { report } = &results {
        for issue in &report.issues {
            eprintln!("Issue: {} - {}", issue.check_name, issue.message);
        }
    }
    assert!(results.is_success());
}

#[tokio::test]
async fn test_lineitem_high_cardinality() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Lineitem table has the highest cardinality in TPC-H
    let lineitem_df = ctx.table("lineitem").await.unwrap();
    ctx.register_table("data", lineitem_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("lineitem_cardinality")
        .check(
            Check::builder("composite_key_parts")
                .level(Level::Info)
                // Order key should have high cardinality
                .has_approx_count_distinct("l_orderkey", Assertion::GreaterThan(10000.0))
                // Line number has lower cardinality (1-7 typically)
                .has_approx_count_distinct("l_linenumber", Assertion::LessThan(10.0))
                // Return flag has very low cardinality (A, N, R)
                .has_approx_count_distinct("l_returnflag", Assertion::LessThan(5.0))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    if let ValidationResult::Failure { report } = &results {
        for issue in &report.issues {
            eprintln!("Issue: {} - {}", issue.check_name, issue.message);
        }
    }
    assert!(results.is_success());
}

#[tokio::test]
async fn test_constraint_failures() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Test with nation table which has low cardinality
    let nation_df = ctx.table("nation").await.unwrap();
    ctx.register_table("data", nation_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("failing_cardinality_test")
        .check(
            Check::builder("wrong_expectation")
                .level(Level::Error)
                // Nation key has only 25 distinct values, not thousands
                .has_approx_count_distinct("n_nationkey", Assertion::GreaterThan(1000.0))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_failure());

    if let ValidationResult::Failure { report } = &results {
        assert!(!report.issues.is_empty());
        let issue = &report.issues[0];
        assert!(issue.message.contains("does not satisfy assertion"));
    }
}

#[tokio::test]
async fn test_with_optimizer() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    let supplier_df = ctx.table("supplier").await.unwrap();
    ctx.register_table("data", supplier_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("optimized_approx_distinct")
        .with_optimizer(true)
        .check(
            Check::builder("multiple_cardinality_checks")
                .level(Level::Warning)
                // Supplier key should be unique
                .has_approx_count_distinct("s_suppkey", Assertion::GreaterThan(1000.0))
                // Nation key has low cardinality
                .has_approx_count_distinct("s_nationkey", Assertion::LessThan(30.0))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    if let ValidationResult::Failure { report } = &results {
        for issue in &report.issues {
            eprintln!("Issue: {} - {}", issue.check_name, issue.message);
        }
    }
    assert!(results.is_success());
}

#[tokio::test]
async fn test_edge_cases() {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::*;
    use std::sync::Arc;

    let ctx = SessionContext::new();

    // Test with all same values
    let values: Vec<Option<i64>> = vec![Some(42); 1000];

    let schema = Arc::new(Schema::new(vec![Field::new(
        "test_col",
        DataType::Int64,
        true,
    )]));

    let array = Int64Array::from(values);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
    ctx.register_batch("data", batch).unwrap();

    let suite = ValidationSuite::builder("single_value_test")
        .check(
            Check::builder("all_same_values")
                .has_approx_count_distinct("test_col", Assertion::Equals(1.0))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    if let ValidationResult::Failure { report } = &results {
        for issue in &report.issues {
            eprintln!("Issue: {} - {}", issue.check_name, issue.message);
        }
    }
    assert!(results.is_success());
}
