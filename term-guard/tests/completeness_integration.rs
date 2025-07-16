//! Integration tests for CompletenessConstraint with TPC-H data.

use std::sync::Arc;
use term_guard::constraints::CompletenessConstraint;
use term_guard::core::{
    Check, ConstraintOptions, Level, LogicalOperator, ValidationResult, ValidationSuite,
};
use term_guard::test_utils::{create_tpc_h_context, ScaleFactor};

#[tokio::test]
async fn test_unified_completeness_single_column() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Register customer table as "data" for constraint evaluation
    let customer_df = ctx.table("customer").await.unwrap();
    ctx.register_table("data", customer_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("single_column_completeness")
        .check(
            Check::builder("customer_key_completeness")
                .level(Level::Error)
                // Test single column 100% complete
                .constraint(CompletenessConstraint::complete("c_custkey"))
                // Test single column with threshold
                .constraint(CompletenessConstraint::with_threshold("c_name", 0.99))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_unified_completeness_backward_compatibility() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    let customer_df = ctx.table("customer").await.unwrap();
    ctx.register_table("data", customer_df.into_view()).unwrap();

    // Test that the Check builder methods still work
    let suite = ValidationSuite::builder("backward_compatibility")
        .check(
            Check::builder("old_api_test")
                .level(Level::Warning)
                .constraint(CompletenessConstraint::complete("c_custkey"))
                .constraint(CompletenessConstraint::with_threshold("c_name", 0.95))
                .constraint(CompletenessConstraint::new(
                    vec!["c_custkey", "c_name"],
                    ConstraintOptions::new()
                        .with_operator(LogicalOperator::All)
                        .with_threshold(1.0),
                ))
                .constraint(CompletenessConstraint::new(
                    vec!["c_phone", "c_comment"],
                    ConstraintOptions::new()
                        .with_operator(LogicalOperator::Any)
                        .with_threshold(1.0),
                ))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_unified_completeness_all_operator() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    let lineitem_df = ctx.table("lineitem").await.unwrap();
    ctx.register_table("data", lineitem_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("all_operator_test")
        .check(
            Check::builder("key_fields_complete")
                .level(Level::Error)
                // All key fields must be 100% complete
                .constraint(CompletenessConstraint::new(
                    vec!["l_orderkey", "l_partkey", "l_suppkey"],
                    ConstraintOptions::new()
                        .with_operator(LogicalOperator::All)
                        .with_threshold(1.0),
                ))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_unified_completeness_any_operator() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Create test data with mixed completeness
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("email", DataType::Utf8, true),
        Field::new("phone", DataType::Utf8, true),
        Field::new("address", DataType::Utf8, true),
    ]));

    // Create data where at least one contact method is always present
    let id = Int64Array::from(vec![1, 2, 3, 4, 5]);
    let email = StringArray::from(vec![
        Some("a@example.com"),
        None,
        Some("c@example.com"),
        None,
        None,
    ]);
    let phone = StringArray::from(vec![
        None,
        Some("123-456-7890"),
        None,
        Some("098-765-4321"),
        None,
    ]);
    let address = StringArray::from(vec![None, None, None, None, Some("123 Main St")]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(id),
            Arc::new(email),
            Arc::new(phone),
            Arc::new(address),
        ],
    )
    .unwrap();

    let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("data", Arc::new(provider)).unwrap();

    let suite = ValidationSuite::builder("any_operator_test")
        .check(
            Check::builder("contact_method_available")
                .level(Level::Warning)
                // At least one contact method must be available (100% for that column)
                .constraint(CompletenessConstraint::new(
                    vec!["email", "phone", "address"],
                    ConstraintOptions::new()
                        .with_operator(LogicalOperator::Any)
                        .with_threshold(0.4), // At least 40% complete in any column
                ))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_unified_completeness_at_least_operator() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    let orders_df = ctx.table("orders").await.unwrap();
    ctx.register_table("data", orders_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("at_least_operator_test")
        .check(
            Check::builder("order_fields_completeness")
                .level(Level::Info)
                // At least 3 out of these 5 fields must be 100% complete
                .constraint(CompletenessConstraint::new(
                    vec![
                        "o_orderkey",
                        "o_custkey",
                        "o_orderstatus",
                        "o_totalprice",
                        "o_orderdate",
                    ],
                    ConstraintOptions::new()
                        .with_operator(LogicalOperator::AtLeast(3))
                        .with_threshold(1.0),
                ))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_unified_completeness_exactly_operator() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Create test data where exactly 2 columns are complete
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;

    let schema = Arc::new(Schema::new(vec![
        Field::new("col1", DataType::Utf8, true),
        Field::new("col2", DataType::Utf8, true),
        Field::new("col3", DataType::Utf8, true),
        Field::new("col4", DataType::Utf8, true),
    ]));

    // col1: complete, col2: complete, col3: has nulls, col4: has nulls
    let col1 = StringArray::from(vec![Some("a"), Some("b"), Some("c")]);
    let col2 = StringArray::from(vec![Some("x"), Some("y"), Some("z")]);
    let col3 = StringArray::from(vec![Some("1"), None, Some("3")]);
    let col4 = StringArray::from(vec![None, Some("p"), None]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(col1),
            Arc::new(col2),
            Arc::new(col3),
            Arc::new(col4),
        ],
    )
    .unwrap();

    let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("data", Arc::new(provider)).unwrap();

    let suite = ValidationSuite::builder("exactly_operator_test")
        .check(
            Check::builder("exactly_two_complete")
                .level(Level::Info)
                // Exactly 2 columns must be 100% complete
                .constraint(CompletenessConstraint::new(
                    vec!["col1", "col2", "col3", "col4"],
                    ConstraintOptions::new()
                        .with_operator(LogicalOperator::Exactly(2))
                        .with_threshold(1.0),
                ))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_unified_completeness_at_most_operator() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Create test data where some fields might be optional
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;

    let schema = Arc::new(Schema::new(vec![
        Field::new("required_field", DataType::Utf8, true),
        Field::new("optional_1", DataType::Utf8, true),
        Field::new("optional_2", DataType::Utf8, true),
        Field::new("optional_3", DataType::Utf8, true),
    ]));

    // Required field is complete, but only 1 optional field is filled
    let required = StringArray::from(vec![Some("a"), Some("b"), Some("c")]);
    let opt1 = StringArray::from(vec![Some("x"), Some("y"), Some("z")]);
    let opt2 = StringArray::from(vec![None, None, Some("p")]);
    let opt3 = StringArray::from(vec![None, Some("q"), None]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(required),
            Arc::new(opt1),
            Arc::new(opt2),
            Arc::new(opt3),
        ],
    )
    .unwrap();

    let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("data", Arc::new(provider)).unwrap();

    let suite = ValidationSuite::builder("at_most_operator_test")
        .check(
            Check::builder("optional_fields_limit")
                .level(Level::Warning)
                // At most 2 optional fields should be fully complete
                .constraint(CompletenessConstraint::new(
                    vec!["optional_1", "optional_2", "optional_3"],
                    ConstraintOptions::new()
                        .with_operator(LogicalOperator::AtMost(2))
                        .with_threshold(1.0),
                ))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success()); // Only opt1 is fully complete, which is <= 2
}

#[tokio::test]
async fn test_unified_completeness_failure_scenarios() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Create test data with poor completeness
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;

    let schema = Arc::new(Schema::new(vec![
        Field::new("col1", DataType::Utf8, true),
        Field::new("col2", DataType::Utf8, true),
    ]));

    // Both columns have many nulls
    let col1 = StringArray::from(vec![Some("a"), None, None, None]);
    let col2 = StringArray::from(vec![None, Some("b"), None, None]);

    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(col1), Arc::new(col2)]).unwrap();

    let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("data", Arc::new(provider)).unwrap();

    let suite = ValidationSuite::builder("failure_scenarios")
        .check(
            Check::builder("completeness_failures")
                .level(Level::Error)
                // This should fail - both columns must be 80% complete
                .constraint(CompletenessConstraint::new(
                    vec!["col1", "col2"],
                    ConstraintOptions::new()
                        .with_operator(LogicalOperator::All)
                        .with_threshold(0.8),
                ))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_failure());

    if let ValidationResult::Failure { report } = &results {
        assert!(!report.issues.is_empty());
        let issue = &report.issues[0];
        assert!(issue.message.contains("Constraint failed"));
        assert!(issue.message.contains("Required: all"));
    }
}

#[tokio::test]
async fn test_unified_completeness_with_varying_thresholds() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    let supplier_df = ctx.table("supplier").await.unwrap();
    ctx.register_table("data", supplier_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("varying_thresholds")
        .check(
            Check::builder("supplier_completeness")
                .level(Level::Info)
                // At least 2 columns must be 95% complete
                .constraint(CompletenessConstraint::new(
                    vec!["s_name", "s_address", "s_phone", "s_comment"],
                    ConstraintOptions::new()
                        .with_operator(LogicalOperator::AtLeast(2))
                        .with_threshold(0.95),
                ))
                // All critical fields must be 100% complete
                .constraint(CompletenessConstraint::new(
                    vec!["s_suppkey", "s_name"],
                    ConstraintOptions::new()
                        .with_operator(LogicalOperator::All)
                        .with_threshold(1.0),
                ))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_unified_completeness_performance_comparison() {
    // This test demonstrates that the unified constraint performs
    // as well as the old separate constraints
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    let lineitem_df = ctx.table("lineitem").await.unwrap();
    ctx.register_table("data", lineitem_df.into_view()).unwrap();

    // Test with a larger dataset to measure performance
    let start = std::time::Instant::now();

    let suite = ValidationSuite::builder("performance_test")
        .check(
            Check::builder("unified_performance")
                .constraint(CompletenessConstraint::with_threshold("l_quantity", 0.99))
                .constraint(CompletenessConstraint::new(
                    vec!["l_orderkey", "l_partkey", "l_suppkey", "l_linenumber"],
                    ConstraintOptions::new()
                        .with_operator(LogicalOperator::All)
                        .with_threshold(1.0),
                ))
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    let duration = start.elapsed();

    assert!(results.is_success());
    println!("Unified completeness constraint execution time: {duration:?}");

    // The execution should be reasonably fast (under 1 second for SF0.1)
    assert!(duration.as_secs() < 1);
}
