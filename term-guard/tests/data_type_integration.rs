//! Integration tests for DataTypeConstraint with TPC-H data.

use term_guard::constraints::{
    DataTypeConstraint, DataTypeValidation, NumericValidation, StringTypeValidation,
};
use term_guard::core::{Check, Level, ValidationResult, ValidationSuite};
use term_guard::test_utils::{create_tpc_h_context, ScaleFactor};

#[tokio::test]
async fn test_data_type_constraints_on_tpc_h_customer() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Register customer table as "data" for constraint evaluation
    let customer_df = ctx.table("customer").await.unwrap();
    ctx.register_table("data", customer_df.into_view()).unwrap();

    // Test data type constraints on customer table string columns
    let suite = ValidationSuite::builder("tpc_h_data_types")
        .check(
            Check::builder("customer_string_fields")
                .level(Level::Warning)
                // String columns should all match string pattern
                .constraint(DataTypeConstraint::specific_type("c_name", "Utf8").unwrap())
                .constraint(DataTypeConstraint::specific_type("c_address", "Utf8").unwrap())
                .constraint(DataTypeConstraint::specific_type("c_phone", "Utf8").unwrap())
                .constraint(DataTypeConstraint::specific_type("c_mktsegment", "Utf8").unwrap())
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();

    // Debug: print validation results
    if let ValidationResult::Failure { report } = &results {
        println!("Validation failed!");
        for issue in &report.issues {
            println!("Issue: {:?} - {}", issue.level, issue.message);
        }
    }

    // All constraints should pass for TPC-H data
    assert!(
        results.is_success(),
        "Validation should pass for TPC-H customer data"
    );
}

#[tokio::test]
async fn test_data_type_constraints_on_mixed_data() {
    use arrow::array::StringArray;
    use arrow::record_batch::RecordBatch;
    use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use datafusion::prelude::*;
    use std::sync::Arc;

    let ctx = SessionContext::new();

    // Create test data with mixed types as strings
    let mixed_column = StringArray::from(vec![
        Some("123"),
        Some("456.78"),
        Some("true"),
        Some("hello"),
        Some("2024-01-15"),
        Some("999"),
        Some("false"),
        Some("3.14159"),
        None,
        Some("world"),
    ]);

    let schema = Arc::new(Schema::new(vec![Field::new(
        "mixed_data",
        ArrowDataType::Utf8,
        true,
    )]));

    let batch = RecordBatch::try_new(schema, vec![Arc::new(mixed_column)]).unwrap();
    ctx.register_batch("data", batch).unwrap();

    // Test with string type validation
    let suite = ValidationSuite::builder("mixed_type_validation")
        .check(
            Check::builder("string_validation")
                .constraint(
                    DataTypeConstraint::new(
                        "mixed_data",
                        DataTypeValidation::String(StringTypeValidation::NotEmpty),
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
async fn test_data_type_constraints_with_numeric_columns() {
    use arrow::array::{Float64Array, Int64Array};
    use arrow::record_batch::RecordBatch;
    use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use datafusion::prelude::*;
    use std::sync::Arc;

    let ctx = SessionContext::new();

    // Create actual numeric columns
    let int_column = Int64Array::from(vec![Some(1), Some(2), Some(3), Some(4), None]);
    let float_column = Float64Array::from(vec![
        Some(1.5),
        Some(2.7),
        Some(std::f64::consts::PI),
        None,
        Some(4.0),
    ]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("int_col", ArrowDataType::Int64, true),
        Field::new("float_col", ArrowDataType::Float64, true),
    ]));

    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(int_column), Arc::new(float_column)]).unwrap();
    ctx.register_batch("data", batch).unwrap();

    // Test schema-level type validation
    let suite = ValidationSuite::builder("numeric_type_validation")
        .check(
            Check::builder("check_numeric_types")
                .constraint(DataTypeConstraint::specific_type("int_col", "Int64").unwrap())
                .constraint(DataTypeConstraint::specific_type("float_col", "Float64").unwrap())
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_data_type_numeric_validations() {
    use arrow::array::Float64Array;
    use arrow::record_batch::RecordBatch;
    use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use datafusion::prelude::*;
    use std::sync::Arc;

    let ctx = SessionContext::new();

    // Create test data with positive and negative values
    let data = Float64Array::from(vec![
        Some(10.5),
        Some(-5.2),
        Some(0.0),
        Some(123.456),
        Some(-999.0),
    ]);

    let schema = Arc::new(Schema::new(vec![Field::new(
        "numbers",
        ArrowDataType::Float64,
        true,
    )]));

    let batch = RecordBatch::try_new(schema, vec![Arc::new(data)]).unwrap();
    ctx.register_batch("data", batch).unwrap();

    let suite = ValidationSuite::builder("numeric_validation")
        .check(
            Check::builder("non_negative_check")
                .level(Level::Error)
                .constraint(
                    DataTypeConstraint::new(
                        "numbers",
                        DataTypeValidation::Numeric(NumericValidation::NonNegative),
                    )
                    .unwrap(),
                )
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();

    // This should fail because we have negative values
    assert!(results.is_failure());

    if let ValidationResult::Failure { report } = &results {
        assert!(!report.issues.is_empty());
        let error_issues = report
            .issues
            .iter()
            .filter(|issue| issue.level == Level::Error)
            .count();
        assert!(error_issues > 0);
    }
}

#[tokio::test]
async fn test_data_type_string_validations() {
    use arrow::array::StringArray;
    use arrow::record_batch::RecordBatch;
    use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use datafusion::prelude::*;
    use std::sync::Arc;

    let ctx = SessionContext::new();

    // Test edge cases for string validation
    let data = StringArray::from(vec![
        Some("normal string"),
        Some(""),    // Empty string
        Some("   "), // Whitespace only
        Some("another string"),
        None, // Null value
    ]);

    let schema = Arc::new(Schema::new(vec![Field::new(
        "strings",
        ArrowDataType::Utf8,
        true,
    )]));

    let batch = RecordBatch::try_new(schema, vec![Arc::new(data)]).unwrap();
    ctx.register_batch("data", batch).unwrap();

    let suite = ValidationSuite::builder("string_validation")
        .check(
            Check::builder("not_empty_check")
                .level(Level::Error) // Set to Error level to fail the validation
                .constraint(
                    DataTypeConstraint::new(
                        "strings",
                        DataTypeValidation::String(StringTypeValidation::NotEmpty),
                    )
                    .unwrap(),
                )
                .build(),
        )
        .check(
            Check::builder("not_blank_check")
                .level(Level::Error) // Set to Error level to fail the validation
                .constraint(
                    DataTypeConstraint::new(
                        "strings",
                        DataTypeValidation::String(StringTypeValidation::NotBlank),
                    )
                    .unwrap(),
                )
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();

    // Should fail because we have empty and blank strings
    assert!(results.is_failure());

    if let ValidationResult::Failure { report } = &results {
        assert!(!report.issues.is_empty());
        println!(
            "Validation failed as expected with {} issues",
            report.issues.len()
        );
        for issue in &report.issues {
            println!("Issue: {}", issue.message);
        }
    } else {
        panic!("Expected validation to fail but it passed");
    }
}
