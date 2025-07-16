use term_core::core::{Check, Level, ValidationResult, ValidationSuite};
use term_core::test_utils::{create_tpc_h_context, ScaleFactor};

#[tokio::test]
async fn test_string_length_constraints_on_tpc_h_customer() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Register customer table as "data" for constraint evaluation
    let customer_df = ctx.table("customer").await.unwrap();
    ctx.register_table("data", customer_df.into_view()).unwrap();

    // Test string length constraints on customer table columns
    let suite = ValidationSuite::builder("tpc_h_string_length")
        .check(
            Check::builder("customer_name_length")
                .level(Level::Error)
                // Customer names in TPC-H are typically between 5 and 25 characters
                .has_min_length("c_name", 5)
                .has_max_length("c_name", 25)
                .build(),
        )
        .check(
            Check::builder("customer_address_length")
                .level(Level::Warning)
                // Addresses should be at least 10 characters and no more than 40
                .has_min_length("c_address", 10)
                .has_max_length("c_address", 40)
                .build(),
        )
        .check(
            Check::builder("customer_phone_length")
                .level(Level::Error)
                // Phone numbers in TPC-H follow pattern: XX-XXX-XXX-XXXX (15 chars)
                .has_min_length("c_phone", 15)
                .has_max_length("c_phone", 15)
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

    // Verify no critical issues
    if let ValidationResult::Success { report, .. } = &results {
        let critical_issues = report
            .issues
            .iter()
            .filter(|issue| issue.level == Level::Error)
            .count();
        assert_eq!(critical_issues, 0, "Found critical issues in validation");
    }
}

#[tokio::test]
async fn test_string_length_constraints_on_tpc_h_nation() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Register nation table as "data" for constraint evaluation
    let nation_df = ctx.table("nation").await.unwrap();
    ctx.register_table("data", nation_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("nation_string_length")
        .check(
            Check::builder("nation_name_length")
                .level(Level::Error)
                // Nation names are typically short (2-15 characters)
                .has_min_length("n_name", 2)
                .has_max_length("n_name", 15)
                .build(),
        )
        .check(
            Check::builder("nation_comment_length")
                .level(Level::Warning)
                // Comments can be longer but should have reasonable limits
                .has_min_length("n_comment", 20)
                .has_max_length("n_comment", 150)
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_string_length_constraints_on_tpc_h_part() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Register part table as "data" for constraint evaluation
    let part_df = ctx.table("part").await.unwrap();
    ctx.register_table("data", part_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("part_string_validation")
        .check(
            Check::builder("part_names_and_brands")
                .level(Level::Error)
                // Part names and brands have specific length requirements
                .has_min_length("p_name", 10)
                .has_max_length("p_name", 55)
                .has_min_length("p_brand", 5)
                .has_max_length("p_brand", 10)
                .build(),
        )
        .check(
            Check::builder("part_type_length")
                .level(Level::Warning)
                // Part types follow a pattern with consistent length
                .has_min_length("p_type", 15)
                .has_max_length("p_type", 25)
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_string_length_with_nulls() {
    use arrow::array::StringArray;
    use arrow::record_batch::RecordBatch;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::*;
    use std::sync::Arc;

    let ctx = SessionContext::new();

    // Create test data with some NULL values
    let data = StringArray::from(vec![
        Some("short"),
        Some("medium length string"),
        None, // NULL should be ignored
        Some("another medium string"),
        Some("tiny"),
        None, // Another NULL
    ]);

    let schema = Arc::new(Schema::new(vec![Field::new(
        "text_column",
        DataType::Utf8,
        true,
    )]));

    let batch = RecordBatch::try_new(schema, vec![Arc::new(data)]).unwrap();
    ctx.register_batch("data", batch).unwrap();

    let suite = ValidationSuite::builder("null_handling")
        .check(
            Check::builder("length_with_nulls")
                .has_min_length("text_column", 4) // "tiny" is 4 chars
                .has_max_length("text_column", 25) // All non-null values fit
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_string_length_failures() {
    use arrow::array::StringArray;
    use arrow::record_batch::RecordBatch;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::*;
    use std::sync::Arc;

    let ctx = SessionContext::new();

    // Create test data that will fail constraints
    let data = StringArray::from(vec![
        Some("a"),                                                   // Too short
        Some("bb"),                                                  // Too short
        Some("proper"),                                              // OK
        Some("this is a very long string that exceeds our maximum"), // Too long
    ]);

    let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)]));

    let batch = RecordBatch::try_new(schema, vec![Arc::new(data)]).unwrap();
    ctx.register_batch("data", batch).unwrap();

    let suite = ValidationSuite::builder("expected_failures")
        .check(
            Check::builder("strict_length_check")
                .level(Level::Error)
                .has_min_length("text", 5)
                .has_max_length("text", 20)
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();

    // This should fail because some strings don't meet the criteria
    assert!(results.is_failure());

    // Check that we have issues reported
    if let ValidationResult::Failure { report } = &results {
        // Should have at least 2 issues (min length and max length failures)
        assert!(report.issues.len() >= 2);

        // Check that we have error-level issues
        let error_issues = report
            .issues
            .iter()
            .filter(|issue| issue.level == Level::Error)
            .count();
        assert!(
            error_issues > 0,
            "Expected error-level issues for failed constraints"
        );

        // Check that failure messages mention length (case-insensitive)
        let length_issues = report
            .issues
            .iter()
            .filter(|issue| issue.message.to_lowercase().contains("length"))
            .count();
        assert!(
            length_issues >= 2,
            "Expected issues mentioning 'length' but found {}",
            length_issues
        );
    } else {
        panic!("Expected validation to fail");
    }
}

#[tokio::test]
async fn test_string_length_with_optimizer() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Register customer table as "data" for constraint evaluation
    let customer_df = ctx.table("customer").await.unwrap();
    ctx.register_table("data", customer_df.into_view()).unwrap();

    // Test with optimizer enabled - multiple string length checks should be optimized
    let suite = ValidationSuite::builder("optimized_string_checks")
        .with_optimizer(true)
        .check(
            Check::builder("comprehensive_string_validation")
                .level(Level::Error)
                // Multiple constraints on the same table
                .has_min_length("c_name", 5)
                .has_max_length("c_name", 25)
                .has_min_length("c_phone", 15)
                .has_max_length("c_phone", 15)
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();

    // Debug: print validation results if failed
    if let ValidationResult::Failure { report } = &results {
        println!("Optimizer test failed!");
        for issue in &report.issues {
            println!("Issue: {:?} - {}", issue.level, issue.message);
        }
    }

    assert!(results.is_success(), "Optimized validation should pass");

    // All string length constraints should pass - verify via report
    if let ValidationResult::Success { report, .. } = &results {
        // Should have no error-level issues
        let error_count = report
            .issues
            .iter()
            .filter(|issue| issue.level == Level::Error)
            .count();
        assert_eq!(
            error_count, 0,
            "No errors expected for comprehensive validation"
        );
    }
}
