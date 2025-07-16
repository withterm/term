//! Comprehensive tests for the new unified builder API.

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use std::sync::Arc;
use term_core::constraints::{
    Assertion, CompletenessConstraint, FormatOptions, FormatType, NullHandling, UniquenessOptions,
    UniquenessType,
};
use term_core::core::builder_extensions::{CompletenessOptions, StatisticalOptions};
use term_core::core::{Check, Level, ValidationResult, ValidationSuite};

/// Creates a test context with sample user data.
async fn create_user_context() -> SessionContext {
    let ctx = SessionContext::new();

    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("email", DataType::Utf8, true),
        Field::new("phone", DataType::Utf8, true),
        Field::new("age", DataType::Float64, true),
        Field::new("salary", DataType::Float64, true),
    ]));

    let user_ids = Int64Array::from(vec![1, 2, 3, 4, 5]);
    let emails = StringArray::from(vec![
        Some("user1@example.com"),
        Some("user2@example.com"),
        Some("invalid-email"),
        None,
        Some("user5@example.com"),
    ]);
    let phones = StringArray::from(vec![
        Some("555-123-4567"),
        Some("555-234-5678"),
        None,
        Some("invalid"),
        Some("555-345-6789"),
    ]);
    let ages = Float64Array::from(vec![
        Some(25.0),
        Some(30.0),
        Some(35.0),
        Some(40.0),
        Some(45.0),
    ]);
    let salaries = Float64Array::from(vec![
        Some(50000.0),
        Some(60000.0),
        Some(70000.0),
        Some(80000.0),
        Some(90000.0),
    ]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(user_ids),
            Arc::new(emails),
            Arc::new(phones),
            Arc::new(ages),
            Arc::new(salaries),
        ],
    )
    .unwrap();

    let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("data", Arc::new(provider)).unwrap();

    ctx
}

#[tokio::test]
async fn test_completeness_api() {
    let ctx = create_user_context().await;

    // Test full completeness
    let suite = ValidationSuite::builder("completeness_test")
        .check(
            Check::builder("full_completeness")
                .completeness(
                    "user_id",
                    CompletenessOptions::full().into_constraint_options(),
                )
                .build(),
        )
        .build();

    let result = suite.run(&ctx).await.unwrap();
    assert!(matches!(result, ValidationResult::Success { .. }));

    // Test threshold completeness
    let suite = ValidationSuite::builder("threshold_test")
        .check(
            Check::builder("email_completeness")
                .completeness(
                    "email",
                    CompletenessOptions::threshold(0.6).into_constraint_options(),
                )
                .build(),
        )
        .build();

    let result = suite.run(&ctx).await.unwrap();
    assert!(matches!(result, ValidationResult::Success { .. }));

    // Test any completeness
    let suite = ValidationSuite::builder("any_test")
        .check(
            Check::builder("contact_info")
                .completeness(
                    vec!["email", "phone"],
                    CompletenessOptions::any().into_constraint_options(),
                )
                .build(),
        )
        .build();

    let result = suite.run(&ctx).await.unwrap();
    assert!(matches!(result, ValidationResult::Success { .. }));

    // Test at least N completeness
    let suite = ValidationSuite::builder("at_least_test")
        .check(
            Check::builder("contact_info_min")
                .completeness(
                    vec!["email", "phone"],
                    CompletenessOptions::at_least(1).into_constraint_options(),
                )
                .build(),
        )
        .build();

    let result = suite.run(&ctx).await.unwrap();
    assert!(matches!(result, ValidationResult::Success { .. }));
}

#[tokio::test]
async fn test_statistics_api() {
    let ctx = create_user_context().await;

    // Test single statistic
    let suite = ValidationSuite::builder("single_stat")
        .check(
            Check::builder("age_min")
                .statistics(
                    "age",
                    StatisticalOptions::new().min(Assertion::GreaterThanOrEqual(20.0)),
                )
                .unwrap()
                .build(),
        )
        .build();

    let result = suite.run(&ctx).await.unwrap();
    assert!(matches!(result, ValidationResult::Success { .. }));

    // Test multiple statistics optimized
    let suite = ValidationSuite::builder("multi_stats")
        .check(
            Check::builder("salary_stats")
                .statistics(
                    "salary",
                    StatisticalOptions::new()
                        .min(Assertion::GreaterThan(40000.0))
                        .max(Assertion::LessThan(100000.0))
                        .mean(Assertion::Between(60000.0, 80000.0)),
                )
                .unwrap()
                .build(),
        )
        .build();

    let result = suite.run(&ctx).await.unwrap();
    assert!(matches!(result, ValidationResult::Success { .. }));

    // Test percentile
    let suite = ValidationSuite::builder("percentile_test")
        .check(
            Check::builder("age_percentile")
                .statistics(
                    "age",
                    StatisticalOptions::new()
                        .median(Assertion::Equals(35.0))
                        .percentile(0.9, Assertion::LessThanOrEqual(45.0)),
                )
                .unwrap()
                .build(),
        )
        .build();

    let result = suite.run(&ctx).await.unwrap();
    assert!(matches!(result, ValidationResult::Success { .. }));
}

#[tokio::test]
async fn test_format_api() {
    let ctx = create_user_context().await;

    // Test email validation
    let suite = ValidationSuite::builder("email_format")
        .check(
            Check::builder("email_check")
                .has_format("email", FormatType::Email, 0.6, FormatOptions::default())
                .build(),
        )
        .build();

    let result = suite.run(&ctx).await.unwrap();
    assert!(matches!(result, ValidationResult::Success { .. }));

    // Test phone validation with options
    let suite = ValidationSuite::builder("phone_format")
        .check(
            Check::builder("phone_check")
                .has_format(
                    "phone",
                    FormatType::Regex(r"^\d{3}-\d{3}-\d{4}$".to_string()),
                    0.5,
                    FormatOptions::new().null_is_valid(true),
                )
                .build(),
        )
        .build();

    let result = suite.run(&ctx).await.unwrap();
    assert!(matches!(result, ValidationResult::Success { .. }));
}

#[tokio::test]
async fn test_uniqueness_api() {
    let ctx = create_user_context().await;

    // Test full uniqueness
    let suite = ValidationSuite::builder("uniqueness_test")
        .check(
            Check::builder("user_id_unique")
                .uniqueness(
                    vec!["user_id"],
                    UniquenessType::FullUniqueness { threshold: 1.0 },
                    UniquenessOptions::default(),
                )
                .build(),
        )
        .build();

    let result = suite.run(&ctx).await.unwrap();
    assert!(matches!(result, ValidationResult::Success { .. }));

    // Test uniqueness with nulls
    let suite = ValidationSuite::builder("email_uniqueness")
        .check(
            Check::builder("email_unique")
                .uniqueness(
                    vec!["email"],
                    UniquenessType::UniqueWithNulls {
                        threshold: 0.8,
                        null_handling: NullHandling::Exclude,
                    },
                    UniquenessOptions::default(),
                )
                .build(),
        )
        .build();

    let result = suite.run(&ctx).await.unwrap();
    assert!(matches!(result, ValidationResult::Success { .. }));
}

#[tokio::test]
async fn test_complex_builder_api() {
    let ctx = create_user_context().await;

    // Use the with_constraints helper for complex validation
    let suite = ValidationSuite::builder("complex_validation")
        .check(
            Check::builder("user_validation")
                .level(Level::Error)
                .with_constraints(|builder| {
                    builder
                        .completeness(
                            "user_id",
                            CompletenessOptions::full().into_constraint_options(),
                        )
                        .completeness(
                            "email",
                            CompletenessOptions::threshold(0.7).into_constraint_options(),
                        )
                        .has_format("email", FormatType::Email, 0.6, FormatOptions::default())
                        .statistics(
                            "age",
                            StatisticalOptions::new()
                                .min(Assertion::GreaterThanOrEqual(0.0))
                                .max(Assertion::LessThan(150.0)),
                        )
                        .unwrap()
                        .uniqueness(
                            vec!["user_id"],
                            UniquenessType::FullUniqueness { threshold: 1.0 },
                            UniquenessOptions::default(),
                        )
                })
                .build(),
        )
        .build();

    let result = suite.run(&ctx).await.unwrap();
    match result {
        ValidationResult::Success { report, .. } => {
            // All checks should have been evaluated
            assert!(report.metrics.total_checks >= 1);
        }
        ValidationResult::Failure { .. } => {
            panic!("Complex validation should have succeeded");
        }
    }
}

#[tokio::test]
async fn test_convenience_methods() {
    let ctx = create_user_context().await;

    // Test email convenience method
    let suite = ValidationSuite::builder("email_convenience")
        .check(
            Check::builder("email_validation")
                .email("email", 0.6)
                .build(),
        )
        .build();

    let result = suite.run(&ctx).await.unwrap();
    assert!(matches!(result, ValidationResult::Success { .. }));

    // Test primary key convenience method
    let suite = ValidationSuite::builder("primary_key")
        .check(
            Check::builder("pk_validation")
                .primary_key(vec!["user_id"])
                .build(),
        )
        .build();

    let result = suite.run(&ctx).await.unwrap();
    assert!(matches!(result, ValidationResult::Success { .. }));

    // Test value range convenience method
    let suite = ValidationSuite::builder("value_range")
        .check(
            Check::builder("age_range")
                .value_range("age", 0.0, 100.0)
                .unwrap()
                .build(),
        )
        .build();

    let result = suite.run(&ctx).await.unwrap();
    assert!(matches!(result, ValidationResult::Success { .. }));
}

#[tokio::test]
async fn test_backward_compatibility() {
    let ctx = create_user_context().await;

    // Old API should still work (with deprecation warnings)
    #[allow(deprecated)]
    let suite = ValidationSuite::builder("old_api")
        .check(
            Check::builder("old_methods")
                .constraint(CompletenessConstraint::complete("user_id"))
                .constraint(CompletenessConstraint::with_threshold("email", 0.6))
                .has_mean("age", Assertion::Between(20.0, 50.0))
                .has_min("salary", Assertion::GreaterThan(0.0))
                .build(),
        )
        .build();

    let result = suite.run(&ctx).await.unwrap();
    assert!(matches!(result, ValidationResult::Success { .. }));
}

#[tokio::test]
async fn test_error_handling() {
    // Test valid percentile range (0.0 to 1.0)
    let result = StatisticalOptions::new().percentile(0.95, Assertion::LessThan(100.0));

    // This should succeed with valid percentile
    let check_result = Check::builder("valid_percentile").statistics("column", result);

    // The statistics method should accept valid percentiles
    assert!(
        check_result.is_ok(),
        "Valid percentile should not cause an error"
    );

    // Test that the new API handles edge cases gracefully
    let _ctx = create_user_context().await;

    // Test with empty column list (should be caught during construction)
    let empty_columns: Vec<String> = vec![];
    let check = Check::builder("empty_columns")
        .completeness(
            empty_columns.clone(),
            CompletenessOptions::full().into_constraint_options(),
        )
        .build();

    // Empty column list should still create a valid check
    assert_eq!(check.constraints().len(), 1);

    // Test with very long column name
    let long_column = "a".repeat(1000);
    let check = Check::builder("long_column")
        .completeness(
            long_column,
            CompletenessOptions::full().into_constraint_options(),
        )
        .build();

    // Long column name should still create a valid check
    assert_eq!(check.constraints().len(), 1);
}

#[test]
fn test_options_builders() {
    // Test StatisticalOptions
    let stats = StatisticalOptions::new()
        .min(Assertion::GreaterThan(0.0))
        .max(Assertion::LessThan(100.0))
        .mean(Assertion::Between(40.0, 60.0))
        .standard_deviation(Assertion::LessThan(20.0));

    assert!(stats.is_multi());
    let stats_vec = stats.into_statistics();
    assert_eq!(stats_vec.len(), 4);
}

#[tokio::test]
async fn test_complex_real_world_scenario() {
    let ctx = create_user_context().await;

    // Real-world user data validation
    let suite = ValidationSuite::builder("user_data_quality")
        .description("Comprehensive user data validation")
        .check(
            Check::builder("critical_fields")
                .level(Level::Error)
                .description("Critical fields must be valid")
                // Primary key validation
                .completeness(
                    "user_id",
                    CompletenessOptions::full().into_constraint_options(),
                )
                .uniqueness(
                    vec!["user_id"],
                    UniquenessType::FullUniqueness { threshold: 1.0 },
                    UniquenessOptions::default(),
                )
                // Contact information - both should have high completeness individually
                .completeness(
                    "email",
                    CompletenessOptions::threshold(0.8).into_constraint_options(),
                )
                .completeness(
                    "phone",
                    CompletenessOptions::threshold(0.6).into_constraint_options(),
                )
                .build(),
        )
        .check(
            Check::builder("data_quality")
                .level(Level::Warning)
                .description("Data quality checks")
                // Format validation
                .has_format("email", FormatType::Email, 0.6, FormatOptions::default())
                .has_format(
                    "phone",
                    FormatType::Regex(r"^\d{3}-\d{3}-\d{4}$".to_string()),
                    0.5,
                    FormatOptions::new().null_is_valid(true),
                )
                // Statistical validation
                .statistics(
                    "age",
                    StatisticalOptions::new()
                        .min(Assertion::GreaterThanOrEqual(18.0))
                        .max(Assertion::LessThanOrEqual(100.0))
                        .mean(Assertion::Between(25.0, 65.0)),
                )
                .unwrap()
                .statistics(
                    "salary",
                    StatisticalOptions::new()
                        .min(Assertion::GreaterThan(0.0))
                        .median(Assertion::GreaterThan(50000.0)),
                )
                .unwrap()
                .build(),
        )
        .build();

    let result = suite.run(&ctx).await.unwrap();

    match result {
        ValidationResult::Success { report, .. } => {
            println!("Validation Report:");
            println!("  Total checks: {}", report.metrics.total_checks);
            println!("  Passed: {}", report.metrics.passed_checks);
            println!("  Failed: {}", report.metrics.failed_checks);

            // Should have evaluated all checks
            assert!(report.metrics.total_checks >= 2);
            assert!(report.metrics.passed_checks > 0);
        }
        ValidationResult::Failure { report } => {
            // Expected to succeed, but let's check for warnings
            println!("Validation issues found:");
            for issue in &report.issues {
                println!(
                    "  {} - {}: {}",
                    issue.level, issue.constraint_name, issue.message
                );
            }
            // The test should still fail if there are errors
            if report.has_errors() {
                panic!(
                    "Validation had errors: {:?}",
                    report.issues_by_level(Level::Error)
                );
            }
        }
    }
}
