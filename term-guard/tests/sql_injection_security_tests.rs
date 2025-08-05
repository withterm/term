//! Security tests for SQL injection prevention in table names

use datafusion::prelude::*;
use std::io::Write;
use tempfile::NamedTempFile;
use term_guard::core::{Check, ConstraintOptions, Level, ValidationContext, ValidationSuite};
use term_guard::sources::{CsvSource, DataSource};

#[tokio::test]
async fn test_sql_injection_prevention_drop_table() {
    // Attempt SQL injection with DROP TABLE
    let result = ValidationContext::new("test; DROP TABLE users; --");
    assert!(result.is_err(), "Should reject table name with DROP TABLE");

    let result = ValidationContext::new("test'; DROP TABLE users; --");
    assert!(
        result.is_err(),
        "Should reject table name with DROP TABLE variant"
    );
}

#[tokio::test]
async fn test_sql_injection_prevention_union_select() {
    // Attempt SQL injection with UNION SELECT
    let result = ValidationContext::new("test' UNION SELECT * FROM users --");
    assert!(
        result.is_err(),
        "Should reject table name with UNION SELECT"
    );

    let result = ValidationContext::new("test UNION ALL SELECT 1,2,3");
    assert!(result.is_err(), "Should reject table name with UNION");
}

#[tokio::test]
async fn test_sql_injection_prevention_comments() {
    // Attempt SQL injection with comments
    let result = ValidationContext::new("test--comment");
    assert!(result.is_err(), "Should reject table name with SQL comment");

    let result = ValidationContext::new("test/*comment*/");
    assert!(
        result.is_err(),
        "Should reject table name with SQL block comment"
    );

    let result = ValidationContext::new("test#comment");
    assert!(
        result.is_err(),
        "Should reject table name with hash comment"
    );
}

#[tokio::test]
async fn test_sql_injection_prevention_quotes() {
    // Attempt SQL injection with quotes - single quotes should be rejected
    let result = ValidationContext::new("test'");
    assert!(
        result.is_err(),
        "Should reject table name with single quote"
    );

    // Double quotes are now rejected for security (stricter validation)
    let result = ValidationContext::new("test_with_quote\"inside");
    assert!(
        result.is_err(),
        "Should reject table name with double quote"
    );

    let result = ValidationContext::new("test' OR '1'='1");
    assert!(
        result.is_err(),
        "Should reject table name with OR condition"
    );
}

#[tokio::test]
async fn test_sql_injection_prevention_semicolon() {
    // Attempt SQL injection with semicolons
    let result = ValidationContext::new("test;SELECT * FROM users");
    assert!(result.is_err(), "Should reject table name with semicolon");

    let result = ValidationContext::new("test;DELETE FROM users");
    assert!(
        result.is_err(),
        "Should reject table name with DELETE statement"
    );
}

#[tokio::test]
async fn test_sql_injection_prevention_whitespace_tricks() {
    // Attempt SQL injection with whitespace tricks
    let result = ValidationContext::new("test\nSELECT * FROM users");
    assert!(result.is_err(), "Should reject table name with newline");

    let result = ValidationContext::new("test\rSELECT * FROM users");
    assert!(
        result.is_err(),
        "Should reject table name with carriage return"
    );

    let result = ValidationContext::new("test\tSELECT * FROM users");
    assert!(result.is_err(), "Should reject table name with tab");
}

#[tokio::test]
async fn test_sql_injection_prevention_unicode_tricks() {
    // Attempt SQL injection with unicode tricks
    let result = ValidationContext::new("test\u{0000}SELECT");
    assert!(result.is_err(), "Should reject table name with null byte");

    let result = ValidationContext::new("test\u{202e}TCELES");
    assert!(
        result.is_err(),
        "Should reject table name with right-to-left override"
    );
}

#[tokio::test]
async fn test_sql_injection_prevention_empty_and_special() {
    // Test empty and special cases
    let result = ValidationContext::new("");
    assert!(result.is_err(), "Should reject empty table name");

    let result = ValidationContext::new("   ");
    assert!(result.is_err(), "Should reject whitespace-only table name");

    let result = ValidationContext::new(".");
    assert!(result.is_err(), "Should reject single dot");

    let result = ValidationContext::new("..");
    assert!(result.is_err(), "Should reject double dot");
}

#[tokio::test]
async fn test_valid_table_names_accepted() {
    // Test that valid table names are accepted
    let valid_names = vec![
        "users",
        "customer_data",
        "sales_2024",
        "inventory_items",
        "product_catalog",
        "UPPERCASE_TABLE",
        "MixedCase_Table",
        "table123",
        "table_with_underscores",
        "very_long_table_name_with_many_underscores_123",
    ];

    for name in valid_names {
        let result = ValidationContext::new(name);
        assert!(result.is_ok(), "Should accept valid table name: {name}");

        let ctx = result.unwrap();
        assert_eq!(ctx.table_name(), name);
        // Escaped name should be quoted
        assert!(ctx.escaped_table_name().starts_with('"'));
        assert!(ctx.escaped_table_name().ends_with('"'));
    }
}

#[tokio::test]
async fn test_escaped_table_name_in_constraint() {
    // Create a test CSV file
    let mut temp_file = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(temp_file, "id,name,value").unwrap();
    writeln!(temp_file, "1,Alice,100").unwrap();
    writeln!(temp_file, "2,Bob,200").unwrap();
    writeln!(temp_file, "3,Charlie,").unwrap(); // Missing value
    temp_file.flush().unwrap();

    // Create context and register data with a safe custom table name
    let ctx = SessionContext::new();
    let csv_source = CsvSource::new(temp_file.path().to_string_lossy().to_string()).unwrap();
    csv_source.register(&ctx, "test_table_123").await.unwrap();

    // Create validation suite with completeness constraint
    let check = Check::builder("completeness_check")
        .level(Level::Error)
        .completeness("value", ConstraintOptions::new().with_threshold(0.8))
        .build();

    let suite = ValidationSuite::builder("test_suite")
        .table_name("test_table_123")
        .check(check)
        .build();

    // Run validation with custom table name
    let result = suite.run(&ctx).await.unwrap();

    // Should fail because only 2/3 values are complete (66.7% < 80%)
    assert!(result.is_failure());
    assert!(result.report().has_errors());
    // Check for percentage in message - could be 66.66% or 66.67% depending on rounding
    let message = &result.report().issues[0].message;
    assert!(
        message.contains("66.6"),
        "Expected message to contain percentage around 66.6%, got: {message}"
    );
}

#[tokio::test]
async fn test_sql_injection_attempt_in_validation() {
    // Create a test CSV file
    let mut temp_file = NamedTempFile::new().unwrap();
    writeln!(temp_file, "id,name").unwrap();
    writeln!(temp_file, "1,Test").unwrap();
    temp_file.flush().unwrap();

    // Create context and register data
    let ctx = SessionContext::new();
    let csv_source = CsvSource::new(temp_file.path().to_string_lossy().to_string()).unwrap();
    csv_source.register(&ctx, "safe_table").await.unwrap();

    // Try to validate with a malicious table name by creating a suite with invalid table name
    let check = Check::builder("completeness_check")
        .level(Level::Error)
        .completeness("name", ConstraintOptions::new().with_threshold(1.0))
        .build();

    let suite_result = ValidationSuite::builder("test_suite")
        .table_name("safe_table; DROP TABLE users; --")
        .check(check)
        .build();

    // Building the suite itself should fail due to invalid table name during run
    let result = suite_result.run(&ctx).await;
    assert!(
        result.is_err(),
        "Should reject validation with malicious table name"
    );
}

#[tokio::test]
async fn test_table_name_with_special_chars_escaping() {
    // Test that special characters in valid table names are properly escaped
    let ctx = ValidationContext::new("table_with_underscore").unwrap();
    assert_eq!(ctx.escaped_table_name(), "\"table_with_underscore\"");

    let ctx = ValidationContext::new("TABLE123").unwrap();
    assert_eq!(ctx.escaped_table_name(), "\"TABLE123\"");

    let ctx = ValidationContext::new("MixedCaseTable").unwrap();
    assert_eq!(ctx.escaped_table_name(), "\"MixedCaseTable\"");
}

#[tokio::test]
async fn test_concurrent_validation_with_different_tables() {
    // Test that concurrent validations with different table names work correctly
    let mut temp_file1 = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(temp_file1, "id,value").unwrap();
    writeln!(temp_file1, "1,100").unwrap();
    temp_file1.flush().unwrap();

    let mut temp_file2 = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(temp_file2, "id,value").unwrap();
    writeln!(temp_file2, "1,").unwrap(); // Missing value
    temp_file2.flush().unwrap();

    // Create contexts
    let ctx1 = SessionContext::new();
    let ctx2 = SessionContext::new();

    // Register different tables
    let csv_source1 = CsvSource::new(temp_file1.path().to_string_lossy().to_string()).unwrap();
    csv_source1.register(&ctx1, "table_complete").await.unwrap();

    let csv_source2 = CsvSource::new(temp_file2.path().to_string_lossy().to_string()).unwrap();
    csv_source2
        .register(&ctx2, "table_incomplete")
        .await
        .unwrap();

    // Create validation suites with different table names
    let check1 = Check::builder("completeness_check")
        .level(Level::Error)
        .completeness("value", ConstraintOptions::new().with_threshold(1.0))
        .build();

    let suite1 = ValidationSuite::builder("suite1")
        .table_name("table_complete")
        .check(check1)
        .build();

    let check2 = Check::builder("completeness_check")
        .level(Level::Error)
        .completeness("value", ConstraintOptions::new().with_threshold(1.0))
        .build();

    let suite2 = ValidationSuite::builder("suite2")
        .table_name("table_incomplete")
        .check(check2)
        .build();

    // Run concurrent validations
    let (result1, result2) = tokio::join!(suite1.run(&ctx1), suite2.run(&ctx2));

    // First should succeed, second should fail
    assert!(result1.unwrap().is_success());
    assert!(result2.unwrap().is_failure());
}
