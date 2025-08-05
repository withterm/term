//! Focused test to verify ValidationContext security works properly

use term_guard::core::ValidationContext;

#[test]
fn test_validation_context_sql_injection_prevention() {
    // Test that dangerous SQL patterns are rejected
    let dangerous_names = vec![
        "test; DROP TABLE users",
        "test'; DROP TABLE users; --",
        "test' OR '1'='1",
        "test--comment",
        "test/*comment*/",
        "test UNION SELECT * FROM passwords",
        "",             // empty
        "   ",          // whitespace only
        "test\0null",   // null byte
        "test\nSELECT", // newline
    ];

    for name in dangerous_names {
        let result = ValidationContext::new(name);
        assert!(
            result.is_err(),
            "Should reject dangerous table name: '{name}'"
        );
    }

    // Test that valid names are accepted and properly escaped
    let valid_names = vec![
        ("users", "\"users\""),
        ("customer_data", "\"customer_data\""),
        ("sales_2024", "\"sales_2024\""),
        ("TABLE123", "\"TABLE123\""),
        ("my_table", "\"my_table\""),
    ];

    for (name, expected_escaped) in valid_names {
        let ctx = ValidationContext::new(name).unwrap();
        assert_eq!(ctx.table_name(), name);
        assert_eq!(ctx.escaped_table_name(), expected_escaped);
    }
}

#[test]
fn test_validation_context_rejects_quotes() {
    // Double quotes should now be rejected for security
    let result = ValidationContext::new("table_with_\"quote");
    assert!(
        result.is_err(),
        "Should reject table name with double quote"
    );
}

#[test]
fn test_validation_context_rejects_single_quotes() {
    // Single quotes should be rejected as they're dangerous
    let result = ValidationContext::new("table_with_'quote");
    assert!(result.is_err());
}
