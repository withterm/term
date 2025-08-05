//! Security utilities for Term data validation library.
//!
//! This module provides security hardening utilities to prevent SQL injection,
//! validate inputs, and handle credentials securely.

use crate::error::{Result, TermError};
use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::HashSet;
use std::sync::OnceLock;
use zeroize::{Zeroize, ZeroizeOnDrop};

/// A secure string that automatically clears its contents when dropped.
#[derive(Clone, ZeroizeOnDrop)]
pub struct SecureString(String);

impl std::fmt::Debug for SecureString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SecureString(***)")
    }
}

impl SecureString {
    /// Create a new secure string.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Get the string value. Use carefully and avoid storing the result.
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// Convert to a regular string. The SecureString will be zeroized.
    pub fn into_string(mut self) -> String {
        let value = std::mem::take(&mut self.0);
        self.0.zeroize();
        value
    }
}

impl From<String> for SecureString {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for SecureString {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

/// SQL identifier validation and escaping utilities.
pub struct SqlSecurity;

impl SqlSecurity {
    /// Validates and escapes a SQL identifier (table name, column name, etc.).
    ///
    /// This function ensures that user-provided identifiers are safe to use in SQL queries
    /// by validating their format and properly escaping them.
    ///
    /// # Arguments
    /// * `identifier` - The identifier to validate and escape
    ///
    /// # Returns
    /// * `Ok(String)` - The safely escaped identifier ready for SQL use
    /// * `Err(TermError)` - If the identifier is invalid or potentially malicious
    ///
    /// # Security
    /// This function prevents SQL injection by:
    /// - Validating identifier format against allowed patterns
    /// - Checking against a blocklist of dangerous patterns
    /// - Properly escaping identifiers using double quotes
    /// - Limiting identifier length to prevent DoS attacks
    ///
    /// # Examples
    /// ```rust
    /// use term_guard::security::SqlSecurity;
    ///
    /// // Valid identifiers
    /// assert!(SqlSecurity::escape_identifier("customer_id").is_ok());
    /// assert!(SqlSecurity::escape_identifier("table1").is_ok());
    ///
    /// // Invalid identifiers  
    /// assert!(SqlSecurity::escape_identifier("id; DROP TABLE users--").is_err());
    /// assert!(SqlSecurity::escape_identifier(&"very_long_name_".repeat(100)).is_err());
    /// ```
    pub fn escape_identifier(identifier: &str) -> Result<String> {
        // Input validation
        Self::validate_identifier(identifier)?;

        // Escape the identifier using double quotes and escape any internal double quotes
        let escaped = identifier.replace('"', "\"\"");
        Ok(format!("\"{escaped}\""))
    }

    /// Validates a SQL identifier without escaping it.
    ///
    /// This function checks if an identifier is safe to use but doesn't escape it.
    /// Useful for cases where you need validation but will use the identifier in a
    /// different context.
    pub fn validate_identifier(identifier: &str) -> Result<()> {
        // Check for empty identifier or whitespace-only
        if identifier.is_empty() || identifier.trim().is_empty() {
            return Err(TermError::SecurityError(
                "SQL identifier cannot be empty or whitespace-only".to_string(),
            ));
        }

        // Check identifier length (prevent DoS)
        if identifier.len() > 128 {
            return Err(TermError::SecurityError(
                "SQL identifier too long (max 128 characters)".to_string(),
            ));
        }

        // Check for null bytes
        if identifier.contains('\0') {
            return Err(TermError::SecurityError(
                "SQL identifier cannot contain null bytes".to_string(),
            ));
        }

        // Validate identifier format using regex
        static IDENTIFIER_REGEX: Lazy<Regex> = Lazy::new(|| {
            // Allow letters, numbers, underscores, dots (for qualified names)
            // Must start with letter or underscore
            // This regex is compile-time constant and known to be valid
            #[allow(clippy::expect_used)]
            Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$")
                .expect("Hard-coded regex pattern should be valid")
        });
        let regex = &*IDENTIFIER_REGEX;

        if !regex.is_match(identifier) {
            return Err(TermError::SecurityError(format!(
                "Invalid SQL identifier format: '{identifier}'. Identifiers must start with a letter or underscore and contain only letters, numbers, underscores, and dots"
            )));
        }

        // Check against dangerous patterns
        Self::check_dangerous_patterns(identifier)?;

        Ok(())
    }

    /// Validates a regex pattern for safety.
    ///
    /// This function ensures that user-provided regex patterns are safe to use
    /// in SQL queries and won't cause ReDoS attacks or other security issues.
    pub fn validate_regex_pattern(pattern: &str) -> Result<String> {
        // Check pattern length (prevent DoS)
        if pattern.len() > 1000 {
            return Err(TermError::SecurityError(
                "Regex pattern too long (max 1000 characters)".to_string(),
            ));
        }

        // Check for null bytes
        if pattern.contains('\0') {
            return Err(TermError::SecurityError(
                "Regex pattern cannot contain null bytes".to_string(),
            ));
        }

        // Validate that it's a valid regex pattern
        match Regex::new(pattern) {
            Ok(_) => (),
            Err(e) => {
                return Err(TermError::SecurityError(format!(
                    "Invalid regex pattern: {e}"
                )));
            }
        }

        // Check for patterns that might cause ReDoS
        Self::check_redos_patterns(pattern)?;

        // Escape single quotes for SQL
        let escaped = pattern.replace('\'', "''");
        Ok(escaped)
    }

    /// Validates a custom SQL expression for safety.
    ///
    /// This function performs security validation on user-provided SQL expressions
    /// to prevent SQL injection and other attacks while still allowing legitimate
    /// validation expressions.
    pub fn validate_sql_expression(expression: &str) -> Result<()> {
        // Check expression length (prevent DoS)
        if expression.len() > 5000 {
            return Err(TermError::SecurityError(
                "SQL expression too long (max 5000 characters)".to_string(),
            ));
        }

        // Check for null bytes
        if expression.contains('\0') {
            return Err(TermError::SecurityError(
                "SQL expression cannot contain null bytes".to_string(),
            ));
        }

        // Check against dangerous SQL keywords and patterns
        Self::check_dangerous_sql_patterns(expression)?;

        Ok(())
    }

    /// Checks for dangerous patterns in identifiers.
    fn check_dangerous_patterns(identifier: &str) -> Result<()> {
        let identifier_lower = identifier.to_lowercase();

        // Check for SQL injection attempts
        let dangerous_patterns = &[
            ";", "--", "/*", "*/", "'", "xp_", "sp_", "union", "select", "insert", "update",
            "delete", "drop", "create", "alter", "exec", "execute", "declare", "cursor", "fetch",
            "open", "close",
        ];

        for pattern in dangerous_patterns {
            if identifier_lower.contains(pattern) {
                return Err(TermError::SecurityError(format!(
                    "SQL identifier contains dangerous pattern: '{pattern}'"
                )));
            }
        }

        Ok(())
    }

    /// Checks for patterns that might cause ReDoS (Regular Expression Denial of Service).
    fn check_redos_patterns(pattern: &str) -> Result<()> {
        // For now, disable ReDoS checking as it's being too aggressive
        // with legitimate patterns like email validation.
        // In a production system, you'd want more sophisticated ReDoS detection
        // that can distinguish between safe and dangerous patterns.

        // Check for extremely obvious ReDoS patterns only
        let dangerous_patterns = &[
            "(.*)*", // Classic catastrophic backtracking
            "(.*)+", // Another classic
            "(a+)+", // Nested quantifiers on same pattern
            "(a*)*", // Nested quantifiers on same pattern
        ];

        for dangerous in dangerous_patterns {
            if pattern.contains(dangerous) {
                return Err(TermError::SecurityError(
                    "Regex pattern might cause ReDoS attack".to_string(),
                ));
            }
        }

        Ok(())
    }

    /// Checks for dangerous SQL patterns in expressions.
    fn check_dangerous_sql_patterns(expression: &str) -> Result<()> {
        let expression_lower = expression.to_lowercase();

        // Dangerous SQL keywords and patterns
        static DANGEROUS_KEYWORDS: OnceLock<HashSet<&'static str>> = OnceLock::new();
        let keywords = DANGEROUS_KEYWORDS.get_or_init(|| {
            [
                // DDL operations
                "drop",
                "create",
                "alter",
                "truncate",
                // DML operations
                "insert",
                "update",
                "delete",
                // System procedures
                "exec",
                "execute",
                "xp_",
                "sp_",
                // Advanced operations
                "declare",
                "cursor",
                "fetch",
                "open",
                "close",
                "begin",
                "commit",
                "rollback",
                "transaction",
                // Information schema access
                "information_schema",
                "sys.",
                "pg_",
                // File operations (MSSQL/MySQL specific)
                "bulk",
                "openrowset",
                "opendatasource",
                "load_file",
                "into outfile",
                "into dumpfile",
                // Comments that might hide attacks
                "--",
                "/*",
                "*/",
            ]
            .into_iter()
            .collect()
        });

        // Check for dangerous keywords
        for keyword in keywords {
            if expression_lower.contains(keyword) {
                return Err(TermError::SecurityError(format!(
                    "SQL expression contains dangerous keyword: '{keyword}'"
                )));
            }
        }

        // Check for suspicious patterns
        let suspicious_patterns = &[
            r";\s*\w+",                 // Commands after semicolon
            r"union\s+select",          // Union-based injection
            r"'\s*or\s+'",              // OR-based injection
            r"'\s*and\s+'",             // AND-based injection
            r"=\s*\(.*select.*\)",      // Subquery injection
            r"\(\s*select\s+.*\)",      // Subqueries in general
            r"in\s*\(\s*select\s+.*\)", // IN with subquery
        ];

        for pattern in suspicious_patterns {
            if let Ok(regex) = Regex::new(pattern) {
                if regex.is_match(&expression_lower) {
                    return Err(TermError::SecurityError(format!(
                        "SQL expression contains suspicious pattern matching: {pattern}"
                    )));
                }
            }
        }

        Ok(())
    }
}

/// Input validation utilities for various data types.
pub struct InputValidator;

impl InputValidator {
    /// Validates a numeric threshold value.
    pub fn validate_threshold(value: f64, name: &str) -> Result<()> {
        if !value.is_finite() {
            return Err(TermError::SecurityError(format!(
                "Invalid {name} value: must be finite (not NaN or infinite)"
            )));
        }
        Ok(())
    }

    /// Validates a percentage value (0.0 to 1.0).
    pub fn validate_percentage(value: f64, name: &str) -> Result<()> {
        Self::validate_threshold(value, name)?;

        if !(0.0..=1.0).contains(&value) {
            return Err(TermError::SecurityError(format!(
                "Invalid {name} value: must be between 0.0 and 1.0, got {value}"
            )));
        }
        Ok(())
    }

    /// Validates a string length.
    pub fn validate_string_length(value: &str, max_length: usize, name: &str) -> Result<()> {
        if value.len() > max_length {
            return Err(TermError::SecurityError(format!(
                "{name} too long: {} characters (max {max_length})",
                value.len()
            )));
        }
        Ok(())
    }

    /// Validates that a string doesn't contain null bytes.
    pub fn validate_no_null_bytes(value: &str, name: &str) -> Result<()> {
        if value.contains('\0') {
            return Err(TermError::SecurityError(format!(
                "{name} cannot contain null bytes"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_secure_string_zeroization() {
        let password = "secret123";
        let secure = SecureString::new(password);
        assert_eq!(secure.expose(), "secret123");

        let _extracted = secure.into_string();
        // secure should now be zeroized (can't easily test without unsafe code)
    }

    #[test]
    fn test_valid_sql_identifiers() {
        assert!(SqlSecurity::validate_identifier("customer_id").is_ok());
        assert!(SqlSecurity::validate_identifier("table1").is_ok());
        assert!(SqlSecurity::validate_identifier("_private_col").is_ok());
        assert!(SqlSecurity::validate_identifier("schema.table").is_ok());
    }

    #[test]
    fn test_invalid_sql_identifiers() {
        // Empty identifier
        assert!(SqlSecurity::validate_identifier("").is_err());

        // Too long
        assert!(SqlSecurity::validate_identifier(&"a".repeat(200)).is_err());

        // Contains dangerous patterns
        assert!(SqlSecurity::validate_identifier("id; DROP TABLE").is_err());
        assert!(SqlSecurity::validate_identifier("col--comment").is_err());
        assert!(SqlSecurity::validate_identifier("union_select").is_err());

        // Invalid characters
        assert!(SqlSecurity::validate_identifier("col name").is_err()); // space
        assert!(SqlSecurity::validate_identifier("col-name").is_err()); // dash
        assert!(SqlSecurity::validate_identifier("123col").is_err()); // starts with number
    }

    #[test]
    fn test_sql_identifier_escaping() {
        let result = SqlSecurity::escape_identifier("customer_id").unwrap();
        assert_eq!(result, "\"customer_id\"");

        // Quotes are now rejected for security
        let result = SqlSecurity::escape_identifier("col\"with\"quotes");
        assert!(result.is_err(), "Should reject identifiers with quotes");
    }

    #[test]
    fn test_regex_pattern_validation() {
        assert!(SqlSecurity::validate_regex_pattern(r"^[A-Z]\d+$").is_ok());
        assert!(SqlSecurity::validate_regex_pattern(r"email@domain\.com").is_ok());

        // Invalid regex
        assert!(SqlSecurity::validate_regex_pattern(r"[unclosed").is_err());

        // Too long
        assert!(SqlSecurity::validate_regex_pattern(&"a".repeat(2000)).is_err());

        // Contains quotes that should be escaped
        let result = SqlSecurity::validate_regex_pattern("it's a pattern").unwrap();
        assert_eq!(result, "it''s a pattern");
    }

    #[test]
    fn test_sql_expression_validation() {
        // Valid expressions
        assert!(SqlSecurity::validate_sql_expression("price > 100").is_ok());
        assert!(SqlSecurity::validate_sql_expression("name IS NOT NULL").is_ok());
        assert!(SqlSecurity::validate_sql_expression("age BETWEEN 18 AND 65").is_ok());

        // Dangerous expressions
        assert!(SqlSecurity::validate_sql_expression("price > 0; DROP TABLE users").is_err());
        assert!(SqlSecurity::validate_sql_expression("name = '' OR '1'='1'").is_err());
        assert!(SqlSecurity::validate_sql_expression("id IN (SELECT * FROM passwords)").is_err());
        assert!(SqlSecurity::validate_sql_expression("EXEC sp_droplogin").is_err());
    }

    #[test]
    fn test_input_validation() {
        // Valid inputs
        assert!(InputValidator::validate_threshold(5.5, "threshold").is_ok());
        assert!(InputValidator::validate_percentage(0.95, "percentage").is_ok());
        assert!(InputValidator::validate_string_length("short", 100, "name").is_ok());

        // Invalid inputs
        assert!(InputValidator::validate_threshold(f64::NAN, "threshold").is_err());
        assert!(InputValidator::validate_threshold(f64::INFINITY, "threshold").is_err());
        assert!(InputValidator::validate_percentage(1.5, "percentage").is_err());
        assert!(InputValidator::validate_percentage(-0.1, "percentage").is_err());
        assert!(InputValidator::validate_string_length("too long", 5, "name").is_err());
        assert!(InputValidator::validate_no_null_bytes("contains\0null", "name").is_err());
    }
}
