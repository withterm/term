//! Validation context for passing runtime information to constraints.
//!
//! This module provides a context object that can be used to pass runtime
//! information (like table names) to constraints during evaluation.

use crate::prelude::Result;
use crate::security::SqlSecurity;
use std::sync::Arc;

/// Runtime context for validation operations.
///
/// This struct holds runtime information that constraints need during evaluation,
/// such as the name of the table being validated. This allows constraints to
/// work with any table name rather than being hardcoded to "data".
#[derive(Debug, Clone)]
pub struct ValidationContext {
    /// The original table name provided by the user
    original_table_name: Arc<str>,
    /// The safely escaped table name for use in SQL queries
    escaped_table_name: Arc<str>,
}

impl Default for ValidationContext {
    fn default() -> Self {
        // "data" is a safe default table name
        Self::new("data").expect("Default table name 'data' should always be valid")
    }
}

impl ValidationContext {
    /// Creates a new validation context with the specified table name.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table to validate
    ///
    /// # Returns
    ///
    /// * `Ok(ValidationContext)` - If the table name is valid
    /// * `Err(TermError)` - If the table name contains dangerous patterns or is invalid
    ///
    /// # Security
    ///
    /// This function validates and safely escapes the table name to prevent SQL injection.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_guard::core::ValidationContext;
    ///
    /// let ctx = ValidationContext::new("customer_data").unwrap();
    /// assert_eq!(ctx.table_name(), "customer_data");
    /// assert_eq!(ctx.escaped_table_name(), "\"customer_data\"");
    /// ```
    pub fn new(table_name: impl Into<Arc<str>>) -> Result<Self> {
        let table_name_str = table_name.into();

        // Validate and escape the table name for SQL safety
        let escaped = SqlSecurity::escape_identifier(&table_name_str)?;

        Ok(Self {
            original_table_name: table_name_str,
            escaped_table_name: escaped.into(),
        })
    }

    /// Creates a validation context for the default table name "data".
    ///
    /// This is provided for backward compatibility with existing code.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_guard::core::ValidationContext;
    ///
    /// let ctx = ValidationContext::default();
    /// assert_eq!(ctx.table_name(), "data");
    /// ```
    pub fn with_default_table() -> Self {
        Self::default()
    }

    /// Returns the original name of the table being validated (unescaped).
    pub fn table_name(&self) -> &str {
        &self.original_table_name
    }

    /// Returns the safely escaped table name for use in SQL queries.
    ///
    /// This name has been validated and escaped to prevent SQL injection.
    /// It includes quotes and proper escaping of any special characters.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_guard::core::ValidationContext;
    ///
    /// let ctx = ValidationContext::new("customer_data").unwrap();
    /// assert_eq!(ctx.escaped_table_name(), "\"customer_data\"");
    /// ```
    pub fn escaped_table_name(&self) -> &str {
        &self.escaped_table_name
    }
}

// Thread-local storage for the current validation context.
// This allows constraints to access the validation context without
// requiring changes to the Constraint trait interface.
tokio::task_local! {
    pub static CURRENT_CONTEXT: ValidationContext;
}

/// Gets the current validation context.
///
/// Returns the default context if no context has been set.
pub fn current_validation_context() -> ValidationContext {
    CURRENT_CONTEXT
        .try_with(|ctx| ctx.clone())
        .unwrap_or_else(|_| ValidationContext::default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_context_creation() {
        let ctx = ValidationContext::new("test_table").unwrap();
        assert_eq!(ctx.table_name(), "test_table");
        assert_eq!(ctx.escaped_table_name(), "\"test_table\"");
    }

    #[test]
    fn test_validation_context_sql_injection_prevention() {
        // Test dangerous table names are rejected
        assert!(ValidationContext::new("table; DROP TABLE users").is_err());
        assert!(ValidationContext::new("table' OR '1'='1").is_err());
        assert!(ValidationContext::new("table--comment").is_err());
        assert!(ValidationContext::new("").is_err());

        // Test valid table names with special chars
        let ctx = ValidationContext::new("table_with_underscore").unwrap();
        assert_eq!(ctx.escaped_table_name(), "\"table_with_underscore\"");
    }

    #[test]
    fn test_validation_context_default() {
        let ctx = ValidationContext::default();
        assert_eq!(ctx.table_name(), "data");
    }

    #[tokio::test]
    async fn test_task_local_context() {
        // Default context
        assert_eq!(current_validation_context().table_name(), "data");

        // Set custom context using task local
        let custom_ctx = ValidationContext::new("custom_table").unwrap();
        CURRENT_CONTEXT
            .scope(custom_ctx, async {
                assert_eq!(current_validation_context().table_name(), "custom_table");
            })
            .await;

        // Back to default
        assert_eq!(current_validation_context().table_name(), "data");
    }

    #[tokio::test]
    async fn test_nested_contexts() {
        let ctx1 = ValidationContext::new("table1").unwrap();
        let ctx2 = ValidationContext::new("table2").unwrap();

        CURRENT_CONTEXT
            .scope(ctx1, async {
                assert_eq!(current_validation_context().table_name(), "table1");

                CURRENT_CONTEXT
                    .scope(ctx2, async {
                        assert_eq!(current_validation_context().table_name(), "table2");
                    })
                    .await;

                assert_eq!(current_validation_context().table_name(), "table1");
            })
            .await;

        assert_eq!(current_validation_context().table_name(), "data");
    }
}
