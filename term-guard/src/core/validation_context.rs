//! Validation context for passing runtime information to constraints.
//!
//! This module provides a context object that can be used to pass runtime
//! information (like table names) to constraints during evaluation.

use std::sync::Arc;

/// Runtime context for validation operations.
///
/// This struct holds runtime information that constraints need during evaluation,
/// such as the name of the table being validated. This allows constraints to
/// work with any table name rather than being hardcoded to "data".
#[derive(Debug, Clone)]
pub struct ValidationContext {
    /// The name of the table being validated
    table_name: Arc<str>,
}

impl ValidationContext {
    /// Creates a new validation context with the specified table name.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table to validate
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_guard::core::ValidationContext;
    ///
    /// let ctx = ValidationContext::new("customer_data");
    /// assert_eq!(ctx.table_name(), "customer_data");
    /// ```
    pub fn new(table_name: impl Into<Arc<str>>) -> Self {
        Self {
            table_name: table_name.into(),
        }
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
    pub fn default() -> Self {
        Self::new("data")
    }

    /// Returns the name of the table being validated.
    pub fn table_name(&self) -> &str {
        &self.table_name
    }
}

impl Default for ValidationContext {
    fn default() -> Self {
        Self::default()
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
        let ctx = ValidationContext::new("test_table");
        assert_eq!(ctx.table_name(), "test_table");
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
        let custom_ctx = ValidationContext::new("custom_table");
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
        let ctx1 = ValidationContext::new("table1");
        let ctx2 = ValidationContext::new("table2");

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