//! Error types for the Term data validation library.
//!
//! This module provides a comprehensive error handling strategy using `thiserror`
//! for automatic error trait implementations. All errors in the Term library
//! are represented by the `TermError` enum.

use thiserror::Error;

/// The main error type for the Term library.
///
/// This enum represents all possible errors that can occur during
/// data validation operations.
#[derive(Error, Debug)]
pub enum TermError {
    /// Error that occurs when a validation check fails.
    #[error("Validation failed: {message}")]
    ValidationFailed {
        /// Human-readable error message
        message: String,
        /// Name of the check that failed
        check: String,
        /// Optional underlying error that caused the validation failure
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Error that occurs when a constraint evaluation fails.
    #[error("Constraint evaluation failed for '{constraint}': {message}")]
    ConstraintEvaluation {
        /// Name of the constraint that failed
        constraint: String,
        /// Detailed error message
        message: String,
    },

    /// Error from DataFusion operations.
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    /// Error from Arrow operations.
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// Error from data source operations.
    #[error("Data source error: {message}")]
    DataSource {
        /// Type of data source (e.g., "CSV", "Parquet", "Database")
        source_type: String,
        /// Detailed error message
        message: String,
        /// Optional underlying error
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Error from I/O operations.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Error when parsing or processing data.
    #[error("Parse error: {0}")]
    Parse(String),

    /// Error related to configuration.
    #[error("Configuration error: {0}")]
    Configuration(String),

    /// Error from serialization/deserialization operations.
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Error from OpenTelemetry operations.
    #[error("OpenTelemetry error: {0}")]
    OpenTelemetry(String),

    /// Error when a required column is not found in the dataset.
    #[error("Column '{column}' not found in dataset")]
    ColumnNotFound { column: String },

    /// Error when data types don't match expected types.
    #[error("Type mismatch: expected {expected}, found {found}")]
    TypeMismatch { expected: String, found: String },

    /// Error when an operation is not supported.
    #[error("Operation not supported: {0}")]
    NotSupported(String),

    /// Generic internal error for unexpected conditions.
    #[error("Internal error: {0}")]
    Internal(String),

    /// Security-related error.
    #[error("Security error: {0}")]
    SecurityError(String),
}

/// A type alias for `Result<T, TermError>`.
///
/// This is the standard `Result` type used throughout the Term library.
///
/// # Examples
///
/// ```rust,ignore
/// use term_core::error::Result;
///
/// fn validate_data() -> Result<()> {
///     // validation logic here
///     Ok(())
/// }
/// ```
pub type Result<T> = std::result::Result<T, TermError>;

impl TermError {
    /// Creates a new validation failed error with the given message and check name.
    pub fn validation_failed(check: impl Into<String>, message: impl Into<String>) -> Self {
        Self::ValidationFailed {
            message: message.into(),
            check: check.into(),
            source: None,
        }
    }

    /// Creates a new validation failed error with a source error.
    pub fn validation_failed_with_source(
        check: impl Into<String>,
        message: impl Into<String>,
        source: Box<dyn std::error::Error + Send + Sync>,
    ) -> Self {
        Self::ValidationFailed {
            message: message.into(),
            check: check.into(),
            source: Some(source),
        }
    }

    /// Creates a new data source error.
    pub fn data_source(source_type: impl Into<String>, message: impl Into<String>) -> Self {
        Self::DataSource {
            source_type: source_type.into(),
            message: message.into(),
            source: None,
        }
    }

    /// Creates a new data source error with a source error.
    pub fn data_source_with_source(
        source_type: impl Into<String>,
        message: impl Into<String>,
        source: Box<dyn std::error::Error + Send + Sync>,
    ) -> Self {
        Self::DataSource {
            source_type: source_type.into(),
            message: message.into(),
            source: Some(source),
        }
    }

    /// Creates a new constraint evaluation error.
    pub fn constraint_evaluation(
        constraint: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::ConstraintEvaluation {
            constraint: constraint.into(),
            message: message.into(),
        }
    }
}

/// Extension trait for adding context to errors.
pub trait ErrorContext<T> {
    /// Adds context to an error.
    fn context(self, msg: &str) -> Result<T>;

    /// Adds context with a lazy message.
    fn with_context<F>(self, f: F) -> Result<T>
    where
        F: FnOnce() -> String;
}

impl<T, E> ErrorContext<T> for std::result::Result<T, E>
where
    E: Into<TermError>,
{
    fn context(self, msg: &str) -> Result<T> {
        self.map_err(|e| {
            let base_error = e.into();
            match base_error {
                TermError::Internal(inner) => TermError::Internal(format!("{}: {}", msg, inner)),
                other => TermError::Internal(format!("{}: {}", msg, other)),
            }
        })
    }

    fn with_context<F>(self, f: F) -> Result<T>
    where
        F: FnOnce() -> String,
    {
        self.map_err(|e| {
            let msg = f();
            let base_error = e.into();
            match base_error {
                TermError::Internal(inner) => TermError::Internal(format!("{}: {}", msg, inner)),
                other => TermError::Internal(format!("{}: {}", msg, other)),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn test_validation_failed_error() {
        let err = TermError::validation_failed("completeness_check", "Too many null values");
        assert_eq!(err.to_string(), "Validation failed: Too many null values");
    }

    #[test]
    fn test_error_with_source() {
        let source = std::io::Error::new(std::io::ErrorKind::NotFound, "File not found");
        let err = TermError::validation_failed_with_source(
            "file_check",
            "Could not read validation file",
            Box::new(source),
        );

        // Check that source is preserved
        assert!(err.source().is_some());
    }

    #[test]
    fn test_data_source_error() {
        let err = TermError::data_source("CSV", "Invalid file format");
        assert_eq!(err.to_string(), "Data source error: Invalid file format");
    }

    #[test]
    fn test_column_not_found() {
        let err = TermError::ColumnNotFound {
            column: "user_id".to_string(),
        };
        assert_eq!(err.to_string(), "Column 'user_id' not found in dataset");
    }

    #[test]
    fn test_type_mismatch() {
        let err = TermError::TypeMismatch {
            expected: "Int64".to_string(),
            found: "Utf8".to_string(),
        };
        assert_eq!(err.to_string(), "Type mismatch: expected Int64, found Utf8");
    }

    #[test]
    fn test_error_context() {
        fn failing_operation() -> Result<()> {
            Err(TermError::Internal("Something went wrong".to_string()))
        }

        let result = failing_operation().context("During data validation");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("During data validation"));
    }
}
