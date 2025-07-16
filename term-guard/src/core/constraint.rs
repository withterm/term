//! Constraint trait and related types for validation rules.

use crate::prelude::*;
use async_trait::async_trait;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;

/// The status of a constraint evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConstraintStatus {
    /// The constraint check passed
    Success,
    /// The constraint check failed
    Failure,
    /// The constraint check was skipped (e.g., no data)
    Skipped,
}

impl ConstraintStatus {
    /// Returns true if this is a Success status.
    pub fn is_success(&self) -> bool {
        matches!(self, ConstraintStatus::Success)
    }

    /// Returns true if this is a Failure status.
    pub fn is_failure(&self) -> bool {
        matches!(self, ConstraintStatus::Failure)
    }

    /// Returns true if this is a Skipped status.
    pub fn is_skipped(&self) -> bool {
        matches!(self, ConstraintStatus::Skipped)
    }
}

/// The result of evaluating a constraint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstraintResult {
    /// The status of the constraint evaluation
    pub status: ConstraintStatus,
    /// Optional metric value computed during evaluation
    pub metric: Option<f64>,
    /// Optional message providing additional context
    pub message: Option<String>,
}

impl ConstraintResult {
    /// Creates a successful constraint result.
    pub fn success() -> Self {
        Self {
            status: ConstraintStatus::Success,
            metric: None,
            message: None,
        }
    }

    /// Creates a successful constraint result with a metric.
    pub fn success_with_metric(metric: f64) -> Self {
        Self {
            status: ConstraintStatus::Success,
            metric: Some(metric),
            message: None,
        }
    }

    /// Creates a failed constraint result.
    pub fn failure(message: impl Into<String>) -> Self {
        Self {
            status: ConstraintStatus::Failure,
            metric: None,
            message: Some(message.into()),
        }
    }

    /// Creates a failed constraint result with a metric.
    pub fn failure_with_metric(metric: f64, message: impl Into<String>) -> Self {
        Self {
            status: ConstraintStatus::Failure,
            metric: Some(metric),
            message: Some(message.into()),
        }
    }

    /// Creates a skipped constraint result.
    pub fn skipped(message: impl Into<String>) -> Self {
        Self {
            status: ConstraintStatus::Skipped,
            metric: None,
            message: Some(message.into()),
        }
    }
}

/// Metadata associated with a constraint.
///
/// This struct provides extensible metadata that can be attached to constraints
/// for better observability and reporting.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConstraintMetadata {
    /// The column(s) this constraint operates on
    pub columns: Vec<String>,
    /// A human-readable description of what this constraint validates
    pub description: Option<String>,
    /// Additional key-value pairs for custom metadata
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub custom: HashMap<String, String>,
}

impl ConstraintMetadata {
    /// Creates a new metadata instance with no columns.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates metadata for a single column constraint.
    pub fn for_column(column: impl Into<String>) -> Self {
        Self {
            columns: vec![column.into()],
            description: None,
            custom: HashMap::new(),
        }
    }

    /// Creates metadata for a multi-column constraint.
    pub fn for_columns<I, S>(columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            columns: columns.into_iter().map(Into::into).collect(),
            description: None,
            custom: HashMap::new(),
        }
    }

    /// Sets the description.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Adds a custom metadata entry.
    pub fn with_custom(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.custom.insert(key.into(), value.into());
        self
    }
}

/// A validation constraint that can be evaluated against data.
///
/// This trait defines the interface for all validation rules in the Term library.
/// Implementations should be stateless and reusable across multiple validations.
///
/// # Examples
///
/// ```rust,ignore
/// use term_guard::core::{Constraint, ConstraintResult, ConstraintMetadata};
/// use async_trait::async_trait;
///
/// struct CompletenessConstraint {
///     column: String,
///     threshold: f64,
/// }
///
/// #[async_trait]
/// impl Constraint for CompletenessConstraint {
///     async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult> {
///         // Implementation here
///         Ok(ConstraintResult::success())
///     }
///
///     fn name(&self) -> &str {
///         "completeness"
///     }
///
///     fn metadata(&self) -> ConstraintMetadata {
///         ConstraintMetadata::for_column(&self.column)
///             .with_description(format!("Checks completeness >= {}", self.threshold))
///     }
/// }
/// ```
#[async_trait]
pub trait Constraint: Debug + Send + Sync {
    /// Evaluates the constraint against the data in the session context.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The DataFusion session context containing the data to validate
    ///
    /// # Returns
    ///
    /// A `Result` containing the constraint evaluation result or an error
    async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult>;

    /// Returns the name of the constraint.
    fn name(&self) -> &str;

    /// Returns the column this constraint operates on (if single-column).
    ///
    /// Implementors should override this method if they operate on a single column.
    /// The default implementation returns None.
    fn column(&self) -> Option<&str> {
        None
    }

    /// Returns a description of what this constraint validates.
    ///
    /// Implementors should override this method to provide a description.
    /// The default implementation returns None.
    fn description(&self) -> Option<&str> {
        None
    }

    /// Returns the metadata associated with this constraint.
    ///
    /// The default implementation returns empty metadata for backward compatibility.
    /// Implementors should override this method to provide meaningful metadata.
    fn metadata(&self) -> ConstraintMetadata {
        ConstraintMetadata::new()
    }
}

/// A boxed constraint for use in collections.
pub type BoxedConstraint = Box<dyn Constraint>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constraint_metadata_builder() {
        let metadata = ConstraintMetadata::for_column("user_id")
            .with_description("Checks user ID validity")
            .with_custom("severity", "high")
            .with_custom("category", "identity");

        assert_eq!(metadata.columns, vec!["user_id"]);
        assert_eq!(
            metadata.description,
            Some("Checks user ID validity".to_string())
        );
        assert_eq!(metadata.custom.get("severity"), Some(&"high".to_string()));
        assert_eq!(
            metadata.custom.get("category"),
            Some(&"identity".to_string())
        );
    }

    #[test]
    fn test_constraint_metadata_multi_column() {
        let metadata = ConstraintMetadata::for_columns(vec!["first_name", "last_name"])
            .with_description("Checks name completeness");

        assert_eq!(metadata.columns, vec!["first_name", "last_name"]);
        assert_eq!(
            metadata.description,
            Some("Checks name completeness".to_string())
        );
    }

    #[test]
    fn test_constraint_result_builders() {
        let success = ConstraintResult::success();
        assert_eq!(success.status, ConstraintStatus::Success);
        assert!(success.metric.is_none());
        assert!(success.message.is_none());

        let success_with_metric = ConstraintResult::success_with_metric(0.95);
        assert_eq!(success_with_metric.status, ConstraintStatus::Success);
        assert_eq!(success_with_metric.metric, Some(0.95));

        let failure = ConstraintResult::failure("Validation failed");
        assert_eq!(failure.status, ConstraintStatus::Failure);
        assert_eq!(failure.message, Some("Validation failed".to_string()));

        let failure_with_metric = ConstraintResult::failure_with_metric(0.3, "Below threshold");
        assert_eq!(failure_with_metric.status, ConstraintStatus::Failure);
        assert_eq!(failure_with_metric.metric, Some(0.3));
        assert_eq!(
            failure_with_metric.message,
            Some("Below threshold".to_string())
        );

        let skipped = ConstraintResult::skipped("No data");
        assert_eq!(skipped.status, ConstraintStatus::Skipped);
        assert_eq!(skipped.message, Some("No data".to_string()));
    }
}
