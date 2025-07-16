//! Validation severity levels.

use serde::{Deserialize, Serialize};
use std::fmt;

/// The severity level of a validation check.
///
/// This enum represents different levels of importance for validation checks,
/// allowing users to categorize issues by their impact on data quality.
/// Levels are ordered by severity: Error > Warning > Info.
///
/// # Usage Guidelines
///
/// - **Error**: Use for critical data quality issues that prevent processing
///   - Missing required fields
///   - Primary key violations
///   - Data integrity violations
///   - Invalid data formats that break downstream systems
///
/// - **Warning**: Use for issues that should be investigated but don't block processing
///   - Data quality below expected thresholds
///   - Unusual patterns or outliers
///   - Missing optional but recommended fields
///   - Performance-impacting data issues
///
/// - **Info**: Use for informational metrics and observations
///   - Data distribution statistics
///   - Row counts and cardinality metrics
///   - Performance benchmarks
///   - Data profiling results
///
/// # Examples
///
/// ```rust
/// use term_core::core::{Check, Level, ConstraintOptions};
/// use term_core::constraints::Assertion;
///
/// // Critical validations that must pass
/// let critical_check = Check::builder("required_fields")
///     .level(Level::Error)
///     .completeness("transaction_id", ConstraintOptions::new().with_threshold(1.0))
///     .completeness("amount", ConstraintOptions::new().with_threshold(1.0))
///     .build();
///
/// // Quality checks that flag potential issues
/// let quality_check = Check::builder("data_quality")
///     .level(Level::Warning)
///     .completeness("email", ConstraintOptions::new().with_threshold(0.95))
///     .validates_regex("phone", r"^\+?\d{3}-\d{3}-\d{4}$", 0.90)
///     .build();
///
/// // Informational metrics
/// let metrics_check = Check::builder("data_profile")
///     .level(Level::Info)
///     .has_size(Assertion::GreaterThan(0.0))
///     .has_mean("order_value", Assertion::Between(10.0, 1000.0))
///     .build();
/// ```
///
/// # Comparison
///
/// ```rust
/// use term_core::core::Level;
///
/// let critical_level = Level::Error;
/// let warning_level = Level::Warning;
/// let info_level = Level::Info;
///
/// assert!(critical_level > warning_level);
/// assert!(warning_level > info_level);
/// ```
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    /// Informational level - Used for metrics and non-critical observations
    Info = 0,
    /// Warning level - Indicates potential issues that should be reviewed
    #[default]
    Warning = 1,
    /// Error level - Indicates critical data quality issues that must be addressed
    Error = 2,
}

impl Level {
    /// Returns the string representation of the level.
    pub fn as_str(&self) -> &'static str {
        match self {
            Level::Info => "info",
            Level::Warning => "warning",
            Level::Error => "error",
        }
    }

    /// Checks if this level is at least as severe as another level.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::Level;
    ///
    /// assert!(Level::Error.is_at_least(Level::Warning));
    /// assert!(Level::Warning.is_at_least(Level::Warning));
    /// assert!(!Level::Info.is_at_least(Level::Error));
    /// ```
    pub fn is_at_least(&self, other: Level) -> bool {
        *self >= other
    }
}

impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_level_ordering() {
        assert!(Level::Error > Level::Warning);
        assert!(Level::Warning > Level::Info);
        assert!(Level::Error > Level::Info);
    }

    #[test]
    fn test_level_display() {
        assert_eq!(Level::Info.to_string(), "info");
        assert_eq!(Level::Warning.to_string(), "warning");
        assert_eq!(Level::Error.to_string(), "error");
    }

    #[test]
    fn test_level_is_at_least() {
        assert!(Level::Error.is_at_least(Level::Info));
        assert!(Level::Error.is_at_least(Level::Warning));
        assert!(Level::Error.is_at_least(Level::Error));
        assert!(Level::Warning.is_at_least(Level::Info));
        assert!(Level::Warning.is_at_least(Level::Warning));
        assert!(!Level::Warning.is_at_least(Level::Error));
        assert!(Level::Info.is_at_least(Level::Info));
        assert!(!Level::Info.is_at_least(Level::Warning));
        assert!(!Level::Info.is_at_least(Level::Error));
    }

    #[test]
    fn test_level_serde() {
        let json = serde_json::to_string(&Level::Error).unwrap();
        assert_eq!(json, "\"error\"");

        let level: Level = serde_json::from_str("\"warning\"").unwrap();
        assert_eq!(level, Level::Warning);
    }
}
