//! Validation result types.

use super::Level;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Metrics collected during validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationMetrics {
    /// Total number of checks executed
    pub total_checks: usize,
    /// Number of checks that passed
    pub passed_checks: usize,
    /// Number of checks that failed
    pub failed_checks: usize,
    /// Number of checks that were skipped
    pub skipped_checks: usize,
    /// Total execution time in milliseconds
    pub execution_time_ms: u64,
    /// Custom metrics collected during validation
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub custom_metrics: HashMap<String, f64>,
}

impl ValidationMetrics {
    /// Creates new validation metrics with all counts set to zero.
    pub fn new() -> Self {
        Self {
            total_checks: 0,
            passed_checks: 0,
            failed_checks: 0,
            skipped_checks: 0,
            execution_time_ms: 0,
            custom_metrics: HashMap::new(),
        }
    }

    /// Returns the success rate as a percentage (0.0 to 100.0).
    pub fn success_rate(&self) -> f64 {
        if self.total_checks == 0 {
            100.0
        } else {
            (self.passed_checks as f64 / self.total_checks as f64) * 100.0
        }
    }
}

impl Default for ValidationMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// A detailed validation issue found during checks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationIssue {
    /// The name of the check that found the issue
    pub check_name: String,
    /// The name of the constraint that failed
    pub constraint_name: String,
    /// The severity level of the issue
    pub level: Level,
    /// A description of the issue
    pub message: String,
    /// Optional metric value associated with the issue
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metric: Option<f64>,
}

/// A validation report containing all issues found.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationReport {
    /// The name of the validation suite that was run
    pub suite_name: String,
    /// Timestamp when the validation was run (ISO 8601 format)
    pub timestamp: String,
    /// Overall validation metrics
    pub metrics: ValidationMetrics,
    /// List of issues found during validation
    pub issues: Vec<ValidationIssue>,
}

impl ValidationReport {
    /// Creates a new validation report.
    pub fn new(suite_name: impl Into<String>) -> Self {
        Self {
            suite_name: suite_name.into(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            metrics: ValidationMetrics::new(),
            issues: Vec::new(),
        }
    }

    /// Adds an issue to the report.
    pub fn add_issue(&mut self, issue: ValidationIssue) {
        self.issues.push(issue);
    }

    /// Returns true if there are any error-level issues.
    pub fn has_errors(&self) -> bool {
        self.issues.iter().any(|issue| issue.level == Level::Error)
    }

    /// Returns true if there are any warning-level issues.
    pub fn has_warnings(&self) -> bool {
        self.issues
            .iter()
            .any(|issue| issue.level == Level::Warning)
    }

    /// Gets all issues of a specific level.
    pub fn issues_by_level(&self, level: Level) -> Vec<&ValidationIssue> {
        self.issues
            .iter()
            .filter(|issue| issue.level == level)
            .collect()
    }
}

/// The result of running a validation suite.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum ValidationResult {
    /// Validation completed successfully with no critical issues
    Success {
        /// Validation metrics
        metrics: ValidationMetrics,
        /// Detailed report (may contain warnings or info)
        report: ValidationReport,
    },
    /// Validation failed due to one or more critical issues
    Failure {
        /// Detailed report containing the issues
        report: ValidationReport,
    },
}

impl ValidationResult {
    /// Creates a successful validation result.
    pub fn success(metrics: ValidationMetrics, report: ValidationReport) -> Self {
        ValidationResult::Success { metrics, report }
    }

    /// Creates a failed validation result.
    pub fn failure(report: ValidationReport) -> Self {
        ValidationResult::Failure { report }
    }

    /// Returns true if the validation succeeded.
    pub fn is_success(&self) -> bool {
        matches!(self, ValidationResult::Success { .. })
    }

    /// Returns true if the validation failed.
    pub fn is_failure(&self) -> bool {
        matches!(self, ValidationResult::Failure { .. })
    }

    /// Returns the validation report.
    pub fn report(&self) -> &ValidationReport {
        match self {
            ValidationResult::Success { report, .. } => report,
            ValidationResult::Failure { report } => report,
        }
    }

    /// Returns the validation metrics if available (only for success).
    pub fn metrics(&self) -> Option<&ValidationMetrics> {
        match self {
            ValidationResult::Success { metrics, .. } => Some(metrics),
            ValidationResult::Failure { .. } => None,
        }
    }

    /// Formats the validation result as JSON.
    ///
    /// This is a convenience method that uses the `JsonFormatter` to output
    /// the result as structured JSON.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use term_core::core::{ValidationResult, ValidationReport, ValidationMetrics};
    /// # let metrics = ValidationMetrics::new();
    /// # let report = ValidationReport::new("test");
    /// # let result = ValidationResult::success(metrics, report);
    /// let json_output = result.to_json().unwrap();
    /// println!("{}", json_output);
    /// ```
    pub fn to_json(&self) -> crate::prelude::Result<String> {
        use crate::formatters::{JsonFormatter, ResultFormatter};
        JsonFormatter::new().format(self)
    }

    /// Formats the validation result as JSON with pretty printing.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use term_core::core::{ValidationResult, ValidationReport, ValidationMetrics};
    /// # let metrics = ValidationMetrics::new();
    /// # let report = ValidationReport::new("test");
    /// # let result = ValidationResult::success(metrics, report);
    /// let pretty_json = result.to_json_pretty().unwrap();
    /// println!("{}", pretty_json);
    /// ```
    pub fn to_json_pretty(&self) -> crate::prelude::Result<String> {
        use crate::formatters::{JsonFormatter, ResultFormatter};
        JsonFormatter::new().with_pretty(true).format(self)
    }

    /// Formats the validation result in a human-readable format.
    ///
    /// This is a convenience method that uses the `HumanFormatter` to output
    /// the result in a format suitable for console display.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use term_core::core::{ValidationResult, ValidationReport, ValidationMetrics};
    /// # let metrics = ValidationMetrics::new();
    /// # let report = ValidationReport::new("test");
    /// # let result = ValidationResult::success(metrics, report);
    /// let human_output = result.to_human().unwrap();
    /// println!("{}", human_output);
    /// ```
    pub fn to_human(&self) -> crate::prelude::Result<String> {
        use crate::formatters::{HumanFormatter, ResultFormatter};
        HumanFormatter::new().format(self)
    }

    /// Formats the validation result as Markdown.
    ///
    /// This is a convenience method that uses the `MarkdownFormatter` to output
    /// the result in Markdown format suitable for documentation.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use term_core::core::{ValidationResult, ValidationReport, ValidationMetrics};
    /// # let metrics = ValidationMetrics::new();
    /// # let report = ValidationReport::new("test");
    /// # let result = ValidationResult::success(metrics, report);
    /// let markdown_output = result.to_markdown().unwrap();
    /// println!("{}", markdown_output);
    /// ```
    pub fn to_markdown(&self) -> crate::prelude::Result<String> {
        use crate::formatters::{MarkdownFormatter, ResultFormatter};
        MarkdownFormatter::new().format(self)
    }

    /// Formats the validation result using a custom formatter.
    ///
    /// # Arguments
    ///
    /// * `formatter` - The formatter to use
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use term_core::core::{ValidationResult, ValidationReport, ValidationMetrics};
    /// use term_core::formatters::{ResultFormatter, HumanFormatter, FormatterConfig};
    /// # let metrics = ValidationMetrics::new();
    /// # let report = ValidationReport::new("test");
    /// # let result = ValidationResult::success(metrics, report);
    ///
    /// let config = FormatterConfig::minimal();
    /// let formatter = HumanFormatter::with_config(config);
    /// let output = result.format_with(&formatter).unwrap();
    /// println!("{}", output);
    /// ```
    pub fn format_with<F: crate::formatters::ResultFormatter>(
        &self,
        formatter: &F,
    ) -> crate::prelude::Result<String> {
        formatter.format(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_metrics_success_rate() {
        let mut metrics = ValidationMetrics::new();
        assert_eq!(metrics.success_rate(), 100.0);

        metrics.total_checks = 10;
        metrics.passed_checks = 8;
        assert_eq!(metrics.success_rate(), 80.0);
    }

    #[test]
    fn test_validation_report() {
        let mut report = ValidationReport::new("test_suite");
        assert!(!report.has_errors());
        assert!(!report.has_warnings());

        report.add_issue(ValidationIssue {
            check_name: "test_check".to_string(),
            constraint_name: "test_constraint".to_string(),
            level: Level::Error,
            message: "Test error".to_string(),
            metric: Some(0.5),
        });

        assert!(report.has_errors());
        assert_eq!(report.issues_by_level(Level::Error).len(), 1);
    }

    #[test]
    fn test_validation_result() {
        let metrics = ValidationMetrics::new();
        let report = ValidationReport::new("test_suite");

        let success_result = ValidationResult::success(metrics, report.clone());
        assert!(success_result.is_success());
        assert!(!success_result.is_failure());
        assert!(success_result.metrics().is_some());

        let failure_result = ValidationResult::failure(report);
        assert!(!failure_result.is_success());
        assert!(failure_result.is_failure());
        assert!(failure_result.metrics().is_none());
    }

    #[test]
    fn test_validation_result_formatting() {
        let metrics = ValidationMetrics::new();
        let mut report = ValidationReport::new("test_suite");

        // Add a test issue
        report.add_issue(ValidationIssue {
            check_name: "test_check".to_string(),
            constraint_name: "test_constraint".to_string(),
            level: Level::Warning,
            message: "Test warning message".to_string(),
            metric: Some(0.8),
        });

        let result = ValidationResult::success(metrics, report);

        // Test JSON formatting
        let json_output = result.to_json().unwrap();
        assert!(json_output.contains("\"status\": \"success\""));
        assert!(json_output.contains("test_suite"));

        // Test pretty JSON formatting
        let pretty_json = result.to_json_pretty().unwrap();
        assert!(pretty_json.contains("\"status\": \"success\""));
        // Pretty JSON should contain the same content
        assert!(pretty_json.contains("test_suite"));

        // Test human formatting
        let human_output = result.to_human().unwrap();
        assert!(human_output.contains("Validation PASSED"));
        assert!(human_output.contains("test_suite"));

        // Test markdown formatting
        let markdown_output = result.to_markdown().unwrap();
        assert!(markdown_output.contains("## ✅ Validation Report - PASSED"));
        assert!(markdown_output.contains("test_suite"));
    }
}
