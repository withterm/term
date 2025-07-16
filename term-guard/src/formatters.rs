//! Result formatting and reporting for Term validation results.
//!
//! This module provides different formatters for validation results, allowing
//! users to output results in various formats like JSON, human-readable text,
//! or Markdown for documentation purposes.
//!
//! # Examples
//!
//! ```rust
//! use term_guard::formatters::{ResultFormatter, JsonFormatter, HumanFormatter};
//! use term_guard::core::ValidationResult;
//!
//! let formatter = HumanFormatter::new();
//! // let result: ValidationResult = /* ... */;
//! // let output = formatter.format(&result);
//! ```

use crate::core::{Level, ValidationReport, ValidationResult};
use crate::prelude::*;
use serde_json;
use std::fmt::Write;

/// Configuration options for formatting validation results.
#[derive(Debug, Clone)]
pub struct FormatterConfig {
    /// Include detailed metrics in output
    pub include_metrics: bool,
    /// Include individual issue details
    pub include_issues: bool,
    /// Include custom metrics from constraints
    pub include_custom_metrics: bool,
    /// Maximum number of issues to display (-1 for all)
    pub max_issues: i32,
    /// Whether to use colorized output (for human formatter)
    pub use_colors: bool,
    /// Whether to include timestamps in output
    pub include_timestamps: bool,
}

impl Default for FormatterConfig {
    fn default() -> Self {
        Self {
            include_metrics: true,
            include_issues: true,
            include_custom_metrics: true,
            max_issues: -1, // Show all issues by default
            use_colors: true,
            include_timestamps: true,
        }
    }
}

impl FormatterConfig {
    /// Creates a minimal configuration showing only summary.
    pub fn minimal() -> Self {
        Self {
            include_metrics: true,
            include_issues: false,
            include_custom_metrics: false,
            max_issues: 0,
            use_colors: false,
            include_timestamps: false,
        }
    }

    /// Creates a detailed configuration showing everything.
    pub fn detailed() -> Self {
        Self {
            include_metrics: true,
            include_issues: true,
            include_custom_metrics: true,
            max_issues: -1,
            use_colors: true,
            include_timestamps: true,
        }
    }

    /// Creates a configuration suitable for CI/CD environments.
    pub fn ci() -> Self {
        Self {
            include_metrics: true,
            include_issues: true,
            include_custom_metrics: false,
            max_issues: 50, // Limit output in CI
            use_colors: false,
            include_timestamps: true,
        }
    }

    /// Sets whether to include detailed metrics.
    pub fn with_metrics(mut self, include: bool) -> Self {
        self.include_metrics = include;
        self
    }

    /// Sets whether to include individual issues.
    pub fn with_issues(mut self, include: bool) -> Self {
        self.include_issues = include;
        self
    }

    /// Sets the maximum number of issues to display.
    pub fn with_max_issues(mut self, max: i32) -> Self {
        self.max_issues = max;
        self
    }

    /// Sets whether to use colorized output.
    pub fn with_colors(mut self, use_colors: bool) -> Self {
        self.use_colors = use_colors;
        self
    }
}

/// Trait for formatting validation results into different output formats.
///
/// This trait provides a uniform interface for converting validation results
/// into various formats like JSON, human-readable text, or Markdown.
///
/// # Examples
///
/// ```rust
/// use term_guard::formatters::{ResultFormatter, JsonFormatter};
/// use term_guard::core::ValidationResult;
///
/// struct MyCustomFormatter;
///
/// impl ResultFormatter for MyCustomFormatter {
///     fn format(&self, result: &ValidationResult) -> term_guard::prelude::Result<String> {
///         let success = result.is_success();
///         Ok(format!("Custom format: {success}"))
///     }
/// }
/// ```
pub trait ResultFormatter {
    /// Formats a validation result into a string representation.
    ///
    /// # Arguments
    ///
    /// * `result` - The validation result to format
    ///
    /// # Returns
    ///
    /// A formatted string representation of the result
    fn format(&self, result: &ValidationResult) -> Result<String>;

    /// Formats a validation result with custom configuration.
    ///
    /// # Arguments
    ///
    /// * `result` - The validation result to format
    /// * `config` - Configuration options for formatting
    ///
    /// # Returns
    ///
    /// A formatted string representation of the result
    fn format_with_config(
        &self,
        result: &ValidationResult,
        _config: &FormatterConfig,
    ) -> Result<String> {
        // Default implementation ignores config and uses standard format
        self.format(result)
    }
}

/// Formats validation results as structured JSON.
///
/// This formatter outputs the complete validation result as JSON,
/// making it suitable for programmatic consumption and integration
/// with other tools.
///
/// # Examples
///
/// ```rust
/// use term_guard::formatters::{ResultFormatter, JsonFormatter, FormatterConfig};
/// # use term_guard::core::{ValidationResult, ValidationReport, ValidationMetrics};
/// # let metrics = ValidationMetrics::new();
/// # let report = ValidationReport::new("test");
/// # let result = ValidationResult::success(metrics, report);
///
/// let formatter = JsonFormatter::new();
/// let json_output = formatter.format(&result).unwrap();
/// println!("{}", json_output);
/// ```
#[derive(Debug, Clone)]
pub struct JsonFormatter {
    config: FormatterConfig,
    pretty: bool,
}

impl JsonFormatter {
    /// Creates a new JSON formatter with default configuration.
    pub fn new() -> Self {
        Self {
            config: FormatterConfig::default(),
            pretty: true,
        }
    }

    /// Creates a new JSON formatter with the specified configuration.
    pub fn with_config(config: FormatterConfig) -> Self {
        Self {
            config,
            pretty: true,
        }
    }

    /// Sets whether to use pretty-printed JSON.
    pub fn with_pretty(mut self, pretty: bool) -> Self {
        self.pretty = pretty;
        self
    }
}

impl Default for JsonFormatter {
    fn default() -> Self {
        Self::new()
    }
}

impl ResultFormatter for JsonFormatter {
    fn format(&self, result: &ValidationResult) -> Result<String> {
        self.format_with_config(result, &self.config)
    }

    fn format_with_config(
        &self,
        result: &ValidationResult,
        config: &FormatterConfig,
    ) -> Result<String> {
        // Create a filtered representation based on config
        let filtered_result = filter_result_for_config(result, config);

        if self.pretty {
            serde_json::to_string_pretty(&filtered_result).map_err(|e| {
                TermError::Internal(format!("Failed to serialize result to JSON: {e}"))
            })
        } else {
            serde_json::to_string(&filtered_result).map_err(|e| {
                TermError::Internal(format!("Failed to serialize result to JSON: {e}"))
            })
        }
    }
}

/// Formats validation results in a human-readable format suitable for console output.
///
/// This formatter creates nicely formatted, colorized output that's easy to read
/// in terminals and logs. It includes summary information, issue details, and
/// optional metrics.
///
/// # Examples
///
/// ```rust
/// use term_guard::formatters::{ResultFormatter, HumanFormatter, FormatterConfig};
/// # use term_guard::core::{ValidationResult, ValidationReport, ValidationMetrics};
/// # let metrics = ValidationMetrics::new();
/// # let report = ValidationReport::new("test");
/// # let result = ValidationResult::success(metrics, report);
///
/// let formatter = HumanFormatter::new();
/// let human_output = formatter.format(&result).unwrap();
/// println!("{}", human_output);
/// ```
#[derive(Debug, Clone)]
pub struct HumanFormatter {
    config: FormatterConfig,
}

impl HumanFormatter {
    /// Creates a new human formatter with default configuration.
    pub fn new() -> Self {
        Self {
            config: FormatterConfig::default(),
        }
    }

    /// Creates a new human formatter with the specified configuration.
    pub fn with_config(config: FormatterConfig) -> Self {
        Self { config }
    }
}

impl Default for HumanFormatter {
    fn default() -> Self {
        Self::new()
    }
}

impl ResultFormatter for HumanFormatter {
    fn format(&self, result: &ValidationResult) -> Result<String> {
        self.format_with_config(result, &self.config)
    }

    fn format_with_config(
        &self,
        result: &ValidationResult,
        config: &FormatterConfig,
    ) -> Result<String> {
        let mut output = String::new();
        let report = result.report();

        // Header
        writeln!(output).unwrap();
        if result.is_success() {
            if config.use_colors {
                writeln!(output, "✅ \x1b[32mValidation PASSED\x1b[0m").unwrap();
            } else {
                writeln!(output, "✅ Validation PASSED").unwrap();
            }
        } else if config.use_colors {
            writeln!(output, "❌ \x1b[31mValidation FAILED\x1b[0m").unwrap();
        } else {
            writeln!(output, "❌ Validation FAILED").unwrap();
        }

        writeln!(output).unwrap();
        writeln!(output, "Suite: {}", report.suite_name).unwrap();

        if config.include_timestamps {
            writeln!(output, "Timestamp: {}", report.timestamp).unwrap();
        }

        // Metrics summary
        if config.include_metrics {
            writeln!(output).unwrap();
            writeln!(output, "📊 Summary Statistics:").unwrap();
            writeln!(output, "   Total Checks: {}", report.metrics.total_checks).unwrap();

            if config.use_colors {
                writeln!(
                    output,
                    "   ✅ Passed: \x1b[32m{}\x1b[0m",
                    report.metrics.passed_checks
                )
                .unwrap();
                writeln!(
                    output,
                    "   ❌ Failed: \x1b[31m{}\x1b[0m",
                    report.metrics.failed_checks
                )
                .unwrap();
                writeln!(
                    output,
                    "   ⏭️  Skipped: \x1b[33m{}\x1b[0m",
                    report.metrics.skipped_checks
                )
                .unwrap();
            } else {
                writeln!(output, "   ✅ Passed: {}", report.metrics.passed_checks).unwrap();
                writeln!(output, "   ❌ Failed: {}", report.metrics.failed_checks).unwrap();
                writeln!(output, "   ⏭️  Skipped: {}", report.metrics.skipped_checks).unwrap();
            }

            writeln!(
                output,
                "   Success Rate: {:.1}%",
                report.metrics.success_rate()
            )
            .unwrap();
            writeln!(
                output,
                "   Execution Time: {}ms",
                report.metrics.execution_time_ms
            )
            .unwrap();
        }

        // Custom metrics
        if config.include_custom_metrics && !report.metrics.custom_metrics.is_empty() {
            writeln!(output).unwrap();
            writeln!(output, "📈 Custom Metrics:").unwrap();
            for (name, value) in &report.metrics.custom_metrics {
                writeln!(output, "   {name}: {value:.3}").unwrap();
            }
        }

        // Issues
        if config.include_issues && !report.issues.is_empty() {
            writeln!(output).unwrap();
            writeln!(output, "🔍 Issues Found:").unwrap();

            let issues_to_show = if config.max_issues < 0 {
                report.issues.as_slice()
            } else {
                let max = config.max_issues as usize;
                &report.issues[..std::cmp::min(max, report.issues.len())]
            };

            for (i, issue) in issues_to_show.iter().enumerate() {
                writeln!(output).unwrap();
                let level_symbol = match issue.level {
                    Level::Error => {
                        if config.use_colors {
                            "\x1b[31m🚨\x1b[0m"
                        } else {
                            "🚨"
                        }
                    }
                    Level::Warning => {
                        if config.use_colors {
                            "\x1b[33m⚠️\x1b[0m"
                        } else {
                            "⚠️"
                        }
                    }
                    Level::Info => {
                        if config.use_colors {
                            "\x1b[34mℹ️\x1b[0m"
                        } else {
                            "ℹ️"
                        }
                    }
                };

                writeln!(
                    output,
                    "   {level_symbol} Issue #{}: {}",
                    i + 1,
                    issue.constraint_name
                )
                .unwrap();
                writeln!(output, "      Check: {}", issue.check_name).unwrap();
                writeln!(output, "      Level: {:?}", issue.level).unwrap();
                writeln!(output, "      Message: {}", issue.message).unwrap();

                if let Some(metric) = issue.metric {
                    writeln!(output, "      Metric: {metric:.3}").unwrap();
                }
            }

            if report.issues.len() > issues_to_show.len() {
                writeln!(output).unwrap();
                writeln!(
                    output,
                    "   ... and {} more issues (use --max-issues to show more)",
                    report.issues.len() - issues_to_show.len()
                )
                .unwrap();
            }
        }

        writeln!(output).unwrap();
        Ok(output)
    }
}

/// Formats validation results as Markdown suitable for documentation.
///
/// This formatter creates Markdown output that can be included in reports,
/// documentation, or README files. It provides a clean, structured format
/// with proper heading hierarchy.
///
/// # Examples
///
/// ```rust
/// use term_guard::formatters::{ResultFormatter, MarkdownFormatter};
/// # use term_guard::core::{ValidationResult, ValidationReport, ValidationMetrics};
/// # let metrics = ValidationMetrics::new();
/// # let report = ValidationReport::new("test");
/// # let result = ValidationResult::success(metrics, report);
///
/// let formatter = MarkdownFormatter::new();
/// let markdown_output = formatter.format(&result).unwrap();
/// println!("{}", markdown_output);
/// ```
#[derive(Debug, Clone)]
pub struct MarkdownFormatter {
    config: FormatterConfig,
    heading_level: u8,
}

impl MarkdownFormatter {
    /// Creates a new Markdown formatter with default configuration.
    pub fn new() -> Self {
        Self {
            config: FormatterConfig::default(),
            heading_level: 2,
        }
    }

    /// Creates a new Markdown formatter with the specified configuration.
    pub fn with_config(config: FormatterConfig) -> Self {
        Self {
            config,
            heading_level: 2,
        }
    }

    /// Sets the base heading level for the output.
    pub fn with_heading_level(mut self, level: u8) -> Self {
        self.heading_level = level.clamp(1, 6);
        self
    }
}

impl Default for MarkdownFormatter {
    fn default() -> Self {
        Self::new()
    }
}

impl ResultFormatter for MarkdownFormatter {
    fn format(&self, result: &ValidationResult) -> Result<String> {
        self.format_with_config(result, &self.config)
    }

    fn format_with_config(
        &self,
        result: &ValidationResult,
        config: &FormatterConfig,
    ) -> Result<String> {
        let mut output = String::new();
        let report = result.report();
        let h = "#".repeat(self.heading_level as usize);

        // Main heading
        if result.is_success() {
            writeln!(output, "{h} ✅ Validation Report - PASSED").unwrap();
        } else {
            writeln!(output, "{h} ❌ Validation Report - FAILED").unwrap();
        }

        writeln!(output).unwrap();
        writeln!(output, "**Suite:** {}", report.suite_name).unwrap();

        if config.include_timestamps {
            writeln!(output, "**Timestamp:** {}", report.timestamp).unwrap();
        }

        // Summary table
        if config.include_metrics {
            writeln!(output).unwrap();
            writeln!(output, "{h}# Summary").unwrap();
            writeln!(output).unwrap();
            writeln!(output, "| Metric | Value |").unwrap();
            writeln!(output, "|--------|-------|").unwrap();
            writeln!(output, "| Total Checks | {} |", report.metrics.total_checks).unwrap();
            writeln!(output, "| Passed | {} |", report.metrics.passed_checks).unwrap();
            writeln!(output, "| Failed | {} |", report.metrics.failed_checks).unwrap();
            writeln!(output, "| Skipped | {} |", report.metrics.skipped_checks).unwrap();
            writeln!(
                output,
                "| Success Rate | {:.1}% |",
                report.metrics.success_rate()
            )
            .unwrap();
            writeln!(
                output,
                "| Execution Time | {}ms |",
                report.metrics.execution_time_ms
            )
            .unwrap();
        }

        // Custom metrics
        if config.include_custom_metrics && !report.metrics.custom_metrics.is_empty() {
            writeln!(output).unwrap();
            writeln!(output, "{h}# Custom Metrics").unwrap();
            writeln!(output).unwrap();
            writeln!(output, "| Metric | Value |").unwrap();
            writeln!(output, "|--------|-------|").unwrap();
            for (name, value) in &report.metrics.custom_metrics {
                writeln!(output, "| {name} | {value:.3} |").unwrap();
            }
        }

        // Issues
        if config.include_issues && !report.issues.is_empty() {
            writeln!(output).unwrap();
            writeln!(output, "{h}# Issues").unwrap();
            writeln!(output).unwrap();

            let issues_to_show = if config.max_issues < 0 {
                report.issues.as_slice()
            } else {
                let max = config.max_issues as usize;
                &report.issues[..std::cmp::min(max, report.issues.len())]
            };

            for (i, issue) in issues_to_show.iter().enumerate() {
                let level_emoji = match issue.level {
                    Level::Error => "🚨",
                    Level::Warning => "⚠️",
                    Level::Info => "ℹ️",
                };

                writeln!(
                    output,
                    "{h}## {level_emoji} Issue #{}: {}",
                    i + 1,
                    issue.constraint_name
                )
                .unwrap();
                writeln!(output).unwrap();
                writeln!(output, "- **Check:** {}", issue.check_name).unwrap();
                writeln!(output, "- **Level:** {:?}", issue.level).unwrap();
                writeln!(output, "- **Message:** {}", issue.message).unwrap();

                if let Some(metric) = issue.metric {
                    writeln!(output, "- **Metric:** {metric:.3}").unwrap();
                }

                writeln!(output).unwrap();
            }

            if report.issues.len() > issues_to_show.len() {
                writeln!(
                    output,
                    "> **Note:** {} additional issues not shown in this report.",
                    report.issues.len() - issues_to_show.len()
                )
                .unwrap();
                writeln!(output).unwrap();
            }
        }

        Ok(output)
    }
}

/// Helper function to filter validation result based on configuration.
fn filter_result_for_config(
    result: &ValidationResult,
    config: &FormatterConfig,
) -> ValidationResult {
    match result {
        ValidationResult::Success { metrics, report } => {
            let filtered_report = filter_report(report, config);
            let filtered_metrics = if config.include_metrics {
                metrics.clone()
            } else {
                let mut minimal_metrics = metrics.clone();
                if !config.include_custom_metrics {
                    minimal_metrics.custom_metrics.clear();
                }
                minimal_metrics
            };
            ValidationResult::Success {
                metrics: filtered_metrics,
                report: filtered_report,
            }
        }
        ValidationResult::Failure { report } => ValidationResult::Failure {
            report: filter_report(report, config),
        },
    }
}

/// Helper function to filter validation report based on configuration.
fn filter_report(report: &ValidationReport, config: &FormatterConfig) -> ValidationReport {
    let mut filtered_report = report.clone();

    if !config.include_issues {
        filtered_report.issues.clear();
    } else if config.max_issues >= 0 {
        let max = config.max_issues as usize;
        filtered_report.issues.truncate(max);
    }

    if !config.include_custom_metrics {
        filtered_report.metrics.custom_metrics.clear();
    }

    if !config.include_timestamps {
        filtered_report.timestamp = String::new();
    }

    filtered_report
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Level, ValidationIssue, ValidationMetrics, ValidationReport};

    fn create_test_result() -> ValidationResult {
        let mut metrics = ValidationMetrics::new();
        metrics.total_checks = 10;
        metrics.passed_checks = 8;
        metrics.failed_checks = 2;
        metrics.skipped_checks = 0;
        metrics.execution_time_ms = 150;
        metrics
            .custom_metrics
            .insert("data.completeness".to_string(), 0.85);

        let mut report = ValidationReport::new("test_suite");
        report.add_issue(ValidationIssue {
            check_name: "completeness_check".to_string(),
            constraint_name: "completeness".to_string(),
            level: Level::Error,
            message: "Column has insufficient completeness".to_string(),
            metric: Some(0.75),
        });

        report.add_issue(ValidationIssue {
            check_name: "size_check".to_string(),
            constraint_name: "size".to_string(),
            level: Level::Warning,
            message: "Dataset size is below expected range".to_string(),
            metric: Some(150.0),
        });

        report.metrics = metrics.clone();
        ValidationResult::failure(report)
    }

    #[test]
    fn test_formatter_config() {
        let config = FormatterConfig::default();
        assert!(config.include_metrics);
        assert!(config.include_issues);
        assert!(config.use_colors);

        let minimal = FormatterConfig::minimal();
        assert!(minimal.include_metrics);
        assert!(!minimal.include_issues);
        assert!(!minimal.use_colors);

        let ci = FormatterConfig::ci();
        assert!(!ci.use_colors);
        assert_eq!(ci.max_issues, 50);
    }

    #[test]
    fn test_json_formatter() {
        let result = create_test_result();
        let formatter = JsonFormatter::new();

        let output = formatter.format(&result).unwrap();
        assert!(output.contains("\"status\": \"failure\""));
        assert!(output.contains("\"test_suite\""));
        assert!(output.contains("completeness_check"));

        // Test with config
        let config = FormatterConfig::minimal();
        let output = formatter.format_with_config(&result, &config).unwrap();
        assert!(output.contains("\"status\": \"failure\""));
    }

    #[test]
    fn test_human_formatter() {
        let result = create_test_result();
        let formatter = HumanFormatter::new();

        let output = formatter.format(&result).unwrap();
        assert!(output.contains("Validation FAILED"));
        assert!(output.contains("test_suite"));
        assert!(output.contains("Total Checks: 10"));
        assert!(output.contains("completeness_check"));

        // Test without colors
        let config = FormatterConfig::default().with_colors(false);
        let output = formatter.format_with_config(&result, &config).unwrap();
        assert!(output.contains("Validation FAILED"));
        assert!(!output.contains("\x1b["));
    }

    #[test]
    fn test_markdown_formatter() {
        let result = create_test_result();
        let formatter = MarkdownFormatter::new();

        let output = formatter.format(&result).unwrap();
        assert!(output.contains("## ❌ Validation Report - FAILED"));
        assert!(output.contains("**Suite:** test_suite"));
        assert!(output.contains("| Total Checks | 10 |"));
        assert!(output.contains("### 🚨 Issue #1: completeness"));

        // Test with different heading level
        let formatter = MarkdownFormatter::new().with_heading_level(1);
        let output = formatter.format(&result).unwrap();
        assert!(output.contains("# ❌ Validation Report - FAILED"));
    }

    #[test]
    fn test_config_max_issues() {
        let result = create_test_result();
        let config = FormatterConfig::default().with_max_issues(1);

        let formatter = HumanFormatter::new();
        let output = formatter.format_with_config(&result, &config).unwrap();
        assert!(output.contains("Issue #1"));
        assert!(output.contains("... and 1 more issues"));
    }
}
