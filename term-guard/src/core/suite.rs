//! Validation suite for organizing and running checks.

use super::{
    result::{ValidationIssue, ValidationMetrics, ValidationReport},
    Check, ConstraintStatus, Level, ValidationResult,
};
// use crate::optimizer::QueryOptimizer; // TODO: Re-enable once TermContext integration is resolved
use crate::prelude::*;
use crate::telemetry::{utils, TermSpan, TermTelemetry};
use datafusion::prelude::*;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, instrument, warn};

/// A collection of validation checks to be run together.
///
/// A `ValidationSuite` groups related checks and provides a way to execute
/// them against data. It supports telemetry integration for monitoring
/// validation performance and results.
///
/// # Examples
///
/// ```rust
/// use term_guard::core::ValidationSuite;
/// use term_guard::telemetry::TermTelemetry;
///
/// let suite = ValidationSuite::builder("data_quality_suite")
///     .description("Comprehensive data quality validation")
///     .build();
///
/// // Or with telemetry configuration:
/// # #[cfg(feature = "telemetry")]
/// # {
/// let telemetry = TermTelemetry::disabled();
/// let suite_with_telemetry = ValidationSuite::builder("data_quality_suite")
///     .with_telemetry(telemetry)
///     .build();
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ValidationSuite {
    /// The name of the validation suite
    name: String,
    /// Optional description of the suite's purpose
    description: Option<String>,
    /// The checks to run as part of this suite
    checks: Vec<Arc<Check>>,
    /// Optional telemetry configuration
    telemetry: Option<Arc<TermTelemetry>>,
    /// Optional query optimizer for improving performance
    use_optimizer: bool,
    /// The name of the table to validate (defaults to "data")
    table_name: String,
}

impl ValidationSuite {
    /// Runs the validation suite sequentially without optimization.
    async fn run_sequential(
        &self,
        ctx: &SessionContext,
        report: &mut ValidationReport,
        metrics: &mut ValidationMetrics,
        has_errors: &mut bool,
        #[allow(unused_variables)] start_time: &Instant,
        _suite_span: &mut TermSpan,
    ) -> Result<()> {
        for check in &self.checks {
            debug!(
                check.name = %check.name(),
                check.level = ?check.level(),
                check.constraints = check.constraints().len(),
                "Running validation check"
            );
            #[allow(unused_variables)]
            let check_start = Instant::now();

            // Create telemetry span for the check
            let _check_span = if let Some(telemetry) = &self.telemetry {
                telemetry.start_check_span(check.name(), check.constraints().len())
            } else {
                TermSpan::noop()
            };

            for constraint in check.constraints() {
                metrics.total_checks += 1;

                // Create telemetry span for the constraint
                let mut constraint_span = if let Some(telemetry) = &self.telemetry {
                    // Try to extract column information from constraint metadata if available
                    let column = None; // TODO: Extract from constraint if possible
                    telemetry.start_constraint_span(constraint.name(), column)
                } else {
                    TermSpan::noop()
                };

                // Run constraint evaluation with the proper table context
                let validation_ctx = crate::core::ValidationContext::new(self.table_name.clone());
                let result = crate::core::validation_context::CURRENT_CONTEXT
                    .scope(validation_ctx, constraint.evaluate(ctx))
                    .await;
                
                match result {
                    Ok(result) => {
                        // Record constraint result in telemetry
                        if let Some(telemetry) = &self.telemetry {
                            if telemetry.detailed_metrics {
                                utils::record_constraint_result(&mut constraint_span, &result);
                            }
                        }

                        match result.status {
                            ConstraintStatus::Success => {
                                metrics.passed_checks += 1;
                                debug!(
                                    constraint.name = %constraint.name(),
                                    check.name = %check.name(),
                                    constraint.metric = ?result.metric,
                                    "Constraint passed"
                                );

                                // Record success in metrics
                                #[cfg(feature = "telemetry")]
                                if let Some(telemetry) = &self.telemetry {
                                    if let Some(metrics_collector) = telemetry.metrics() {
                                        let attrs = vec![
                                            opentelemetry::KeyValue::new(
                                                "check.name",
                                                check.name().to_string(),
                                            ),
                                            opentelemetry::KeyValue::new(
                                                "check.type",
                                                constraint.name().to_string(),
                                            ),
                                            opentelemetry::KeyValue::new("check.passed", true),
                                        ];
                                        metrics_collector.increment_checks_passed(&attrs);
                                    }
                                }
                            }
                            ConstraintStatus::Failure => {
                                metrics.failed_checks += 1;
                                let failure_message = result.message.clone().unwrap_or_else(|| {
                                    let name = constraint.name();
                                    format!("Constraint {name} failed")
                                });
                                let issue = ValidationIssue {
                                    check_name: check.name().to_string(),
                                    constraint_name: constraint.name().to_string(),
                                    level: check.level(),
                                    message: failure_message.clone(),
                                    metric: result.metric,
                                };

                                if check.level() == Level::Error {
                                    *has_errors = true;
                                }

                                warn!(
                                    constraint.name = %constraint.name(),
                                    check.name = %check.name(),
                                    check.level = ?check.level(),
                                    failure.message = %issue.message,
                                    constraint.metric = ?result.metric,
                                    "Constraint failed"
                                );
                                report.add_issue(issue);

                                // Record failure in metrics
                                #[cfg(feature = "telemetry")]
                                if let Some(telemetry) = &self.telemetry {
                                    if let Some(metrics_collector) = telemetry.metrics() {
                                        let attrs = vec![
                                            opentelemetry::KeyValue::new(
                                                "check.name",
                                                check.name().to_string(),
                                            ),
                                            opentelemetry::KeyValue::new(
                                                "check.type",
                                                constraint.name().to_string(),
                                            ),
                                            opentelemetry::KeyValue::new("check.passed", false),
                                            opentelemetry::KeyValue::new(
                                                "failure.reason",
                                                failure_message,
                                            ),
                                        ];
                                        metrics_collector.increment_checks_failed(&attrs);
                                    }
                                }
                            }
                            ConstraintStatus::Skipped => {
                                metrics.skipped_checks += 1;
                                debug!(
                                    constraint.name = %constraint.name(),
                                    check.name = %check.name(),
                                    skip.reason = %result.message.as_deref().unwrap_or("No reason provided"),
                                    "Constraint skipped"
                                );
                            }
                        }

                        // Record custom metrics
                        if let Some(metric_value) = result.metric {
                            let check_name = check.name();
                            let constraint_name = constraint.name();
                            let metric_name = format!("{check_name}.{constraint_name}");
                            metrics
                                .custom_metrics
                                .insert(metric_name.clone(), metric_value);

                            // Record to OpenTelemetry metrics
                            #[cfg(feature = "telemetry")]
                            if let Some(telemetry) = &self.telemetry {
                                if let Some(metrics_collector) = telemetry.metrics() {
                                    let attrs = vec![
                                        opentelemetry::KeyValue::new("metric.name", metric_name),
                                        opentelemetry::KeyValue::new(
                                            "check.name",
                                            check.name().to_string(),
                                        ),
                                        opentelemetry::KeyValue::new(
                                            "constraint.type",
                                            constraint.name().to_string(),
                                        ),
                                    ];
                                    metrics_collector.record_custom_metric(metric_value, &attrs);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        // Record error in telemetry
                        constraint_span.record_error(&e as &dyn std::error::Error);

                        metrics.failed_checks += 1;
                        let issue = ValidationIssue {
                            check_name: check.name().to_string(),
                            constraint_name: constraint.name().to_string(),
                            level: check.level(),
                            message: format!("Error evaluating constraint: {e}"),
                            metric: None,
                        };

                        if check.level() == Level::Error {
                            *has_errors = true;
                        }

                        error!(
                            constraint.name = %constraint.name(),
                            check.name = %check.name(),
                            error = %e,
                            error.type = "constraint_evaluation",
                            "Error evaluating constraint"
                        );
                        report.add_issue(issue);
                    }
                }
            }

            // Record check duration in metrics
            #[cfg(feature = "telemetry")]
            if let Some(telemetry) = &self.telemetry {
                if let Some(metrics_collector) = telemetry.metrics() {
                    let check_duration = check_start.elapsed().as_secs_f64();
                    let attrs = vec![
                        opentelemetry::KeyValue::new("check.name", check.name().to_string()),
                        opentelemetry::KeyValue::new(
                            "check.constraint_count",
                            check.constraints().len() as i64,
                        ),
                    ];
                    metrics_collector.record_check_duration(check_duration, &attrs);
                }
            }
        }

        Ok(())
    }

    /// Records final metrics for the validation suite.
    fn record_final_metrics(
        &self,
        metrics: &ValidationMetrics,
        has_errors: bool,
        start_time: &Instant,
        suite_span: &mut TermSpan,
    ) {
        // Avoid unused variable warning when telemetry is disabled
        let _ = start_time;

        // Record suite duration in metrics
        #[cfg(feature = "telemetry")]
        if let Some(telemetry) = &self.telemetry {
            if let Some(metrics_collector) = telemetry.metrics() {
                let suite_duration = start_time.elapsed().as_secs_f64();
                let attrs = vec![
                    opentelemetry::KeyValue::new("suite.name", self.name.clone()),
                    opentelemetry::KeyValue::new("suite.passed", !has_errors),
                    opentelemetry::KeyValue::new("checks.total", metrics.total_checks as i64),
                    opentelemetry::KeyValue::new("checks.passed", metrics.passed_checks as i64),
                    opentelemetry::KeyValue::new("checks.failed", metrics.failed_checks as i64),
                ];
                metrics_collector.record_validation_duration(suite_duration, &attrs);

                // Record validation failure if there were errors
                if has_errors {
                    metrics_collector.increment_validation_failures(&attrs);
                }
            }
        }

        // Record final metrics in telemetry span
        if let Some(telemetry) = &self.telemetry {
            if telemetry.record_timing {
                utils::record_validation_metrics(
                    suite_span,
                    metrics.passed_checks as u32,
                    metrics.failed_checks as u32,
                    metrics.skipped_checks as u32,
                    metrics.execution_time_ms,
                );
            }
        }

        info!(
            suite.name = %self.name,
            metrics.passed = metrics.passed_checks,
            metrics.failed = metrics.failed_checks,
            metrics.skipped = metrics.skipped_checks,
            metrics.total = metrics.total_checks,
            metrics.duration_ms = metrics.execution_time_ms,
            metrics.success_rate = %format!("{:.2}%", metrics.success_rate()),
            suite.result = %if has_errors { "failed" } else { "passed" },
            "Validation suite completed"
        );
    }

    /// Creates a new builder for constructing a validation suite.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the validation suite
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_guard::core::ValidationSuite;
    ///
    /// let builder = ValidationSuite::builder("my_suite");
    /// ```
    pub fn builder(name: impl Into<String>) -> ValidationSuiteBuilder {
        ValidationSuiteBuilder::new(name)
    }

    /// Returns the name of the validation suite.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the description of the validation suite if available.
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    /// Returns the checks in this validation suite.
    pub fn checks(&self) -> &[Arc<Check>] {
        &self.checks
    }

    /// Returns whether telemetry is enabled for this suite.
    pub fn telemetry_enabled(&self) -> bool {
        self.telemetry.is_some()
    }

    /// Returns the telemetry configuration for this suite.
    pub fn telemetry(&self) -> Option<&Arc<TermTelemetry>> {
        self.telemetry.as_ref()
    }

    /// Returns whether the query optimizer is enabled for this suite.
    pub fn optimizer_enabled(&self) -> bool {
        self.use_optimizer
    }

    /// Runs the validation suite against the provided data.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The DataFusion session context containing the data to validate
    ///
    /// # Returns
    ///
    /// A `Result` containing the validation result or an error
    #[instrument(skip(self, ctx), fields(
        suite.name = %self.name,
        suite.checks = self.checks.len(),
        telemetry.enabled = self.telemetry_enabled()
    ))]
    pub async fn run(&self, ctx: &SessionContext) -> Result<ValidationResult> {
        info!(
            suite.name = %self.name,
            suite.checks = self.checks.len(),
            suite.description = ?self.description,
            "Starting validation suite"
        );
        let start_time = Instant::now();

        // Start active validation guard for metrics
        #[cfg(feature = "telemetry")]
        let _active_guard = if let Some(telemetry) = &self.telemetry {
            telemetry.metrics().map(|m| m.start_validation())
        } else {
            None
        };

        // Create telemetry span for the entire suite
        let mut suite_span = if let Some(telemetry) = &self.telemetry {
            telemetry.start_suite_span(&self.name, self.checks.len())
        } else {
            TermSpan::noop()
        };

        // Record validation run start in metrics
        #[cfg(feature = "telemetry")]
        if let Some(telemetry) = &self.telemetry {
            if let Some(metrics) = telemetry.metrics() {
                let attrs = vec![
                    opentelemetry::KeyValue::new("suite.name", self.name.clone()),
                    opentelemetry::KeyValue::new("check.count", self.checks.len() as i64),
                ];
                metrics.increment_validation_runs(&attrs);

                // Try to get row count from the data table
                let table_query = format!("SELECT COUNT(*) as row_count FROM {}", self.table_name);
                if let Ok(df) = ctx.sql(&table_query).await {
                    if let Ok(batches) = df.collect().await {
                        if !batches.is_empty() && batches[0].num_rows() > 0 {
                            if let Some(array) = batches[0]
                                .column(0)
                                .as_any()
                                .downcast_ref::<arrow::array::Int64Array>()
                            {
                                let row_count = array.value(0) as u64;
                                metrics.add_rows_processed(row_count, &attrs);
                            }
                        }
                    }
                }
            }
        }

        let mut report = ValidationReport::new(&self.name);
        let mut metrics = ValidationMetrics::new();
        let mut has_errors = false;

        // Use optimizer if enabled
        if self.use_optimizer {
            // TODO: Implement optimized execution once TermContext integration is resolved
            // For now, fall back to sequential execution
            warn!("Query optimizer is not yet implemented, falling back to sequential execution");
            self.run_sequential(
                ctx,
                &mut report,
                &mut metrics,
                &mut has_errors,
                &start_time,
                &mut suite_span,
            )
            .await?;
        } else {
            // Non-optimized execution path
            self.run_sequential(
                ctx,
                &mut report,
                &mut metrics,
                &mut has_errors,
                &start_time,
                &mut suite_span,
            )
            .await?;
        }

        metrics.execution_time_ms = start_time.elapsed().as_millis() as u64;
        report.metrics = metrics.clone();

        // Record final metrics and complete
        self.record_final_metrics(&metrics, has_errors, &start_time, &mut suite_span);

        info!(
            suite.name = %self.name,
            metrics.passed = metrics.passed_checks,
            metrics.failed = metrics.failed_checks,
            "Validation suite completed (optimized)"
        );

        if has_errors {
            Ok(ValidationResult::failure(report))
        } else {
            Ok(ValidationResult::success(metrics, report))
        }
    }
}

/// Builder for constructing `ValidationSuite` instances.
///
/// # Examples
///
/// ```rust
/// use term_guard::core::{ValidationSuite, Check, Level};
/// use term_guard::telemetry::TermTelemetry;
///
/// let suite = ValidationSuite::builder("quality_checks")
///     .description("Data quality validation suite")
///     .check(
///         Check::builder("completeness")
///             .level(Level::Error)
///             .build()
///     )
///     .build();
///
/// // Or with telemetry:
/// # #[cfg(feature = "telemetry")]
/// # {
/// let telemetry = TermTelemetry::disabled();
/// let suite_with_telemetry = ValidationSuite::builder("quality_checks")
///     .with_telemetry(telemetry)
///     .build();
/// # }
/// ```
#[derive(Debug)]
pub struct ValidationSuiteBuilder {
    name: String,
    description: Option<String>,
    checks: Vec<Arc<Check>>,
    telemetry: Option<Arc<TermTelemetry>>,
    use_optimizer: bool,
    table_name: String,
}

impl ValidationSuiteBuilder {
    /// Creates a new validation suite builder with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: None,
            checks: Vec::new(),
            telemetry: None,
            use_optimizer: false,
            table_name: "data".to_string(),
        }
    }

    /// Sets the description for the validation suite.
    ///
    /// # Arguments
    ///
    /// * `description` - A description of the suite's purpose
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Sets the table name to validate.
    ///
    /// By default, validation runs against a table named "data". Use this method
    /// to validate a different table, which is especially useful when working with
    /// database sources.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table to validate
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_guard::core::ValidationSuite;
    ///
    /// let suite = ValidationSuite::builder("customer_validation")
    ///     .table_name("customer_transactions")
    ///     .build();
    /// ```
    pub fn table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = table_name.into();
        self
    }

    /// Adds a check to the validation suite.
    ///
    /// # Arguments
    ///
    /// * `check` - The check to add
    pub fn check(mut self, check: Check) -> Self {
        self.checks.push(Arc::new(check));
        self
    }

    /// Adds multiple checks to the validation suite.
    ///
    /// # Arguments
    ///
    /// * `checks` - An iterator of checks to add
    pub fn checks<I>(mut self, checks: I) -> Self
    where
        I: IntoIterator<Item = Check>,
    {
        self.checks.extend(checks.into_iter().map(Arc::new));
        self
    }

    /// Sets the telemetry configuration for the suite.
    ///
    /// # Arguments
    ///
    /// * `telemetry` - The telemetry configuration to use
    pub fn with_telemetry(mut self, telemetry: TermTelemetry) -> Self {
        self.telemetry = Some(Arc::new(telemetry));
        self
    }

    /// Sets whether to use the query optimizer for execution.
    ///
    /// When enabled, the suite will attempt to optimize constraint execution
    /// by batching similar queries together. If optimization fails, it will
    /// fall back to sequential execution.
    ///
    /// # Arguments
    ///
    /// * `enabled` - Whether to enable query optimization
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_guard::core::ValidationSuite;
    ///
    /// let suite = ValidationSuite::builder("optimized_suite")
    ///     .with_optimizer(true)
    ///     .build();
    /// ```
    pub fn with_optimizer(mut self, enabled: bool) -> Self {
        self.use_optimizer = enabled;
        self
    }

    /// Builds the `ValidationSuite` instance.
    ///
    /// # Returns
    ///
    /// The constructed `ValidationSuite`
    pub fn build(self) -> ValidationSuite {
        ValidationSuite {
            name: self.name,
            description: self.description,
            checks: self.checks,
            telemetry: self.telemetry,
            use_optimizer: self.use_optimizer,
            table_name: self.table_name,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_suite_builder() {
        let suite = ValidationSuite::builder("test_suite")
            .description("Test validation suite")
            .check(Check::builder("test_check").build())
            .build();

        assert_eq!(suite.name(), "test_suite");
        assert_eq!(suite.description(), Some("Test validation suite"));
        assert!(!suite.telemetry_enabled()); // No telemetry configured
        assert_eq!(suite.checks().len(), 1);
    }

    #[test]
    fn test_validation_suite_default_telemetry() {
        let suite = ValidationSuite::builder("test_suite").build();
        assert!(!suite.telemetry_enabled()); // Telemetry is disabled by default (BYOT pattern)
    }

    #[cfg(feature = "telemetry")]
    #[test]
    fn test_validation_suite_with_telemetry() {
        let telemetry = TermTelemetry::disabled();
        let suite = ValidationSuite::builder("test_suite")
            .with_telemetry(telemetry)
            .build();
        assert!(suite.telemetry_enabled());
    }

    #[test]
    fn test_validation_suite_with_optimizer() {
        let suite = ValidationSuite::builder("test_suite")
            .with_optimizer(true)
            .build();
        assert!(suite.optimizer_enabled());

        let suite_no_opt = ValidationSuite::builder("test_suite")
            .with_optimizer(false)
            .build();
        assert!(!suite_no_opt.optimizer_enabled());

        // Default should be no optimizer
        let suite_default = ValidationSuite::builder("test_suite").build();
        assert!(!suite_default.optimizer_enabled());
    }
}
