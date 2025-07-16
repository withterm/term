//! OpenTelemetry integration for Term validation library.
//!
//! This module provides telemetry configuration and utilities following the
//! BYOT (Bring Your Own Tracer/Meter) pattern. Term does not initialize its own
//! OpenTelemetry SDK - users must configure and pass in their tracer and optionally a meter.
//!
//! # Feature Gate
//!
//! All telemetry functionality is behind the `telemetry` feature flag to ensure
//! zero overhead when telemetry is not needed.
//!
//! # Tracing
//!
//! Term provides comprehensive distributed tracing with spans for:
//! - Validation suite execution
//! - Individual check execution
//! - Constraint evaluation
//! - Data source loading
//!
//! # Metrics
//!
//! When a meter is provided, Term collects the following metrics:
//!
//! ## Histograms
//! - `data.validation.duration` - Duration of complete validation suite execution
//! - `data.validation.check.duration` - Duration of individual validation checks
//! - `data.processing.load.duration` - Time to load data for validation
//! - `data.validation.custom_metric` - Custom business metrics from constraints
//!
//! ## Counters  
//! - `data.validation.total` - Total number of validation runs
//! - `data.validation.rows` - Total number of rows processed
//! - `data.validation.failures` - Total number of failed validations
//! - `data.validation.checks.passed` - Total number of passed checks
//! - `data.validation.checks.failed` - Total number of failed checks
//!
//! ## Gauges
//! - Active validations (tracked internally)
//! - `data.validation.memory` - Memory usage of validation process (Linux only)
//!
//! # Examples
//!
//! ## Basic Tracing Only
//!
//! ```rust,ignore
//! use term_guard::telemetry::TermTelemetry;
//! use opentelemetry::trace::Tracer;
//!
//! // User configures their own tracer
//! let tracer = opentelemetry_jaeger::new_agent_pipeline()
//!     .with_service_name("my_validation_service")
//!     .install_simple()?;
//!
//! // Create telemetry configuration
//! let telemetry = TermTelemetry::new(tracer);
//!
//! // Use with validation suite
//! let suite = ValidationSuite::builder("my_suite")
//!     .with_telemetry(telemetry)
//!     .build();
//! ```
//!
//! ## With Metrics
//!
//! ```rust,ignore
//! use term_guard::telemetry::TermTelemetry;
//! use opentelemetry::{global, trace::Tracer};
//!
//! // User configures their own tracer and meter
//! let tracer = global::tracer("my_service");
//! let meter = global::meter("my_service");
//!
//! // Create telemetry configuration with metrics
//! let telemetry = TermTelemetry::new(tracer)
//!     .with_meter(&meter)?;
//!
//! // Use with validation suite
//! let suite = ValidationSuite::builder("my_suite")
//!     .with_telemetry(telemetry)
//!     .build();
//! ```

#[cfg(feature = "telemetry")]
use opentelemetry::{
    global::{BoxedSpan, BoxedTracer},
    metrics::{Counter, Histogram, Meter, ObservableGauge},
    trace::{Span, Status, Tracer},
    KeyValue,
};

#[cfg(feature = "telemetry")]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "telemetry")]
use std::sync::Arc;

/// Gets current memory usage in bytes.
#[cfg(feature = "telemetry")]
fn get_memory_usage() -> Result<u64, std::io::Error> {
    // This is a simplified implementation. In production, you might want to use
    // platform-specific APIs or crates like `sysinfo` for more accurate measurement.
    #[cfg(target_os = "linux")]
    {
        use std::fs;
        let status = fs::read_to_string("/proc/self/status")?;
        for line in status.lines() {
            if line.starts_with("VmRSS:") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    if let Ok(kb) = parts[1].parse::<u64>() {
                        return Ok(kb * 1024); // Convert KB to bytes
                    }
                }
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "VmRSS not found",
        ))
    }

    #[cfg(not(target_os = "linux"))]
    {
        // For non-Linux platforms, return a placeholder or use platform-specific APIs
        Ok(0)
    }
}

/// Metrics collection for validation operations.
///
/// This struct contains all the metrics that Term collects during validation
/// following OpenTelemetry semantic conventions.
#[cfg(feature = "telemetry")]
pub struct ValidationMetrics {
    // Histograms for timing
    validation_duration: Histogram<f64>,
    check_duration: Histogram<f64>,
    data_load_duration: Histogram<f64>,

    // Counters for throughput
    rows_processed: Counter<u64>,
    validation_runs: Counter<u64>,
    validation_failures: Counter<u64>,
    checks_passed: Counter<u64>,
    checks_failed: Counter<u64>,

    // Gauges for current state
    active_validations: Arc<AtomicU64>,
    memory_usage_bytes: Option<ObservableGauge<u64>>,

    // Custom metric histogram for business metrics
    custom_metrics: Histogram<f64>,
}

#[cfg(feature = "telemetry")]
impl ValidationMetrics {
    /// Creates a new ValidationMetrics instance with the provided meter.
    pub fn new(meter: &Meter) -> crate::prelude::Result<Self> {
        let active_validations = Arc::new(AtomicU64::new(0));

        // Create memory usage gauge
        let memory_usage = meter
            .u64_observable_gauge("data.validation.memory")
            .with_description("Memory usage of validation process in bytes")
            .with_unit("By")
            .with_callback(move |observer| {
                // Get current memory usage
                if let Ok(usage) = get_memory_usage() {
                    observer.observe(usage, &[]);
                }
            })
            .try_init()
            .ok();

        Ok(Self {
            // Duration metrics
            validation_duration: meter
                .f64_histogram("data.validation.duration")
                .with_description("Duration of complete validation suite execution")
                .with_unit("s")
                .init(),

            check_duration: meter
                .f64_histogram("data.validation.check.duration")
                .with_description("Duration of individual validation checks")
                .with_unit("s")
                .init(),

            data_load_duration: meter
                .f64_histogram("data.processing.load.duration")
                .with_description("Time to load data for validation")
                .with_unit("s")
                .init(),

            // Count metrics
            rows_processed: meter
                .u64_counter("data.validation.rows")
                .with_description("Total number of rows processed during validation")
                .with_unit("1")
                .init(),

            validation_runs: meter
                .u64_counter("data.validation.total")
                .with_description("Total number of validation runs")
                .with_unit("1")
                .init(),

            validation_failures: meter
                .u64_counter("data.validation.failures")
                .with_description("Total number of failed validations")
                .with_unit("1")
                .init(),

            checks_passed: meter
                .u64_counter("data.validation.checks.passed")
                .with_description("Total number of passed checks")
                .with_unit("1")
                .init(),

            checks_failed: meter
                .u64_counter("data.validation.checks.failed")
                .with_description("Total number of failed checks")
                .with_unit("1")
                .init(),

            // Gauge for active validations
            active_validations,
            memory_usage_bytes: memory_usage,

            // Custom metrics histogram
            custom_metrics: meter
                .f64_histogram("data.validation.custom_metric")
                .with_description("Custom business metrics from validation constraints")
                .with_unit("1")
                .init(),
        })
    }

    /// Records the duration of a validation suite execution.
    pub fn record_validation_duration(&self, duration_secs: f64, attributes: &[KeyValue]) {
        self.validation_duration.record(duration_secs, attributes);
    }

    /// Records the duration of a check execution.
    pub fn record_check_duration(&self, duration_secs: f64, attributes: &[KeyValue]) {
        self.check_duration.record(duration_secs, attributes);
    }

    /// Records the duration of data loading.
    pub fn record_data_load_duration(&self, duration_secs: f64, attributes: &[KeyValue]) {
        self.data_load_duration.record(duration_secs, attributes);
    }

    /// Increments the rows processed counter.
    pub fn add_rows_processed(&self, count: u64, attributes: &[KeyValue]) {
        self.rows_processed.add(count, attributes);
    }

    /// Increments the validation runs counter.
    pub fn increment_validation_runs(&self, attributes: &[KeyValue]) {
        self.validation_runs.add(1, attributes);
    }

    /// Increments the validation failures counter.
    pub fn increment_validation_failures(&self, attributes: &[KeyValue]) {
        self.validation_failures.add(1, attributes);
    }

    /// Increments the checks passed counter.
    pub fn increment_checks_passed(&self, attributes: &[KeyValue]) {
        self.checks_passed.add(1, attributes);
    }

    /// Increments the checks failed counter.
    pub fn increment_checks_failed(&self, attributes: &[KeyValue]) {
        self.checks_failed.add(1, attributes);
    }

    /// Increments the active validations gauge.
    pub fn start_validation(&self) -> ActiveValidationGuard {
        self.active_validations.fetch_add(1, Ordering::Relaxed);
        ActiveValidationGuard {
            counter: Arc::clone(&self.active_validations),
        }
    }

    /// Records a custom business metric.
    pub fn record_custom_metric(&self, value: f64, attributes: &[KeyValue]) {
        self.custom_metrics.record(value, attributes);
    }
}

// Manual Debug implementation for ValidationMetrics
#[cfg(feature = "telemetry")]
impl std::fmt::Debug for ValidationMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValidationMetrics")
            .field(
                "active_validations",
                &self.active_validations.load(Ordering::Relaxed),
            )
            .field("has_memory_gauge", &self.memory_usage_bytes.is_some())
            .finish()
    }
}

/// Guard that decrements active validations on drop.
#[cfg(feature = "telemetry")]
pub struct ActiveValidationGuard {
    counter: Arc<AtomicU64>,
}

#[cfg(feature = "telemetry")]
impl Drop for ActiveValidationGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Configuration for Term's telemetry integration.
///
/// This struct follows the BYOT (Bring Your Own Tracer) pattern where
/// users configure their own OpenTelemetry setup and pass in a tracer.
/// Term will use this tracer to create spans and record metrics.
#[derive(Debug)]
pub struct TermTelemetry {
    #[cfg(feature = "telemetry")]
    tracer: BoxedTracer,

    #[cfg(feature = "telemetry")]
    metrics: Option<Arc<ValidationMetrics>>,

    /// Whether to record detailed constraint-level metrics
    pub detailed_metrics: bool,

    /// Whether to record execution timing information
    pub record_timing: bool,

    /// Custom attributes to add to all spans
    pub custom_attributes: std::collections::HashMap<String, String>,
}

impl TermTelemetry {
    /// Creates a new telemetry configuration with a user-provided tracer.
    ///
    /// # Arguments
    ///
    /// * `tracer` - An OpenTelemetry tracer configured by the user
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use term_guard::telemetry::TermTelemetry;
    ///
    /// let tracer = opentelemetry_jaeger::new_agent_pipeline()
    ///     .with_service_name("my_service")
    ///     .install_simple()?;
    ///     
    /// let telemetry = TermTelemetry::new(tracer);
    /// ```
    #[cfg(feature = "telemetry")]
    pub fn new(tracer: BoxedTracer) -> Self {
        Self {
            tracer,
            metrics: None,
            detailed_metrics: true,
            record_timing: true,
            custom_attributes: std::collections::HashMap::new(),
        }
    }

    /// Creates a disabled telemetry configuration.
    ///
    /// This is useful when telemetry feature is enabled but you want to
    /// disable telemetry for specific validation runs.
    pub fn disabled() -> Self {
        Self {
            #[cfg(feature = "telemetry")]
            tracer: opentelemetry::global::tracer("noop"),
            #[cfg(feature = "telemetry")]
            metrics: None,
            detailed_metrics: false,
            record_timing: false,
            custom_attributes: std::collections::HashMap::new(),
        }
    }

    /// Sets the meter for metrics collection.
    ///
    /// # Arguments
    ///
    /// * `meter` - An OpenTelemetry meter configured by the user
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use term_guard::telemetry::TermTelemetry;
    ///
    /// let meter = opentelemetry::global::meter("term");
    /// let telemetry = TermTelemetry::new(tracer)
    ///     .with_meter(&meter)?;
    /// ```
    #[cfg(feature = "telemetry")]
    pub fn with_meter(mut self, meter: &Meter) -> crate::prelude::Result<Self> {
        self.metrics = Some(Arc::new(ValidationMetrics::new(meter)?));
        Ok(self)
    }

    /// Gets a reference to the metrics if available.
    #[cfg(feature = "telemetry")]
    pub fn metrics(&self) -> Option<&Arc<ValidationMetrics>> {
        self.metrics.as_ref()
    }

    /// Sets whether to record detailed constraint-level metrics.
    pub fn with_detailed_metrics(mut self, enabled: bool) -> Self {
        self.detailed_metrics = enabled;
        self
    }

    /// Sets whether to record execution timing information.
    pub fn with_timing(mut self, enabled: bool) -> Self {
        self.record_timing = enabled;
        self
    }

    /// Adds a custom attribute that will be added to all spans.
    pub fn with_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.custom_attributes.insert(key.into(), value.into());
        self
    }

    /// Adds multiple custom attributes.
    pub fn with_attributes<I, K, V>(mut self, attributes: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        for (key, value) in attributes {
            self.custom_attributes.insert(key.into(), value.into());
        }
        self
    }

    /// Creates a new span for a validation suite.
    ///
    /// # Arguments
    ///
    /// * `suite_name` - Name of the validation suite
    /// * `check_count` - Number of checks in the suite
    #[cfg(feature = "telemetry")]
    pub fn start_suite_span(&self, suite_name: &str, check_count: usize) -> TermSpan {
        let mut span = self.tracer.start(format!("validation_suite.{suite_name}"));

        // Add standard attributes
        span.set_attribute(opentelemetry::KeyValue::new(
            "validation.suite.name",
            suite_name.to_string(),
        ));
        span.set_attribute(opentelemetry::KeyValue::new(
            "validation.suite.check_count",
            check_count as i64,
        ));
        span.set_attribute(opentelemetry::KeyValue::new("validation.type", "suite"));

        // Add custom attributes
        for (key, value) in &self.custom_attributes {
            span.set_attribute(opentelemetry::KeyValue::new(key.clone(), value.clone()));
        }

        TermSpan::new(span)
    }

    /// Creates a span when telemetry feature is disabled.
    #[cfg(not(feature = "telemetry"))]
    pub fn start_suite_span(&self, _suite_name: &str, _check_count: usize) -> TermSpan {
        TermSpan::noop()
    }

    /// Creates a new span for a validation check.
    #[cfg(feature = "telemetry")]
    pub fn start_check_span(&self, check_name: &str, constraint_count: usize) -> TermSpan {
        let mut span = self.tracer.start(format!("validation_check.{check_name}"));

        span.set_attribute(opentelemetry::KeyValue::new(
            "validation.check.name",
            check_name.to_string(),
        ));
        span.set_attribute(opentelemetry::KeyValue::new(
            "validation.check.constraint_count",
            constraint_count as i64,
        ));
        span.set_attribute(opentelemetry::KeyValue::new("validation.type", "check"));

        for (key, value) in &self.custom_attributes {
            span.set_attribute(opentelemetry::KeyValue::new(key.clone(), value.clone()));
        }

        TermSpan::new(span)
    }

    #[cfg(not(feature = "telemetry"))]
    pub fn start_check_span(&self, _check_name: &str, _constraint_count: usize) -> TermSpan {
        TermSpan::noop()
    }

    /// Creates a new span for constraint evaluation.
    #[cfg(feature = "telemetry")]
    pub fn start_constraint_span(&self, constraint_name: &str, column: Option<&str>) -> TermSpan {
        let mut span = self
            .tracer
            .start(format!("validation_constraint.{constraint_name}"));

        span.set_attribute(opentelemetry::KeyValue::new(
            "validation.constraint.name",
            constraint_name.to_string(),
        ));
        span.set_attribute(opentelemetry::KeyValue::new(
            "validation.type",
            "constraint",
        ));

        if let Some(col) = column {
            span.set_attribute(opentelemetry::KeyValue::new(
                "validation.constraint.column",
                col.to_string(),
            ));
        }

        for (key, value) in &self.custom_attributes {
            span.set_attribute(opentelemetry::KeyValue::new(key.clone(), value.clone()));
        }

        TermSpan::new(span)
    }

    #[cfg(not(feature = "telemetry"))]
    pub fn start_constraint_span(&self, _constraint_name: &str, _column: Option<&str>) -> TermSpan {
        TermSpan::noop()
    }

    /// Creates a new span for data source operations.
    #[cfg(feature = "telemetry")]
    pub fn start_datasource_span(&self, source_type: &str, table_name: &str) -> TermSpan {
        let mut span = self.tracer.start(format!("data_source.{source_type}"));

        span.set_attribute(opentelemetry::KeyValue::new(
            "data_source.type",
            source_type.to_string(),
        ));
        span.set_attribute(opentelemetry::KeyValue::new(
            "data_source.table_name",
            table_name.to_string(),
        ));
        span.set_attribute(opentelemetry::KeyValue::new(
            "validation.type",
            "data_source",
        ));

        for (key, value) in &self.custom_attributes {
            span.set_attribute(opentelemetry::KeyValue::new(key.clone(), value.clone()));
        }

        TermSpan::new(span)
    }

    #[cfg(not(feature = "telemetry"))]
    pub fn start_datasource_span(&self, _source_type: &str, _table_name: &str) -> TermSpan {
        TermSpan::noop()
    }
}

/// A wrapper around OpenTelemetry spans that provides a consistent interface
impl Clone for TermTelemetry {
    fn clone(&self) -> Self {
        Self {
            #[cfg(feature = "telemetry")]
            tracer: opentelemetry::global::tracer("noop"), // Use noop tracer for clones
            #[cfg(feature = "telemetry")]
            metrics: self.metrics.clone(), // Metrics can be shared via Arc
            detailed_metrics: self.detailed_metrics,
            record_timing: self.record_timing,
            custom_attributes: self.custom_attributes.clone(),
        }
    }
}

/// regardless of whether the telemetry feature is enabled.
pub struct TermSpan {
    #[cfg(feature = "telemetry")]
    span: BoxedSpan,

    #[cfg(not(feature = "telemetry"))]
    _phantom: std::marker::PhantomData<()>,
}

impl TermSpan {
    #[cfg(feature = "telemetry")]
    fn new(span: BoxedSpan) -> Self {
        Self { span }
    }

    /// Creates a no-op span that does nothing.
    /// This is used when telemetry is disabled.
    pub fn noop() -> Self {
        Self {
            #[cfg(feature = "telemetry")]
            span: opentelemetry::global::tracer("noop").start("noop"),
            #[cfg(not(feature = "telemetry"))]
            _phantom: std::marker::PhantomData,
        }
    }

    /// Records an event on this span.
    #[cfg(feature = "telemetry")]
    pub fn add_event(&mut self, name: impl Into<String>, attributes: Vec<opentelemetry::KeyValue>) {
        self.span.add_event(name.into(), attributes);
    }

    #[cfg(not(feature = "telemetry"))]
    pub fn add_event(&mut self, _name: impl Into<String>, _attributes: Vec<()>) {
        // No-op when telemetry is disabled
    }

    /// Sets an attribute on this span.
    #[cfg(feature = "telemetry")]
    pub fn set_attribute(&mut self, kv: opentelemetry::KeyValue) {
        self.span.set_attribute(kv);
    }

    #[cfg(not(feature = "telemetry"))]
    pub fn set_attribute(&mut self, _kv: ()) {
        // No-op when telemetry is disabled
    }

    /// Sets the status of this span.
    #[cfg(feature = "telemetry")]
    pub fn set_status(&mut self, status: Status) {
        self.span.set_status(status);
    }

    #[cfg(not(feature = "telemetry"))]
    pub fn set_status(&mut self, _status: ()) {
        // No-op when telemetry is disabled
    }

    /// Records an error on this span.
    #[cfg(feature = "telemetry")]
    pub fn record_error(&mut self, error: &dyn std::error::Error) {
        self.span.record_error(error);
        self.span.set_status(Status::Error {
            description: error.to_string().into(),
        });
    }

    #[cfg(not(feature = "telemetry"))]
    pub fn record_error(&mut self, _error: &dyn std::error::Error) {
        // No-op when telemetry is disabled
    }
}

impl Drop for TermSpan {
    #[cfg(feature = "telemetry")]
    fn drop(&mut self) {
        self.span.end();
    }

    #[cfg(not(feature = "telemetry"))]
    fn drop(&mut self) {
        // No-op when telemetry is disabled
    }
}

/// Utility functions for telemetry integration.
pub mod utils {
    use super::*;

    /// Records validation metrics as span attributes.
    #[cfg(feature = "telemetry")]
    pub fn record_validation_metrics(
        span: &mut TermSpan,
        passed: u32,
        failed: u32,
        skipped: u32,
        duration_ms: u64,
    ) {
        span.set_attribute(opentelemetry::KeyValue::new(
            "validation.metrics.passed",
            passed as i64,
        ));
        span.set_attribute(opentelemetry::KeyValue::new(
            "validation.metrics.failed",
            failed as i64,
        ));
        span.set_attribute(opentelemetry::KeyValue::new(
            "validation.metrics.skipped",
            skipped as i64,
        ));
        span.set_attribute(opentelemetry::KeyValue::new(
            "validation.metrics.total",
            (passed + failed + skipped) as i64,
        ));
        span.set_attribute(opentelemetry::KeyValue::new(
            "validation.duration_ms",
            duration_ms as i64,
        ));
    }

    #[cfg(not(feature = "telemetry"))]
    pub fn record_validation_metrics(
        _span: &mut TermSpan,
        _passed: u32,
        _failed: u32,
        _skipped: u32,
        _duration_ms: u64,
    ) {
        // No-op when telemetry is disabled
    }

    /// Records constraint result as span attributes.
    #[cfg(feature = "telemetry")]
    pub fn record_constraint_result(span: &mut TermSpan, result: &crate::core::ConstraintResult) {
        use crate::core::ConstraintStatus;

        let status_str = match result.status {
            ConstraintStatus::Success => "success",
            ConstraintStatus::Failure => "failure",
            ConstraintStatus::Skipped => "skipped",
        };

        span.set_attribute(opentelemetry::KeyValue::new(
            "validation.constraint.status",
            status_str,
        ));

        if let Some(metric) = result.metric {
            span.set_attribute(opentelemetry::KeyValue::new(
                "validation.constraint.metric",
                metric,
            ));
        }

        if let Some(ref message) = result.message {
            span.set_attribute(opentelemetry::KeyValue::new(
                "validation.constraint.message",
                message.clone(),
            ));
        }
    }

    #[cfg(not(feature = "telemetry"))]
    pub fn record_constraint_result(_span: &mut TermSpan, _result: &crate::core::ConstraintResult) {
        // No-op when telemetry is disabled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disabled_telemetry() {
        let telemetry = TermTelemetry::disabled();
        assert!(!telemetry.detailed_metrics);
        assert!(!telemetry.record_timing);
    }

    #[test]
    fn test_telemetry_configuration() {
        let telemetry = TermTelemetry::disabled()
            .with_detailed_metrics(true)
            .with_timing(true)
            .with_attribute("service.name", "test_service")
            .with_attributes([("env", "test"), ("version", "1.0.0")]);

        assert!(telemetry.detailed_metrics);
        assert!(telemetry.record_timing);
        assert_eq!(
            telemetry.custom_attributes.get("service.name"),
            Some(&"test_service".to_string())
        );
        assert_eq!(
            telemetry.custom_attributes.get("env"),
            Some(&"test".to_string())
        );
        assert_eq!(
            telemetry.custom_attributes.get("version"),
            Some(&"1.0.0".to_string())
        );
    }

    #[test]
    fn test_noop_span_operations() {
        let telemetry = TermTelemetry::disabled();
        let mut span = telemetry.start_suite_span("test_suite", 5);

        // These should not panic when telemetry is disabled
        span.add_event("test_event", vec![]);
        #[cfg(feature = "telemetry")]
        span.set_attribute(opentelemetry::KeyValue::new("test_key", "test_value"));
        span.record_error(&std::io::Error::new(
            std::io::ErrorKind::Other,
            "test error",
        ));
    }

    #[cfg(feature = "telemetry")]
    #[test]
    fn test_telemetry_with_noop_tracer() {
        let tracer = opentelemetry::global::tracer("test");
        let telemetry = TermTelemetry::new(tracer);

        // Test that spans can be created
        let _suite_span = telemetry.start_suite_span("test_suite", 3);
        let _check_span = telemetry.start_check_span("test_check", 2);
        let _constraint_span =
            telemetry.start_constraint_span("test_constraint", Some("test_column"));
        let _datasource_span = telemetry.start_datasource_span("csv", "test_table");
    }
}
