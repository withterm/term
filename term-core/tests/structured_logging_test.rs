//! Integration tests for structured logging functionality.

use datafusion::prelude::*;
use term_core::constraints::{Assertion, CompletenessConstraint, SizeConstraint};
use term_core::core::{Check, Level, ValidationSuite};

/// Test helper to capture structured logs
struct LogCapture {
    logs: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
}

impl LogCapture {
    fn new() -> Self {
        Self {
            logs: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    fn captured_logs(&self) -> Vec<String> {
        self.logs.lock().unwrap().clone()
    }
}

impl std::io::Write for LogCapture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let s = String::from_utf8_lossy(buf).to_string();
        self.logs.lock().unwrap().push(s);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

async fn create_test_context() -> SessionContext {
    let ctx = SessionContext::new();

    // Create test data
    let sql = r#"
        SELECT 
            id,
            CASE WHEN id <= 8 THEN 'user_' || id::VARCHAR ELSE NULL END as username,
            CASE WHEN id <= 9 THEN id * 10 ELSE NULL END as age
        FROM (
            SELECT * FROM generate_series(1, 10) as t(id)
        ) as data
    "#;

    ctx.register_table("data", ctx.sql(sql).await.unwrap().into_view())
        .unwrap();

    ctx
}

#[tokio::test]
async fn test_structured_logging_fields() {
    // Create a log capture writer
    let capture = LogCapture::new();
    let capture_clone = capture.logs.clone();

    // Set up tracing subscriber with JSON output to our capture
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_writer(move || LogCapture {
            logs: capture_clone.clone(),
        })
        .with_env_filter("info,term_core=debug")
        .finish();

    // Use the subscriber for this test
    let _guard = tracing::subscriber::set_default(subscriber);

    // Create and run a validation
    let ctx = create_test_context().await;
    let suite = ValidationSuite::builder("test_suite")
        .check(
            Check::builder("size_check")
                .constraint(SizeConstraint::new(Assertion::Equals(10.0)))
                .build(),
        )
        .build();

    let _ = suite.run(&ctx).await.unwrap();

    // Check that logs were captured
    let logs = capture.captured_logs();
    assert!(!logs.is_empty(), "Should have captured some logs");

    // Verify structured fields are present
    let combined_logs = logs.join("");

    // Check for key structured fields
    assert!(
        combined_logs.contains(r#""suite.name":"test_suite""#),
        "Should contain suite name"
    );
    assert!(
        combined_logs.contains(r#""level":"INFO""#),
        "Should contain log level"
    );
    assert!(
        combined_logs.contains(r#""level":"DEBUG""#),
        "Should contain debug logs"
    );
    assert!(
        combined_logs.contains(r#""message":"Starting validation suite""#),
        "Should contain start message"
    );
    assert!(
        combined_logs.contains(r#""message":"Validation suite completed""#),
        "Should contain completion message"
    );
}

#[tokio::test]
async fn test_constraint_logging() {
    let capture = LogCapture::new();
    let capture_clone = capture.logs.clone();

    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_writer(move || LogCapture {
            logs: capture_clone.clone(),
        })
        .with_env_filter("debug,term_core=debug")
        .finish();

    let _guard = tracing::subscriber::set_default(subscriber);

    let ctx = create_test_context().await;
    let suite = ValidationSuite::builder("test_suite")
        .check(
            Check::builder("completeness_check")
                .level(Level::Warning)
                .constraint(CompletenessConstraint::with_threshold("username", 0.9))
                .build(),
        )
        .build();

    let _ = suite.run(&ctx).await.unwrap();

    let logs = capture.captured_logs();
    let combined_logs = logs.join("");

    // Check constraint-specific logging
    assert!(
        combined_logs.contains(r#""constraint.name":"completeness""#),
        "Should log constraint name"
    );
    assert!(
        combined_logs.contains(r#""constraint.column":"username""#),
        "Should log column name"
    );
    assert!(
        combined_logs.contains(r#""constraint.threshold":"0.9""#),
        "Should log threshold"
    );
}

#[tokio::test]
async fn test_failure_logging() {
    let capture = LogCapture::new();
    let capture_clone = capture.logs.clone();

    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_writer(move || LogCapture {
            logs: capture_clone.clone(),
        })
        .with_env_filter("info,term_core=debug")
        .finish();

    let _guard = tracing::subscriber::set_default(subscriber);

    let ctx = create_test_context().await;
    let suite = ValidationSuite::builder("test_suite")
        .check(
            Check::builder("completeness_check")
                .level(Level::Error)
                .constraint(CompletenessConstraint::with_threshold("username", 0.95)) // Will fail - only 80% complete
                .build(),
        )
        .build();

    let result = suite.run(&ctx).await.unwrap();
    assert!(!result.is_success(), "Validation should fail");

    let logs = capture.captured_logs();
    let combined_logs = logs.join("");

    // Check failure logging
    assert!(
        combined_logs.contains(r#""level":"WARN""#),
        "Should have warning for failure"
    );
    assert!(
        combined_logs.contains(r#""message":"Constraint failed""#),
        "Should log constraint failure"
    );
    assert!(
        combined_logs.contains(r#""check.level":"Error""#),
        "Should log check level"
    );
}

#[tokio::test]
async fn test_metrics_in_logs() {
    let capture = LogCapture::new();
    let capture_clone = capture.logs.clone();

    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_writer(move || LogCapture {
            logs: capture_clone.clone(),
        })
        .with_env_filter("info")
        .finish();

    let _guard = tracing::subscriber::set_default(subscriber);

    let ctx = create_test_context().await;
    let suite = ValidationSuite::builder("test_suite")
        .check(
            Check::builder("size_check")
                .constraint(SizeConstraint::new(Assertion::Equals(10.0)))
                .build(),
        )
        .check(
            Check::builder("completeness_check")
                .constraint(CompletenessConstraint::with_threshold("age", 0.9))
                .build(),
        )
        .build();

    let _ = suite.run(&ctx).await.unwrap();

    let logs = capture.captured_logs();
    let combined_logs = logs.join("");

    // Check metrics in completion log
    assert!(
        combined_logs.contains(r#""metrics.passed":2"#),
        "Should log passed count"
    );
    assert!(
        combined_logs.contains(r#""metrics.failed":0"#),
        "Should log failed count"
    );
    assert!(
        combined_logs.contains(r#""metrics.total":2"#),
        "Should log total count"
    );
    assert!(
        combined_logs.contains(r#""metrics.duration_ms""#),
        "Should log duration"
    );
    assert!(
        combined_logs.contains(r#""metrics.success_rate""#),
        "Should log success rate"
    );
}
