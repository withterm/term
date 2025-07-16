//! Integration tests for OpenTelemetry metrics collection.

#[cfg(feature = "telemetry")]
mod metrics_tests {
    use datafusion::prelude::*;
    use opentelemetry::{global, KeyValue};
    use term_core::constraints::{Assertion, CompletenessConstraint, SizeConstraint};
    use term_core::core::ConstraintOptions;
    use term_core::core::{Check, Level, ValidationSuite};
    use term_core::telemetry::TermTelemetry;

    async fn create_test_context() -> SessionContext {
        let ctx = SessionContext::new();

        // Create test data with some nulls
        let sql = r#"
            SELECT 
                id,
                CASE WHEN id % 10 = 0 THEN NULL ELSE 'user_' || id::VARCHAR END as username,
                CASE WHEN id % 5 = 0 THEN NULL ELSE (id % 100 + 18) END as age
            FROM (
                SELECT * FROM generate_series(1, 100) as t(id)
            ) as data
        "#;

        ctx.register_table("data", ctx.sql(sql).await.unwrap().into_view())
            .unwrap();

        ctx
    }

    #[tokio::test]
    async fn test_metrics_api() {
        // This test verifies that the metrics API works correctly
        // We can't easily test the actual metrics collection without a complex setup

        // Create a basic meter (noop in tests)
        let tracer = global::tracer("test");
        let meter = global::meter("term");

        // Create telemetry configuration with metrics
        let telemetry = TermTelemetry::new(tracer).with_meter(&meter).unwrap();

        // Verify metrics are available
        assert!(telemetry.metrics().is_some());

        // Create validation suite
        let suite = ValidationSuite::builder("metrics_test_suite")
            .with_telemetry(telemetry)
            .check(
                Check::builder("completeness_check")
                    .level(Level::Warning)
                    .constraint(CompletenessConstraint::new(
                        "username",
                        ConstraintOptions::new().with_threshold(0.9),
                    ))
                    .build(),
            )
            .check(
                Check::builder("size_check")
                    .level(Level::Error)
                    .constraint(SizeConstraint::new(Assertion::Equals(100.0)))
                    .build(),
            )
            .build();

        // Run validation
        let ctx = create_test_context().await;
        let result = suite.run(&ctx).await.unwrap();

        // Verify the validation succeeded
        assert!(result.is_success());
    }

    #[tokio::test]
    async fn test_metrics_zero_overhead_when_disabled() {
        // Create telemetry without metrics (BYOT pattern - user decides what to provide)
        let tracer = global::tracer("test");
        let telemetry = TermTelemetry::new(tracer);

        // Create validation suite
        let suite = ValidationSuite::builder("no_metrics_suite")
            .with_telemetry(telemetry)
            .check(
                Check::builder("test_check")
                    .constraint(SizeConstraint::new(Assertion::GreaterThan(0.0)))
                    .build(),
            )
            .build();

        // Run validation
        let ctx = create_test_context().await;
        let result = suite.run(&ctx).await.unwrap();

        // Validation should work without metrics
        assert!(result.is_success());
    }

    #[tokio::test]
    async fn test_metrics_with_telemetry_features() {
        // Test various telemetry features
        let tracer = global::tracer("test");
        let meter = global::meter("term");

        // Create telemetry with all features enabled
        let telemetry = TermTelemetry::new(tracer)
            .with_meter(&meter)
            .unwrap()
            .with_detailed_metrics(true)
            .with_timing(true)
            .with_attribute("environment", "test")
            .with_attribute("team", "data-quality");

        // Access metrics to trigger initialization
        let metrics = telemetry.metrics().unwrap();

        // Test metric recording methods
        let attrs = vec![KeyValue::new("test", "true")];

        // These should not panic
        metrics.record_validation_duration(1.5, &attrs);
        metrics.record_check_duration(0.5, &attrs);
        metrics.record_data_load_duration(2.0, &attrs);
        metrics.add_rows_processed(1000, &attrs);
        metrics.increment_validation_runs(&attrs);
        metrics.increment_validation_failures(&attrs);
        metrics.increment_checks_passed(&attrs);
        metrics.increment_checks_failed(&attrs);

        // Test active validation guard
        {
            let _guard = metrics.start_validation();
            // Guard should automatically decrement on drop
        }

        // Create and run a simple validation
        let suite = ValidationSuite::builder("test_suite")
            .with_telemetry(telemetry)
            .check(
                Check::builder("test")
                    .constraint(SizeConstraint::new(Assertion::GreaterThan(0.0)))
                    .build(),
            )
            .build();

        let ctx = create_test_context().await;
        let result = suite.run(&ctx).await.unwrap();
        assert!(result.is_success());
    }
}
