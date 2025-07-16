//! Example demonstrating structured logging with OpenTelemetry integration.
//!
//! This example shows how Term uses structured logging with:
//! - Trace ID correlation
//! - Structured fields
//! - Multiple log levels
//! - Performance-sensitive logging

use datafusion::prelude::*;
use std::error::Error;
#[cfg(feature = "telemetry")]
use term_core::constraints::CompletenessConstraint;
use term_core::constraints::{Assertion, SizeConstraint};
use term_core::core::ConstraintOptions;
#[cfg(feature = "telemetry")]
use term_core::core::Level;
use term_core::core::{Check, ValidationSuite};

#[cfg(feature = "telemetry")]
use term_core::telemetry::TermTelemetry;

#[cfg(feature = "telemetry")]
use opentelemetry::{global, trace::TracerProvider, KeyValue};

#[cfg(feature = "telemetry")]
use opentelemetry_sdk::{
    trace::{Sampler, TracerProvider as SdkTracerProvider},
    Resource,
};

#[cfg(feature = "telemetry")]
use tracing_opentelemetry::OpenTelemetryLayer;

#[cfg(feature = "telemetry")]
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn Error>> {
    println!("Term Structured Logging Example\n");

    #[cfg(feature = "telemetry")]
    {
        // Initialize OpenTelemetry tracer
        let provider = SdkTracerProvider::builder()
            .with_config(
                opentelemetry_sdk::trace::Config::default()
                    .with_sampler(Sampler::AlwaysOn)
                    .with_resource(Resource::new([
                        KeyValue::new("service.name", "term-logging-example"),
                        KeyValue::new("service.version", "0.1.0"),
                    ])),
            )
            .build();

        // Set the global tracer provider
        global::set_tracer_provider(provider.clone());

        // Get a tracer
        let tracer = provider.tracer("term");

        // Configure tracing subscriber with structured logging
        // This setup ensures trace IDs are automatically included in logs
        let telemetry_layer = OpenTelemetryLayer::new(tracer.clone());

        let env_filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("info,term_core=debug"));

        // Initialize tracing subscriber with:
        // - JSON formatting for structured logs
        // - OpenTelemetry layer for trace correlation
        // - Environment filter for log levels
        let subscriber = tracing_subscriber::fmt()
            .json() // Use JSON format for structured logs
            .with_env_filter(env_filter)
            .finish()
            .with(telemetry_layer);

        subscriber.init();

        println!("✅ Structured logging initialized with:");
        println!("   - JSON formatted output");
        println!("   - OpenTelemetry trace correlation");
        println!("   - Log levels: info,term_core=debug");
        println!();

        // Create Term telemetry configuration
        let telemetry = TermTelemetry::new(global::BoxedTracer::new(Box::new(tracer)))
            .with_detailed_metrics(true)
            .with_timing(true)
            .with_attribute("environment", "example")
            .with_attribute("dataset", "synthetic");

        // Create a sample dataset
        let ctx = SessionContext::new();

        // Create sample data with some nulls to trigger different log paths
        let sql = r#"
            SELECT 
                ROW_NUMBER() OVER () as id,
                CASE 
                    WHEN ROW_NUMBER() OVER () % 10 = 0 THEN NULL 
                    ELSE 'user_' || (ROW_NUMBER() OVER ())::VARCHAR 
                END as username,
                CASE 
                    WHEN ROW_NUMBER() OVER () % 20 = 0 THEN NULL
                    ELSE (ROW_NUMBER() OVER () % 100 + 18)
                END as age,
                (RANDOM() * 100000)::INTEGER as score
            FROM (
                SELECT * FROM generate_series(1, 100) as t(i)
            )
        "#;

        ctx.register_table("data", ctx.sql(sql).await?.into_view())?;

        println!("✅ Sample dataset created with 100 rows");
        println!();

        // Create validation suite with multiple checks to demonstrate various log levels
        let suite = ValidationSuite::builder("user_data_validation")
            .description("Example validation with structured logging")
            .with_telemetry(telemetry)
            .check(
                Check::builder("id_completeness")
                    .level(Level::Error)
                    .constraint(CompletenessConstraint::new(
                        "id",
                        ConstraintOptions::new().with_threshold(1.0),
                    ))
                    .build(),
            )
            .check(
                Check::builder("username_completeness")
                    .level(Level::Warning)
                    .constraint(CompletenessConstraint::new(
                        "username",
                        ConstraintOptions::new().with_threshold(0.95),
                    )) // Will fail - only 90% complete
                    .build(),
            )
            .check(
                Check::builder("age_completeness")
                    .level(Level::Warning)
                    .constraint(CompletenessConstraint::new(
                        "age",
                        ConstraintOptions::new().with_threshold(0.90),
                    )) // Will pass - 95% complete
                    .build(),
            )
            .check(
                Check::builder("data_volume")
                    .level(Level::Error)
                    .constraint(SizeConstraint::new(Assertion::Between(50.0, 150.0)))
                    .build(),
            )
            .build();

        println!("🔍 Running validation suite...");
        println!("   Watch the structured log output below:");
        println!("   - Each log has a trace_id for correlation");
        println!("   - Structured fields make logs queryable");
        println!("   - Different log levels based on context");
        println!();
        println!("{}", "=".repeat(80));
        println!();

        // Run the validation suite - this will generate structured logs
        let result = suite.run(&ctx).await?;

        println!();
        println!("{}", "=".repeat(80));
        println!();

        // Display results
        if result.is_success() {
            println!("✅ All validations passed!");
        } else {
            println!("⚠️  Some validations had issues:");
            for issue in &result.report().issues {
                println!(
                    "   - {}: {} ({})",
                    issue.check_name, issue.message, issue.level
                );
            }
        }

        println!();
        println!("📊 Validation Metrics:");
        if let Some(metrics) = result.metrics() {
            println!("   - Total checks: {}", metrics.total_checks);
            println!("   - Passed: {}", metrics.passed_checks);
            println!("   - Failed: {}", metrics.failed_checks);
            println!("   - Skipped: {}", metrics.skipped_checks);
            println!("   - Duration: {}ms", metrics.execution_time_ms);
            println!("   - Success rate: {:.2}%", metrics.success_rate());
        }

        println!();
        println!("🔍 Log Analysis Tips:");
        println!("   1. All logs contain a trace_id field for correlation");
        println!("   2. Use jq to filter JSON logs: cargo run --example structured_logging_example --features telemetry 2>&1 | jq");
        println!("   3. Filter by level: ... | jq 'select(.level == \"WARN\")'");
        println!("   4. Extract specific fields: ... | jq '{{time, level, message: .fields.message, trace_id}}'");
        println!("   5. Group by trace: ... | jq -s 'group_by(.trace_id)'");

        // Shutdown telemetry
        global::shutdown_tracer_provider();

        println!();
        println!("🏁 Example completed successfully!");
    }

    #[cfg(not(feature = "telemetry"))]
    {
        // Fallback logging without telemetry
        tracing_subscriber::fmt()
            .with_env_filter("info,term_core=debug")
            .init();

        println!("❌ This example requires the 'telemetry' feature to be enabled.");
        println!("Run with: cargo run --example structured_logging_example --features telemetry");

        // Still demonstrate basic logging
        let ctx = SessionContext::new();
        ctx.sql("CREATE TABLE data AS SELECT 1 as id, 'test' as name")
            .await?;

        let suite = ValidationSuite::builder("basic_validation")
            .check(
                Check::builder("test_check")
                    .constraint(SizeConstraint::new(Assertion::Equals(1.0)))
                    .build(),
            )
            .build();

        let result = suite.run(&ctx).await?;
        println!(
            "\n✅ Basic validation completed: {}",
            if result.is_success() {
                "PASSED"
            } else {
                "FAILED"
            }
        );
    }

    Ok(())
}
