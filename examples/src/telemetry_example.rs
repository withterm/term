//! Example demonstrating OpenTelemetry integration with Term validation.
//!
//! This example shows how to:
//! 1. Configure OpenTelemetry with a console exporter
//! 2. Create a TermTelemetry configuration
//! 3. Run validation with telemetry spans and metrics
//!
//! Note: This example uses the `telemetry` feature flag.

use datafusion::prelude::*;
use std::error::Error;
use term_guard::core::{Check, Level, ValidationSuite};
use term_guard::prelude::*;

#[cfg(feature = "telemetry")]
use opentelemetry::{global, KeyValue};

#[cfg(feature = "telemetry")]
use opentelemetry_sdk::{
    metrics::{PeriodicReader, SdkMeterProvider},
    trace::SdkTracerProvider,
    Resource,
};

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn Error>> {
    // Initialize console logging
    tracing_subscriber::fmt::init();

    println!("Term OpenTelemetry Integration Example\n");

    #[cfg(feature = "telemetry")]
    {
        // Initialize OpenTelemetry with console exporter for demo purposes
        let provider = SdkTracerProvider::builder()
            .with_resource(
                Resource::builder()
                    .with_service_name("term-validation-example")
                    .with_attribute(KeyValue::new("service.version", "0.1.0"))
                    .build(),
            )
            .build();

        // Set the global tracer provider
        global::set_tracer_provider(provider.clone());

        // Set up metrics provider with InMemory exporter for demonstration
        let metrics_exporter = opentelemetry_sdk::metrics::InMemoryMetricExporter::default();
        let metrics_exporter_clone = metrics_exporter.clone();
        let reader = PeriodicReader::builder(metrics_exporter)
            .with_interval(std::time::Duration::from_secs(5))
            .build();
        let meter_provider = SdkMeterProvider::builder()
            .with_resource(
                Resource::builder()
                    .with_service_name("term-validation-example")
                    .with_attribute(KeyValue::new("service.version", "0.1.0"))
                    .build(),
            )
            .with_reader(reader)
            .build();

        // Set the global meter provider
        global::set_meter_provider(meter_provider);

        // Get a tracer for Term
        let tracer = global::tracer("term");

        // Get a meter for Term
        let meter = global::meter("term");

        // Create Term telemetry configuration
        let telemetry = TermTelemetry::new(tracer)
            .with_meter(&meter)?
            .with_detailed_metrics(true)
            .with_timing(true)
            .with_attribute("environment", "example")
            .with_attribute("dataset", "synthetic");

        println!("✅ OpenTelemetry tracing configured with console exporter");
        println!("✅ OpenTelemetry metrics configured with InMemory exporter");
        println!("✅ Term telemetry configuration created with tracing and metrics");
        println!();

        // Create a sample dataset
        let ctx = SessionContext::new();

        // Create sample data using DataFusion
        let sql = r#"
            SELECT 
                ROW_NUMBER() OVER () as id,
                CASE 
                    WHEN ROW_NUMBER() OVER () % 10 = 0 THEN NULL 
                    ELSE 'user_' || (ROW_NUMBER() OVER ())::VARCHAR 
                END as username,
                CASE 
                    WHEN ROW_NUMBER() OVER () % 5 = 0 THEN NULL
                    ELSE (ROW_NUMBER() OVER () % 100 + 18)
                END as age,
                (RANDOM() * 100000)::INTEGER as score
            FROM (
                SELECT * FROM generate_series(1, 1000) as t(i)
            )
        "#;

        ctx.register_table("users", ctx.sql(sql).await?.into_view())?;

        println!("✅ Sample dataset created (1000 users with some null values)");

        // Create validation suite with telemetry
        let suite = ValidationSuite::builder("user_data_quality")
            .description("Comprehensive user data validation with telemetry")
            .with_telemetry(telemetry)
            .check(
                Check::builder("id_completeness")
                    .level(Level::Error)
                    .constraint(constraints::completeness("id", None))
                    .build(),
            )
            .check(
                Check::builder("username_validation")
                    .level(Level::Warning)
                    .constraint(constraints::completeness("username", Some(0.9))) // Allow 10% nulls
                    .build(),
            )
            .check(
                Check::builder("age_validation")
                    .level(Level::Warning)
                    .constraint(constraints::completeness("age", Some(0.8))) // Allow 20% nulls
                    .build(),
            )
            .check(
                Check::builder("data_volume")
                    .level(Level::Error)
                    .constraint(constraints::has_size(1000, None)) // Exactly 1000 rows
                    .build(),
            )
            .build();

        println!(
            "✅ Validation suite created with {} checks",
            suite.checks().len()
        );
        println!("📊 Running validation with telemetry instrumentation...\n");

        // Run the validation suite - this will generate telemetry spans
        let result = suite.run(&ctx).await?;

        // Display results
        println!("🎯 Validation Results:");

        if result.is_success() {
            println!("✅ All validations passed!");
            println!("Metrics: {:?}", result.metrics());
        } else {
            println!("⚠️  Some validations failed or had warnings:");
            for issue in &result.report().issues {
                println!("  - {}: {}", issue.check_name, issue.message);
            }
        }

        println!("\n📈 Telemetry captured:");
        println!("  Tracing:");
        println!("    - Suite-level span with execution metrics");
        println!("    - Check-level spans for each validation");
        println!("    - Constraint-level spans with detailed results");
        println!("    - Timing information for performance monitoring");

        // Force metrics export by waiting a moment
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Get the exported metrics
        let exported_metrics = metrics_exporter_clone
            .get_finished_metrics()
            .unwrap_or_default();
        println!(
            "  Metrics collected: {} metric families",
            exported_metrics.len()
        );

        if !exported_metrics.is_empty() {
            println!("  Sample captured metrics:");
            for (i, resource_metrics) in exported_metrics.iter().take(3).enumerate() {
                if let Some(scope_metrics) = resource_metrics.scope_metrics().next() {
                    if let Some(metric) = scope_metrics.metrics().next() {
                        println!("    {}. {}: {}", i + 1, metric.name(), metric.description());
                    }
                }
            }
        } else {
            println!("  Metrics types that would be captured:");
            println!("    - Validation duration histogram");
            println!("    - Rows processed counter");
            println!("    - Checks passed/failed counters");
            println!("    - Active validations gauge");
            println!("    - Memory usage gauge");
        }

        println!(
            "    
        Note: Metrics are stored in memory by the InMemory exporter for inspection.
              In production, you would use exporters that send metrics to monitoring
              systems like Prometheus, Jaeger, or cloud services."
        );

        // In a real application, telemetry data would be exported to:
        // - Jaeger for distributed tracing
        // - Prometheus for metrics
        // - Cloud monitoring services (AWS X-Ray, Google Cloud Trace, etc.)

        // Shutdown telemetry
        let _ = provider.shutdown();
        // Note: In a real app, you'd also shut down the meter provider

        println!("\n🏁 Example completed successfully!");
    }

    #[cfg(not(feature = "telemetry"))]
    {
        println!("❌ This example requires the 'telemetry' feature to be enabled.");
        println!("Run with: cargo run --example telemetry_example --features telemetry");

        // Still run a basic validation without telemetry
        let ctx = SessionContext::new();

        // Create simple test data
        ctx.sql("CREATE TABLE test_data AS SELECT 1 as id, 'test' as name")
            .await?;

        let suite = ValidationSuite::builder("basic_validation")
            .check(
                Check::builder("test_check")
                    .constraint(constraints::has_size(1, None))
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

#[cfg(feature = "telemetry")]
mod constraints {
    //! Constraint helper functions for the example

    use term_guard::constraints::*;
    use term_guard::core::ConstraintOptions;

    /// Creates a completeness constraint
    pub fn completeness(column: &str, threshold: Option<f64>) -> CompletenessConstraint {
        CompletenessConstraint::new(
            column,
            ConstraintOptions::new().with_threshold(threshold.unwrap_or(1.0)),
        )
    }

    /// Creates a size constraint  
    pub fn has_size(expected: usize, tolerance: Option<f64>) -> SizeConstraint {
        if let Some(tol) = tolerance {
            let lower = (expected as f64 * (1.0 - tol)) as usize;
            let upper = (expected as f64 * (1.0 + tol)) as usize;
            SizeConstraint::new(Assertion::Between(lower as f64, upper as f64))
        } else {
            SizeConstraint::new(Assertion::Equals(expected as f64))
        }
    }
}

#[cfg(not(feature = "telemetry"))]
mod constraints {
    //! Constraint helper functions for the non-telemetry example

    use std::sync::Arc;
    use term_guard::constraints::*;
    use term_guard::core::Constraint;

    /// Creates a size constraint  
    pub fn has_size(expected: usize, _tolerance: Option<f64>) -> SizeConstraint {
        SizeConstraint::new(Assertion::Equals(expected as f64))
    }
}
