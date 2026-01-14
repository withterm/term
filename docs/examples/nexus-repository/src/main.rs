//! Nexus Repository Example - Demonstrating metrics persistence with Term Nexus
//!
//! This example shows how to:
//! 1. Connect to Term Nexus and verify connectivity
//! 2. Run validation checks and store metrics with tags
//! 3. Query historical metrics for trend analysis

use anyhow::Result;
use chrono::Utc;
use datafusion::prelude::SessionContext;
use std::env;
use term_guard::analyzers::context::AnalyzerContext;
use term_guard::analyzers::types::MetricValue;
use term_guard::constraints::Assertion;
use term_guard::core::{Check, ConstraintOptions, Level, ValidationResult, ValidationSuite};
use term_guard::nexus::{NexusConfig, NexusRepository};
use term_guard::repository::{MetricsRepository, ResultKey};
use term_guard::sources::{CsvSource, DataSource};

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Term Nexus Repository Example ===\n");

    // Step 1: Configure the Nexus connection
    let api_key = env::var("TERM_API_KEY").unwrap_or_else(|_| "demo-api-key".to_string());

    let config = NexusConfig::new(&api_key)
        .with_endpoint("http://localhost:8080")
        .with_buffer_size(100)
        .with_batch_size(10);

    println!("Connecting to Term Nexus...");

    // Step 2: Create repository and verify connectivity
    let repository = NexusRepository::new(config)?;

    match repository.health_check().await {
        Ok(health) => {
            println!("Connected to Term Nexus v{}", health.version);
        }
        Err(e) => {
            eprintln!("Failed to connect to Nexus: {}", e);
            eprintln!("\nMake sure the Nexus API is running at http://localhost:8080");
            return Ok(());
        }
    }
    println!();

    // Step 3: Load sample data
    println!("Loading item data...");
    let ctx = SessionContext::new();

    let source = CsvSource::new("data/items.csv")?;
    source.register(&ctx, "items").await?;
    println!("Loaded items table\n");

    // Step 4: Create validation suite
    let suite = ValidationSuite::builder("item_quality_checks")
        .description("Data quality checks for item inventory")
        .table_name("items")
        .check(
            Check::builder("completeness")
                .level(Level::Error)
                .completeness("id", ConstraintOptions::new().with_threshold(1.0))
                .completeness("name", ConstraintOptions::new().with_threshold(1.0))
                .completeness("price", ConstraintOptions::new().with_threshold(1.0))
                .build(),
        )
        .check(
            Check::builder("validity")
                .level(Level::Error)
                .has_min("price", Assertion::GreaterThanOrEqual(0.01))
                .has_min("quantity", Assertion::GreaterThanOrEqual(0.0))
                .build(),
        )
        .check(
            Check::builder("uniqueness")
                .level(Level::Error)
                .validates_uniqueness(vec!["id"], 1.0)
                .build(),
        )
        .build();

    // Step 5: Run validation
    println!("Running validation checks...");
    let results = suite.run(&ctx).await?;

    // Step 6: Display results
    let (passed, total, report) = match &results {
        ValidationResult::Success { metrics, report } => {
            (metrics.passed_checks, metrics.total_checks, report)
        }
        ValidationResult::Failure { report } => {
            (report.metrics.passed_checks, report.metrics.total_checks, report)
        }
    };

    println!(
        "\nValidation complete: {}/{} checks passed\n",
        passed, total
    );

    for issue in &report.issues {
        let icon = match issue.level {
            Level::Error => "X",
            Level::Warning => "!",
            Level::Info => "i",
        };
        println!("[{}] {}: {}", icon, issue.check_name, issue.message);
        if let Some(metric) = issue.metric {
            println!("    metric: {:.2}", metric);
        }
    }

    // Step 7: Create result key with tags for this validation run
    let result_key = ResultKey::new(Utc::now().timestamp_millis())
        .with_tag("environment", "development")
        .with_tag("pipeline", "daily-inventory")
        .with_tag("dataset", "items");

    println!("\n--- Storing Metrics to Nexus ---");
    println!("Result Key: {}", result_key.timestamp);
    println!("Tags: {:?}", result_key.tags);

    // Step 8: Convert results to AnalyzerContext and save
    let mut context = AnalyzerContext::with_dataset("items");

    // Store summary metrics
    context.store_metric(
        "validation.passed_checks",
        MetricValue::Long(passed as i64),
    );
    context.store_metric(
        "validation.total_checks",
        MetricValue::Long(total as i64),
    );
    context.store_metric(
        "validation.success_rate",
        MetricValue::Double(if total > 0 {
            (passed as f64 / total as f64) * 100.0
        } else {
            100.0
        }),
    );

    // Store issue count by level
    let error_count = report.issues.iter().filter(|i| i.level == Level::Error).count();
    let warning_count = report.issues.iter().filter(|i| i.level == Level::Warning).count();
    context.store_metric("validation.error_count", MetricValue::Long(error_count as i64));
    context.store_metric("validation.warning_count", MetricValue::Long(warning_count as i64));

    // Save to Nexus
    repository.save(result_key.clone(), context).await?;
    println!("Metrics queued for upload");

    // Force flush to ensure metrics are sent
    repository.flush().await?;
    println!("Metrics uploaded to Nexus\n");

    // Step 9: Graceful shutdown
    let stats = repository.shutdown().await?;
    if let Some(s) = stats {
        println!(
            "Worker stats: {} uploaded, {} failed",
            s.metrics_uploaded, s.metrics_failed
        );
    }

    println!("\nExample complete! Metrics are now stored in Term Nexus.");
    println!("You can query them using the Nexus API or run this example again");
    println!("to see historical comparison.");

    Ok(())
}
