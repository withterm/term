//! Basic validation example demonstrating Term's core functionality.
//!
//! This example shows how to:
//! - Create a validation suite with multiple checks
//! - Run completeness, uniqueness, and statistical constraints
//! - Interpret validation results
//!
//! Run with:
//! ```bash
//! cargo run --example basic_validation
//! ```

use datafusion::prelude::*;
use term_core::constraints::{
    Assertion, CompletenessConstraint, StatisticType, StatisticalConstraint, UniquenessConstraint,
};
use term_core::core::{Check, Level, ValidationSuite};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a DataFusion session context
    let ctx = SessionContext::new();

    // Create sample data
    let csv_data = r#"customer_id,name,email,age,registration_date,last_purchase_amount
1,Alice Johnson,alice@example.com,28,2023-01-15,150.50
2,Bob Smith,bob@example.com,35,2023-02-20,200.00
3,Carol Davis,carol@example.com,42,2023-03-10,75.25
4,David Wilson,david@example.com,31,2023-04-05,300.00
5,Eve Brown,eve@example.com,26,2023-05-12,125.75
6,Frank Miller,,38,2023-06-18,180.00
7,Grace Lee,grace@example.com,29,2023-07-22,
8,Henry Taylor,henry@example.com,,2023-08-30,220.50
9,Iris Martinez,iris@example.com,33,2023-09-14,195.25
10,Jack Anderson,jack@example.com,45,2023-10-25,400.00"#;

    // Write to a temporary file
    let temp_dir = std::env::temp_dir();
    let file_path = temp_dir.join("customers.csv");
    std::fs::write(&file_path, csv_data)?;

    // Register the CSV file as a table
    ctx.register_csv(
        "data",
        file_path.to_str().unwrap(),
        CsvReadOptions::default(),
    )
    .await?;

    println!("Running basic validation example...\n");

    // Create a validation suite
    let suite = ValidationSuite::builder("customer_data_quality")
        .description("Validate customer data quality")
        .check(
            Check::builder("completeness_checks")
                .level(Level::Error)
                .description("Ensure critical fields are complete")
                .constraint(CompletenessConstraint::with_threshold("customer_id", 1.0))
                .constraint(CompletenessConstraint::with_threshold("name", 1.0))
                .constraint(CompletenessConstraint::with_threshold("email", 0.9)) // Allow 10% missing emails
                .constraint(CompletenessConstraint::with_threshold("age", 0.8)) // Allow 20% missing ages
                .build(),
        )
        .check(
            Check::builder("uniqueness_checks")
                .level(Level::Error)
                .description("Ensure unique identifiers")
                .constraint(UniquenessConstraint::full_uniqueness("customer_id", 1.0).unwrap())
                .constraint(UniquenessConstraint::full_uniqueness("email", 1.0).unwrap())
                .build(),
        )
        .check(
            Check::builder("statistical_checks")
                .level(Level::Warning)
                .description("Validate statistical properties")
                .constraint(
                    StatisticalConstraint::new(
                        "age",
                        StatisticType::Min,
                        Assertion::GreaterThan(0.0),
                    )
                    .unwrap(),
                )
                .constraint(
                    StatisticalConstraint::new(
                        "age",
                        StatisticType::Max,
                        Assertion::LessThan(120.0),
                    )
                    .unwrap(),
                )
                .constraint(
                    StatisticalConstraint::new(
                        "last_purchase_amount",
                        StatisticType::Mean,
                        Assertion::GreaterThan(0.0),
                    )
                    .unwrap(),
                )
                .build(),
        )
        .build();

    // Run the validation
    let result = suite.run(&ctx).await?;

    // Display results
    println!("Validation Suite: {}", suite.name());
    if let Some(desc) = suite.description() {
        println!("Description: {}", desc);
    }
    println!("\n{}", "=".repeat(60));

    if result.is_failure() {
        println!("❌ Validation FAILED\n");
    } else {
        println!("✅ Validation PASSED\n");
    }

    // Display metrics
    let report = result.report();
    println!("Summary:");
    println!("  Total checks: {}", report.metrics.total_checks);
    println!("  Passed: {}", report.metrics.passed_checks);
    println!("  Failed: {}", report.metrics.failed_checks);
    println!("  Success rate: {:.1}%", report.metrics.success_rate());
    println!("  Execution time: {}ms", report.metrics.execution_time_ms);

    // Display issues if any
    if !report.issues.is_empty() {
        println!("\nIssues found:");
        for issue in &report.issues {
            let icon = match issue.level {
                Level::Error => "🔴",
                Level::Warning => "🟡",
                Level::Info => "🔵",
            };
            println!(
                "{} [{}] {}.{}: {}",
                icon, issue.level, issue.check_name, issue.constraint_name, issue.message
            );
            if let Some(metric) = issue.metric {
                println!("    Metric value: {:.4}", metric);
            }
        }
    }

    // Display custom metrics
    if !report.metrics.custom_metrics.is_empty() {
        println!("\nCustom Metrics:");
        for (name, value) in &report.metrics.custom_metrics {
            println!("  {}: {:.4}", name, value);
        }
    }

    // Clean up
    std::fs::remove_file(&file_path).ok();

    Ok(())
}
