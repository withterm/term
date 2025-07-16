//! Example demonstrating column count validation in Term.

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use std::sync::Arc;
use term_guard::constraints::Assertion;
use term_guard::core::{Check, Level, ValidationSuite};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Column Count Validation Example ===\n");

    // Create sample datasets with different column counts
    let ctx = SessionContext::new();

    // Test 1: Wide table (many columns)
    println!("Test 1: Wide table validation");
    register_wide_table(&ctx).await?;
    validate_wide_table(&ctx).await?;

    // Test 2: Schema evolution scenario
    println!("\nTest 2: Schema evolution validation");
    demonstrate_schema_evolution().await?;

    // Test 3: Dynamic column validation
    println!("\nTest 3: Dynamic column requirements");
    demonstrate_dynamic_validation().await?;

    Ok(())
}

async fn register_wide_table(ctx: &SessionContext) -> Result<(), Box<dyn std::error::Error>> {
    // Create a table with 20 columns
    let fields: Vec<Field> = (0..20)
        .map(|i| Field::new(format!("metric_{i}"), DataType::Int64, true))
        .collect();

    let schema = Arc::new(Schema::new(fields));

    // Create sample data
    let arrays: Vec<Arc<dyn arrow::array::Array>> = (0..20)
        .map(|i| {
            Arc::new(Int64Array::from(vec![
                Some(i as i64 * 100),
                Some(i as i64 * 200),
                Some(i as i64 * 300),
            ])) as Arc<dyn arrow::array::Array>
        })
        .collect();

    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    ctx.register_batch("data", batch)?;

    Ok(())
}

async fn validate_wide_table(ctx: &SessionContext) -> Result<(), Box<dyn std::error::Error>> {
    let suite = ValidationSuite::builder("wide_table_validation")
        .check(
            Check::builder("exact_column_count")
                .level(Level::Error)
                .has_column_count(Assertion::Equals(20.0))
                .build(),
        )
        .check(
            Check::builder("minimum_metrics")
                .level(Level::Warning)
                .has_column_count(Assertion::GreaterThanOrEqual(15.0))
                .build(),
        )
        .check(
            Check::builder("maximum_metrics")
                .level(Level::Warning)
                .has_column_count(Assertion::LessThanOrEqual(25.0))
                .build(),
        )
        .build();

    let results = suite.run(ctx).await?;
    print_results(&results);

    Ok(())
}

async fn demonstrate_schema_evolution() -> Result<(), Box<dyn std::error::Error>> {
    // Simulate schema evolution over time
    let versions = vec![
        ("v1.0", vec!["id", "name", "email"]),
        ("v1.1", vec!["id", "name", "email", "phone"]),
        (
            "v2.0",
            vec!["id", "name", "email", "phone", "address", "created_at"],
        ),
    ];

    for (version, columns) in versions {
        println!("  Schema {version}: {} columns", columns.len());

        let ctx = SessionContext::new();

        // Create schema with specified columns
        let fields: Vec<Field> = columns
            .iter()
            .map(|name| Field::new(name.to_string(), DataType::Utf8, true))
            .collect();

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::new_empty(schema);
        ctx.register_batch("data", batch)?;

        // Validate against expected schema size
        let suite = ValidationSuite::builder(format!("schema_{version}"))
            .check(
                Check::builder("schema_requirements")
                    .level(Level::Error)
                    // Minimum required columns (original schema)
                    .has_column_count(Assertion::GreaterThanOrEqual(3.0))
                    // Maximum allowed columns (prevent schema bloat)
                    .has_column_count(Assertion::LessThanOrEqual(10.0))
                    .build(),
            )
            .build();

        let results = suite.run(&ctx).await?;
        match results {
            term_guard::core::ValidationResult::Success { .. } => {
                println!("    ✅ Schema validation passed");
            }
            term_guard::core::ValidationResult::Failure { .. } => {
                println!("    ❌ Schema validation failed");
            }
        }
    }

    Ok(())
}

async fn demonstrate_dynamic_validation() -> Result<(), Box<dyn std::error::Error>> {
    // Example: Different column requirements based on data type
    let data_types = vec![
        ("user_profile", 8, 12), // 8-12 columns expected
        ("transaction", 15, 20), // 15-20 columns expected
        ("log_entry", 5, 8),     // 5-8 columns expected
    ];

    for (data_type, min_cols, max_cols) in data_types {
        println!("  Validating {data_type} data ({min_cols}-{max_cols} columns expected)");

        let ctx = SessionContext::new();

        // Create a table with a specific number of columns
        let actual_cols = match data_type {
            "user_profile" => 10,
            "transaction" => 18,
            "log_entry" => 6,
            _ => 10,
        };

        let fields: Vec<Field> = (0..actual_cols)
            .map(|i| Field::new(format!("col_{i}"), DataType::Utf8, true))
            .collect();

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::new_empty(schema);
        ctx.register_batch("data", batch)?;

        // Dynamic validation based on data type
        let suite = ValidationSuite::builder(format!("{data_type}_validation"))
            .check(
                Check::builder("column_range")
                    .level(Level::Error)
                    .has_column_count(Assertion::Between(min_cols as f64, max_cols as f64))
                    .build(),
            )
            .build();

        let results = suite.run(&ctx).await?;
        match results {
            term_guard::core::ValidationResult::Success { report, .. } => {
                println!("    ✅ Validation passed - {actual_cols} columns found");
                if let Some(metric) = report.metrics.custom_metrics.get("column_count") {
                    println!("    Metric: {metric:?}");
                }
            }
            term_guard::core::ValidationResult::Failure { report } => {
                println!("    ❌ Validation failed");
                for issue in &report.issues {
                    println!("      - {}", issue.message);
                }
            }
        }
    }

    Ok(())
}

fn print_results(results: &term_guard::core::ValidationResult) {
    match results {
        term_guard::core::ValidationResult::Success { report, .. } => {
            println!("✅ Validation PASSED");
            println!("  Total checks: {}", report.metrics.total_checks);
            println!("  Passed checks: {}", report.metrics.passed_checks);

            if !report.issues.is_empty() {
                println!("\n  Warnings:");
                for issue in &report.issues {
                    println!("    ⚠️  {}: {}", issue.check_name, issue.message);
                }
            }
        }
        term_guard::core::ValidationResult::Failure { report } => {
            println!("❌ Validation FAILED");
            println!("\n  Issues found:");
            for issue in &report.issues {
                println!(
                    "    {} {}: {}",
                    match issue.level {
                        Level::Error => "🔴",
                        Level::Warning => "🟡",
                        Level::Info => "🔵",
                    },
                    issue.check_name,
                    issue.message
                );
            }
        }
    }
}
