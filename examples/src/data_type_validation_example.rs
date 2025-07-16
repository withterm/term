//! Example demonstrating data type validation constraints in Term.

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use std::sync::Arc;
use term_guard::constraints::{DataTypeConstraint, DataTypeValidation, NumericValidation};
use term_guard::core::{Check, Level, ValidationSuite};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Data Type Validation Example ===\n");

    // Create a sample dataset with mixed data types
    let ctx = create_sample_context().await?;

    // Create validation suite with data type constraints
    let suite = ValidationSuite::builder("data_type_validation")
        .with_optimizer(true)
        .check(
            Check::builder("user_data_types")
                .level(Level::Error)
                // Validate user_id should be integers
                .has_consistent_data_type("user_id", 0.95)
                // Validate email should match string pattern (all are strings)
                .has_consistent_data_type("email", 1.0)
                // Validate age should be integers
                .constraint(
                    DataTypeConstraint::new(
                        "age",
                        DataTypeValidation::Numeric(NumericValidation::NonNegative),
                    )
                    .unwrap(),
                )
                // Validate created_date should be dates
                .has_consistent_data_type("created_date", 0.95)
                .build(),
        )
        .check(
            Check::builder("transaction_data_types")
                .level(Level::Warning)
                // Validate amount should be numeric (float pattern)
                .has_consistent_data_type("amount", 0.95)
                // Validate is_premium should be boolean
                .has_consistent_data_type("is_premium", 0.9)
                .build(),
        )
        .build();

    // Run validation
    println!("Running data type validation checks...\n");
    let results = suite.run(&ctx).await?;

    // Display results
    match &results {
        term_guard::core::ValidationResult::Success { report, .. } => {
            println!("✅ Validation PASSED");
            println!("Total checks run: {}", report.metrics.total_checks);
            println!("Passed checks: {}", report.metrics.passed_checks);

            if !report.issues.is_empty() {
                println!("\nWarnings:");
                for issue in &report.issues {
                    println!("  ⚠️  {}: {}", issue.check_name, issue.message);
                }
            }
        }
        term_guard::core::ValidationResult::Failure { report } => {
            println!("❌ Validation FAILED");
            println!("\nIssues found:");
            for issue in &report.issues {
                println!(
                    "  {} {}: {}",
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

    // Demonstrate type detection with detailed analysis
    println!("\n=== Detailed Type Analysis ===");
    analyze_column_types(&ctx, "user_id").await?;
    analyze_column_types(&ctx, "amount").await?;
    analyze_column_types(&ctx, "is_premium").await?;

    Ok(())
}

async fn create_sample_context() -> Result<SessionContext, Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();

    // Create sample data with various data types
    let user_data = [
        vec![
            "123",
            "user1@example.com",
            "25",
            "2024-01-15",
            "99.99",
            "true",
        ],
        vec![
            "456",
            "user2@example.com",
            "30",
            "2024-02-20",
            "149.50",
            "false",
        ],
        vec![
            "789",
            "user3@example.com",
            "invalid",
            "2024-03-10",
            "299.00",
            "TRUE",
        ],
        vec![
            "1001",
            "user4@example.com",
            "28",
            "not-a-date",
            "invalid",
            "1",
        ],
        vec![
            "abc",
            "user5@example.com",
            "35",
            "2024-04-05",
            "199.99",
            "0",
        ],
        vec![
            "2002",
            "user6@example.com",
            "40",
            "2024-05-15",
            "89.99",
            "yes",
        ], // Invalid boolean
    ];

    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, false),
        Field::new("age", DataType::Utf8, false),
        Field::new("created_date", DataType::Utf8, false),
        Field::new("amount", DataType::Utf8, false),
        Field::new("is_premium", DataType::Utf8, false),
    ]));

    let mut arrays: Vec<Arc<dyn arrow::array::Array>> = Vec::new();
    for col_idx in 0..6 {
        let values: Vec<Option<&str>> = user_data.iter().map(|row| Some(row[col_idx])).collect();
        arrays.push(Arc::new(StringArray::from(values)));
    }

    let batch = RecordBatch::try_new(schema, arrays)?;
    ctx.register_batch("data", batch)?;

    Ok(ctx)
}

async fn analyze_column_types(
    ctx: &SessionContext,
    column: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\nAnalyzing column '{column}' type distribution:");

    // Count different data type patterns
    let sql = format!(
        "SELECT 
            COUNT(CASE WHEN {column} ~ '^-?\\d+$' THEN 1 END) as integer_count,
            COUNT(CASE WHEN {column} ~ '^-?\\d*\\.?\\d+([eE][+-]?\\d+)?$' THEN 1 END) as float_count,
            COUNT(CASE WHEN {column} ~ '^(true|false|TRUE|FALSE|True|False|0|1)$' THEN 1 END) as boolean_count,
            COUNT(CASE WHEN {column} ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}$' THEN 1 END) as date_count,
            COUNT(*) as total
         FROM data"
    );

    let df = ctx.sql(&sql).await?;
    let batches = df.collect().await?;

    if let Some(batch) = batches.first() {
        let integer_count = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        let float_count = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        let boolean_count = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        let date_count = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        let total = batch
            .column(4)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);

        println!(
            "  - Integer pattern: {integer_count} ({:.1}%)",
            (integer_count as f64 / total as f64) * 100.0
        );
        println!(
            "  - Float pattern: {float_count} ({:.1}%)",
            (float_count as f64 / total as f64) * 100.0
        );
        println!(
            "  - Boolean pattern: {boolean_count} ({:.1}%)",
            (boolean_count as f64 / total as f64) * 100.0
        );
        println!(
            "  - Date pattern: {date_count} ({:.1}%)",
            (date_count as f64 / total as f64) * 100.0
        );
        println!("  - Total values: {total}");
    }

    Ok(())
}
