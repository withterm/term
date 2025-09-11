//! Basic validation example demonstrating Term's core features

use term::core::{ValidationSuite, Check, ValidationRunner, ConstraintStatus};
use term::sources::{DataSource, CsvSource};
use datafusion::prelude::SessionContext;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Term Basic Validation Example ===\n");
    
    // Step 1: Create a DataFusion context
    let ctx = SessionContext::new();
    
    // Step 2: Register the CSV data source
    println!("Loading customer data...");
    let source = CsvSource::builder()
        .path("data/customers.csv")
        .has_header(true)
        .infer_schema(true)
        .build()?;
    
    source.register(&ctx, "customers").await?;
    println!("✓ Data loaded successfully\n");
    
    // Step 3: Create validation checks
    println!("Creating validation suite...");
    
    let suite = ValidationSuite::builder("customer_validation")
        .description("Ensure customer data quality standards")
        
        // Check 1: Critical fields must be complete
        .add_check(
            Check::new("critical_fields")
                .is_complete("customer_id")
                .is_complete("email")
                .is_complete("created_date")
        )
        
        // Check 2: IDs must be unique
        .add_check(
            Check::new("unique_identifiers")
                .is_unique("customer_id")
                .is_unique("email")
        )
        
        // Check 3: Email format validation
        .add_check(
            Check::new("email_format")
                .has_pattern("email", r"^[^@]+@[^@]+\.[^@]+$")
                .with_threshold(0.99)  // 99% must be valid
        )
        
        // Check 4: Age constraints
        .add_check(
            Check::new("age_validation")
                .is_non_negative("age")
                .has_min("age", 18.0)
                .has_max("age", 120.0)
        )
        
        // Check 5: Optional fields quality
        .add_check(
            Check::new("optional_fields")
                .has_completeness("phone", 0.7)  // At least 70% have phone
                .has_completeness("address", 0.5) // At least 50% have address
        )
        
        .build();
    
    println!("✓ Suite created with {} checks\n", suite.check_count());
    
    // Step 4: Run the validation
    println!("Running validation checks...");
    let runner = ValidationRunner::new();
    let results = runner.run(&suite, &ctx).await?;
    
    // Step 5: Process and display results
    println!("\n=== Validation Results ===\n");
    
    let total_checks = results.check_results().len();
    let passed_checks = results
        .check_results()
        .iter()
        .filter(|r| r.status() == &ConstraintStatus::Success)
        .count();
    
    println!("Summary: {}/{} checks passed\n", passed_checks, total_checks);
    
    // Display detailed results for each check
    for result in results.check_results() {
        let status_icon = match result.status() {
            ConstraintStatus::Success => "✅",
            ConstraintStatus::Failure => "❌",
            ConstraintStatus::Warning => "⚠️",
        };
        
        println!("{} Check: {}", status_icon, result.check_name());
        
        if let Some(message) = result.message() {
            println!("   Message: {}", message);
        }
        
        if let Some(metric) = result.metric() {
            println!("   Metric: {:.2}", metric);
        }
        
        // Show individual constraint results
        for constraint_result in result.constraint_results() {
            let constraint_icon = match constraint_result.status {
                ConstraintStatus::Success => "  ✓",
                ConstraintStatus::Failure => "  ✗",
                ConstraintStatus::Warning => "  ⚠",
            };
            
            println!("{} {}", 
                constraint_icon, 
                constraint_result.constraint_name
            );
            
            if let Some(metric) = constraint_result.metric {
                println!("     Value: {:.2}", metric);
            }
        }
        println!();
    }
    
    // Step 6: Determine overall status
    if results.all_checks_passed() {
        println!("🎉 All validation checks passed! Data quality is good.");
    } else {
        println!("⚠️  Some validation checks failed. Please review the data.");
        
        // List failed checks for easy reference
        let failed_checks: Vec<_> = results
            .check_results()
            .iter()
            .filter(|r| r.status() == &ConstraintStatus::Failure)
            .map(|r| r.check_name())
            .collect();
        
        if !failed_checks.is_empty() {
            println!("\nFailed checks:");
            for check in failed_checks {
                println!("  - {}", check);
            }
        }
    }
    
    Ok(())
}