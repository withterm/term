//! Example demonstrating how to migrate from Deequ to Term.
//!
//! This example shows:
//! - Side-by-side comparison of Deequ and Term APIs
//! - How to translate common Deequ patterns to Term
//! - Equivalent functionality between the two libraries
//!
//! Run with:
//! ```bash
//! cargo run --example deequ_migration
//! ```

use datafusion::prelude::*;
use term_guard::constraints::{
    Assertion, CompletenessConstraint, ContainmentConstraint, CustomSqlConstraint,
    FormatConstraint, SizeConstraint, StatisticType, StatisticalConstraint, UniquenessConstraint,
};
use term_guard::core::{Check, Level, ValidationSuite};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Deequ to Term Migration Example\n");
    println!("This example shows how to translate common Deequ patterns to Term.\n");

    // Create sample data
    let ctx = create_sample_data().await?;

    println!("=== 1. Basic Validation Suite ===\n");
    basic_validation_example(&ctx).await?;

    println!("\n=== 2. Constraint Suggestions (Profiling) ===\n");
    constraint_suggestions_example(&ctx).await?;

    println!("\n=== 3. Anomaly Detection ===\n");
    anomaly_detection_example(&ctx).await?;

    println!("\n=== 4. Custom Assertions ===\n");
    custom_assertions_example(&ctx).await?;

    Ok(())
}

async fn create_sample_data() -> Result<SessionContext, Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();

    // Create sales data similar to what you might use with Deequ
    let csv_data = r#"transaction_id,product_id,quantity,price,discount,total,customer_type,region
1001,P123,2,50.00,0.10,90.00,Premium,North
1002,P456,1,75.50,0.00,75.50,Regular,South
1003,P789,3,25.00,0.15,63.75,Premium,East
1004,P123,1,50.00,0.05,47.50,Regular,West
1005,P456,2,75.50,0.20,120.80,Premium,North
1006,,1,30.00,0.00,30.00,Regular,South
1007,P789,4,25.00,0.25,75.00,Premium,East
1008,P123,2,50.00,0.00,100.00,Regular,North
1009,P456,1,75.50,0.10,67.95,Premium,South
1010,P789,2,25.00,0.05,47.50,Regular,West"#;

    let temp_file = std::env::temp_dir().join("sales_data.csv");
    std::fs::write(&temp_file, csv_data)?;

    ctx.register_csv(
        "data",
        temp_file.to_str().unwrap(),
        CsvReadOptions::default(),
    )
    .await?;

    std::fs::remove_file(&temp_file).ok();

    Ok(ctx)
}

async fn basic_validation_example(ctx: &SessionContext) -> Result<(), Box<dyn std::error::Error>> {
    println!("In Deequ (Scala):");
    println!("```scala");
    println!("val verificationResult = VerificationSuite()");
    println!("  .onData(df)");
    println!("  .addCheck(");
    println!("    Check(CheckLevel.Error, \"Review Check\")");
    println!("      .hasSize(_ >= 10)");
    println!("      .isComplete(\"transaction_id\")");
    println!("      .isUnique(\"transaction_id\")");
    println!("      .isComplete(\"product_id\", _ >= 0.9)");
    println!("      .isNonNegative(\"quantity\")");
    println!("      .isPositive(\"total\"))");
    println!("  .run()");
    println!("```\n");

    println!("In Term (Rust):");
    println!("```rust");
    println!("let suite = ValidationSuite::builder(\"sales_validation\")");
    println!("    .check(");
    println!("        Check::builder(\"review_check\")");
    println!("            .level(Level::Error)");
    println!("            .constraint(SizeConstraint::at_least(10))");
    println!(
        "            .constraint(CompletenessConstraint::with_threshold(\"transaction_id\", 1.0))"
    );
    println!("            .constraint(UniquenessConstraint::new(\"transaction_id\"))");
    println!(
        "            .constraint(CompletenessConstraint::with_threshold(\"product_id\", 0.9))"
    );
    println!("            .constraint(StatisticalConstraint::new(\"quantity\", StatisticType::Min, Assertion::GreaterThanOrEqual(0.0)))");
    println!("            .constraint(StatisticalConstraint::new(\"total\", StatisticType::Min, Assertion::GreaterThan(0.0)))");
    println!("            .build(),");
    println!("    )");
    println!("    .build();");
    println!("```\n");

    // Run the Term version
    let suite = ValidationSuite::builder("sales_validation")
        .check(
            Check::builder("review_check")
                .level(Level::Error)
                .constraint(SizeConstraint::new(Assertion::GreaterThanOrEqual(10.0)))
                .constraint(CompletenessConstraint::with_threshold(
                    "transaction_id",
                    1.0,
                ))
                .constraint(UniquenessConstraint::full_uniqueness("transaction_id", 1.0).unwrap())
                .constraint(CompletenessConstraint::with_threshold("product_id", 0.9))
                .constraint(
                    StatisticalConstraint::new(
                        "quantity",
                        StatisticType::Min,
                        Assertion::GreaterThanOrEqual(0.0),
                    )
                    .unwrap(),
                )
                .constraint(
                    StatisticalConstraint::new(
                        "total",
                        StatisticType::Min,
                        Assertion::GreaterThan(0.0),
                    )
                    .unwrap(),
                )
                .build(),
        )
        .build();

    let result = suite.run(ctx).await?;
    display_validation_result(&result);

    Ok(())
}

async fn constraint_suggestions_example(
    ctx: &SessionContext,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("In Deequ (Scala):");
    println!("```scala");
    println!("val suggestionResult = ConstraintSuggestionRunner()");
    println!("  .onData(df)");
    println!("  .addConstraintRule(CompleteIfCompleteRule())");
    println!("  .addConstraintRule(RetainTypeRule())");
    println!("  .addConstraintRule(UniqueIfApproximatelyUniqueRule())");
    println!("  .run()");
    println!("```\n");

    println!("In Term (Rust):");
    println!("```rust");
    println!("// Term focuses on explicit constraint definition");
    println!("// For profiling, analyze data first:");
    println!("let completeness_check = Check::builder(\"completeness_profile\")");
    println!("    .level(Level::Warning)");
    println!("    .constraint(CompletenessConstraint::with_threshold(\"transaction_id\", 1.0))");
    println!("    .constraint(CompletenessConstraint::with_threshold(\"product_id\", 1.0))");
    println!("    .constraint(CompletenessConstraint::with_threshold(\"customer_type\", 1.0))");
    println!("    .build();");
    println!("```\n");

    // Run profiling checks
    let profiling_suite = ValidationSuite::builder("data_profiling")
        .check(
            Check::builder("completeness_profile")
                .level(Level::Warning)
                .constraint(CompletenessConstraint::with_threshold(
                    "transaction_id",
                    1.0,
                ))
                .constraint(CompletenessConstraint::with_threshold("product_id", 1.0))
                .constraint(CompletenessConstraint::with_threshold("customer_type", 1.0))
                .build(),
        )
        .check(
            Check::builder("uniqueness_profile")
                .level(Level::Warning)
                .constraint(UniquenessConstraint::full_uniqueness("transaction_id", 1.0).unwrap())
                .constraint(UniquenessConstraint::full_uniqueness("product_id", 1.0).unwrap())
                .build(),
        )
        .build();

    let result = profiling_suite.run(ctx).await?;
    display_validation_result(&result);

    Ok(())
}

async fn anomaly_detection_example(ctx: &SessionContext) -> Result<(), Box<dyn std::error::Error>> {
    println!("In Deequ (Scala):");
    println!("```scala");
    println!("val verificationResult = VerificationSuite()");
    println!("  .onData(df)");
    println!("  .addAnomalyCheck(");
    println!("    RelativeRateOfChangeStrategy(maxRateIncrease = Some(2.0)),");
    println!("    Size())");
    println!("  .run()");
    println!("```\n");

    println!("In Term (Rust):");
    println!("```rust");
    println!("// Term uses explicit range checks for anomaly detection");
    println!("let anomaly_check = Check::builder(\"anomaly_detection\")");
    println!("    .level(Level::Warning)");
    println!("    .constraint(SizeConstraint::between(8, 15))");
    println!("    .constraint(StatisticalConstraint::new(\"total\", StatisticType::Mean, Assertion::Between(50.0, 100.0)))");
    println!("    .constraint(StatisticalConstraint::new(\"price\", StatisticType::StandardDeviation, Assertion::LessThanOrEqual(30.0)))");
    println!("    .build();");
    println!("```\n");

    // Run anomaly detection
    let anomaly_suite = ValidationSuite::builder("anomaly_detection")
        .check(
            Check::builder("data_anomalies")
                .level(Level::Warning)
                .constraint(SizeConstraint::new(Assertion::Between(8.0, 15.0)))
                .constraint(
                    StatisticalConstraint::new(
                        "total",
                        StatisticType::Mean,
                        Assertion::Between(50.0, 100.0),
                    )
                    .unwrap(),
                )
                .constraint(
                    StatisticalConstraint::new(
                        "price",
                        StatisticType::StandardDeviation,
                        Assertion::LessThanOrEqual(30.0),
                    )
                    .unwrap(),
                )
                .build(),
        )
        .build();

    let result = anomaly_suite.run(ctx).await?;
    display_validation_result(&result);

    Ok(())
}

async fn custom_assertions_example(ctx: &SessionContext) -> Result<(), Box<dyn std::error::Error>> {
    println!("In Deequ (Scala):");
    println!("```scala");
    println!("val verificationResult = VerificationSuite()");
    println!("  .onData(df)");
    println!("  .addCheck(");
    println!("    Check(CheckLevel.Error, \"Custom Check\")");
    println!("      .satisfies(\"total = quantity * price * (1 - discount)\", ");
    println!("                 \"Total calculation check\"))");
    println!("  .run()");
    println!("```\n");

    println!("In Term (Rust):");
    println!("```rust");
    println!("let custom_check = Check::builder(\"custom_validations\")");
    println!("    .level(Level::Error)");
    println!("    .constraint(ComplianceConstraint::new(");
    println!("        \"ABS(total - (quantity * price * (1 - discount))) < 0.01\",");
    println!("        \"Total calculation must be correct\",");
    println!("    ))");
    println!("    .constraint(PatternMatchConstraint::new(");
    println!("        \"product_id\",");
    println!("        r\"^P\\\\d{{3}}$\",");
    println!("        \"Product ID must match pattern P###\",");
    println!("    ))");
    println!("    .build();");
    println!("```\n");

    // Run custom validations
    let custom_suite = ValidationSuite::builder("custom_validations")
        .check(
            Check::builder("business_rules")
                .level(Level::Error)
                .constraint(CustomSqlConstraint::new(
                    "SELECT COUNT(*) FROM data WHERE ABS(total - (quantity * price * (1 - discount))) >= 0.01 = 0",
                    Some("Total calculation check"),
                ).unwrap())
                .constraint(FormatConstraint::regex(
                    "product_id",
                    r"^P\d{3}$",
                    0.9, // 90% of values should match the pattern
                ).unwrap())
                .constraint(ContainmentConstraint::new(
                    "customer_type",
                    vec!["Premium", "Regular"],
                ))
                .constraint(ContainmentConstraint::new(
                    "region",
                    vec!["North", "South", "East", "West"],
                ))
                .build(),
        )
        .build();

    let result = custom_suite.run(ctx).await?;
    display_validation_result(&result);

    Ok(())
}

fn display_validation_result(result: &term_guard::core::ValidationResult) {
    let report = result.report();

    println!(
        "Result: {}",
        if result.is_failure() {
            "❌ FAILED"
        } else {
            "✅ PASSED"
        }
    );
    println!("  Checks run: {}", report.metrics.total_checks);
    println!("  Passed: {}", report.metrics.passed_checks);
    println!("  Failed: {}", report.metrics.failed_checks);

    if !report.issues.is_empty() {
        println!("\n  Issues:");
        for issue in &report.issues {
            println!("    - {}: {}", issue.constraint_name, issue.message);
            if let Some(metric) = issue.metric {
                println!("      Metric: {metric:.4}");
            }
        }
    }

    if !report.metrics.custom_metrics.is_empty() {
        println!("\n  Metrics:");
        for (name, value) in &report.metrics.custom_metrics {
            println!("    {name}: {value:.4}");
        }
    }
}
