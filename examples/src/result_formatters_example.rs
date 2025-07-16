//! Example demonstrating the various result formatters in Term.
//!
//! This example shows how to format validation results in different ways:
//! - JSON format for programmatic consumption
//! - Human-readable format for console output
//! - Markdown format for documentation
//! - Custom formatting configurations

use datafusion::prelude::*;
use std::error::Error;
use term_core::constraints::{Assertion, CompletenessConstraint, SizeConstraint};
use term_core::core::ConstraintOptions;
use term_core::core::{Check, Level, ValidationSuite};
use term_core::formatters::{
    FormatterConfig, HumanFormatter, JsonFormatter, MarkdownFormatter, ResultFormatter,
};

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn Error>> {
    // Create a sample dataset with validation issues
    let ctx = SessionContext::new();

    let sql = r#"
        SELECT 
            ROW_NUMBER() OVER () as id,
            CASE 
                WHEN ROW_NUMBER() OVER () % 5 = 0 THEN NULL 
                ELSE 'user_' || (ROW_NUMBER() OVER ())::VARCHAR 
            END as username,
            CASE 
                WHEN ROW_NUMBER() OVER () % 10 = 0 THEN NULL
                ELSE (ROW_NUMBER() OVER () % 100 + 18)
            END as age,
            (RANDOM() * 100000)::INTEGER as score
        FROM (
            SELECT * FROM generate_series(1, 20) as t(i)
        )
    "#;

    ctx.register_table("data", ctx.sql(sql).await?.into_view())?;

    // Create validation suite that will have some failures
    let suite = ValidationSuite::builder("user_data_validation")
        .description("Example validation with multiple issues")
        .check(
            Check::builder("id_completeness")
                .level(Level::Error)
                .constraint(CompletenessConstraint::new(
                    "id",
                    ConstraintOptions::new().with_threshold(1.0),
                )) // Should pass
                .build(),
        )
        .check(
            Check::builder("username_completeness")
                .level(Level::Warning)
                .constraint(CompletenessConstraint::new(
                    "username",
                    ConstraintOptions::new().with_threshold(0.95),
                )) // Will fail - only 80% complete
                .build(),
        )
        .check(
            Check::builder("age_completeness")
                .level(Level::Warning)
                .constraint(CompletenessConstraint::new(
                    "age",
                    ConstraintOptions::new().with_threshold(0.95),
                )) // Will fail - only 90% complete
                .build(),
        )
        .check(
            Check::builder("data_volume")
                .level(Level::Info)
                .constraint(SizeConstraint::new(Assertion::Between(15.0, 25.0))) // Should pass
                .build(),
        )
        .build();

    println!("🔍 Running validation suite...");
    let result = suite.run(&ctx).await?;

    println!("\n{}", "=".repeat(80));
    println!("RESULT FORMATTING EXAMPLES");
    println!("{}", "=".repeat(80));

    // Example 1: Convenience methods
    println!("\n📋 1. CONVENIENCE METHODS");
    println!("{}", "─".repeat(40));

    println!("\n🔹 Quick JSON output:");
    let json = result.to_json()?;
    println!("{}", &json[..200.min(json.len())]); // Show first 200 chars
    if json.len() > 200 {
        println!("... (truncated)");
    }

    println!("\n🔹 Quick human-readable output:");
    println!("{}", result.to_human()?);

    // Example 2: JSON Formatter with different configurations
    println!("\n📋 2. JSON FORMATTER OPTIONS");
    println!("{}", "─".repeat(40));

    println!("\n🔹 Compact JSON (no pretty printing):");
    let compact_formatter = JsonFormatter::new().with_pretty(false);
    let compact_json = compact_formatter.format(&result)?;
    println!("{}", &compact_json[..150.min(compact_json.len())]);
    if compact_json.len() > 150 {
        println!("... (truncated)");
    }

    println!("\n🔹 Minimal JSON (metrics only):");
    let minimal_config = FormatterConfig::minimal();
    let minimal_formatter = JsonFormatter::with_config(minimal_config);
    let minimal_json = minimal_formatter.format(&result)?;
    println!("{}", minimal_json);

    // Example 3: Human Formatter with different configurations
    println!("\n📋 3. HUMAN FORMATTER OPTIONS");
    println!("{}", "─".repeat(40));

    println!("\n🔹 No colors (suitable for logs):");
    let no_color_config = FormatterConfig::default().with_colors(false);
    let no_color_formatter = HumanFormatter::with_config(no_color_config);
    println!("{}", no_color_formatter.format(&result)?);

    println!("\n🔹 Limited issues (max 1):");
    let limited_config = FormatterConfig::default().with_max_issues(1);
    let limited_formatter = HumanFormatter::with_config(limited_config);
    println!("{}", limited_formatter.format(&result)?);

    println!("\n🔹 CI/CD format:");
    let ci_config = FormatterConfig::ci();
    let ci_formatter = HumanFormatter::with_config(ci_config);
    println!("{}", ci_formatter.format(&result)?);

    // Example 4: Markdown Formatter
    println!("\n📋 4. MARKDOWN FORMATTER");
    println!("{}", "─".repeat(40));

    println!("\n🔹 Standard Markdown:");
    let markdown_formatter = MarkdownFormatter::new();
    println!("{}", markdown_formatter.format(&result)?);

    println!("\n🔹 Markdown with heading level 1:");
    let h1_formatter = MarkdownFormatter::new().with_heading_level(1);
    let h1_output = h1_formatter.format(&result)?;
    println!("{}", &h1_output[..300.min(h1_output.len())]);
    if h1_output.len() > 300 {
        println!("... (truncated)");
    }

    // Example 5: Custom formatter implementation
    println!("\n📋 5. CUSTOM FORMATTER");
    println!("{}", "─".repeat(40));

    struct SummaryFormatter;

    impl ResultFormatter for SummaryFormatter {
        fn format(
            &self,
            result: &term_core::core::ValidationResult,
        ) -> term_core::prelude::Result<String> {
            let report = result.report();
            Ok(format!(
                "🏁 SUMMARY: {} suite {} with {}/{} checks passed ({}ms)",
                report.suite_name,
                if result.is_success() {
                    "PASSED"
                } else {
                    "FAILED"
                },
                report.metrics.passed_checks,
                report.metrics.total_checks,
                report.metrics.execution_time_ms
            ))
        }
    }

    let custom_formatter = SummaryFormatter;
    println!("\n🔹 Custom summary formatter:");
    println!("{}", result.format_with(&custom_formatter)?);

    // Example 6: Configuration comparison
    println!("\n📋 6. CONFIGURATION COMPARISON");
    println!("{}", "─".repeat(40));

    let configs = vec![
        ("Default", FormatterConfig::default()),
        ("Minimal", FormatterConfig::minimal()),
        ("Detailed", FormatterConfig::detailed()),
        ("CI/CD", FormatterConfig::ci()),
    ];

    for (name, config) in configs {
        println!("\n🔹 {} configuration:", name);
        println!("   Metrics: {}", config.include_metrics);
        println!("   Issues: {}", config.include_issues);
        println!("   Colors: {}", config.use_colors);
        println!("   Max Issues: {}", config.max_issues);
        println!("   Timestamps: {}", config.include_timestamps);

        let formatter = HumanFormatter::with_config(config);
        let output = formatter.format(&result)?;
        let lines: Vec<&str> = output.lines().take(5).collect();
        println!("   Preview: {}", lines.join(" | "));
    }

    println!("\n🎉 Formatting examples completed!");
    println!("\n💡 Tips:");
    println!("   - Use JSON format for APIs and programmatic processing");
    println!("   - Use human format for console output and debugging");
    println!("   - Use Markdown format for documentation and reports");
    println!("   - Implement custom formatters for specific use cases");
    println!("   - Configure formatters based on your environment (CI, dev, prod)");

    Ok(())
}
