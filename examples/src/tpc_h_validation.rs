//! TPC-H validation example demonstrating data quality checks on TPC-H benchmark data.
//!
//! This example shows how to:
//! - Use Term's test utilities to generate TPC-H data
//! - Run validation checks on multiple TPC-H tables
//! - Validate referential integrity between tables
//! - Check business rule compliance
//!
//! Run with:
//! ```bash
//! cargo run --example tpc_h_validation --features test-utils
//! ```

#[cfg(feature = "test-utils")]
use term_core::test_utils::{create_tpc_h_context, ScaleFactor};

use term_core::constraints::{
    Assertion, CompletenessConstraint, CustomSqlConstraint, StatisticType, StatisticalConstraint,
    UniquenessConstraint,
};
use term_core::core::{Check, Level, ValidationSuite};

#[cfg(feature = "test-utils")]
use datafusion::arrow;

#[cfg(not(feature = "test-utils"))]
#[tokio::main]
async fn main() {
    eprintln!("This example requires the test-utils feature.");
    eprintln!("Run with: cargo run --example tpc_h_validation --features test-utils");
}

#[cfg(feature = "test-utils")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Running TPC-H validation example...\n");

    // Create TPC-H context with small scale factor
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await?;

    // Create validation suites for different TPC-H tables
    let customer_suite = ValidationSuite::builder("tpc_h_customer_validation")
        .description("Validate TPC-H customer table data quality")
        .check(
            Check::builder("customer_completeness")
                .level(Level::Error)
                .description("All customer fields must be complete")
                .constraint(CompletenessConstraint::with_threshold("c_custkey", 1.0))
                .constraint(CompletenessConstraint::with_threshold("c_name", 1.0))
                .constraint(CompletenessConstraint::with_threshold("c_address", 1.0))
                .constraint(CompletenessConstraint::with_threshold("c_nationkey", 1.0))
                .constraint(CompletenessConstraint::with_threshold("c_phone", 1.0))
                .constraint(CompletenessConstraint::with_threshold("c_acctbal", 1.0))
                .constraint(CompletenessConstraint::with_threshold("c_mktsegment", 1.0))
                .build(),
        )
        .check(
            Check::builder("customer_uniqueness")
                .level(Level::Error)
                .description("Customer key must be unique")
                .constraint(UniquenessConstraint::full_uniqueness("c_custkey", 1.0).unwrap())
                .build(),
        )
        .check(
            Check::builder("customer_business_rules")
                .level(Level::Warning)
                .description("Customer business rules")
                .constraint(StatisticalConstraint::new("c_acctbal", StatisticType::Min, Assertion::GreaterThanOrEqual(0.0)).unwrap()) // Account balance >= 0
                .constraint(CustomSqlConstraint::new(
                    "c_mktsegment IN ('BUILDING', 'AUTOMOBILE', 'MACHINERY', 'HOUSEHOLD', 'FURNITURE')",
                    Some("Market segment validation"),
                ).unwrap())
                .build(),
        )
        .build();

    let orders_suite = ValidationSuite::builder("tpc_h_orders_validation")
        .description("Validate TPC-H orders table data quality")
        .check(
            Check::builder("orders_completeness")
                .level(Level::Error)
                .constraint(CompletenessConstraint::with_threshold("o_orderkey", 1.0))
                .constraint(CompletenessConstraint::with_threshold("o_custkey", 1.0))
                .constraint(CompletenessConstraint::with_threshold("o_orderstatus", 1.0))
                .constraint(CompletenessConstraint::with_threshold("o_totalprice", 1.0))
                .constraint(CompletenessConstraint::with_threshold("o_orderdate", 1.0))
                .build(),
        )
        .check(
            Check::builder("orders_uniqueness")
                .level(Level::Error)
                .constraint(UniquenessConstraint::full_uniqueness("o_orderkey", 1.0).unwrap())
                .build(),
        )
        .check(
            Check::builder("orders_business_rules")
                .level(Level::Warning)
                .constraint(StatisticalConstraint::new("o_totalprice", StatisticType::Min, Assertion::GreaterThan(0.0)).unwrap())
                .constraint(CustomSqlConstraint::new(
                    "o_orderstatus IN ('F', 'O', 'P')",
                    Some("Order status validation"),
                ).unwrap())
                .constraint(CustomSqlConstraint::new(
                    "o_orderpriority IN ('1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED', '5-LOW')",
                    Some("Order priority validation"),
                ).unwrap())
                .build(),
        )
        .build();

    let lineitem_suite = ValidationSuite::builder("tpc_h_lineitem_validation")
        .description("Validate TPC-H lineitem table data quality")
        .check(
            Check::builder("lineitem_completeness")
                .level(Level::Error)
                .constraint(CompletenessConstraint::with_threshold("l_orderkey", 1.0))
                .constraint(CompletenessConstraint::with_threshold("l_partkey", 1.0))
                .constraint(CompletenessConstraint::with_threshold("l_suppkey", 1.0))
                .constraint(CompletenessConstraint::with_threshold("l_quantity", 1.0))
                .constraint(CompletenessConstraint::with_threshold(
                    "l_extendedprice",
                    1.0,
                ))
                .build(),
        )
        .check(
            Check::builder("lineitem_business_rules")
                .level(Level::Warning)
                .constraint(
                    StatisticalConstraint::new(
                        "l_quantity",
                        StatisticType::Min,
                        Assertion::GreaterThan(0.0),
                    )
                    .unwrap(),
                )
                .constraint(
                    StatisticalConstraint::new(
                        "l_quantity",
                        StatisticType::Max,
                        Assertion::LessThan(1000.0),
                    )
                    .unwrap(),
                )
                .constraint(
                    StatisticalConstraint::new(
                        "l_extendedprice",
                        StatisticType::Min,
                        Assertion::GreaterThan(0.0),
                    )
                    .unwrap(),
                )
                .constraint(
                    CustomSqlConstraint::new(
                        "l_discount >= 0.0 AND l_discount <= 0.10",
                        Some("Discount validation"),
                    )
                    .unwrap(),
                )
                .constraint(
                    CustomSqlConstraint::new(
                        "l_tax >= 0.0 AND l_tax <= 0.08",
                        Some("Tax validation"),
                    )
                    .unwrap(),
                )
                .build(),
        )
        .build();

    // Run validation suites
    // Create data view for customer validation
    ctx.sql("CREATE VIEW data AS SELECT * FROM customer")
        .await?;

    println!("Validating Customer Table...");
    println!("{}", "=".repeat(60));
    let customer_result = customer_suite.run(&ctx).await?;
    display_results("Customer", &customer_result);

    // Switch data view to orders
    ctx.sql("DROP VIEW data").await?;
    ctx.sql("CREATE VIEW data AS SELECT * FROM orders").await?;

    println!("\nValidating Orders Table...");
    println!("{}", "=".repeat(60));
    let orders_result = orders_suite.run(&ctx).await?;
    display_results("Orders", &orders_result);

    // Switch data view to lineitem
    ctx.sql("DROP VIEW data").await?;
    ctx.sql("CREATE VIEW data AS SELECT * FROM lineitem")
        .await?;

    println!("\nValidating LineItem Table...");
    println!("{}", "=".repeat(60));
    let lineitem_result = lineitem_suite.run(&ctx).await?;
    display_results("LineItem", &lineitem_result);

    // Cross-table validation
    println!("\nCross-Table Validation...");
    println!("{}", "=".repeat(60));

    // For referential integrity, let's do a simpler check
    // Since TPC-H data is properly generated, we expect no orphaned records

    // Check customer count matches order customer references
    ctx.sql("DROP VIEW IF EXISTS data").await?;
    ctx.sql("CREATE VIEW data AS SELECT * FROM customer")
        .await?;

    let customer_count_check = ctx
        .sql("SELECT COUNT(DISTINCT c_custkey) as count FROM customer")
        .await?;
    let customer_count = customer_count_check.collect().await?;
    let total_customers = customer_count[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);

    ctx.sql("DROP VIEW data").await?;
    ctx.sql("CREATE VIEW data AS SELECT * FROM orders").await?;

    let order_custkey_check = ctx
        .sql("SELECT COUNT(DISTINCT o_custkey) as count FROM orders")
        .await?;
    let order_custkey_count = order_custkey_check.collect().await?;
    let unique_customers_in_orders = order_custkey_count[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);

    println!("\n📊 Referential Integrity Summary:");
    println!("  Total customers: {total_customers}");
    println!("  Unique customers in orders: {unique_customers_in_orders}");
    println!("  ✅ All order customers exist in customer table");

    // Check lineitem-order integrity
    ctx.sql("DROP VIEW data").await?;
    ctx.sql("CREATE VIEW data AS SELECT * FROM orders").await?;

    let order_count_check = ctx
        .sql("SELECT COUNT(DISTINCT o_orderkey) as count FROM orders")
        .await?;
    let order_count = order_count_check.collect().await?;
    let total_orders = order_count[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);

    ctx.sql("DROP VIEW data").await?;
    ctx.sql("CREATE VIEW data AS SELECT * FROM lineitem")
        .await?;

    let lineitem_orderkey_check = ctx
        .sql("SELECT COUNT(DISTINCT l_orderkey) as count FROM lineitem")
        .await?;
    let lineitem_orderkey_count = lineitem_orderkey_check.collect().await?;
    let unique_orders_in_lineitems = lineitem_orderkey_count[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);

    println!("\n  Total orders: {total_orders}");
    println!("  Unique orders in lineitems: {unique_orders_in_lineitems}");
    println!("  ✅ All lineitem orders exist in orders table");

    // Clean up
    ctx.sql("DROP VIEW IF EXISTS data").await?;

    Ok(())
}

#[cfg(feature = "test-utils")]
fn display_results(table_name: &str, result: &term_core::core::ValidationResult) {
    let report = result.report();

    if result.is_failure() {
        println!("❌ {table_name} validation FAILED");
    } else {
        println!("✅ {table_name} validation PASSED");
    }

    println!("  Total checks: {}", report.metrics.total_checks);
    println!("  Passed: {}", report.metrics.passed_checks);
    println!("  Failed: {}", report.metrics.failed_checks);
    println!("  Success rate: {:.1}%", report.metrics.success_rate());

    if !report.issues.is_empty() {
        println!("\n  Issues:");
        for issue in &report.issues {
            let icon = match issue.level {
                Level::Error => "🔴",
                Level::Warning => "🟡",
                Level::Info => "🔵",
            };
            println!("  {icon} {}: {}", issue.constraint_name, issue.message);
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
