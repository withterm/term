//! Example demonstrating grouped metrics computation.
//!
//! This example shows how to use Term's grouped analysis capabilities
//! to compute metrics separately for each distinct value in grouping columns,
//! similar to Deequ's grouped analysis feature.

use datafusion::arrow::array::{Float64Array, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use std::sync::Arc;
use term_guard::analyzers::AnalyzerResult;
use term_guard::analyzers::{
    basic::CompletenessAnalyzer, AnalysisRunner, GroupedAnalyzer, GroupingConfig,
};

#[tokio::main]
async fn main() -> AnalyzerResult<()> {
    // Create sample e-commerce data with regions and categories
    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("product_id", DataType::UInt64, false),
        Field::new("price", DataType::Float64, true),
        Field::new("quantity", DataType::UInt64, true),
        Field::new("customer_email", DataType::Utf8, true),
    ]));

    // Sample data with different completeness patterns per region/category
    let regions = StringArray::from(vec![
        "US", "US", "US", "US", "EU", "EU", "EU", "EU", "Asia", "Asia", "Asia", "Asia",
    ]);

    let categories = StringArray::from(vec![
        "Electronics",
        "Electronics",
        "Clothing",
        "Clothing",
        "Electronics",
        "Electronics",
        "Clothing",
        "Clothing",
        "Electronics",
        "Electronics",
        "Clothing",
        "Clothing",
    ]);

    let product_ids = UInt64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);

    // Price has varying completeness by region
    let prices = Float64Array::from(vec![
        Some(99.99),
        Some(149.99),
        Some(29.99),
        Some(39.99), // US: 100% complete
        Some(89.99),
        None,
        Some(24.99),
        None, // EU: 50% complete
        None,
        None,
        Some(19.99),
        Some(34.99), // Asia: 50% complete
    ]);

    // Quantity has varying completeness by category
    let quantities = UInt64Array::from(vec![
        Some(10),
        Some(5),
        None,
        None, // Electronics more complete
        Some(20),
        Some(15),
        None,
        None,
        Some(30),
        Some(25),
        Some(40),
        Some(35),
    ]);

    // Customer email has different patterns
    let emails = StringArray::from(vec![
        Some("user1@example.com"),
        Some("user2@example.com"),
        None,
        None,
        None,
        None,
        Some("user5@example.eu"),
        Some("user6@example.eu"),
        Some("user9@example.asia"),
        None,
        Some("user11@example.asia"),
        None,
    ]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(regions),
            Arc::new(categories),
            Arc::new(product_ids),
            Arc::new(prices),
            Arc::new(quantities),
            Arc::new(emails),
        ],
    )?;

    // Create DataFusion context
    let ctx = SessionContext::new();
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("sales_data", Arc::new(table))?;

    println!("🔍 Grouped Metrics Analysis Example\n");
    println!("{}", "=".repeat(60));

    // Example 1: Completeness by region
    println!("\n📊 Example 1: Price completeness grouped by region");
    println!("{}", "-".repeat(40));

    let price_by_region = CompletenessAnalyzer::new("price")
        .with_grouping(GroupingConfig::new(vec!["region".to_string()]));

    let runner = AnalysisRunner::new().add(price_by_region);
    let results = runner.run(&ctx).await?;

    if let Some(metric) = results.get_metric("price_completeness_grouped_by_region") {
        println!("Results: {metric:#?}");
    }

    // Example 2: Completeness by region AND category
    println!("\n📊 Example 2: Quantity completeness grouped by region and category");
    println!("{}", "-".repeat(40));

    let quantity_by_region_category = CompletenessAnalyzer::new("quantity").with_grouping(
        GroupingConfig::new(vec!["region".to_string(), "category".to_string()]).with_max_groups(20), // Limit for memory management
    );

    let runner = AnalysisRunner::new().add(quantity_by_region_category);
    let results = runner.run(&ctx).await?;

    if let Some(metric) = results.get_metric("quantity_completeness_grouped_by_region_category") {
        println!("Results: {metric:#?}");
    }

    // Example 3: Email completeness with overall metric
    println!("\n📊 Example 3: Email completeness by category (with overall)");
    println!("{}", "-".repeat(40));

    let email_by_category = CompletenessAnalyzer::new("customer_email").with_grouping(
        GroupingConfig::new(vec!["category".to_string()]).with_overall(true), // Include overall metric across all groups
    );

    let runner = AnalysisRunner::new().add(email_by_category);
    let results = runner.run(&ctx).await?;

    if let Some(metric) = results.get_metric("customer_email_completeness_grouped_by_category") {
        println!("Results: {metric:#?}");
    }

    // Example 4: High-cardinality grouping with limit
    println!("\n📊 Example 4: Handling high-cardinality groups");
    println!("{}", "-".repeat(40));

    // In real scenarios, you might group by customer_id or similar high-cardinality columns
    let high_cardinality = CompletenessAnalyzer::new("price").with_grouping(
        GroupingConfig::new(vec!["product_id".to_string()]).with_max_groups(5), // Only keep top 5 groups
    );

    let runner = AnalysisRunner::new().add(high_cardinality);
    let results = runner.run(&ctx).await?;

    if let Some(metric) = results.get_metric("price_completeness_grouped_by_product_id") {
        println!("Results (limited to top 5): {metric:#?}");
    }

    println!("\n✅ Grouped metrics analysis complete!");
    println!("\n💡 Use cases for grouped metrics:");
    println!("  - Monitor data quality per geographic region");
    println!("  - Track completeness by product category");
    println!("  - Analyze patterns across customer segments");
    println!("  - Detect quality issues in specific data slices");

    Ok(())
}
