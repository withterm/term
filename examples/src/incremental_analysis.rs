//! Example demonstrating incremental analysis for partitioned datasets.
//!
//! This example shows how to use Term's incremental computation framework
//! to efficiently analyze growing datasets without reprocessing historical data.

use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use std::sync::Arc;
use tempfile::TempDir;
use term_guard::analyzers::basic::{CompletenessAnalyzer, MeanAnalyzer, SizeAnalyzer};
use term_guard::analyzers::incremental::{
    FileSystemStateStore, IncrementalAnalysisRunner, IncrementalConfig,
};
use term_guard::analyzers::MetricValue;
use term_guard::core::{ValidationContext, CURRENT_CONTEXT};

/// Simulates loading a daily partition of sales data
async fn load_sales_partition(date: &str) -> SessionContext {
    let ctx = SessionContext::new();

    // Simulate different data for different dates
    let (orders, amounts, regions) = match date {
        "2024-01-01" => (
            vec![101, 102, 103],
            vec![Some(150.0), Some(200.0), Some(175.0)],
            vec![Some("US"), Some("EU"), Some("US")],
        ),
        "2024-01-02" => (
            vec![104, 105, 106, 107],
            vec![Some(300.0), None, Some(425.0), Some(190.0)],
            vec![Some("APAC"), Some("US"), Some("EU"), Some("US")],
        ),
        "2024-01-03" => (
            vec![108, 109, 110],
            vec![Some(250.0), Some(180.0), Some(320.0)],
            vec![Some("EU"), Some("APAC"), Some("US")],
        ),
        _ => (vec![], vec![], vec![]),
    };

    if !orders.is_empty() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("amount", DataType::Float64, true),
            Field::new("region", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(orders)),
                Arc::new(Float64Array::from(amounts)),
                Arc::new(StringArray::from(
                    regions
                        .into_iter()
                        .map(|s| s.map(String::from))
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();

        ctx.register_batch("sales", batch).unwrap();
    }

    ctx
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for logging
    tracing_subscriber::fmt::init();

    println!("=== Term Incremental Analysis Example ===\n");

    // Create a temporary directory for state storage
    let state_dir = TempDir::new()?;
    println!("State storage directory: {:?}\n", state_dir.path());

    // Configure incremental analysis
    let config = IncrementalConfig {
        fail_fast: false,
        save_empty_states: false,
        max_merge_batch_size: 10,
    };

    // Create state store and runner
    let state_store = FileSystemStateStore::new(state_dir.path())?;
    let runner = IncrementalAnalysisRunner::with_config(Box::new(state_store), config)
        .add_analyzer(SizeAnalyzer::new())
        .add_analyzer(CompletenessAnalyzer::new("amount"))
        .add_analyzer(MeanAnalyzer::new("amount"));

    // Set up validation context for "sales" table
    let validation_ctx = ValidationContext::new("sales");

    // Process daily partitions incrementally
    CURRENT_CONTEXT
        .scope(validation_ctx, async {
            println!("--- Day 1: Processing 2024-01-01 ---");
            let ctx1 = load_sales_partition("2024-01-01").await;
            let result1 = runner.analyze_partition(&ctx1, "2024-01-01").await?;
            print_metrics("Day 1 Metrics", &result1);

            println!("\n--- Day 2: Processing 2024-01-02 ---");
            let ctx2 = load_sales_partition("2024-01-02").await;
            let result2 = runner.analyze_partition(&ctx2, "2024-01-02").await?;
            print_metrics("Day 2 Metrics", &result2);

            println!("\n--- Day 3: Processing 2024-01-03 ---");
            let ctx3 = load_sales_partition("2024-01-03").await;
            let result3 = runner.analyze_partition(&ctx3, "2024-01-03").await?;
            print_metrics("Day 3 Metrics", &result3);

            println!("\n--- Aggregate Analysis: All Days ---");
            let all_partitions = vec![
                "2024-01-01".to_string(),
                "2024-01-02".to_string(),
                "2024-01-03".to_string(),
            ];
            let aggregate_result = runner.analyze_partitions(&all_partitions).await?;
            print_metrics("Aggregate Metrics", &aggregate_result);

            // Demonstrate incremental update
            println!("\n--- Incremental Update: Late data for 2024-01-02 ---");
            println!("(Simulating late-arriving data being added to existing partition)");

            // Create new data to add to existing partition
            let ctx_update = {
                let ctx = SessionContext::new();
                let schema = Arc::new(Schema::new(vec![
                    Field::new("order_id", DataType::Int64, false),
                    Field::new("amount", DataType::Float64, true),
                    Field::new("region", DataType::Utf8, true),
                ]));

                let batch = RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(Int64Array::from(vec![111, 112])),
                        Arc::new(Float64Array::from(vec![Some(500.0), Some(350.0)])),
                        Arc::new(StringArray::from(vec![Some("US"), Some("EU")])),
                    ],
                )
                .unwrap();

                ctx.register_batch("sales", batch).unwrap();
                ctx
            };

            let updated_result = runner
                .analyze_incremental(&ctx_update, "2024-01-02")
                .await?;
            print_metrics("Updated Day 2 Metrics", &updated_result);

            // Show new aggregate after update
            println!("\n--- Updated Aggregate: All Days ---");
            let final_aggregate = runner.analyze_partitions(&all_partitions).await?;
            print_metrics("Final Aggregate Metrics", &final_aggregate);

            // List all stored partitions
            println!("\n--- Stored Partitions ---");
            let partitions = runner.list_partitions().await?;
            for partition in partitions {
                println!("  - {partition}");
            }

            Ok::<(), Box<dyn std::error::Error>>(())
        })
        .await?;

    println!("\n=== Example Complete ===");
    Ok(())
}

fn print_metrics(title: &str, context: &term_guard::analyzers::AnalyzerContext) {
    println!("{title}");
    println!("{}", "-".repeat(title.len()));

    if let Some(MetricValue::Long(size)) = context.get_metric("size") {
        println!("  Total Records: {size}");
    }

    if let Some(MetricValue::Double(completeness)) = context.get_metric("completeness.amount") {
        println!("  Amount Completeness: {:.2}%", completeness * 100.0);
    }

    if let Some(MetricValue::Double(mean)) = context.get_metric("mean.amount") {
        println!("  Average Amount: ${mean:.2}");
    }

    if context.has_errors() {
        println!("  Errors encountered: {}", context.errors().len());
    }
}
