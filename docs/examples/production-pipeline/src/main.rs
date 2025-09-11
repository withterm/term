//! Production pipeline example showing Term in a real-world scenario

use term::core::{ValidationSuite, Check, ValidationRunner, ConstraintStatus};
use term::sources::{DataSource, ParquetSource, CsvSource};
use term::analyzers::{AnalyzerRunner, StandardAnalyzers};
use datafusion::prelude::SessionContext;
use anyhow::Result;
use chrono::{DateTime, Utc};
use std::time::Duration;
use tokio::time;
use tracing::{info, warn, error, instrument};
use tracing_subscriber;

#[derive(Debug)]
struct PipelineConfig {
    batch_interval: Duration,
    alert_threshold: f64,
    export_metrics: bool,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            batch_interval: Duration::from_secs(300), // 5 minutes
            alert_threshold: 0.95,
            export_metrics: true,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();
    
    info!("Starting Term production pipeline");
    
    let config = PipelineConfig::default();
    
    // Run continuous validation pipeline
    run_pipeline(config).await?;
    
    Ok(())
}

#[instrument(skip(config))]
async fn run_pipeline(config: PipelineConfig) -> Result<()> {
    let mut interval = time::interval(config.batch_interval);
    let mut batch_number = 0;
    
    loop {
        interval.tick().await;
        batch_number += 1;
        
        info!("Processing batch {}", batch_number);
        
        match process_batch(batch_number, &config).await {
            Ok(success_rate) => {
                if success_rate < config.alert_threshold {
                    warn!(
                        "Batch {} validation below threshold: {:.2}% (threshold: {:.2}%)",
                        batch_number,
                        success_rate * 100.0,
                        config.alert_threshold * 100.0
                    );
                    send_alert(batch_number, success_rate).await?;
                } else {
                    info!("Batch {} validation successful: {:.2}%", 
                        batch_number, 
                        success_rate * 100.0
                    );
                }
                
                if config.export_metrics {
                    export_metrics(batch_number, success_rate).await?;
                }
            }
            Err(e) => {
                error!("Batch {} processing failed: {}", batch_number, e);
                handle_failure(batch_number, e).await?;
            }
        }
    }
}

#[instrument]
async fn process_batch(batch_number: u64, config: &PipelineConfig) -> Result<f64> {
    // Create a new session context for this batch
    let ctx = SessionContext::new();
    
    // Load data sources for this batch
    load_batch_data(&ctx, batch_number).await?;
    
    // Create comprehensive validation suite
    let suite = create_validation_suite(batch_number);
    
    // Run validations
    let runner = ValidationRunner::new();
    let results = runner.run(&suite, &ctx).await?;
    
    // Calculate success rate
    let total = results.check_results().len();
    let passed = results
        .check_results()
        .iter()
        .filter(|r| r.status() == &ConstraintStatus::Success)
        .count();
    
    let success_rate = passed as f64 / total as f64;
    
    // Run profiling for detailed metrics
    if config.export_metrics {
        run_profiling(&ctx).await?;
    }
    
    // Store results for audit
    store_validation_results(batch_number, &results).await?;
    
    Ok(success_rate)
}

async fn load_batch_data(ctx: &SessionContext, batch_number: u64) -> Result<()> {
    // Load main transaction data
    let transactions_path = format!("data/batch_{}/transactions.parquet", batch_number);
    let transactions = ParquetSource::builder()
        .path(&transactions_path)
        .build()?;
    transactions.register(ctx, "transactions").await?;
    
    // Load customer dimension
    let customers_path = format!("data/batch_{}/customers.csv", batch_number);
    let customers = CsvSource::builder()
        .path(&customers_path)
        .has_header(true)
        .build()?;
    customers.register(ctx, "customers").await?;
    
    // Load product catalog
    let products_path = format!("data/batch_{}/products.parquet", batch_number);
    let products = ParquetSource::builder()
        .path(&products_path)
        .build()?;
    products.register(ctx, "products").await?;
    
    info!("Loaded data sources for batch {}", batch_number);
    Ok(())
}

fn create_validation_suite(batch_number: u64) -> ValidationSuite {
    ValidationSuite::builder(&format!("batch_{}_validation", batch_number))
        .description("Production data quality checks")
        
        // Transaction integrity checks
        .add_check(
            Check::new("transaction_integrity")
                .is_complete("transaction_id")
                .is_unique("transaction_id")
                .is_complete("customer_id")
                .is_complete("product_id")
                .is_complete("amount")
                .is_complete("timestamp")
        )
        
        // Business rule validations
        .add_check(
            Check::new("business_rules")
                .is_non_negative("amount")
                .has_min("amount", 0.01)
                .has_max("amount", 1000000.0)
                .is_contained_in("status", vec!["pending", "completed", "failed", "refunded"])
                .is_contained_in("payment_method", vec!["credit", "debit", "paypal", "wire"])
        )
        
        // Referential integrity
        .add_check(
            Check::new("referential_integrity")
                .has_foreign_key("customer_id", "customers", "customer_id")
                .has_foreign_key("product_id", "products", "product_id")
        )
        
        // Data freshness
        .add_check(
            Check::new("data_freshness")
                .has_max_age("timestamp", chrono::Duration::hours(1))
        )
        
        // Statistical validations
        .add_check(
            Check::new("statistical_checks")
                .has_mean("amount", 50.0, 500.0)  // Expected range
                .has_stddev("amount", 0.0, 200.0)
                .has_quantile("amount", 0.99, 0.0, 5000.0)  // 99th percentile
        )
        
        // Anomaly detection
        .add_check(
            Check::new("anomaly_detection")
                .has_no_anomalies("amount", 3.0)  // 3 standard deviations
                .has_expected_pattern("customer_id", r"^CUST\d{8}$")
                .has_expected_pattern("product_id", r"^PROD\d{6}$")
        )
        
        .build()
}

#[instrument]
async fn run_profiling(ctx: &SessionContext) -> Result<()> {
    let runner = AnalyzerRunner::new()
        .add_analyzers(StandardAnalyzers::all());
    
    // Profile each table
    for table in &["transactions", "customers", "products"] {
        let metrics = runner.run(ctx, table).await?;
        info!("Profiling results for {}: {:?}", table, metrics.summary());
    }
    
    Ok(())
}

async fn store_validation_results(
    batch_number: u64, 
    results: &term::core::ValidationResult
) -> Result<()> {
    // In production, this would write to a database or data lake
    let timestamp = Utc::now();
    let results_path = format!(
        "results/batch_{}_results_{}.json", 
        batch_number,
        timestamp.format("%Y%m%d_%H%M%S")
    );
    
    let json = serde_json::to_string_pretty(results)?;
    tokio::fs::write(&results_path, json).await?;
    
    info!("Stored validation results to {}", results_path);
    Ok(())
}

async fn send_alert(batch_number: u64, success_rate: f64) -> Result<()> {
    // In production, this would send to PagerDuty, Slack, etc.
    warn!(
        "ALERT: Batch {} validation failed. Success rate: {:.2}%",
        batch_number,
        success_rate * 100.0
    );
    
    // Simulate alert sending
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    Ok(())
}

async fn export_metrics(batch_number: u64, success_rate: f64) -> Result<()> {
    // In production, this would export to Prometheus, DataDog, etc.
    info!(
        "Exporting metrics - batch: {}, success_rate: {:.2}",
        batch_number,
        success_rate
    );
    
    // Simulate metric export
    tokio::time::sleep(Duration::from_millis(50)).await;
    
    Ok(())
}

async fn handle_failure(batch_number: u64, error: anyhow::Error) -> Result<()> {
    error!("Handling failure for batch {}: {}", batch_number, error);
    
    // In production:
    // 1. Send critical alert
    // 2. Store failed batch for reprocessing
    // 3. Switch to degraded mode if needed
    // 4. Log to error tracking system
    
    Ok(())
}