//! Example demonstrating cloud storage data sources with Term validation.

use datafusion::prelude::*;
#[cfg(any(feature = "s3", feature = "gcs", feature = "azure"))]
use term_core::constraints::{
    Assertion, CompletenessConstraint, FormatConstraint, SizeConstraint, StatisticType,
    StatisticalConstraint, UniquenessConstraint,
};
#[cfg(any(feature = "s3", feature = "gcs", feature = "azure"))]
use term_core::core::{Check, Level, ValidationSuite};
use term_core::error::Result;
#[cfg(any(feature = "s3", feature = "gcs", feature = "azure"))]
use term_core::sources::DataSource;

#[cfg(feature = "s3")]
use term_core::sources::S3Source;

#[cfg(feature = "gcs")]
use term_core::sources::GcsSource;

#[cfg(feature = "azure")]
use term_core::sources::AzureBlobSource;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("Term Cloud Storage Example\n");

    // Create a validation context
    #[allow(unused_variables)]
    let ctx = SessionContext::new();

    #[cfg(feature = "s3")]
    {
        println!("=== S3 Example ===");

        // Example 1: S3 with IAM instance credentials
        let s3_iam = S3Source::from_iam(
            "my-bucket".to_string(),
            "data/customers.parquet".to_string(),
            Some("us-east-1".to_string()),
        )
        .await?;

        // Register the data source
        s3_iam.register(&ctx, "customers").await?;
        println!("Registered S3 source: {}", s3_iam.description());

        // Example 2: S3 with access keys
        let s3_keys = S3Source::from_access_key(
            "my-bucket".to_string(),
            "data/orders.csv".to_string(),
            std::env::var("AWS_ACCESS_KEY_ID").unwrap_or("test".to_string()),
            std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or("test".to_string()),
            None,
        )
        .await?;

        s3_keys.register(&ctx, "orders").await?;
        println!("Registered S3 source: {}", s3_keys.description());

        // Create and run validations
        let suite = ValidationSuite::builder("S3 Data Validation")
            .check(
                Check::builder("customer_completeness")
                    .level(Level::Error)
                    .constraint(CompletenessConstraint::with_threshold("customer_id", 1.0))
                    .constraint(CompletenessConstraint::with_threshold("email", 0.95))
                    .build(),
            )
            .check(
                Check::builder("order_validity")
                    .level(Level::Error)
                    .constraint(
                        StatisticalConstraint::new(
                            "amount",
                            StatisticType::Min,
                            Assertion::GreaterThan(0.0),
                        )
                        .unwrap(),
                    )
                    .constraint(UniquenessConstraint::full_uniqueness("order_id", 1.0).unwrap())
                    .build(),
            )
            .build();

        let results = suite.run(&ctx).await?;
        println!("\nS3 Validation Results:");
        display_validation_result(&results);
        println!();
    }

    #[cfg(feature = "gcs")]
    {
        println!("=== Google Cloud Storage Example ===");

        // Example 1: GCS with Application Default Credentials
        let gcs_adc =
            GcsSource::from_adc("my-bucket".to_string(), "data/products.parquet".to_string())
                .await?;

        gcs_adc.register(&ctx, "products").await?;
        println!("Registered GCS source: {}", gcs_adc.description());

        // Example 2: GCS with service account key file
        if let Ok(key_path) = std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
            let gcs_sa = GcsSource::from_service_account_file(
                "my-bucket".to_string(),
                "data/inventory.json".to_string(),
                key_path,
            )
            .await?;

            gcs_sa.register(&ctx, "inventory").await?;
            println!("Registered GCS source: {}", gcs_sa.description());
        }

        // Run validations on GCS data
        let suite = ValidationSuite::builder("GCS Data Validation")
            .check(
                Check::builder("product_quality")
                    .level(Level::Error)
                    .constraint(CompletenessConstraint::with_threshold("product_id", 1.0))
                    .constraint(
                        StatisticalConstraint::new(
                            "price",
                            StatisticType::Min,
                            Assertion::GreaterThanOrEqual(0.0),
                        )
                        .unwrap(),
                    )
                    .build(),
            )
            .build();

        let results = suite.run(&ctx).await?;
        println!("\nGCS Validation Results:");
        display_validation_result(&results);
        println!();
    }

    #[cfg(feature = "azure")]
    {
        println!("=== Azure Blob Storage Example ===");

        // Example 1: Azure with CLI credentials
        let azure_cli = AzureBlobSource::from_azure_cli(
            "mystorageaccount".to_string(),
            "data-container".to_string(),
            "analytics/sales.parquet".to_string(),
        )
        .await?;

        azure_cli.register(&ctx, "sales").await?;
        println!("Registered Azure source: {}", azure_cli.description());

        // Example 2: Azure with access key
        if let Ok(access_key) = std::env::var("AZURE_STORAGE_ACCESS_KEY") {
            let azure_key = AzureBlobSource::from_access_key(
                "mystorageaccount".to_string(),
                "data-container".to_string(),
                "analytics/revenue.csv".to_string(),
                access_key,
            )
            .await?;

            azure_key.register(&ctx, "revenue").await?;
            println!("Registered Azure source: {}", azure_key.description());
        }

        // Run validations on Azure data
        let suite = ValidationSuite::builder("Azure Data Validation")
            .check(
                Check::builder("sales_integrity")
                    .level(Level::Error)
                    .constraint(CompletenessConstraint::with_threshold(
                        "transaction_id",
                        1.0,
                    ))
                    .constraint(
                        StatisticalConstraint::new(
                            "total",
                            StatisticType::Min,
                            Assertion::GreaterThanOrEqual(0.0),
                        )
                        .unwrap(),
                    )
                    .constraint(
                        FormatConstraint::regex("date", r"^\d{4}-\d{2}-\d{2}$", 1.0).unwrap(),
                    )
                    .build(),
            )
            .build();

        let results = suite.run(&ctx).await?;
        println!("\nAzure Validation Results:");
        display_validation_result(&results);
        println!();
    }

    // Example: Cross-cloud validation
    #[cfg(all(feature = "s3", feature = "gcs", feature = "azure"))]
    {
        println!("=== Cross-Cloud Validation Example ===");

        // You can validate data across different cloud providers
        // by registering multiple sources and running checks on all of them

        let suite = ValidationSuite::builder("Cross-Cloud Data Consistency")
            .check(
                Check::builder("customer_consistency")
                    .level(Level::Warning)
                    .constraint(SizeConstraint::new(Assertion::Between(900000.0, 1100000.0))) // 1M customers ±10%
                    .build(),
            )
            .check(
                Check::builder("data_quality")
                    .level(Level::Error)
                    .constraint(CompletenessConstraint::with_threshold("id", 1.0))
                    .constraint(FormatConstraint::regex("name", r"^.{1,255}$", 0.95).unwrap())
                    .build(),
            )
            .build();

        // This would run on all registered tables
        let results = suite.run(&ctx).await?;
        println!("\nCross-Cloud Validation Results:");
        display_validation_result(&results);
    }

    println!("\n✅ Cloud storage examples completed successfully!");
    Ok(())
}

#[allow(dead_code)]
fn display_validation_result(result: &term_core::core::ValidationResult) {
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
}
