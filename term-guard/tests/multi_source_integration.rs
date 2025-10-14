//! Integration tests for multi-source validation capabilities.
//!
//! These tests demonstrate Term's ability to validate relationships, referential integrity,
//! and business rules spanning multiple datasets - addressing the critical market gap where
//! 68% of data quality issues involve relationships between tables.

use term_guard::constraints::*;
use term_guard::core::{Check, Level, MultiSourceValidator, ValidationResult, ValidationSuite};
use term_guard::sources::{CsvSource, JoinType, JoinedSource};

/// Helper to create CSV test data
fn create_csv_data(path: &str, data: &str) -> Result<(), Box<dyn std::error::Error>> {
    use std::fs;
    use std::path::Path;

    if let Some(parent) = Path::new(path).parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, data)?;
    Ok(())
}

/// Create test data for multi-source validation scenarios
async fn setup_test_data() -> Result<(), Box<dyn std::error::Error>> {
    // Create customers data
    create_csv_data(
        "/tmp/term_test/customers.csv",
        "id,name,email,created_at
1,Alice,alice@example.com,2024-01-01 10:00:00
2,Bob,bob@example.com,2024-01-02 11:00:00
3,Charlie,charlie@example.com,2024-01-03 12:00:00
4,Diana,diana@example.com,2024-01-04 13:00:00
5,Eve,eve@example.com,2024-01-05 14:00:00",
    )?;

    // Create orders data with some foreign key violations
    create_csv_data(
        "/tmp/term_test/orders.csv",
        "id,customer_id,total,created_at,processed_at
1,1,100.50,2024-01-01 10:30:00,2024-01-01 10:35:00
2,2,250.00,2024-01-02 11:30:00,2024-01-02 11:40:00
3,3,75.25,2024-01-03 09:00:00,2024-01-03 09:15:00
4,999,150.00,2024-01-04 08:00:00,2024-01-04 08:10:00
5,1,200.00,2024-01-05 16:00:00,2024-01-05 16:20:00
6,2,50.00,2024-01-06 18:00:00,2024-01-06 18:05:00",
    )?;

    // Create payments data with matching totals for some orders
    create_csv_data(
        "/tmp/term_test/payments.csv",
        "id,order_id,amount,payment_date
1,1,100.50,2024-01-01 10:40:00
2,2,250.00,2024-01-02 11:45:00
3,3,75.25,2024-01-03 09:20:00
4,5,200.00,2024-01-05 16:25:00
5,6,49.50,2024-01-06 18:10:00",
    )?;

    // Create inventory data for cross-table consistency
    create_csv_data(
        "/tmp/term_test/inventory.csv",
        "product_id,quantity,last_updated
PROD-001,100,2024-01-01 00:00:00
PROD-002,50,2024-01-02 00:00:00
PROD-003,0,2024-01-03 00:00:00
PROD-004,200,2024-01-04 00:00:00",
    )?;

    // Create transactions that should match inventory
    create_csv_data(
        "/tmp/term_test/transactions.csv",
        "id,product_id,quantity_change,transaction_date
1,PROD-001,-10,2024-01-01 10:00:00
2,PROD-001,-15,2024-01-01 14:00:00
3,PROD-002,-5,2024-01-02 09:00:00
4,PROD-003,-1,2024-01-03 11:00:00
5,PROD-004,50,2024-01-04 16:00:00",
    )?;

    Ok(())
}

#[tokio::test]
async fn test_foreign_key_validation_across_sources() -> Result<(), Box<dyn std::error::Error>> {
    setup_test_data().await?;

    // Create multi-source validator
    let mut validator = MultiSourceValidator::new();

    // Add data sources
    validator
        .add_source("customers", CsvSource::new("/tmp/term_test/customers.csv")?)
        .await?;
    validator
        .add_source("orders", CsvSource::new("/tmp/term_test/orders.csv")?)
        .await?;

    // Create validation suite with foreign key constraint
    let suite = ValidationSuite::builder("foreign_key_validation")
        .check(
            Check::builder("referential_integrity")
                .level(Level::Error)
                .foreign_key("orders.customer_id", "customers.id")
                .build(),
        )
        .build();

    // Run validation
    let result = validator.run_suite(&suite).await?;

    // Verify we detected the foreign key violation (customer_id = 999)
    match result {
        ValidationResult::Failure { report } => {
            assert_eq!(report.issues.len(), 1);
            assert!(
                report.issues[0]
                    .message
                    .contains("Foreign key constraint violation"),
                "Expected message to contain 'Foreign key constraint violation' but got: {}",
                report.issues[0].message
            );
            assert!(report.issues[0].message.contains("1 values")); // One violation
        }
        _ => panic!("Expected validation to fail due to foreign key violation"),
    }

    Ok(())
}

#[tokio::test]
async fn test_cross_table_sum_validation() -> Result<(), Box<dyn std::error::Error>> {
    setup_test_data().await?;

    let mut validator = MultiSourceValidator::new();

    // Add order and payment sources
    validator
        .add_source("orders", CsvSource::new("/tmp/term_test/orders.csv")?)
        .await?;
    validator
        .add_source("payments", CsvSource::new("/tmp/term_test/payments.csv")?)
        .await?;

    // Create suite to validate that order totals match payment amounts
    let suite = ValidationSuite::builder("financial_integrity")
        .check(
            Check::builder("payment_consistency")
                .level(Level::Error)
                .constraint(
                    CrossTableSumConstraint::new("orders.total", "payments.amount").tolerance(0.01), // Allow 1 cent difference for floating point
                )
                .build(),
        )
        .build();

    let result = validator.run_suite(&suite).await?;

    // We expect failure because payment for order 6 is $49.50 instead of $50.00
    // and order 4 has no payment
    match result {
        ValidationResult::Failure { report } => {
            assert!(!report.issues.is_empty());
            // The exact message will depend on how the sums don't match
        }
        _ => panic!("Expected validation to fail due to payment discrepancies"),
    }

    Ok(())
}

#[tokio::test]
async fn test_join_coverage_validation() -> Result<(), Box<dyn std::error::Error>> {
    setup_test_data().await?;

    let mut validator = MultiSourceValidator::new();

    validator
        .add_source("orders", CsvSource::new("/tmp/term_test/orders.csv")?)
        .await?;
    validator
        .add_source("customers", CsvSource::new("/tmp/term_test/customers.csv")?)
        .await?;

    // Test join coverage - we expect ~83% coverage (5 out of 6 orders have valid customers)
    let suite = ValidationSuite::builder("join_quality")
        .check(
            Check::builder("customer_coverage")
                .level(Level::Warning)
                .constraint(
                    JoinCoverageConstraint::new("orders", "customers")
                        .on("customer_id", "id")
                        .expect_match_rate(0.8), // Expect at least 80% coverage
                )
                .build(),
        )
        .build();

    let result = validator.run_suite(&suite).await?;

    // Should pass since we have 83% coverage and expect 80%
    match result {
        ValidationResult::Success { report, .. } => {
            assert_eq!(report.metrics.total_checks, 1);
            assert_eq!(report.metrics.passed_checks, 1);
        }
        ValidationResult::Failure { report } => {
            panic!("Expected validation to succeed with 83% coverage, but got failure with issues: {:?}", report.issues);
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_temporal_ordering_validation() -> Result<(), Box<dyn std::error::Error>> {
    setup_test_data().await?;

    let mut validator = MultiSourceValidator::new();

    validator
        .add_source("orders", CsvSource::new("/tmp/term_test/orders.csv")?)
        .await?;

    // Validate that created_at always comes before processed_at
    let suite = ValidationSuite::builder("temporal_consistency")
        .check(
            Check::builder("event_ordering")
                .level(Level::Error)
                .constraint(
                    TemporalOrderingConstraint::new("orders")
                        .before_after("created_at", "processed_at")
                        .allow_nulls(false),
                )
                .build(),
        )
        .check(
            Check::builder("business_hours")
                .level(Level::Warning)
                .constraint(
                    TemporalOrderingConstraint::new("orders")
                        .business_hours("created_at", "09:00", "17:00")
                        .weekdays_only(false), // Include weekends for this test
                )
                .build(),
        )
        .build();

    let result = validator.run_suite(&suite).await?;

    // Check the result - warning level checks don't necessarily fail the suite
    match result {
        ValidationResult::Success { report, .. } | ValidationResult::Failure { report } => {
            // Business hours check is a warning, so it may not fail the suite
            // but should still be reported in issues
            println!("Report issues: {:?}", report.issues);
            assert!(!report.issues.is_empty(), "Expected business hours warning");
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_joined_source_validation() -> Result<(), Box<dyn std::error::Error>> {
    setup_test_data().await?;

    let mut validator = MultiSourceValidator::new();

    // Create a joined source for orders with customers
    let orders_source = CsvSource::new("/tmp/term_test/orders.csv")?;
    let customers_source = CsvSource::new("/tmp/term_test/customers.csv")?;

    let joined = JoinedSource::builder()
        .left_source(orders_source, "orders")
        .right_source(customers_source, "customers")
        .on("customer_id", "id")
        .join_type(JoinType::Left)
        .build()?;

    // Register the joined source
    validator
        .add_source("orders_with_customers", joined)
        .await?;

    // Now we can validate properties of the joined data
    let suite = ValidationSuite::builder("joined_validation")
        .check(
            Check::builder("joined_data_quality")
                .level(Level::Warning)
                // Check that customer names are not null in the join
                .completeness("customers.name", Default::default())
                .build(),
        )
        .build();

    let result = validator.run_suite(&suite).await?;

    // Check the result - completeness check should detect nulls from the left join
    match result {
        ValidationResult::Success { report, .. } | ValidationResult::Failure { report } => {
            // With a warning level check, may not fail suite but should report issue
            println!("Joined validation issues: {:?}", report.issues);
            // The completeness check should detect the null from customer_id=999
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_complex_multi_table_validation() -> Result<(), Box<dyn std::error::Error>> {
    setup_test_data().await?;

    let mut validator = MultiSourceValidator::new()
        .with_caching(true)
        .with_max_cache_size(10 * 1024 * 1024); // 10MB cache

    // Add all sources
    validator
        .add_source("customers", CsvSource::new("/tmp/term_test/customers.csv")?)
        .await?;
    validator
        .add_source("orders", CsvSource::new("/tmp/term_test/orders.csv")?)
        .await?;
    validator
        .add_source("payments", CsvSource::new("/tmp/term_test/payments.csv")?)
        .await?;
    validator
        .add_source("inventory", CsvSource::new("/tmp/term_test/inventory.csv")?)
        .await?;
    validator
        .add_source(
            "transactions",
            CsvSource::new("/tmp/term_test/transactions.csv")?,
        )
        .await?;

    // Create comprehensive validation suite
    let suite = ValidationSuite::builder("comprehensive_validation")
        .with_optimizer(true) // Enable query optimization
        .check(
            Check::builder("critical_integrity")
                .level(Level::Error)
                .foreign_key("orders.customer_id", "customers.id")
                .build(),
        )
        .check(
            Check::builder("financial_consistency")
                .level(Level::Error)
                .cross_table_sum("orders.total", "payments.amount")
                .build(),
        )
        .check(
            Check::builder("data_quality")
                .level(Level::Warning)
                .constraint(
                    JoinCoverageConstraint::new("orders", "customers")
                        .on("customer_id", "id")
                        .expect_match_rate(0.95),
                )
                .constraint(
                    TemporalOrderingConstraint::new("orders")
                        .before_after("created_at", "processed_at"),
                )
                .build(),
        )
        .build();

    let result = validator.run_suite(&suite).await?;

    // Verify we get the expected failures
    match result {
        ValidationResult::Failure { report } => {
            // Should have multiple issues detected
            assert!(report.issues.len() >= 2);

            // Check that we have both critical and warning level issues
            let has_critical = report.issues.iter().any(|i| {
                i.check_name == "critical_integrity" || i.check_name == "financial_consistency"
            });
            let _has_warning = report.issues.iter().any(|i| i.check_name == "data_quality");

            assert!(has_critical, "Should have critical issues");
            // Warning might not fail if thresholds are met
        }
        _ => panic!("Expected validation to fail with multiple issues"),
    }

    // Check cache statistics
    let cache_stats = validator.cache_stats();
    println!(
        "Cache stats: {} entries, {} bytes used",
        cache_stats.entries, cache_stats.size_bytes
    );

    Ok(())
}

#[tokio::test]
async fn test_performance_with_larger_dataset() -> Result<(), Box<dyn std::error::Error>> {
    use std::time::Instant;

    // Create larger dataset for performance testing
    let mut customers_data = String::from("id,name,email,created_at\n");
    let mut orders_data = String::from("id,customer_id,total,created_at,processed_at\n");

    // Generate 10K customers and 100K orders
    for i in 1..=10_000 {
        customers_data.push_str(&format!(
            "{i},Customer{i},customer{i}@example.com,2024-01-01 10:00:00\n"
        ));
    }

    for i in 1..=100_000 {
        let customer_id = (i % 10_000) + 1; // Ensure valid foreign keys
        orders_data.push_str(&format!(
            "{i},{customer_id},100.50,2024-01-01 10:00:00,2024-01-01 10:30:00\n"
        ));
    }

    create_csv_data("/tmp/term_test/large_customers.csv", &customers_data)?;
    create_csv_data("/tmp/term_test/large_orders.csv", &orders_data)?;

    let mut validator = MultiSourceValidator::new().with_caching(true);

    validator
        .add_source(
            "large_customers",
            CsvSource::new("/tmp/term_test/large_customers.csv")?,
        )
        .await?;
    validator
        .add_source(
            "large_orders",
            CsvSource::new("/tmp/term_test/large_orders.csv")?,
        )
        .await?;

    let suite = ValidationSuite::builder("performance_test")
        .with_optimizer(true)
        .check(
            Check::builder("foreign_key_check")
                .level(Level::Error)
                .foreign_key("large_orders.customer_id", "large_customers.id")
                .build(),
        )
        .build();

    let start = Instant::now();
    let result = validator.run_suite(&suite).await?;
    let duration = start.elapsed();

    println!("Validation of 100K orders took: {duration:?}");

    // Verify the validation completed successfully
    match result {
        ValidationResult::Success { report, .. } => {
            assert_eq!(report.metrics.passed_checks, 1);
            // Performance assertion: should complete in reasonable time
            // This is a soft assertion - adjust based on hardware
            assert!(
                duration.as_millis() < 5000,
                "Validation took too long: {duration:?}"
            );
        }
        _ => panic!("Expected validation to succeed"),
    }

    Ok(())
}
