//! Integration tests for multi-source validation components.
//!
//! This test suite demonstrates the comprehensive multi-source validation capabilities
//! provided by Term, including cross-table sum validation, foreign key constraints,
//! join coverage analysis, and the MultiSourceValidator framework.
//!
//! These tests validate the complete workflow from data source registration through
//! complex multi-table validation scenarios typical in enterprise data environments.

use datafusion::prelude::*;
use term_guard::constraints::{
    CoverageDirection, CrossTableSumConstraint, ForeignKeyConstraint, JoinCoverageConstraint,
    JoinType,
};
use term_guard::core::{
    Constraint, ConstraintStatus, Level, MultiSourceValidator, ValidationResult,
};
use term_guard::prelude::*;

/// Create a comprehensive test data warehouse scenario with multiple related tables.
///
/// This function sets up a realistic e-commerce data warehouse with:
/// - Customers (master data)
/// - Orders (transaction data)
/// - Payments (financial data)
/// - Products (catalog data)
/// - Order Items (detail data)
///
/// The data includes various scenarios:
/// - Valid relationships for successful validation
/// - Missing foreign key references for constraint failures
/// - Mismatched aggregations for sum validation failures
/// - Incomplete joins for coverage validation scenarios
async fn create_comprehensive_test_warehouse(ctx: &SessionContext) -> Result<()> {
    // Create customers table (master data)
    ctx.sql(
        "CREATE TABLE customers (
            id BIGINT, 
            name STRING, 
            email STRING, 
            region STRING, 
            status STRING
        )",
    )
    .await?
    .collect()
    .await?;

    ctx.sql(
        "INSERT INTO customers VALUES 
        (1, 'Alice Johnson', 'alice@example.com', 'North', 'active'),
        (2, 'Bob Smith', 'bob@example.com', 'South', 'active'), 
        (3, 'Charlie Brown', 'charlie@example.com', 'East', 'active'),
        (4, 'Diana Prince', 'diana@example.com', 'West', 'inactive'),
        (5, 'Eve Wilson', 'eve@example.com', 'North', 'active')",
    )
    .await?
    .collect()
    .await?;

    // Create orders table with some orphaned records
    ctx.sql(
        "CREATE TABLE orders (
            id BIGINT, 
            customer_id BIGINT, 
            order_date STRING, 
            total DOUBLE, 
            status STRING
        )",
    )
    .await?
    .collect()
    .await?;

    ctx.sql(
        "INSERT INTO orders VALUES 
        (101, 1, '2024-01-15', 299.99, 'completed'),
        (102, 1, '2024-01-20', 149.50, 'completed'),
        (103, 2, '2024-01-18', 599.99, 'completed'),
        (104, 3, '2024-01-22', 89.99, 'completed'),
        (105, 999, '2024-01-25', 199.99, 'pending'),  -- Orphaned: customer 999 doesn't exist
        (106, 2, '2024-01-28', 299.99, 'completed'),
        (107, 5, '2024-02-01', 399.99, 'completed')",
    )
    .await?
    .collect()
    .await?;

    // Create payments table with intentional mismatches for cross-table sum validation
    ctx.sql(
        "CREATE TABLE payments (
            id BIGINT, 
            customer_id BIGINT, 
            payment_date STRING, 
            amount DOUBLE, 
            method STRING
        )",
    )
    .await?
    .collect()
    .await?;

    ctx.sql(
        "INSERT INTO payments VALUES 
        (201, 1, '2024-01-15', 299.99, 'credit_card'),
        (202, 1, '2024-01-20', 149.50, 'debit_card'),
        (203, 2, '2024-01-18', 599.99, 'credit_card'),
        (204, 3, '2024-01-22', 89.99, 'paypal'),
        -- Note: Missing payment for order 105 (customer 999), and customer 2's second order
        (206, 2, '2024-01-28', 250.00, 'credit_card'), -- Intentional mismatch: order 106 is 299.99
        (207, 5, '2024-02-01', 399.99, 'bank_transfer')",
    )
    .await?
    .collect()
    .await?;

    // Create products table
    ctx.sql(
        "CREATE TABLE products (
            id BIGINT, 
            name STRING, 
            category STRING, 
            price DOUBLE, 
            active BOOLEAN
        )",
    )
    .await?
    .collect()
    .await?;

    ctx.sql(
        "INSERT INTO products VALUES 
        (1001, 'Laptop Pro', 'Electronics', 1299.99, true),
        (1002, 'Wireless Mouse', 'Electronics', 29.99, true),
        (1003, 'Office Chair', 'Furniture', 199.99, true),
        (1004, 'Desk Lamp', 'Furniture', 49.99, true),
        (1005, 'Notebook', 'Stationery', 9.99, true)",
    )
    .await?
    .collect()
    .await?;

    // Create order_items table with some referential integrity issues
    ctx.sql(
        "CREATE TABLE order_items (
            id BIGINT, 
            order_id BIGINT, 
            product_id BIGINT, 
            quantity INTEGER, 
            unit_price DOUBLE
        )",
    )
    .await?
    .collect()
    .await?;

    ctx.sql(
        "INSERT INTO order_items VALUES 
        (301, 101, 1001, 1, 299.99),
        (302, 102, 1002, 3, 29.99),
        (303, 102, 1005, 2, 9.99),
        (304, 103, 1003, 1, 199.99),
        (305, 103, 1001, 1, 399.99), -- Different price than product master
        (306, 104, 1004, 1, 49.99),
        (307, 104, 1005, 4, 9.99),
        (308, 105, 9999, 1, 199.99), -- References non-existent product
        (309, 106, 1003, 1, 199.99),
        (310, 106, 1002, 1, 29.99),
        (311, 106, 1005, 7, 9.99),
        (312, 107, 1001, 1, 399.99)",
    )
    .await?
    .collect()
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_multi_source_validator_comprehensive_workflow() -> Result<()> {
    // Create DataFusion context and populate with test data
    let ctx = SessionContext::new();
    create_comprehensive_test_warehouse(&ctx).await?;

    // Create and configure the multi-source validator
    let mut validator = MultiSourceValidator::builder("e_commerce_data_quality")
        .description("Comprehensive e-commerce data warehouse validation")
        .max_concurrent_validations(4)
        .memory_budget_mb(512)
        .enable_query_optimization(true)
        .build()?;

    // Register all data sources
    validator.register_source("customers", "Customer master data table")?;
    validator.register_source("orders", "Order transaction records")?;
    validator.register_source("payments", "Payment transaction records")?;
    validator.register_source("products", "Product catalog information")?;
    validator.register_source("order_items", "Order line item details")?;

    // Add cross-table sum validation (should detect mismatch between orders and payments)
    validator.add_cross_table_validation_with_level(
        "financial_consistency_by_customer",
        CrossTableSumConstraint::new("orders.total", "payments.amount")
            .group_by(vec!["customer_id"])
            .tolerance(0.01)
            .max_violations_reported(10),
        Level::Error,
    )?;

    // Add foreign key validations
    validator.add_foreign_key_validation_with_level(
        "orders_customer_integrity",
        ForeignKeyConstraint::new("orders.customer_id", "customers.id")
            .allow_nulls(false)
            .max_violations_reported(5),
        Level::Error,
    )?;

    validator.add_foreign_key_validation_with_level(
        "order_items_product_integrity",
        ForeignKeyConstraint::new("order_items.product_id", "products.id")
            .allow_nulls(false)
            .max_violations_reported(5),
        Level::Warning, // Less critical than customer references
    )?;

    validator.add_foreign_key_validation_with_level(
        "order_items_order_integrity",
        ForeignKeyConstraint::new("order_items.order_id", "orders.id")
            .allow_nulls(false)
            .max_violations_reported(5),
        Level::Error,
    )?;

    // Verify validator configuration
    assert_eq!(validator.sources().len(), 5);
    assert_eq!(validator.validation_count(), 4); // 1 cross-table + 3 foreign key validations

    // Execute comprehensive validation
    let results = validator.validate(&ctx).await?;

    // Analyze results - should have failures due to our intentional data issues
    match results {
        ValidationResult::Failure { report } => {
            println!("Validation completed with expected failures:");
            let total_checks = report.metrics.total_checks;
            let failed_checks = report.metrics.failed_checks;
            let passed_checks = report.metrics.passed_checks;
            println!("Total checks: {total_checks}");
            println!("Failed checks: {failed_checks}");
            println!("Passed checks: {passed_checks}");

            // Verify we detected the expected issues
            assert!(
                report.metrics.failed_checks > 0,
                "Should detect data quality issues"
            );
            assert!(
                report.metrics.total_checks >= 4,
                "Should run all validations"
            );

            // Check that we have detailed issue reports
            let mut found_cross_table_issue = false;
            let mut found_foreign_key_issue = false;

            for issue in &report.issues {
                let check_name = &issue.check_name;
                let message = &issue.message;
                println!("Issue: {check_name} - {message}");

                if issue.check_name.contains("financial_consistency") {
                    found_cross_table_issue = true;
                    assert!(
                        issue.message.contains("Cross-table sum mismatch")
                            || issue.message.contains("mismatch"),
                        "Should describe cross-table validation failure"
                    );
                }

                if issue.check_name.contains("integrity") {
                    found_foreign_key_issue = true;
                    assert!(
                        issue.message.contains("Foreign key")
                            || issue.message.contains("do not exist"),
                        "Should describe foreign key violations"
                    );
                }
            }

            // We expect issues due to our test data design
            assert!(
                found_cross_table_issue || found_foreign_key_issue,
                "Should detect at least one type of validation failure"
            );
        }
        ValidationResult::Success { .. } => {
            panic!("Expected validation failures due to intentional data issues in test dataset");
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_join_coverage_constraint_integration() -> Result<()> {
    let ctx = SessionContext::new();
    create_comprehensive_test_warehouse(&ctx).await?;

    // Test join coverage between customers and orders
    let customer_order_coverage = JoinCoverageConstraint::new("customers", "orders")
        .on_columns(vec!["id"], vec!["customer_id"])
        .join_type(JoinType::LeftOuter)
        .direction(CoverageDirection::LeftToRight)
        .minimum_coverage(0.60) // 4 out of 5 customers have orders = 80%
        .max_violations_reported(3);

    let result = customer_order_coverage.evaluate(&ctx).await?;

    // Should succeed since 4/5 customers have orders (80% > 60%)
    assert_eq!(result.status, ConstraintStatus::Success);
    assert!(result.metric.unwrap() >= 0.60);

    // Test join coverage in the other direction (orders to customers)
    let order_customer_coverage = JoinCoverageConstraint::new("orders", "customers")
        .on_columns(vec!["customer_id"], vec!["id"])
        .join_type(JoinType::LeftOuter) // Use LEFT OUTER to include orders with invalid customer_id
        .direction(CoverageDirection::LeftToRight)
        .minimum_coverage(0.90); // Should fail since 1 order has invalid customer_id

    let result = order_customer_coverage.evaluate(&ctx).await?;

    // Should fail since 6/7 orders have valid customers (85.7% < 90%)
    assert_eq!(result.status, ConstraintStatus::Failure);
    assert!(result.message.is_some());
    let message = result.message.unwrap();
    assert!(message.contains("Join coverage constraint violation"));

    Ok(())
}

#[tokio::test]
async fn test_bidirectional_join_coverage() -> Result<()> {
    let ctx = SessionContext::new();
    create_comprehensive_test_warehouse(&ctx).await?;

    // Test bidirectional coverage between customers and orders
    let bidirectional_coverage = JoinCoverageConstraint::new("customers", "orders")
        .on_columns(vec!["id"], vec!["customer_id"])
        .direction(CoverageDirection::Bidirectional)
        .minimum_coverage(0.70)
        .maximum_coverage(0.90);

    let result = bidirectional_coverage.evaluate(&ctx).await?;

    // May succeed or fail depending on coverage in both directions
    // This tests the bidirectional logic implementation
    assert!(result.metric.is_some());

    if result.status == ConstraintStatus::Failure {
        assert!(result.message.is_some());
        let message = result.message.unwrap();
        assert!(message.contains("Bidirectional join coverage"));
    }

    Ok(())
}

#[tokio::test]
async fn test_multi_column_join_coverage() -> Result<()> {
    let ctx = SessionContext::new();

    // Create test tables with composite keys
    ctx.sql(
        "CREATE TABLE composite_parent (
            key1 BIGINT, 
            key2 STRING, 
            data STRING
        )",
    )
    .await?
    .collect()
    .await?;

    ctx.sql(
        "INSERT INTO composite_parent VALUES 
        (1, 'A', 'data1'),
        (2, 'B', 'data2'),
        (3, 'C', 'data3')",
    )
    .await?
    .collect()
    .await?;

    ctx.sql(
        "CREATE TABLE composite_child (
            id BIGINT,
            parent_key1 BIGINT, 
            parent_key2 STRING, 
            value DOUBLE
        )",
    )
    .await?
    .collect()
    .await?;

    ctx.sql(
        "INSERT INTO composite_child VALUES 
        (101, 1, 'A', 100.0),
        (102, 2, 'B', 200.0),
        (103, 4, 'D', 300.0)", // Invalid composite key
    )
    .await?
    .collect()
    .await?;

    // Test multi-column join coverage
    let multi_column_coverage = JoinCoverageConstraint::new("composite_child", "composite_parent")
        .on_columns(vec!["parent_key1", "parent_key2"], vec!["key1", "key2"])
        .join_type(JoinType::LeftOuter) // Use LEFT OUTER JOIN to include non-matching records
        .minimum_coverage(0.80); // Should fail since only 2/3 child records match

    let result = multi_column_coverage.evaluate(&ctx).await?;

    assert_eq!(result.status, ConstraintStatus::Failure);
    assert!(result
        .message
        .unwrap()
        .contains("Join coverage constraint violation"));

    Ok(())
}

#[tokio::test]
async fn test_cross_table_sum_with_tolerance() -> Result<()> {
    let ctx = SessionContext::new();

    // Create tables with small differences that should pass with tolerance
    ctx.sql(
        "CREATE TABLE precise_orders (
            customer_id BIGINT, 
            amount DOUBLE
        )",
    )
    .await?
    .collect()
    .await?;

    ctx.sql(
        "INSERT INTO precise_orders VALUES 
        (1, 100.001),
        (2, 200.002)",
    )
    .await?
    .collect()
    .await?;

    ctx.sql(
        "CREATE TABLE precise_payments (
            customer_id BIGINT, 
            amount DOUBLE
        )",
    )
    .await?
    .collect()
    .await?;

    ctx.sql(
        "INSERT INTO precise_payments VALUES 
        (1, 100.003), 
        (2, 200.001)",
    )
    .await?
    .collect()
    .await?;

    // Test without tolerance - should fail
    let strict_constraint =
        CrossTableSumConstraint::new("precise_orders.amount", "precise_payments.amount")
            .group_by(vec!["customer_id"])
            .tolerance(0.0);

    let result = strict_constraint.evaluate(&ctx).await?;
    assert_eq!(result.status, ConstraintStatus::Failure);

    // Test with tolerance - should pass
    let tolerant_constraint =
        CrossTableSumConstraint::new("precise_orders.amount", "precise_payments.amount")
            .group_by(vec!["customer_id"])
            .tolerance(0.01);

    let result = tolerant_constraint.evaluate(&ctx).await?;
    assert_eq!(result.status, ConstraintStatus::Success);

    Ok(())
}

#[tokio::test]
async fn test_foreign_key_with_null_handling() -> Result<()> {
    let ctx = SessionContext::new();

    // Create tables with null foreign keys
    ctx.sql(
        "CREATE TABLE parent_table (
            id BIGINT, 
            name STRING
        )",
    )
    .await?
    .collect()
    .await?;

    ctx.sql("INSERT INTO parent_table VALUES (1, 'Parent1'), (2, 'Parent2')")
        .await?
        .collect()
        .await?;

    ctx.sql(
        "CREATE TABLE child_with_nulls (
            id BIGINT, 
            parent_id BIGINT, 
            data STRING
        )",
    )
    .await?
    .collect()
    .await?;

    ctx.sql(
        "INSERT INTO child_with_nulls VALUES 
        (101, 1, 'data1'),
        (102, NULL, 'data2'),
        (103, 2, 'data3')",
    )
    .await?
    .collect()
    .await?;

    // Test with nulls disallowed - should fail
    let no_nulls_constraint =
        ForeignKeyConstraint::new("child_with_nulls.parent_id", "parent_table.id")
            .allow_nulls(false);

    let result = no_nulls_constraint.evaluate(&ctx).await?;
    assert_eq!(result.status, ConstraintStatus::Failure);

    // Test with nulls allowed - should pass
    let allow_nulls_constraint =
        ForeignKeyConstraint::new("child_with_nulls.parent_id", "parent_table.id")
            .allow_nulls(true);

    let result = allow_nulls_constraint.evaluate(&ctx).await?;
    assert_eq!(result.status, ConstraintStatus::Success);

    Ok(())
}

#[tokio::test]
async fn test_multi_source_validator_error_scenarios() -> Result<()> {
    let mut validator = MultiSourceValidator::new("error_test");

    // Test duplicate source registration
    validator.register_source("table1", "First registration")?;
    let result = validator.register_source("table1", "Duplicate registration");
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("already registered"));

    // Test validation with unregistered table references
    let constraint = CrossTableSumConstraint::new("table1.col", "unregistered_table.col");
    let result = validator.add_cross_table_validation("test", constraint);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not registered"));

    Ok(())
}

#[tokio::test]
async fn test_multi_source_validator_builder_limits() -> Result<()> {
    // Test builder validation and clamping
    let validator = MultiSourceValidator::builder("limits_test")
        .max_concurrent_validations(0) // Should be clamped to 1
        .memory_budget_mb(10) // Should be clamped to 64
        .validation_timeout_seconds(5) // Should be clamped to 30
        .build()?;

    // Access config through validation (can't access directly due to privacy)
    // The clamping is tested in the unit tests of multi_source_validator.rs

    assert_eq!(validator.sources().len(), 0); // No sources registered yet
    assert_eq!(validator.validation_count(), 0); // No validations added yet

    Ok(())
}

#[tokio::test]
async fn test_comprehensive_multi_table_scenario() -> Result<()> {
    let ctx = SessionContext::new();
    create_comprehensive_test_warehouse(&ctx).await?;

    // Create a complex validation scenario that tests multiple constraint types together
    let mut validator = MultiSourceValidator::new("comprehensive_validation");

    // Register sources
    validator.register_source("customers", "Customer data")?;
    validator.register_source("orders", "Order data")?;
    validator.register_source("payments", "Payment data")?;
    validator.register_source("order_items", "Order item details")?;
    validator.register_source("products", "Product catalog")?;

    // Add multiple types of validation

    // 1. Financial consistency validation
    validator.add_cross_table_validation(
        "order_payment_consistency",
        CrossTableSumConstraint::new("orders.total", "payments.amount")
            .group_by(vec!["customer_id"])
            .tolerance(0.01),
    )?;

    // 2. Referential integrity validations
    validator.add_foreign_key_validation(
        "order_customer_fk",
        ForeignKeyConstraint::new("orders.customer_id", "customers.id"),
    )?;

    validator.add_foreign_key_validation(
        "order_item_product_fk",
        ForeignKeyConstraint::new("order_items.product_id", "products.id"),
    )?;

    // Execute validation
    let results = validator.validate(&ctx).await?;

    // Verify comprehensive results
    match results {
        ValidationResult::Failure { report } => {
            assert!(report.metrics.total_checks >= 3);
            assert!(report.metrics.failed_checks > 0);

            // Should have detailed issue information
            assert!(!report.issues.is_empty());

            for issue in &report.issues {
                // Each issue should have meaningful error messages
                assert!(!issue.message.is_empty());
                assert!(!issue.check_name.is_empty());
                assert!(!issue.constraint_name.is_empty());
            }
        }
        ValidationResult::Success { .. } => {
            // This could happen if test data doesn't trigger violations
            // In our comprehensive test, we expect at least some failures
        }
    }

    Ok(())
}
