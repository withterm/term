//! Integration tests for joined data sources and foreign key validation.

use std::io::Write;
use tempfile::NamedTempFile;
use term_guard::constraints::ForeignKeyConstraint;
use term_guard::core::{Check, Constraint, Level, ValidationSuite};
use term_guard::sources::{CsvSource, DataSource, JoinType, JoinedSource};
use term_guard::test_utils::{create_tpc_h_context, ScaleFactor};

/// Creates test CSV data for customers table
fn create_customers_csv() -> Result<NamedTempFile, Box<dyn std::error::Error>> {
    let mut temp_file = NamedTempFile::with_suffix(".csv")?;
    writeln!(temp_file, "c_custkey,c_name,c_nationkey")?;
    writeln!(temp_file, "1,Customer#000000001,15")?;
    writeln!(temp_file, "2,Customer#000000002,13")?;
    writeln!(temp_file, "3,Customer#000000003,1")?;
    writeln!(temp_file, "4,Customer#000000004,4")?;
    writeln!(temp_file, "5,Customer#000000005,3")?;
    temp_file.flush()?;
    Ok(temp_file)
}

/// Creates test CSV data for orders table with some foreign key violations
fn create_orders_csv() -> Result<NamedTempFile, Box<dyn std::error::Error>> {
    let mut temp_file = NamedTempFile::with_suffix(".csv")?;
    writeln!(temp_file, "o_orderkey,o_custkey,o_orderstatus,o_totalprice")?;
    writeln!(temp_file, "1,1,O,173665.47")?; // Valid FK
    writeln!(temp_file, "2,2,F,46929.18")?; // Valid FK
    writeln!(temp_file, "3,3,F,193846.25")?; // Valid FK
    writeln!(temp_file, "4,999,O,32151.78")?; // Invalid FK - customer 999 doesn't exist
    writeln!(temp_file, "5,998,P,144659.20")?; // Invalid FK - customer 998 doesn't exist
    writeln!(temp_file, "6,1,F,58749.59")?; // Valid FK
    temp_file.flush()?;
    Ok(temp_file)
}

/// Creates test CSV data for nations table
fn create_nations_csv() -> Result<NamedTempFile, Box<dyn std::error::Error>> {
    let mut temp_file = NamedTempFile::with_suffix(".csv")?;
    writeln!(temp_file, "n_nationkey,n_name,n_regionkey")?;
    writeln!(temp_file, "0,ALGERIA,0")?;
    writeln!(temp_file, "1,ARGENTINA,1")?;
    writeln!(temp_file, "2,BRAZIL,1")?;
    writeln!(temp_file, "3,CANADA,1")?;
    writeln!(temp_file, "4,EGYPT,4")?;
    writeln!(temp_file, "13,JORDAN,4")?;
    writeln!(temp_file, "15,MOROCCO,0")?;
    temp_file.flush()?;
    Ok(temp_file)
}

#[tokio::test]
async fn test_joined_source_registration() -> Result<(), Box<dyn std::error::Error>> {
    let customers_file = create_customers_csv()?;
    let orders_file = create_orders_csv()?;

    let customers_source = CsvSource::new(customers_file.path().to_string_lossy().to_string())?;
    let orders_source = CsvSource::new(orders_file.path().to_string_lossy().to_string())?;

    // Create joined source with inner join
    let joined_source = JoinedSource::builder()
        .left_source(orders_source, "orders")
        .right_source(customers_source, "customers")
        .on("o_custkey", "c_custkey")
        .build()?;

    let ctx = datafusion::prelude::SessionContext::new();
    joined_source
        .register(&ctx, "orders_with_customers")
        .await?;

    // Verify the joined table exists and can be queried
    let df = ctx
        .sql("SELECT COUNT(*) as count FROM orders_with_customers")
        .await?;
    let results = df.collect().await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 1);

    // Should have 4 rows (only valid FKs with inner join)
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(count_array.value(0), 4);

    Ok(())
}

#[tokio::test]
async fn test_foreign_key_constraint_success() -> Result<(), Box<dyn std::error::Error>> {
    let customers_file = create_customers_csv()?;
    let orders_file = create_orders_csv()?;

    let customers_source = CsvSource::new(customers_file.path().to_string_lossy().to_string())?;
    let orders_source = CsvSource::new(orders_file.path().to_string_lossy().to_string())?;

    let ctx = datafusion::prelude::SessionContext::new();
    customers_source.register(&ctx, "customers").await?;
    orders_source.register(&ctx, "orders").await?;

    // Create foreign key constraint that should find violations
    let constraint = ForeignKeyConstraint::new("orders.o_custkey", "customers.c_custkey");
    let result = constraint.evaluate(&ctx).await?;

    // Should fail because we have 2 orders with non-existent customers
    assert_eq!(result.status, term_guard::core::ConstraintStatus::Failure);
    assert_eq!(result.metric, Some(2.0)); // 2 violations

    let message = result.message.unwrap();
    assert!(message.contains("Foreign key constraint violation"));
    assert!(message.contains("2 values"));
    assert!(message.contains("orders.o_custkey"));
    assert!(message.contains("customers.c_custkey"));

    Ok(())
}

#[tokio::test]
async fn test_foreign_key_constraint_with_nulls() -> Result<(), Box<dyn std::error::Error>> {
    let mut temp_file = NamedTempFile::with_suffix(".csv")?;
    writeln!(temp_file, "o_orderkey,o_custkey,o_orderstatus")?;
    writeln!(temp_file, "1,1,O")?; // Valid FK
    writeln!(temp_file, "2,,F")?; // NULL FK
    writeln!(temp_file, "3,999,P")?; // Invalid FK
    temp_file.flush()?;

    let customers_file = create_customers_csv()?;
    let customers_source = CsvSource::new(customers_file.path().to_string_lossy().to_string())?;
    let orders_source = CsvSource::new(temp_file.path().to_string_lossy().to_string())?;

    let ctx = datafusion::prelude::SessionContext::new();
    customers_source.register(&ctx, "customers").await?;
    orders_source.register(&ctx, "orders").await?;

    // Test with nulls disallowed (default)
    let constraint = ForeignKeyConstraint::new("orders.o_custkey", "customers.c_custkey");
    let result = constraint.evaluate(&ctx).await?;

    // Should fail - both NULL and 999 are violations
    assert_eq!(result.status, term_guard::core::ConstraintStatus::Failure);

    // Test with nulls allowed
    let constraint =
        ForeignKeyConstraint::new("orders.o_custkey", "customers.c_custkey").allow_nulls(true);
    let result = constraint.evaluate(&ctx).await?;

    // Should still fail because of 999, but only 1 violation now (NULL is allowed)
    assert_eq!(result.status, term_guard::core::ConstraintStatus::Failure);
    assert_eq!(result.metric, Some(1.0)); // Only 999 is a violation

    Ok(())
}

#[tokio::test]
async fn test_multi_table_joined_validation() -> Result<(), Box<dyn std::error::Error>> {
    let customers_file = create_customers_csv()?;
    let orders_file = create_orders_csv()?;
    let nations_file = create_nations_csv()?;

    let customers_source = CsvSource::new(customers_file.path().to_string_lossy().to_string())?;
    let orders_source = CsvSource::new(orders_file.path().to_string_lossy().to_string())?;
    let nations_source = CsvSource::new(nations_file.path().to_string_lossy().to_string())?;

    let ctx = datafusion::prelude::SessionContext::new();

    // Register all tables individually for foreign key validation
    customers_source.register(&ctx, "customers").await?;
    orders_source.register(&ctx, "orders").await?;
    nations_source.register(&ctx, "nations").await?;

    // Create validation suite with multiple foreign key constraints
    let suite = ValidationSuite::builder("multi_table_referential_integrity")
        .table_name("orders") // We'll validate against orders table
        .check(
            Check::builder("order_customer_fk")
                .level(Level::Error)
                .foreign_key("orders.o_custkey", "customers.c_custkey")
                .build(),
        )
        .check(
            Check::builder("customer_nation_fk")
                .level(Level::Error)
                .foreign_key("customers.c_nationkey", "nations.n_nationkey")
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await?;

    // Should have issues from the first check (orders -> customers)
    let report = results.report();
    assert_eq!(report.issues.len(), 1); // Only the failing foreign key should create an issue

    // The suite should fail overall due to foreign key violations
    assert!(!results.is_success());

    Ok(())
}

#[tokio::test]
async fn test_joined_source_with_foreign_key_validation() -> Result<(), Box<dyn std::error::Error>>
{
    let customers_file = create_customers_csv()?;
    let orders_file = create_orders_csv()?;

    let customers_source = CsvSource::new(customers_file.path().to_string_lossy().to_string())?;
    let orders_source = CsvSource::new(orders_file.path().to_string_lossy().to_string())?;

    // Create joined source with LEFT JOIN to include all orders
    let joined_source = JoinedSource::builder()
        .left_source(orders_source, "orders")
        .right_source(customers_source, "customers")
        .on("o_custkey", "c_custkey")
        .join_type(JoinType::Left)
        .build()?;

    let ctx = datafusion::prelude::SessionContext::new();
    joined_source
        .register(&ctx, "orders_with_customers")
        .await?;

    // Also register individual tables for foreign key validation
    let customers_source2 = CsvSource::new(customers_file.path().to_string_lossy().to_string())?;
    let orders_source2 = CsvSource::new(orders_file.path().to_string_lossy().to_string())?;
    customers_source2.register(&ctx, "customers_fk").await?;
    orders_source2.register(&ctx, "orders_fk").await?;

    // Create validation suite that uses both joined data and individual tables
    let suite = ValidationSuite::builder("comprehensive_validation")
        .table_name("orders_with_customers")
        .check(
            Check::builder("referential_integrity")
                .level(Level::Error)
                .foreign_key("orders_fk.o_custkey", "customers_fk.c_custkey")
                .build(),
        )
        .check(
            Check::builder("joined_data_completeness")
                .level(Level::Warning)
                // Check that customer names are populated for valid orders
                .has_min_length("c_name", 5)
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await?;

    // Should have issues from foreign key violations
    let _report = results.report();
    assert!(!results.is_success());

    Ok(())
}

#[tokio::test]
async fn test_tpc_h_foreign_key_validation() -> Result<(), Box<dyn std::error::Error>> {
    // Use actual TPC-H data for more comprehensive testing
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await?;

    // Test foreign key relationships in TPC-H schema
    let suite = ValidationSuite::builder("tpc_h_referential_integrity")
        .table_name("orders")
        .check(
            Check::builder("orders_customer_fk")
                .level(Level::Error)
                .foreign_key("orders.o_custkey", "customer.c_custkey")
                .build(),
        )
        .check(
            Check::builder("customer_nation_fk")
                .level(Level::Error)
                .foreign_key("customer.c_nationkey", "nation.n_nationkey")
                .build(),
        )
        .check(
            Check::builder("nation_region_fk")
                .level(Level::Error)
                .foreign_key("nation.n_regionkey", "region.r_regionkey")
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await?;

    // All foreign keys in TPC-H should be valid
    assert!(
        results.is_success(),
        "TPC-H foreign key validation failed with report: {:?}",
        results.report()
    );

    Ok(())
}

#[tokio::test]
async fn test_complex_joined_source_validation() -> Result<(), Box<dyn std::error::Error>> {
    let customers_file = create_customers_csv()?;
    let orders_file = create_orders_csv()?;
    let nations_file = create_nations_csv()?;

    let customers_source = CsvSource::new(customers_file.path().to_string_lossy().to_string())?;
    let orders_source = CsvSource::new(orders_file.path().to_string_lossy().to_string())?;
    let nations_source = CsvSource::new(nations_file.path().to_string_lossy().to_string())?;

    // Create a complex 3-way join: orders -> customers -> nations
    let orders_customers = JoinedSource::builder()
        .left_source(orders_source, "orders")
        .right_source(customers_source, "customers")
        .on("o_custkey", "c_custkey")
        .join_type(JoinType::Inner)
        .build()?;

    let full_joined = JoinedSource::builder()
        .left_source(orders_customers, "orders_customers")
        .right_source(nations_source, "nations")
        .on("c_nationkey", "n_nationkey")
        .join_type(JoinType::Left)
        .build()?;

    let ctx = datafusion::prelude::SessionContext::new();
    full_joined.register(&ctx, "full_customer_view").await?;

    // Validate the complex joined view
    let df = ctx
        .sql("SELECT COUNT(*) as total, COUNT(n_name) as with_nation FROM full_customer_view")
        .await?;
    let results = df.collect().await?;

    assert_eq!(results.len(), 1);
    let total_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    let nation_array = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();

    // Should have 4 total rows (valid FKs only due to inner join orders->customers)
    assert_eq!(total_array.value(0), 4);
    // All should have nation names since we have nations for all customers
    assert_eq!(nation_array.value(0), 4);

    Ok(())
}
