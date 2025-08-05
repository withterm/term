//! Integration test demonstrating database validation with dynamic table names.

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use term_guard::core::{Check, ValidationSuite};
    use term_guard::sources::{DatabaseConfig, DatabaseSource};
    use datafusion::prelude::SessionContext;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_database_validation_with_custom_table() -> Result<(), Box<dyn std::error::Error>> {
        // Create a temporary SQLite database
        let temp_file = NamedTempFile::new()?;
        let db_path = temp_file.path().to_str().unwrap();
        
        // Initialize database with test data
        let conn = rusqlite::Connection::open(db_path)?;
        
        // Create a table with a custom name (not "data")
        conn.execute(
            "CREATE TABLE customer_orders (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                status TEXT,
                created_at TEXT NOT NULL
            )",
            [],
        )?;
        
        // Insert test data
        conn.execute_batch(
            "INSERT INTO customer_orders (order_id, customer_id, amount, status, created_at) VALUES
            (1, 100, 99.99, 'completed', '2024-01-01'),
            (2, 101, 149.50, 'completed', '2024-01-02'),
            (3, 102, 75.00, 'pending', '2024-01-03'),
            (4, 103, 200.00, NULL, '2024-01-04'),
            (5, 104, 50.00, 'completed', '2024-01-05')"
        )?;
        conn.close().unwrap();
        
        // Create database source
        let db_config = DatabaseConfig::SQLite(db_path.to_string());
        let source = DatabaseSource::new(db_config);
        
        // Create DataFusion context and register the table
        let ctx = SessionContext::new();
        source.register(&ctx, "customer_orders").await?;
        
        // Create validation suite with custom table name
        let suite = ValidationSuite::builder("customer_data_quality")
            .table_name("customer_orders")  // Specify the actual table name
            .add_check(
                Check::new("Order validation")
                    .is_complete("order_id")
                    .is_complete("customer_id")
                    .has_completeness("status", 0.75)  // 4 out of 5 have status
                    .is_non_negative("amount")
                    .has_min("amount", 50.0)
                    .has_max("amount", 200.0)
            )
            .build();
        
        // Run validation
        let result = suite.run(&ctx).await?;
        
        // Verify results
        assert!(result.is_success(), "Validation should pass");
        assert_eq!(result.total_constraints(), 6);
        assert_eq!(result.passed_constraints(), 6);
        
        // Test with a different table name
        conn.execute(
            "CREATE TABLE product_inventory (
                product_id INTEGER PRIMARY KEY,
                quantity INTEGER,
                price REAL
            )",
            [],
        )?;
        
        conn.execute_batch(
            "INSERT INTO product_inventory (product_id, quantity, price) VALUES
            (1, 100, 29.99),
            (2, NULL, 49.99),
            (3, 50, NULL)"
        )?;
        
        // Register the new table
        source.register(&ctx, "product_inventory").await?;
        
        // Create validation suite for the new table
        let inventory_suite = ValidationSuite::builder("inventory_quality")
            .table_name("product_inventory")
            .add_check(
                Check::new("Inventory validation")
                    .has_completeness("quantity", 0.6)  // 2 out of 3
                    .has_completeness("price", 0.6)     // 2 out of 3
            )
            .build();
        
        let inventory_result = inventory_suite.run(&ctx).await?;
        assert!(inventory_result.is_success());
        
        Ok(())
    }
}

#[cfg(not(feature = "sqlite"))]
fn main() {
    println!("This test requires the 'sqlite' feature to be enabled");
    println!("Run with: cargo test --features sqlite");
}