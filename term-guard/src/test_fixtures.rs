//! Common test fixtures for data validation scenarios.
//!
//! This module provides pre-configured test scenarios commonly used in data validation,
//! built on top of the TPC-H test data. These fixtures simplify writing tests for
//! data quality checks.

use crate::error::Result;
use crate::test_utils::{create_tpc_h_context, ScaleFactor};
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use std::sync::Arc;

/// Creates a context with data containing null values for testing completeness checks.
pub async fn create_context_with_nulls() -> Result<SessionContext> {
    let ctx = SessionContext::new();

    // Create a table with various null patterns
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("email", DataType::Utf8, true),
        Field::new("phone", DataType::Utf8, true),
        Field::new("age", DataType::Int64, true),
        Field::new("score", DataType::Float64, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])),
            Arc::new(StringArray::from(vec![
                Some("Alice"),
                None,
                Some("Charlie"),
                Some("David"),
                None,
                Some("Frank"),
                Some("Grace"),
                None,
                Some("Ivan"),
                Some("Jane"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("alice@example.com"),
                Some("bob@example.com"),
                None,
                Some("david@example.com"),
                Some("eve@example.com"),
                None,
                Some("grace@example.com"),
                Some("henry@example.com"),
                None,
                Some("jane@example.com"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("555-0001"),
                Some("555-0002"),
                Some("555-0003"),
                None,
                None,
                None,
                Some("555-0007"),
                Some("555-0008"),
                Some("555-0009"),
                None,
            ])),
            Arc::new(Int64Array::from(vec![
                Some(25),
                Some(30),
                Some(35),
                Some(40),
                None,
                Some(28),
                None,
                Some(33),
                Some(29),
                Some(31),
            ])),
            Arc::new(Float64Array::from(vec![
                Some(85.5),
                Some(92.0),
                None,
                Some(78.5),
                Some(88.0),
                Some(91.5),
                Some(76.0),
                None,
                None,
                Some(83.5),
            ])),
        ],
    )?;

    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("users_with_nulls", Arc::new(table))?;

    Ok(ctx)
}

/// Creates a context with duplicate data for testing uniqueness checks.
pub async fn create_context_with_duplicates() -> Result<SessionContext> {
    let ctx = SessionContext::new();

    // Create a table with duplicate records
    let schema = Arc::new(Schema::new(vec![
        Field::new("transaction_id", DataType::Utf8, false),
        Field::new("user_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("timestamp", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                "TX001", "TX002", "TX003", "TX001", "TX004", // TX001 is duplicated
                "TX005", "TX002", "TX006", "TX007", "TX008", // TX002 is duplicated
            ])),
            Arc::new(Int64Array::from(vec![
                101, 102, 103, 101, 104, 105, 102, 106, 107, 108,
            ])),
            Arc::new(Float64Array::from(vec![
                100.50, 250.00, 75.25, 100.50, 300.00, 150.75, 250.00, 80.00, 425.50, 60.00,
            ])),
            Arc::new(StringArray::from(vec![
                "2024-01-01 10:00:00",
                "2024-01-01 11:00:00",
                "2024-01-01 12:00:00",
                "2024-01-01 10:00:00",
                "2024-01-01 13:00:00",
                "2024-01-01 14:00:00",
                "2024-01-01 11:00:00",
                "2024-01-01 15:00:00",
                "2024-01-01 16:00:00",
                "2024-01-01 17:00:00",
            ])),
            Arc::new(StringArray::from(vec![
                "completed",
                "pending",
                "completed",
                "completed",
                "failed",
                "completed",
                "pending",
                "completed",
                "completed",
                "pending",
            ])),
        ],
    )?;

    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("transactions_with_duplicates", Arc::new(table))?;

    Ok(ctx)
}

/// Creates a context with outlier data for testing statistical checks.
pub async fn create_context_with_outliers() -> Result<SessionContext> {
    let ctx = SessionContext::new();

    // Create a table with outliers
    let schema = Arc::new(Schema::new(vec![
        Field::new("sensor_id", DataType::Int64, false),
        Field::new("temperature", DataType::Float64, false),
        Field::new("humidity", DataType::Float64, false),
        Field::new("pressure", DataType::Float64, false),
        Field::new("timestamp", DataType::Utf8, false),
    ]));

    // Most values are normal, but include some outliers
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
            ])),
            Arc::new(Float64Array::from(vec![
                22.5, 23.0, 22.8, 23.2, 22.9, // Normal temperatures (22-24°C)
                85.0, // Outlier!
                23.1, 22.7, 23.3, 22.6, -40.0, // Outlier!
                23.0, 22.9, 23.4, 22.8,
            ])),
            Arc::new(Float64Array::from(vec![
                45.0, 46.0, 44.5, 45.5, 46.2, // Normal humidity (40-60%)
                45.8, 99.9, 44.9, 45.3, 46.1, // 99.9 is an outlier
                5.0, 45.7, 44.8, 46.0, 45.2, // 5.0 is an outlier
            ])),
            Arc::new(Float64Array::from(vec![
                1013.25, 1013.00, 1012.85, 1013.10, 1012.95, // Normal pressure
                1013.15, 1012.90, 850.0, 1013.05, 1012.80, // 850.0 is an outlier
                1013.20, 1200.0, 1012.75, 1013.30, 1012.85, // 1200.0 is an outlier
            ])),
            Arc::new(StringArray::from(vec![
                "2024-01-01 00:00:00",
                "2024-01-01 00:05:00",
                "2024-01-01 00:10:00",
                "2024-01-01 00:15:00",
                "2024-01-01 00:20:00",
                "2024-01-01 00:25:00",
                "2024-01-01 00:30:00",
                "2024-01-01 00:35:00",
                "2024-01-01 00:40:00",
                "2024-01-01 00:45:00",
                "2024-01-01 00:50:00",
                "2024-01-01 00:55:00",
                "2024-01-01 01:00:00",
                "2024-01-01 01:05:00",
                "2024-01-01 01:10:00",
            ])),
        ],
    )?;

    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("sensor_data_with_outliers", Arc::new(table))?;

    Ok(ctx)
}

/// Creates a context with invalid format data for testing pattern checks.
pub async fn create_context_with_invalid_formats() -> Result<SessionContext> {
    let ctx = SessionContext::new();

    // Create a table with various format issues
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("email", DataType::Utf8, false),
        Field::new("phone", DataType::Utf8, false),
        Field::new("postal_code", DataType::Utf8, false),
        Field::new("credit_card", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])),
            Arc::new(StringArray::from(vec![
                "valid@example.com",
                "invalid-email", // Missing @ and domain
                "another@valid.org",
                "no-at-sign.com", // Missing @
                "user@domain",    // Missing TLD
                "good@email.net",
                "@missing-local.com",       // Missing local part
                "trailing-dot@domain.com.", // Trailing dot
                "spaces in@email.com",      // Spaces
                "valid@subdomain.example.com",
            ])),
            Arc::new(StringArray::from(vec![
                "+1-555-123-4567",
                "555-123-4567",
                "(555) 123-4567",
                "5551234567",
                "123", // Too short
                "+44-20-7123-4567",
                "555-CALL-NOW", // Letters
                "+1(555)123-4567",
                "555.123.4567",
                "1-800-FLOWERS", // Mixed letters/numbers
            ])),
            Arc::new(StringArray::from(vec![
                "12345",
                "12345-6789",
                "ABC123", // Letters in US postal code
                "1234",   // Too short
                "123456", // Too long for US 5-digit
                "90210",
                "K1A 0B1",     // Canadian format
                "12345-67890", // Too long ZIP+4
                "00000",       // All zeros
                "99999",
            ])),
            Arc::new(StringArray::from(vec![
                "4532-1234-5678-9012",  // Valid format
                "5432123456789012",     // Valid format, no dashes
                "1234-5678-9012-3456",  // Valid format
                "1234567890123456",     // Valid format, no dashes
                "1234-5678-9012",       // Too short
                "not-a-credit-card",    // Invalid
                "4532 1234 5678 9012",  // Spaces instead of dashes
                "4532-1234-5678-901X",  // Letter in number
                "4532-1234-5678-90123", // Too long
                "0000-0000-0000-0000",  // All zeros
            ])),
        ],
    )?;

    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("user_data_with_invalid_formats", Arc::new(table))?;

    Ok(ctx)
}

/// Creates a context with time series data containing gaps and anomalies.
pub async fn create_context_with_time_series_issues() -> Result<SessionContext> {
    let ctx = SessionContext::new();

    // Create a table with time series issues
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Utf8, false),
        Field::new("metric_value", DataType::Float64, false),
        Field::new("device_id", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                "2024-01-01 00:00:00",
                "2024-01-01 00:05:00",
                "2024-01-01 00:10:00",
                // Gap: missing 00:15:00
                "2024-01-01 00:20:00",
                "2024-01-01 00:25:00",
                // Gap: missing 00:30:00 and 00:35:00
                "2024-01-01 00:40:00",
                "2024-01-01 00:45:00",
                "2024-01-01 00:50:00",
                "2024-01-01 00:55:00",
                "2024-01-01 01:00:00",
                // Duplicate timestamp
                "2024-01-01 01:00:00",
                "2024-01-01 01:05:00",
            ])),
            Arc::new(Float64Array::from(vec![
                100.0, 102.5, 101.8, 98.5,  // After gap
                150.0, // Sudden spike
                103.2, 50.0, // Sudden drop
                101.5, 102.0, 100.8, 101.2, 101.5,
            ])),
            Arc::new(StringArray::from(vec![
                "device_001",
                "device_001",
                "device_001",
                "device_001",
                "device_001",
                "device_001",
                "device_002",
                "device_002",
                "device_002",
                "device_002",
                "device_001",
                "device_002",
            ])),
            Arc::new(StringArray::from(vec![
                Some("active"),
                Some("active"),
                Some("active"),
                Some("active"),
                Some("warning"),
                Some("active"),
                Some("error"),
                Some("active"),
                Some("active"),
                Some("active"),
                Some("active"),
                None,
            ])),
        ],
    )?;

    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("time_series_with_issues", Arc::new(table))?;

    Ok(ctx)
}

/// Creates a context with relational integrity issues.
pub async fn create_context_with_referential_issues() -> Result<SessionContext> {
    let ctx = SessionContext::new();

    // Create parent table (products)
    let products_schema = Arc::new(Schema::new(vec![
        Field::new("product_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("category", DataType::Utf8, false),
    ]));

    let products_batch = RecordBatch::try_new(
        products_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "Product A",
                "Product B",
                "Product C",
                "Product D",
                "Product E",
            ])),
            Arc::new(StringArray::from(vec![
                "Electronics",
                "Clothing",
                "Electronics",
                "Food",
                "Clothing",
            ])),
        ],
    )?;

    let products_table = MemTable::try_new(products_schema, vec![vec![products_batch]])?;
    ctx.register_table("products", Arc::new(products_table))?;

    // Create child table (orders) with some orphaned records
    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("product_id", DataType::Int64, false),
        Field::new("quantity", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
    ]));

    let orders_batch = RecordBatch::try_new(
        orders_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![
                101, 102, 103, 104, 105, 106, 107, 108,
            ])),
            Arc::new(Int64Array::from(vec![
                1, 2, 3, 6, // Invalid: product_id 6 doesn't exist
                2, 7, // Invalid: product_id 7 doesn't exist
                4, 5,
            ])),
            Arc::new(Int64Array::from(vec![2, 1, 3, 1, 2, 1, 5, 2])),
            Arc::new(Int64Array::from(vec![
                201, 202, 203, 204, 205, 206, 207, 208,
            ])),
        ],
    )?;

    let orders_table = MemTable::try_new(orders_schema, vec![vec![orders_batch]])?;
    ctx.register_table("orders_with_orphans", Arc::new(orders_table))?;

    Ok(ctx)
}

/// Creates a minimal TPC-H context for quick testing.
pub async fn create_minimal_tpc_h_context() -> Result<SessionContext> {
    create_tpc_h_context(ScaleFactor::SF01).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_nulls_fixture() {
        let ctx = create_context_with_nulls().await.unwrap();

        // Verify the table exists and has expected null patterns
        let df = ctx
            .sql("SELECT COUNT(*) as total, COUNT(name) as non_null_names FROM users_with_nulls")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();

        let total = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let non_null_names = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);

        assert_eq!(total, 10);
        assert_eq!(non_null_names, 7); // 3 nulls in name column
    }

    #[tokio::test]
    async fn test_duplicates_fixture() {
        let ctx = create_context_with_duplicates().await.unwrap();

        // Verify duplicates exist
        let df = ctx
            .sql(
                "SELECT transaction_id, COUNT(*) as cnt 
             FROM transactions_with_duplicates 
             GROUP BY transaction_id 
             HAVING COUNT(*) > 1 
             ORDER BY transaction_id",
            )
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();

        assert_eq!(batches[0].num_rows(), 2); // TX001 and TX002 are duplicated
    }

    #[tokio::test]
    async fn test_outliers_fixture() {
        let ctx = create_context_with_outliers().await.unwrap();

        // Check for temperature outliers
        let df = ctx
            .sql(
                "SELECT COUNT(*) as outliers 
             FROM sensor_data_with_outliers 
             WHERE temperature < 0 OR temperature > 50",
            )
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();

        let outliers = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(outliers, 2); // 85.0 and -40.0
    }

    #[tokio::test]
    async fn test_referential_issues_fixture() {
        let ctx = create_context_with_referential_issues().await.unwrap();

        // Find orphaned orders
        let df = ctx
            .sql(
                "SELECT COUNT(*) as orphans 
             FROM orders_with_orphans o 
             LEFT JOIN products p ON o.product_id = p.product_id 
             WHERE p.product_id IS NULL",
            )
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();

        let orphans = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(orphans, 2); // product_id 6 and 7 don't exist
    }
}
