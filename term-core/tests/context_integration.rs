//! Integration tests for TermContext.

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use std::sync::Arc;
use term_core::core::{TermContext, TermContextConfig};
use term_core::prelude::*;
use term_core::test_utils::{create_tpc_h_context as create_full_tpc_h_context, ScaleFactor};

/// Creates a TPC-H style context with sample data for testing.
async fn create_tpc_h_context() -> Result<TermContext> {
    let mut ctx = TermContext::new()?;

    // Create orders table (simplified TPC-H ORDERS)
    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
        Field::new("o_orderstatus", DataType::Utf8, false),
        Field::new("o_totalprice", DataType::Float64, false),
        Field::new("o_orderdate", DataType::Utf8, false),
        Field::new("o_orderpriority", DataType::Utf8, false),
        Field::new("o_clerk", DataType::Utf8, false),
        Field::new("o_shippriority", DataType::Int64, false),
        Field::new("o_comment", DataType::Utf8, true),
    ]));

    let orders_batch = RecordBatch::try_new(
        orders_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(Int64Array::from(vec![370, 781, 1234, 1369, 2])),
            Arc::new(StringArray::from(vec!["O", "O", "F", "O", "F"])),
            Arc::new(Float64Array::from(vec![
                172799.49, 38426.09, 205654.30, 56000.91, 130094.58,
            ])),
            Arc::new(StringArray::from(vec![
                "1996-01-02",
                "1996-12-01",
                "1993-10-14",
                "1995-10-11",
                "1994-07-30",
            ])),
            Arc::new(StringArray::from(vec![
                "5-LOW", "1-URGENT", "5-LOW", "5-LOW", "1-URGENT",
            ])),
            Arc::new(StringArray::from(vec![
                "Clerk#000000951",
                "Clerk#000000880",
                "Clerk#000000955",
                "Clerk#000000124",
                "Clerk#000000925",
            ])),
            Arc::new(Int64Array::from(vec![0, 0, 0, 0, 0])),
            Arc::new(StringArray::from(vec![
                Some("nstructions sleep furiously among"),
                Some("foxes. pending accounts at the pending"),
                Some("sly final accounts boost"),
                Some("sits. slyly regular warthogs cajole"),
                None,
            ])),
        ],
    )
    .map_err(|e| TermError::Internal(format!("Failed to create record batch: {e}")))?;

    let orders_table = MemTable::try_new(orders_schema, vec![vec![orders_batch]])?;
    ctx.register_table_provider("orders", Arc::new(orders_table))
        .await?;

    // Create customer table (simplified TPC-H CUSTOMER)
    let customer_schema = Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int64, false),
        Field::new("c_name", DataType::Utf8, false),
        Field::new("c_address", DataType::Utf8, false),
        Field::new("c_nationkey", DataType::Int64, false),
        Field::new("c_phone", DataType::Utf8, false),
        Field::new("c_acctbal", DataType::Float64, false),
        Field::new("c_mktsegment", DataType::Utf8, false),
        Field::new("c_comment", DataType::Utf8, true),
    ]));

    let customer_batch = RecordBatch::try_new(
        customer_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 370, 781, 1234, 1369])),
            Arc::new(StringArray::from(vec![
                "Customer#000000001",
                "Customer#000000002",
                "Customer#000000370",
                "Customer#000000781",
                "Customer#000001234",
                "Customer#000001369",
            ])),
            Arc::new(StringArray::from(vec![
                "IVhzIApeRb ot,c,E",
                "XSTf4,NCwDVaWNe6tEgvwfmRchLXak",
                "address370",
                "address781",
                "address1234",
                "address1369",
            ])),
            Arc::new(Int64Array::from(vec![15, 13, 1, 12, 5, 8])),
            Arc::new(StringArray::from(vec![
                "25-989-741-2988",
                "23-768-687-3665",
                "11-123-456-7890",
                "22-987-654-3210",
                "15-555-123-4567",
                "18-999-888-7777",
            ])),
            Arc::new(Float64Array::from(vec![
                711.56, 121.65, 4149.43, 3691.04, 1885.38, 8817.14,
            ])),
            Arc::new(StringArray::from(vec![
                "BUILDING",
                "AUTOMOBILE",
                "BUILDING",
                "MACHINERY",
                "HOUSEHOLD",
                "FURNITURE",
            ])),
            Arc::new(StringArray::from(vec![
                Some("to the even, regular platelets."),
                Some("l accounts. blithely ironic theodolites integrate"),
                None,
                Some("ckages. requests sleep slyly"),
                Some("counts detect slyly"),
                Some("lly special packages"),
            ])),
        ],
    )
    .map_err(|e| TermError::Internal(format!("Failed to create record batch: {e}")))?;

    let customer_table = MemTable::try_new(customer_schema, vec![vec![customer_batch]])?;
    ctx.register_table_provider("customer", Arc::new(customer_table))
        .await?;

    Ok(ctx)
}

#[tokio::test]
async fn test_context_with_tpc_h_data() {
    let ctx = create_tpc_h_context().await.unwrap();

    // Verify tables are registered
    assert!(ctx.has_table("orders"));
    assert!(ctx.has_table("customer"));

    let mut tables = ctx.registered_tables();
    tables.sort();
    assert_eq!(tables, vec!["customer", "orders"]);
}

#[tokio::test]
async fn test_query_execution_with_context() {
    let ctx = create_tpc_h_context().await.unwrap();

    // Execute a simple query
    let df = ctx
        .inner()
        .sql("SELECT COUNT(*) as count FROM orders")
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();
    assert_eq!(batches.len(), 1);

    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);

    assert_eq!(count, 5);
}

#[tokio::test]
async fn test_join_query_with_context() {
    let ctx = create_tpc_h_context().await.unwrap();

    // Execute a join query
    let df = ctx
        .inner()
        .sql(
            "
            SELECT 
                o.o_orderkey,
                c.c_name,
                o.o_totalprice
            FROM orders o
            JOIN customer c ON o.o_custkey = c.c_custkey
            WHERE o.o_totalprice > 100000
            ORDER BY o.o_totalprice DESC
        ",
        )
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();
    assert!(!batches.is_empty());

    // Verify results
    let orderkeys = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert!(!orderkeys.is_empty());
}

#[tokio::test]
async fn test_memory_limited_context() {
    // Create context with small memory limit
    let config = TermContextConfig {
        max_memory: 10 * 1024 * 1024, // 10MB
        ..Default::default()
    };

    let ctx = TermContext::with_config(config).unwrap();
    assert_eq!(ctx.config().max_memory, 10 * 1024 * 1024);
}

#[tokio::test]
async fn test_context_table_cleanup() {
    let mut ctx = create_tpc_h_context().await.unwrap();

    // Clear all tables
    ctx.clear_tables().unwrap();

    assert!(ctx.registered_tables().is_empty());
    assert!(!ctx.has_table("orders"));
    assert!(!ctx.has_table("customer"));
}

#[tokio::test]
async fn test_context_drop_cleanup() {
    // Create context in a scope to trigger drop
    {
        let ctx = create_tpc_h_context().await.unwrap();
        assert_eq!(ctx.registered_tables().len(), 2);
    }
    // Context dropped here, cleanup should have occurred
}

#[tokio::test]
async fn test_full_tpc_h_context() {
    // Test the full TPC-H context from test_utils
    let ctx = create_full_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Verify all TPC-H tables are registered
    // Get table names from the catalog
    let catalog = ctx.catalog("datafusion").unwrap();
    let schema = catalog.schema("public").unwrap();
    let mut tables = schema.table_names();
    tables.sort();

    assert_eq!(
        tables,
        vec!["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
    );

    // Run a simple TPC-H query
    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM lineitem WHERE l_quantity > 30")
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();
    assert!(!batches.is_empty());
}
