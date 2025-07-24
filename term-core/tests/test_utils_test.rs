//! Tests for the TPC-H test utilities module.

use arrow::array::{Int64Array, StringArray};
use term_core::test_utils::{create_tpc_h_context, ScaleFactor};

#[tokio::test]
async fn test_scale_factor_row_counts() {
    // Test SF0.1
    let sf01_counts = ScaleFactor::SF01.row_counts();
    assert_eq!(sf01_counts.customer, 15_000);
    assert_eq!(sf01_counts.orders, 150_000);
    assert_eq!(sf01_counts.lineitem, 600_000);
    assert_eq!(sf01_counts.part, 20_000);
    assert_eq!(sf01_counts.partsupp, 80_000);
    assert_eq!(sf01_counts.supplier, 1_000);
    assert_eq!(sf01_counts.nation, 25);
    assert_eq!(sf01_counts.region, 5);

    // Test SF1
    let sf1_counts = ScaleFactor::SF1.row_counts();
    assert_eq!(sf1_counts.customer, 150_000);
    assert_eq!(sf1_counts.orders, 1_500_000);
    assert_eq!(sf1_counts.lineitem, 6_000_000);
    assert_eq!(sf1_counts.part, 200_000);
    assert_eq!(sf1_counts.partsupp, 800_000);
    assert_eq!(sf1_counts.supplier, 10_000);
    assert_eq!(sf1_counts.nation, 25);
    assert_eq!(sf1_counts.region, 5);

    // Test SF10
    let sf10_counts = ScaleFactor::SF10.row_counts();
    assert_eq!(sf10_counts.customer, 1_500_000);
    assert_eq!(sf10_counts.orders, 15_000_000);
    assert_eq!(sf10_counts.lineitem, 60_000_000);
    assert_eq!(sf10_counts.part, 2_000_000);
    assert_eq!(sf10_counts.partsupp, 8_000_000);
    assert_eq!(sf10_counts.supplier, 100_000);
    assert_eq!(sf10_counts.nation, 25);
    assert_eq!(sf10_counts.region, 5);
}

#[tokio::test]
async fn test_tpc_h_context_creation() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Verify all tables are registered
    // Get table names from the catalog
    let catalog = ctx.catalog("datafusion").unwrap();
    let schema = catalog.schema("public").unwrap();
    let mut tables = schema.table_names();
    tables.sort();
    assert_eq!(tables.len(), 8);
    assert_eq!(
        tables,
        vec!["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
    );
}

#[tokio::test]
async fn test_region_table_data() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    let df = ctx
        .sql("SELECT * FROM region ORDER BY r_regionkey")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 5);

    // Verify region keys
    let regionkeys = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(regionkeys.value(0), 0);
    assert_eq!(regionkeys.value(1), 1);
    assert_eq!(regionkeys.value(2), 2);
    assert_eq!(regionkeys.value(3), 3);
    assert_eq!(regionkeys.value(4), 4);

    // Verify region names
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "AFRICA");
    assert_eq!(names.value(1), "AMERICA");
    assert_eq!(names.value(2), "ASIA");
    assert_eq!(names.value(3), "EUROPE");
    assert_eq!(names.value(4), "MIDDLE EAST");
}

#[tokio::test]
async fn test_nation_table_data() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    let df = ctx.sql("SELECT COUNT(*) as cnt FROM nation").await.unwrap();
    let batches = df.collect().await.unwrap();

    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 25);

    // Verify nation has correct columns
    let df = ctx
        .sql("SELECT n_nationkey, n_name, n_regionkey FROM nation WHERE n_nationkey = 0")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 1);
}

#[tokio::test]
async fn test_customer_table_structure() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Verify customer table structure
    let df = ctx.sql("SELECT * FROM customer LIMIT 1").await.unwrap();
    let schema = df.schema();

    assert_eq!(schema.fields().len(), 8);
    assert_eq!(schema.field(0).name(), "c_custkey");
    assert_eq!(schema.field(1).name(), "c_name");
    assert_eq!(schema.field(2).name(), "c_address");
    assert_eq!(schema.field(3).name(), "c_nationkey");
    assert_eq!(schema.field(4).name(), "c_phone");
    assert_eq!(schema.field(5).name(), "c_acctbal");
    assert_eq!(schema.field(6).name(), "c_mktsegment");
    assert_eq!(schema.field(7).name(), "c_comment");
}

#[tokio::test]
async fn test_orders_lineitem_relationship() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Every lineitem should have a valid order
    let df = ctx
        .sql(
            "SELECT COUNT(*) as cnt FROM lineitem l 
         LEFT JOIN orders o ON l.l_orderkey = o.o_orderkey 
         WHERE o.o_orderkey IS NULL",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let orphan_count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        orphan_count, 0,
        "All lineitems should have corresponding orders"
    );
}

#[tokio::test]
async fn test_part_partsupp_relationship() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Every partsupp should have a valid part
    let df = ctx
        .sql(
            "SELECT COUNT(*) as cnt FROM partsupp ps 
         LEFT JOIN part p ON ps.ps_partkey = p.p_partkey 
         WHERE p.p_partkey IS NULL",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let orphan_count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        orphan_count, 0,
        "All partsupp records should have corresponding parts"
    );
}

#[tokio::test]
async fn test_supplier_nation_relationship() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Every supplier should have a valid nation
    let df = ctx
        .sql(
            "SELECT COUNT(*) as cnt FROM supplier s 
         LEFT JOIN nation n ON s.s_nationkey = n.n_nationkey 
         WHERE n.n_nationkey IS NULL",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let orphan_count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        orphan_count, 0,
        "All suppliers should have valid nation keys"
    );
}

#[tokio::test]
async fn test_deterministic_data_generation() {
    // Create two contexts with the same scale factor
    let ctx1 = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();
    let ctx2 = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Query the same data from both contexts
    let df1 = ctx1
        .sql("SELECT o_orderkey, o_totalprice FROM orders ORDER BY o_orderkey LIMIT 5")
        .await
        .unwrap();
    let batches1 = df1.collect().await.unwrap();

    let df2 = ctx2
        .sql("SELECT o_orderkey, o_totalprice FROM orders ORDER BY o_orderkey LIMIT 5")
        .await
        .unwrap();
    let batches2 = df2.collect().await.unwrap();

    // Verify the data is identical
    let orderkeys1 = batches1[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let orderkeys2 = batches2[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    for i in 0..5 {
        assert_eq!(orderkeys1.value(i), orderkeys2.value(i));
    }
}

#[tokio::test]
async fn test_limited_row_counts() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Verify row counts are limited as expected
    let table_counts = vec![
        ("customer", 1000),
        ("orders", 1000),
        ("lineitem", 5000),
        ("part", 1000),
        ("partsupp", 4000),
        ("supplier", 100),
        ("nation", 25),
        ("region", 5),
    ];

    for (table, expected_max) in table_counts {
        let df = ctx
            .sql(&format!("SELECT COUNT(*) as cnt FROM {table}"))
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);

        assert!(
            count <= expected_max,
            "{table} has {count} rows, expected <= {expected_max}"
        );
        assert!(count > 0, "{table} should have at least 1 row");
    }
}

#[tokio::test]
async fn test_tpc_h_query_1() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Simplified TPC-H Query 1: Pricing Summary Report
    let df = ctx
        .sql(
            "SELECT 
            l_returnflag,
            l_linestatus,
            COUNT(*) as count_order,
            SUM(l_quantity) as sum_qty,
            SUM(l_extendedprice) as sum_base_price,
            AVG(l_quantity) as avg_qty,
            AVG(l_extendedprice) as avg_price,
            AVG(l_discount) as avg_disc
         FROM lineitem
         WHERE l_shipdate <= '1998-09-01'
         GROUP BY l_returnflag, l_linestatus
         ORDER BY l_returnflag, l_linestatus",
        )
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();
    assert!(!batches.is_empty());
    assert!(batches[0].num_rows() > 0);
}

#[tokio::test]
async fn test_tpc_h_query_3() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Simplified TPC-H Query 3: Shipping Priority
    let df = ctx
        .sql(
            "SELECT 
            l.l_orderkey,
            SUM(l.l_extendedprice * (1 - l.l_discount)) as revenue,
            o.o_orderdate,
            o.o_shippriority
         FROM customer c
         JOIN orders o ON c.c_custkey = o.o_custkey
         JOIN lineitem l ON l.l_orderkey = o.o_orderkey
         WHERE c.c_mktsegment = 'BUILDING'
           AND o.o_orderdate < '1995-03-15'
           AND l.l_shipdate > '1995-03-15'
         GROUP BY l.l_orderkey, o.o_orderdate, o.o_shippriority
         ORDER BY revenue DESC, o.o_orderdate
         LIMIT 10",
        )
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();
    // May have 0 rows due to date filters on limited test data
    assert_eq!(batches.len(), 1);
}

#[tokio::test]
async fn test_scale_factor_data_volume() {
    // Test that different scale factors produce different data volumes
    let ctx_sf01 = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();
    let ctx_sf1 = create_tpc_h_context(ScaleFactor::SF1).await.unwrap();

    // Count rows in orders table for each scale factor
    let df_sf01 = ctx_sf01
        .sql("SELECT COUNT(*) as cnt FROM orders")
        .await
        .unwrap();
    let batches_sf01 = df_sf01.collect().await.unwrap();
    let count_sf01 = batches_sf01[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);

    let df_sf1 = ctx_sf1
        .sql("SELECT COUNT(*) as cnt FROM orders")
        .await
        .unwrap();
    let batches_sf1 = df_sf1.collect().await.unwrap();
    let count_sf1 = batches_sf1[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);

    // SF1 should have more data than SF01 (but both are limited for testing)
    assert!(count_sf1 >= count_sf01);
}
