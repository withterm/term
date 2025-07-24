//! Test utilities for Term validation library.
//!
//! This module provides utilities for generating test data, including TPC-H benchmark data
//! at various scale factors for comprehensive testing.

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use std::sync::Arc;

use crate::prelude::*;

/// Scale factors for TPC-H data generation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ScaleFactor {
    /// 0.1 scale factor (~10MB data)
    SF01,
    /// 1.0 scale factor (~100MB data)
    SF1,
    /// 10.0 scale factor (~1GB data)
    SF10,
}

impl ScaleFactor {
    /// Returns the numeric scale factor value.
    pub fn value(&self) -> f64 {
        match self {
            ScaleFactor::SF01 => 0.1,
            ScaleFactor::SF1 => 1.0,
            ScaleFactor::SF10 => 10.0,
        }
    }

    /// Returns the expected number of rows for each table at this scale factor.
    pub fn row_counts(&self) -> TpcHRowCounts {
        let base = TpcHRowCounts {
            customer: 150_000,
            orders: 1_500_000,
            lineitem: 6_000_000,
            part: 200_000,
            partsupp: 800_000,
            supplier: 10_000,
            nation: 25,
            region: 5,
        };

        base.scale(self.value())
    }
}

/// Row counts for TPC-H tables.
#[derive(Debug, Clone)]
pub struct TpcHRowCounts {
    pub customer: usize,
    pub orders: usize,
    pub lineitem: usize,
    pub part: usize,
    pub partsupp: usize,
    pub supplier: usize,
    pub nation: usize,
    pub region: usize,
}

impl TpcHRowCounts {
    /// Scale the row counts by a factor.
    pub fn scale(&self, factor: f64) -> Self {
        Self {
            customer: (self.customer as f64 * factor) as usize,
            orders: (self.orders as f64 * factor) as usize,
            lineitem: (self.lineitem as f64 * factor) as usize,
            part: (self.part as f64 * factor) as usize,
            partsupp: (self.partsupp as f64 * factor) as usize,
            supplier: (self.supplier as f64 * factor) as usize,
            nation: self.nation, // Nation and region tables are fixed size
            region: self.region,
        }
    }
}

/// Creates a SessionContext with TPC-H data at the specified scale factor.
///
/// # Arguments
///
/// * `scale` - The scale factor for data generation
///
/// # Returns
///
/// A SessionContext with all TPC-H tables registered
///
/// # Example
///
/// ```rust,no_run
/// use term_core::test_utils::{create_tpc_h_context, ScaleFactor};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let ctx = create_tpc_h_context(ScaleFactor::SF01).await?;
/// // Use ctx for testing...
/// # Ok(())
/// # }
/// ```
pub async fn create_tpc_h_context(scale: ScaleFactor) -> Result<SessionContext> {
    let ctx = SessionContext::new();

    // For now, create simplified TPC-H tables with deterministic data
    // In the future, we can expand this to generate more realistic data

    register_region(&ctx).await?;
    register_nation(&ctx).await?;
    register_customer(&ctx, scale).await?;
    register_orders(&ctx, scale).await?;
    register_lineitem(&ctx, scale).await?;

    // Add simplified versions of other tables for now
    register_supplier(&ctx, scale).await?;
    register_part(&ctx, scale).await?;
    register_partsupp(&ctx, scale).await?;

    Ok(ctx)
}

/// Register the REGION table.
async fn register_region(ctx: &SessionContext) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("r_regionkey", DataType::Int64, false),
        Field::new("r_name", DataType::Utf8, false),
        Field::new("r_comment", DataType::Utf8, true),
    ]));

    let regions = [
        (0, "AFRICA", "lar deposits. blithely final packages cajole"),
        (1, "AMERICA", "hs use ironic, even requests. s"),
        (2, "ASIA", "ges. thinly even pinto beans ca"),
        (3, "EUROPE", "ly final courts cajole furiously final excuse"),
        (
            4,
            "MIDDLE EAST",
            "uickly special accounts cajole carefully blithely close requests",
        ),
    ];

    let r_regionkey: Vec<i64> = regions.iter().map(|(k, _, _)| *k).collect();
    let r_name: Vec<&str> = regions.iter().map(|(_, n, _)| *n).collect();
    let r_comment: Vec<Option<&str>> = regions.iter().map(|(_, _, c)| Some(*c)).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(r_regionkey)) as ArrayRef,
            Arc::new(StringArray::from(r_name)) as ArrayRef,
            Arc::new(StringArray::from(r_comment)) as ArrayRef,
        ],
    )?;

    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("region", Arc::new(table))?;
    Ok(())
}

/// Register the NATION table.
async fn register_nation(ctx: &SessionContext) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("n_nationkey", DataType::Int64, false),
        Field::new("n_name", DataType::Utf8, false),
        Field::new("n_regionkey", DataType::Int64, false),
        Field::new("n_comment", DataType::Utf8, true),
    ]));

    let nations = vec![
        (0, "ALGERIA", 0),
        (1, "ARGENTINA", 1),
        (2, "BRAZIL", 1),
        (3, "CANADA", 1),
        (4, "EGYPT", 4),
        (5, "ETHIOPIA", 0),
        (6, "FRANCE", 3),
        (7, "GERMANY", 3),
        (8, "INDIA", 2),
        (9, "INDONESIA", 2),
        (10, "IRAN", 4),
        (11, "IRAQ", 4),
        (12, "JAPAN", 2),
        (13, "JORDAN", 4),
        (14, "KENYA", 0),
        (15, "MOROCCO", 0),
        (16, "MOZAMBIQUE", 0),
        (17, "PERU", 1),
        (18, "CHINA", 2),
        (19, "ROMANIA", 3),
        (20, "SAUDI ARABIA", 4),
        (21, "VIETNAM", 2),
        (22, "RUSSIA", 3),
        (23, "UNITED KINGDOM", 3),
        (24, "UNITED STATES", 1),
    ];

    let n_nationkey: Vec<i64> = nations.iter().map(|(k, _, _)| *k).collect();
    let n_name: Vec<&str> = nations.iter().map(|(_, n, _)| *n).collect();
    let n_regionkey: Vec<i64> = nations.iter().map(|(_, _, r)| *r).collect();
    let n_comment: Vec<Option<&str>> = (0..25)
        .map(|_| Some("special dependencies among the nations"))
        .collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(n_nationkey)) as ArrayRef,
            Arc::new(StringArray::from(n_name)) as ArrayRef,
            Arc::new(Int64Array::from(n_regionkey)) as ArrayRef,
            Arc::new(StringArray::from(n_comment)) as ArrayRef,
        ],
    )?;

    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("nation", Arc::new(table))?;
    Ok(())
}

/// Register the CUSTOMER table.
async fn register_customer(ctx: &SessionContext, scale: ScaleFactor) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int64, false),
        Field::new("c_name", DataType::Utf8, false),
        Field::new("c_address", DataType::Utf8, false),
        Field::new("c_nationkey", DataType::Int64, false),
        Field::new("c_phone", DataType::Utf8, false),
        Field::new("c_acctbal", DataType::Float64, false),
        Field::new("c_mktsegment", DataType::Utf8, false),
        Field::new("c_comment", DataType::Utf8, true),
    ]));

    let segments = [
        "AUTOMOBILE",
        "BUILDING",
        "FURNITURE",
        "HOUSEHOLD",
        "MACHINERY",
    ];
    let row_count = scale.row_counts().customer.min(1000); // Limit for testing

    let c_custkey: Vec<i64> = (1..=row_count).map(|i| i as i64).collect();
    let c_name: Vec<String> = (1..=row_count)
        .map(|i| format!("Customer#{i:09}"))
        .collect();
    let c_address: Vec<String> = (1..=row_count)
        .map(|i| {
            let addr_num = i % 100;
            format!("Address {addr_num}")
        })
        .collect();
    let c_nationkey: Vec<i64> = (1..=row_count).map(|i| (i % 25) as i64).collect();
    let c_phone: Vec<String> = (1..=row_count)
        .map(|i| {
            format!(
                "{}-{:03}-{:03}-{:04}",
                10 + (i % 25),
                i % 1000,
                (i * 7) % 1000,
                (i * 13) % 10000
            )
        })
        .collect();
    let c_acctbal: Vec<f64> = (1..=row_count)
        .map(|i| ((i * 31) % 10000) as f64 / 100.0)
        .collect();
    let c_mktsegment: Vec<&str> = (1..=row_count).map(|i| segments[i % 5]).collect();
    let c_comment: Vec<Option<String>> = (1..=row_count)
        .map(|i| {
            if i % 10 == 0 {
                None
            } else {
                Some(format!("Customer comment {i}"))
            }
        })
        .collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(c_custkey)) as ArrayRef,
            Arc::new(StringArray::from(c_name)) as ArrayRef,
            Arc::new(StringArray::from(c_address)) as ArrayRef,
            Arc::new(Int64Array::from(c_nationkey)) as ArrayRef,
            Arc::new(StringArray::from(c_phone)) as ArrayRef,
            Arc::new(Float64Array::from(c_acctbal)) as ArrayRef,
            Arc::new(StringArray::from(c_mktsegment)) as ArrayRef,
            Arc::new(StringArray::from(c_comment)) as ArrayRef,
        ],
    )?;

    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("customer", Arc::new(table))?;
    Ok(())
}

/// Register the ORDERS table.
async fn register_orders(ctx: &SessionContext, scale: ScaleFactor) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
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

    let statuses = ["F", "O", "P"];
    let priorities = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"];
    let row_count = scale.row_counts().orders.min(1000); // Limit for testing
    let customer_count = scale.row_counts().customer.min(1000);

    let o_orderkey: Vec<i64> = (1..=row_count).map(|i| i as i64).collect();
    let o_custkey: Vec<i64> = (1..=row_count)
        .map(|i| ((i * 13) % customer_count + 1) as i64)
        .collect();
    let o_orderstatus: Vec<&str> = (1..=row_count).map(|i| statuses[i % 3]).collect();
    let o_totalprice: Vec<f64> = (1..=row_count)
        .map(|i| 1000.0 + ((i * 137) % 50000) as f64)
        .collect();
    let o_orderdate: Vec<String> = (1..=row_count)
        .map(|i| {
            let year = 2 + (i % 7);
            let month = 1 + (i % 12);
            let day = 1 + (i % 28);
            format!("199{year}-{month:02}-{day:02}")
        })
        .collect();
    let o_orderpriority: Vec<&str> = (1..=row_count).map(|i| priorities[i % 5]).collect();
    let o_clerk: Vec<String> = (1..=row_count)
        .map(|i| {
            let clerk_id = (i * 7) % 1000 + 1;
            format!("Clerk#{clerk_id:09}")
        })
        .collect();
    let o_shippriority: Vec<i64> = (1..=row_count).map(|_| 0).collect();
    let o_comment: Vec<Option<String>> = (1..=row_count)
        .map(|i| {
            if i % 8 == 0 {
                None
            } else {
                Some(format!("Order comment {i}"))
            }
        })
        .collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(o_orderkey)) as ArrayRef,
            Arc::new(Int64Array::from(o_custkey)) as ArrayRef,
            Arc::new(StringArray::from(o_orderstatus)) as ArrayRef,
            Arc::new(Float64Array::from(o_totalprice)) as ArrayRef,
            Arc::new(StringArray::from(o_orderdate)) as ArrayRef,
            Arc::new(StringArray::from(o_orderpriority)) as ArrayRef,
            Arc::new(StringArray::from(o_clerk)) as ArrayRef,
            Arc::new(Int64Array::from(o_shippriority)) as ArrayRef,
            Arc::new(StringArray::from(o_comment)) as ArrayRef,
        ],
    )?;

    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("orders", Arc::new(table))?;
    Ok(())
}

/// Register the LINEITEM table.
async fn register_lineitem(ctx: &SessionContext, scale: ScaleFactor) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_partkey", DataType::Int64, false),
        Field::new("l_suppkey", DataType::Int64, false),
        Field::new("l_linenumber", DataType::Int64, false),
        Field::new("l_quantity", DataType::Float64, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_discount", DataType::Float64, false),
        Field::new("l_tax", DataType::Float64, false),
        Field::new("l_returnflag", DataType::Utf8, false),
        Field::new("l_linestatus", DataType::Utf8, false),
        Field::new("l_shipdate", DataType::Utf8, false),
        Field::new("l_commitdate", DataType::Utf8, false),
        Field::new("l_receiptdate", DataType::Utf8, false),
        Field::new("l_shipinstruct", DataType::Utf8, false),
        Field::new("l_shipmode", DataType::Utf8, false),
        Field::new("l_comment", DataType::Utf8, true),
    ]));

    let returnflags = ["R", "A", "N"];
    let linestatuses = ["O", "F"];
    let shipinstructs = [
        "DELIVER IN PERSON",
        "COLLECT COD",
        "NONE",
        "TAKE BACK RETURN",
    ];
    let shipmodes = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"];

    let row_count = scale.row_counts().lineitem.min(5000); // Limit for testing
    let orders_count = scale.row_counts().orders.min(1000);

    let mut l_orderkey = Vec::with_capacity(row_count);
    let mut l_partkey = Vec::with_capacity(row_count);
    let mut l_suppkey = Vec::with_capacity(row_count);
    let mut l_linenumber = Vec::with_capacity(row_count);
    let mut l_quantity = Vec::with_capacity(row_count);
    let mut l_extendedprice = Vec::with_capacity(row_count);
    let mut l_discount = Vec::with_capacity(row_count);
    let mut l_tax = Vec::with_capacity(row_count);
    let mut l_returnflag = Vec::with_capacity(row_count);
    let mut l_linestatus = Vec::with_capacity(row_count);
    let mut l_shipdate = Vec::with_capacity(row_count);
    let mut l_commitdate = Vec::with_capacity(row_count);
    let mut l_receiptdate = Vec::with_capacity(row_count);
    let mut l_shipinstruct = Vec::with_capacity(row_count);
    let mut l_shipmode = Vec::with_capacity(row_count);
    let mut l_comment = Vec::with_capacity(row_count);

    // Generate line items - average 4 per order
    let mut item_count = 0;
    for order_id in 1..=orders_count {
        let lines_for_order = 1 + (order_id % 7); // 1-7 lines per order
        for line_num in 1..=lines_for_order {
            if item_count >= row_count {
                break;
            }

            l_orderkey.push(order_id as i64);
            l_partkey.push(((order_id * 17 + line_num * 7) % 1000 + 1) as i64);
            l_suppkey.push(((order_id * 13 + line_num * 5) % 100 + 1) as i64);
            l_linenumber.push(line_num as i64);

            let quantity = 1.0 + (item_count % 50) as f64;
            let price = 100.0 + ((item_count * 37) % 2000) as f64;
            l_quantity.push(quantity);
            l_extendedprice.push(quantity * price);
            l_discount.push((item_count % 11) as f64 / 100.0);
            l_tax.push((item_count % 9) as f64 / 100.0);
            l_returnflag.push(returnflags[item_count % 3]);
            l_linestatus.push(linestatuses[item_count % 2]);
            l_shipdate.push(format!(
                "199{}-{:02}-{:02}",
                2 + (item_count % 7),
                1 + (item_count % 12),
                1 + (item_count % 28)
            ));
            l_commitdate.push(format!(
                "199{}-{:02}-{:02}",
                2 + ((item_count + 30) % 7),
                1 + ((item_count + 15) % 12),
                1 + ((item_count + 10) % 28)
            ));
            l_receiptdate.push(format!(
                "199{}-{:02}-{:02}",
                2 + ((item_count + 60) % 7),
                1 + ((item_count + 30) % 12),
                1 + ((item_count + 20) % 28)
            ));
            l_shipinstruct.push(shipinstructs[item_count % 4]);
            l_shipmode.push(shipmodes[item_count % 7]);
            l_comment.push(if item_count % 10 == 0 {
                None
            } else {
                Some(format!("Line comment {item_count}"))
            });

            item_count += 1;
        }
        if item_count >= row_count {
            break;
        }
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(l_orderkey)) as ArrayRef,
            Arc::new(Int64Array::from(l_partkey)) as ArrayRef,
            Arc::new(Int64Array::from(l_suppkey)) as ArrayRef,
            Arc::new(Int64Array::from(l_linenumber)) as ArrayRef,
            Arc::new(Float64Array::from(l_quantity)) as ArrayRef,
            Arc::new(Float64Array::from(l_extendedprice)) as ArrayRef,
            Arc::new(Float64Array::from(l_discount)) as ArrayRef,
            Arc::new(Float64Array::from(l_tax)) as ArrayRef,
            Arc::new(StringArray::from(l_returnflag)) as ArrayRef,
            Arc::new(StringArray::from(l_linestatus)) as ArrayRef,
            Arc::new(StringArray::from(l_shipdate)) as ArrayRef,
            Arc::new(StringArray::from(l_commitdate)) as ArrayRef,
            Arc::new(StringArray::from(l_receiptdate)) as ArrayRef,
            Arc::new(StringArray::from(l_shipinstruct)) as ArrayRef,
            Arc::new(StringArray::from(l_shipmode)) as ArrayRef,
            Arc::new(StringArray::from(l_comment)) as ArrayRef,
        ],
    )?;

    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("lineitem", Arc::new(table))?;
    Ok(())
}

/// Register the SUPPLIER table.
async fn register_supplier(ctx: &SessionContext, scale: ScaleFactor) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("s_suppkey", DataType::Int64, false),
        Field::new("s_name", DataType::Utf8, false),
        Field::new("s_address", DataType::Utf8, false),
        Field::new("s_nationkey", DataType::Int64, false),
        Field::new("s_phone", DataType::Utf8, false),
        Field::new("s_acctbal", DataType::Float64, false),
        Field::new("s_comment", DataType::Utf8, true),
    ]));

    let row_count = scale.row_counts().supplier.min(100); // Limit for testing

    let s_suppkey: Vec<i64> = (1..=row_count).map(|i| i as i64).collect();
    let s_name: Vec<String> = (1..=row_count)
        .map(|i| format!("Supplier#{i:09}"))
        .collect();
    let s_address: Vec<String> = (1..=row_count)
        .map(|i| {
            let addr_num = i % 50;
            format!("Supplier Address {addr_num}")
        })
        .collect();
    let s_nationkey: Vec<i64> = (1..=row_count).map(|i| (i % 25) as i64).collect();
    let s_phone: Vec<String> = (1..=row_count)
        .map(|i| {
            format!(
                "{}-{:03}-{:03}-{:04}",
                10 + (i % 25),
                i % 1000,
                (i * 3) % 1000,
                (i * 7) % 10000
            )
        })
        .collect();
    let s_acctbal: Vec<f64> = (1..=row_count)
        .map(|i| -999.99 + ((i * 47) % 11000) as f64)
        .collect();
    let s_comment: Vec<Option<String>> = (1..=row_count)
        .map(|i| {
            if i % 7 == 0 {
                None
            } else {
                Some(format!("Supplier comment {i}"))
            }
        })
        .collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(s_suppkey)) as ArrayRef,
            Arc::new(StringArray::from(s_name)) as ArrayRef,
            Arc::new(StringArray::from(s_address)) as ArrayRef,
            Arc::new(Int64Array::from(s_nationkey)) as ArrayRef,
            Arc::new(StringArray::from(s_phone)) as ArrayRef,
            Arc::new(Float64Array::from(s_acctbal)) as ArrayRef,
            Arc::new(StringArray::from(s_comment)) as ArrayRef,
        ],
    )?;

    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("supplier", Arc::new(table))?;
    Ok(())
}

/// Register the PART table.
async fn register_part(ctx: &SessionContext, scale: ScaleFactor) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("p_partkey", DataType::Int64, false),
        Field::new("p_name", DataType::Utf8, false),
        Field::new("p_mfgr", DataType::Utf8, false),
        Field::new("p_brand", DataType::Utf8, false),
        Field::new("p_type", DataType::Utf8, false),
        Field::new("p_size", DataType::Int64, false),
        Field::new("p_container", DataType::Utf8, false),
        Field::new("p_retailprice", DataType::Float64, false),
        Field::new("p_comment", DataType::Utf8, true),
    ]));

    let types = ["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"];
    let containers = ["SM BAG", "LG BOX", "MED PACK", "JUMBO JAR", "WRAP CASE"];
    let row_count = scale.row_counts().part.min(1000); // Limit for testing

    let p_partkey: Vec<i64> = (1..=row_count).map(|i| i as i64).collect();
    let p_name: Vec<String> = (1..=row_count).map(|i| format!("Part Name {i}")).collect();
    let p_mfgr: Vec<String> = (1..=row_count)
        .map(|i| {
            let mfg_num = 1 + (i % 5);
            format!("Manufacturer#{mfg_num}")
        })
        .collect();
    let p_brand: Vec<String> = (1..=row_count)
        .map(|i| {
            let brand1 = 1 + (i % 5);
            let brand2 = 1 + ((i * 3) % 5);
            format!("Brand#{brand1}{brand2}")
        })
        .collect();
    let p_type: Vec<String> = (1..=row_count)
        .map(|i| {
            let part_type = types[i % types.len()];
            format!("{part_type} BRASS")
        })
        .collect();
    let p_size: Vec<i64> = (1..=row_count).map(|i| (1 + (i % 50)) as i64).collect();
    let p_container: Vec<&str> = (1..=row_count)
        .map(|i| containers[i % containers.len()])
        .collect();
    let p_retailprice: Vec<f64> = (1..=row_count)
        .map(|i| 900.0 + (i as f64 * 0.1) + 1.0)
        .collect();
    let p_comment: Vec<Option<String>> = (1..=row_count)
        .map(|i| {
            if i % 6 == 0 {
                None
            } else {
                Some(format!("Part comment {i}"))
            }
        })
        .collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(p_partkey)) as ArrayRef,
            Arc::new(StringArray::from(p_name)) as ArrayRef,
            Arc::new(StringArray::from(p_mfgr)) as ArrayRef,
            Arc::new(StringArray::from(p_brand)) as ArrayRef,
            Arc::new(StringArray::from(p_type)) as ArrayRef,
            Arc::new(Int64Array::from(p_size)) as ArrayRef,
            Arc::new(StringArray::from(p_container)) as ArrayRef,
            Arc::new(Float64Array::from(p_retailprice)) as ArrayRef,
            Arc::new(StringArray::from(p_comment)) as ArrayRef,
        ],
    )?;

    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("part", Arc::new(table))?;
    Ok(())
}

/// Register the PARTSUPP table.
async fn register_partsupp(ctx: &SessionContext, scale: ScaleFactor) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ps_partkey", DataType::Int64, false),
        Field::new("ps_suppkey", DataType::Int64, false),
        Field::new("ps_availqty", DataType::Int64, false),
        Field::new("ps_supplycost", DataType::Float64, false),
        Field::new("ps_comment", DataType::Utf8, true),
    ]));

    let row_count = scale.row_counts().partsupp.min(2000); // Limit for testing
    let parts_count = scale.row_counts().part.min(1000);
    let suppliers_count = scale.row_counts().supplier.min(100);

    let mut ps_partkey = Vec::with_capacity(row_count);
    let mut ps_suppkey = Vec::with_capacity(row_count);
    let mut ps_availqty = Vec::with_capacity(row_count);
    let mut ps_supplycost = Vec::with_capacity(row_count);
    let mut ps_comment = Vec::with_capacity(row_count);

    // Each part has multiple suppliers (average 4)
    let mut item_count = 0;
    for part_id in 1..=parts_count {
        let suppliers_for_part = 1 + (part_id % 5); // 1-5 suppliers per part
        for supp_num in 0..suppliers_for_part {
            if item_count >= row_count {
                break;
            }

            ps_partkey.push(part_id as i64);
            ps_suppkey.push(((part_id * 7 + supp_num * 13) % suppliers_count + 1) as i64);
            ps_availqty.push((1 + (item_count % 9999)) as i64);
            ps_supplycost.push(1.0 + ((item_count * 23) % 1000) as f64);
            ps_comment.push(if item_count % 5 == 0 {
                None
            } else {
                Some(format!("PartSupp comment {item_count}"))
            });

            item_count += 1;
        }
        if item_count >= row_count {
            break;
        }
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ps_partkey)) as ArrayRef,
            Arc::new(Int64Array::from(ps_suppkey)) as ArrayRef,
            Arc::new(Int64Array::from(ps_availqty)) as ArrayRef,
            Arc::new(Float64Array::from(ps_supplycost)) as ArrayRef,
            Arc::new(StringArray::from(ps_comment)) as ArrayRef,
        ],
    )?;

    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("partsupp", Arc::new(table))?;
    Ok(())
}

/// Creates a small test dataset for quick unit tests.
///
/// This creates a minimal dataset with just a few rows for each table,
/// suitable for testing basic functionality without the overhead of
/// generating full TPC-H data.
pub async fn create_test_context() -> Result<SessionContext> {
    let ctx = SessionContext::new();

    // Just create orders table with minimal data
    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
        Field::new("o_orderstatus", DataType::Utf8, false),
        Field::new("o_totalprice", DataType::Float64, false),
        Field::new("o_orderdate", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        orders_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
            Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500])) as ArrayRef,
            Arc::new(StringArray::from(vec!["F", "O", "F", "O", "P"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![
                1000.0, 2000.0, 3000.0, 4000.0, 5000.0,
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                "1998-01-01",
                "1998-01-02",
                "1998-01-03",
                "1998-01-04",
                "1998-01-05",
            ])) as ArrayRef,
        ],
    )?;

    let table = MemTable::try_new(orders_schema, vec![vec![batch]])?;
    ctx.register_table("orders", Arc::new(table))?;

    Ok(ctx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_scale_factor_values() {
        assert_eq!(ScaleFactor::SF01.value(), 0.1);
        assert_eq!(ScaleFactor::SF1.value(), 1.0);
        assert_eq!(ScaleFactor::SF10.value(), 10.0);
    }

    #[tokio::test]
    async fn test_row_counts() {
        let counts = ScaleFactor::SF01.row_counts();
        assert_eq!(counts.customer, 15_000);
        assert_eq!(counts.orders, 150_000);
        assert_eq!(counts.nation, 25); // Fixed size
        assert_eq!(counts.region, 5); // Fixed size
    }

    #[tokio::test]
    async fn test_create_test_context() {
        let ctx = create_test_context().await.unwrap();

        // Verify the orders table exists
        let df = ctx.sql("SELECT COUNT(*) FROM orders").await.unwrap();
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
    async fn test_create_tpc_h_context_sf01() {
        let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

        // Verify all tables are registered
        let tables = [
            "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
        ];
        for table in &tables {
            let df = ctx
                .sql(&format!("SELECT COUNT(*) FROM {table}"))
                .await
                .unwrap();
            let batches = df.collect().await.unwrap();
            assert!(!batches.is_empty(), "Table {table} should have data");
        }
    }

    #[tokio::test]
    async fn test_tpc_h_query_execution() {
        let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

        // Test a simple TPC-H style query
        let df = ctx
            .sql(
                r#"
            SELECT 
                l_returnflag,
                l_linestatus,
                COUNT(*) as count_order
            FROM lineitem
            GROUP BY l_returnflag, l_linestatus
            ORDER BY l_returnflag, l_linestatus
        "#,
            )
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        assert!(!batches.is_empty());
    }

    #[tokio::test]
    async fn test_tpc_h_join_query() {
        let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

        // Test a join query
        let df = ctx
            .sql(
                r#"
            SELECT 
                COUNT(*) as order_count
            FROM orders o
            JOIN customer c ON o.o_custkey = c.c_custkey
            WHERE o.o_totalprice > 10000
        "#,
            )
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        assert!(!batches.is_empty());
    }
}
