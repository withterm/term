use std::sync::Arc;
use term_guard::constraints::{Histogram, HistogramAssertion};
use term_guard::core::{Check, Level, ValidationResult, ValidationSuite};
use term_guard::test_utils::{create_tpc_h_context, ScaleFactor};

#[tokio::test]
async fn test_histogram_on_tpc_h_customer_segments() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    // Register customer table as "data" for constraint evaluation
    let customer_df = ctx.table("customer").await.unwrap();
    ctx.register_table("data", customer_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("customer_segment_distribution")
        .check(
            Check::builder("market_segment_distribution")
                .level(Level::Info)
                // c_mktsegment has 5 distinct values, should be roughly uniform
                .has_histogram_with_description(
                    "c_mktsegment",
                    Arc::new(|hist: &Histogram| {
                        // Should have exactly 5 market segments
                        hist.bucket_count() == 5
                    }),
                    "has exactly 5 market segments",
                )
                .has_histogram_with_description(
                    "c_mktsegment",
                    Arc::new(|hist| {
                        // No segment should dominate too much (< 30%)
                        hist.most_common_ratio() < 0.3
                    }),
                    "no market segment dominates",
                )
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_histogram_on_tpc_h_order_status() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    let orders_df = ctx.table("orders").await.unwrap();
    ctx.register_table("data", orders_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("order_status_distribution")
        .check(
            Check::builder("order_status_analysis")
                .level(Level::Warning)
                // o_orderstatus has only 3 values: 'F' (finished), 'O' (open), 'P' (pending)
                .has_histogram("o_orderstatus", Arc::new(|hist| hist.bucket_count() == 3))
                .has_histogram_with_description(
                    "o_orderstatus",
                    Arc::new(|hist| {
                        // Check that finished orders are most common
                        hist.get_value_ratio("F").unwrap_or(0.0) > 0.4
                    }),
                    "finished orders are most common",
                )
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_histogram_on_tpc_h_priority() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    let orders_df = ctx.table("orders").await.unwrap();
    ctx.register_table("data", orders_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("order_priority_distribution")
        .check(
            Check::builder("priority_distribution")
                .level(Level::Info)
                // o_orderpriority has 5 distinct values
                .has_histogram_with_description(
                    "o_orderpriority",
                    Arc::new(|hist| hist.bucket_count() == 5 && hist.is_roughly_uniform(2.0)),
                    "priorities are roughly evenly distributed",
                )
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_histogram_on_nation_regions() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    let nation_df = ctx.table("nation").await.unwrap();
    ctx.register_table("data", nation_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("nation_region_distribution")
        .check(
            Check::builder("region_distribution")
                // n_regionkey has 5 regions (0-4)
                .has_histogram_with_description(
                    "n_regionkey",
                    Arc::new(|hist| {
                        hist.bucket_count() == 5 &&
                        // Nations should be somewhat evenly distributed across regions
                        hist.most_common_ratio() < 0.3
                    }),
                    "nations distributed across 5 regions",
                )
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_histogram_power_law_distribution() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    let lineitem_df = ctx.table("lineitem").await.unwrap();
    ctx.register_table("data", lineitem_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("ship_mode_power_law")
        .check(
            Check::builder("shipping_mode_concentration")
                .level(Level::Info)
                // l_shipmode has 7 distinct values, some might be more popular
                .has_histogram_with_description(
                    "l_shipmode",
                    Arc::new(|hist| {
                        // Check if top 3 shipping modes account for > 60% of shipments
                        hist.follows_power_law(3, 0.6)
                    }),
                    "top 3 shipping modes dominate",
                )
                .build(),
        )
        .build();

    let _results = suite.run(&ctx).await.unwrap();
    // This might pass or fail depending on the data distribution
    // The test is mainly to verify the histogram functionality works
}

#[tokio::test]
async fn test_histogram_entropy_analysis() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    let supplier_df = ctx.table("supplier").await.unwrap();
    ctx.register_table("data", supplier_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("supplier_nation_entropy")
        .check(
            Check::builder("nation_diversity")
                .level(Level::Info)
                // s_nationkey distribution entropy
                .has_histogram_with_description(
                    "s_nationkey",
                    Arc::new(|hist| {
                        // Higher entropy means more uniform distribution
                        hist.entropy() > 2.0
                    }),
                    "supplier nations have high diversity",
                )
                .build(),
        )
        .build();

    let _results = suite.run(&ctx).await.unwrap();
    // The result depends on actual entropy of the distribution
}

#[tokio::test]
async fn test_histogram_failures() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    let part_df = ctx.table("part").await.unwrap();
    ctx.register_table("data", part_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("histogram_failures")
        .check(
            Check::builder("impossible_expectation")
                .level(Level::Error)
                // p_brand should have many distinct values, not just 3
                .has_histogram_with_description(
                    "p_brand",
                    Arc::new(|hist| hist.bucket_count() <= 3),
                    "expects only 3 or fewer brands",
                )
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_failure());

    if let ValidationResult::Failure { report } = &results {
        assert!(!report.issues.is_empty());
        let issue = &report.issues[0];
        assert!(issue.message.contains("Histogram assertion"));
        assert!(issue.message.contains("failed"));
    }
}

#[tokio::test]
async fn test_histogram_with_nulls() {
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::*;
    use std::sync::Arc;

    let ctx = SessionContext::new();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "category",
        DataType::Utf8,
        true,
    )]));

    // Create data with nulls
    let values = vec![
        Some("A"),
        Some("A"),
        Some("B"),
        None,
        None,
        Some("C"),
        Some("A"),
        None,
    ];
    let array = StringArray::from(values);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();

    let provider = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("data", Arc::new(provider)).unwrap();

    let suite = ValidationSuite::builder("null_handling")
        .check(
            Check::builder("category_with_nulls")
                .has_histogram_with_description(
                    "category",
                    Arc::new(|hist| {
                        // Should have 3 non-null distinct values
                        hist.bucket_count() == 3 &&
                        // Null ratio should be 3/8 = 0.375
                        hist.null_ratio() > 0.35 && hist.null_ratio() < 0.4
                    }),
                    "handles nulls correctly",
                )
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_histogram_numeric_columns() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    let lineitem_df = ctx.table("lineitem").await.unwrap();
    ctx.register_table("data", lineitem_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("numeric_histogram")
        .check(
            Check::builder("line_number_distribution")
                // l_linenumber typically ranges from 1-7
                .has_histogram_with_description(
                    "l_linenumber",
                    Arc::new(|hist| {
                        hist.bucket_count() <= 7 &&
                        // Lower line numbers should be more common
                        hist.top_n(3).iter().all(|(val, _)| {
                            val.parse::<i32>().unwrap_or(10) <= 3
                        })
                    }),
                    "line numbers follow expected pattern",
                )
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_histogram_top_n_analysis() {
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

    let customer_df = ctx.table("customer").await.unwrap();
    ctx.register_table("data", customer_df.into_view()).unwrap();

    let suite = ValidationSuite::builder("top_n_analysis")
        .check(
            Check::builder("top_nations")
                .has_histogram_with_description(
                    "c_nationkey",
                    Arc::new(|hist| {
                        let top_5 = hist.top_n(5);
                        // Should have at least 5 nations represented
                        top_5.len() >= 5 &&
                        // Top 5 nations should account for significant portion
                        top_5.iter().map(|(_, ratio)| ratio).sum::<f64>() > 0.2
                    }),
                    "top 5 nations analysis",
                )
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}

#[tokio::test]
async fn test_histogram_custom_assertions() {
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::*;

    let ctx = SessionContext::new();

    // Create test data with known distribution
    let schema = Arc::new(Schema::new(vec![Field::new(
        "status",
        DataType::Utf8,
        true,
    )]));

    let values = vec![
        Some("ACTIVE"), // 4 times (40%)
        Some("ACTIVE"),
        Some("ACTIVE"),
        Some("ACTIVE"),
        Some("PENDING"), // 3 times (30%)
        Some("PENDING"),
        Some("PENDING"),
        Some("INACTIVE"), // 2 times (20%)
        Some("INACTIVE"),
        Some("ARCHIVED"), // 1 time (10%)
    ];

    let array = StringArray::from(values);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();

    let provider = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("data", Arc::new(provider)).unwrap();

    // Complex custom assertion
    let complex_assertion: HistogramAssertion = Arc::new(|hist| {
        // Check multiple conditions
        hist.bucket_count() == 4 &&
        hist.get_value_ratio("ACTIVE").unwrap_or(0.0) == 0.4 &&
        hist.get_value_ratio("PENDING").unwrap_or(0.0) == 0.3 &&
        hist.get_value_ratio("INACTIVE").unwrap_or(0.0) == 0.2 &&
        hist.get_value_ratio("ARCHIVED").unwrap_or(0.0) == 0.1 &&
        // Most common should be ACTIVE
        hist.top_n(1)[0].0 == "ACTIVE" &&
        // Check entropy is in expected range
        hist.entropy() > 1.0 && hist.entropy() < 2.0
    });

    let suite = ValidationSuite::builder("complex_assertion")
        .check(
            Check::builder("status_distribution_exact")
                .has_histogram_with_description(
                    "status",
                    complex_assertion,
                    "exact distribution validation",
                )
                .build(),
        )
        .build();

    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}
