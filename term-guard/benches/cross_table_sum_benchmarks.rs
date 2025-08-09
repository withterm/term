//! Performance benchmarks for CrossTableSumConstraint operations
//!
//! This benchmark suite evaluates the performance characteristics of cross-table sum
//! operations across different data sizes, grouping scenarios, and query complexity.
//! It specifically focuses on real-world usage patterns and scalability.

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use rand::prelude::*;
use std::sync::Arc;
use term_guard::constraints::{CrossTableSumBuilder, CrossTableSumConstraint};
use term_guard::core::Constraint;

/// Data scales to benchmark against
const DATA_SCALES: &[usize] = &[1_000, 10_000, 100_000, 1_000_000];

/// Group cardinalities for grouped benchmarks
const GROUP_CARDINALITIES: &[usize] = &[10, 100, 1_000, 10_000];

/// Creates a pair of related tables with the specified number of rows and optional mismatch rate
async fn create_cross_table_test_data(
    ctx: &SessionContext,
    rows: usize,
    mismatch_rate: f64,
    groups: Option<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut rng = thread_rng();

    // Create orders table
    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, true),
        Field::new("product_id", DataType::Int64, true),
        Field::new("region", DataType::Utf8, true),
        Field::new("total", DataType::Float64, false),
    ]));

    let mut order_ids = Vec::with_capacity(rows);
    let mut customer_ids = Vec::with_capacity(rows);
    let mut product_ids = Vec::with_capacity(rows);
    let mut regions = Vec::with_capacity(rows);
    let mut totals = Vec::with_capacity(rows);

    let group_count = groups.unwrap_or(1);
    let regions_list = ["North", "South", "East", "West", "Central"];

    for i in 0..rows {
        order_ids.push(i as i64);
        customer_ids.push(Some((i % group_count) as i64));
        product_ids.push(Some((i % (group_count * 5)) as i64));
        regions.push(Some(regions_list[i % regions_list.len()].to_string()));

        // Generate amounts between 10.0 and 1000.0
        let amount = rng.gen_range(10.0..1000.0);
        totals.push(amount);
    }

    let orders_batch = RecordBatch::try_new(
        orders_schema.clone(),
        vec![
            Arc::new(Int64Array::from(order_ids)),
            Arc::new(Int64Array::from(customer_ids)),
            Arc::new(Int64Array::from(product_ids)),
            Arc::new(StringArray::from(regions)),
            Arc::new(Float64Array::from(totals.clone())),
        ],
    )?;

    let orders_table = MemTable::try_new(orders_schema, vec![vec![orders_batch]])?;
    ctx.register_table("orders", Arc::new(orders_table))?;

    // Create payments table with potential mismatches
    let payments_schema = Arc::new(Schema::new(vec![
        Field::new("payment_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, true),
        Field::new("product_id", DataType::Int64, true),
        Field::new("region", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, false),
    ]));

    let mut payment_ids = Vec::with_capacity(rows);
    let mut payment_customer_ids = Vec::with_capacity(rows);
    let mut payment_product_ids = Vec::with_capacity(rows);
    let mut payment_regions = Vec::with_capacity(rows);
    let mut amounts = Vec::with_capacity(rows);

    for i in 0..rows {
        payment_ids.push(i as i64);
        payment_customer_ids.push(Some((i % group_count) as i64));
        payment_product_ids.push(Some((i % (group_count * 5)) as i64));
        payment_regions.push(Some(regions_list[i % regions_list.len()].to_string()));

        // Introduce controlled mismatches
        let amount = if rng.gen::<f64>() < mismatch_rate {
            // Intentional mismatch
            totals[i] + rng.gen_range(1.0..100.0)
        } else {
            // Matching amount
            totals[i]
        };
        amounts.push(amount);
    }

    let payments_batch = RecordBatch::try_new(
        payments_schema.clone(),
        vec![
            Arc::new(Int64Array::from(payment_ids)),
            Arc::new(Int64Array::from(payment_customer_ids)),
            Arc::new(Int64Array::from(payment_product_ids)),
            Arc::new(StringArray::from(payment_regions)),
            Arc::new(Float64Array::from(amounts)),
        ],
    )?;

    let payments_table = MemTable::try_new(payments_schema, vec![vec![payments_batch]])?;
    ctx.register_table("payments", Arc::new(payments_table))?;

    Ok(())
}

/// Benchmark cross-table sum validation without grouping
fn bench_cross_table_sum_no_grouping(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("cross_table_sum_no_grouping");
    group.sample_size(10); // Reduce sample size for large datasets

    for &rows in DATA_SCALES {
        group.bench_with_input(BenchmarkId::new("validate", rows), &rows, |b, &rows| {
            b.iter_batched(
                || {
                    // Setup
                    let ctx = SessionContext::new();
                    let constraint =
                        CrossTableSumConstraint::new("orders.total", "payments.amount")
                            .tolerance(0.01);
                    (ctx, constraint)
                },
                |(ctx, constraint)| {
                    rt.block_on(async {
                        // Create test data with 5% mismatch rate
                        create_cross_table_test_data(&ctx, rows, 0.05, None)
                            .await
                            .unwrap();

                        // Benchmark the validation
                        let result = constraint.evaluate(black_box(&ctx)).await;
                        black_box(result.unwrap());
                    })
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

/// Benchmark cross-table sum validation with grouping
fn bench_cross_table_sum_with_grouping(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("cross_table_sum_grouped");
    group.sample_size(10);

    for &rows in DATA_SCALES.iter().take(3) {
        // Limit to smaller datasets for grouped operations
        for &groups in GROUP_CARDINALITIES.iter().take(3) {
            if groups > rows / 10 {
                continue;
            } // Skip if too many groups

            group.bench_with_input(
                BenchmarkId::new("validate", format!("{rows}rows_{groups}groups")),
                &(rows, groups),
                |b, &(rows, groups)| {
                    b.iter_batched(
                        || {
                            let ctx = SessionContext::new();
                            let constraint =
                                CrossTableSumConstraint::new("orders.total", "payments.amount")
                                    .group_by(vec!["customer_id"])
                                    .tolerance(0.01);
                            (ctx, constraint)
                        },
                        |(ctx, constraint)| {
                            rt.block_on(async {
                                create_cross_table_test_data(&ctx, rows, 0.05, Some(groups))
                                    .await
                                    .unwrap();

                                let result = constraint.evaluate(black_box(&ctx)).await;
                                black_box(result.unwrap());
                            })
                        },
                        BatchSize::LargeInput,
                    );
                },
            );
        }
    }

    group.finish();
}

/// Benchmark violation collection performance  
fn bench_violation_collection(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("violation_collection");
    group.sample_size(10);

    let violation_limits = [0, 10, 100, 1000];

    for &limit in &violation_limits {
        group.bench_with_input(
            BenchmarkId::new("violations", limit),
            &limit,
            |b, &limit| {
                b.iter_batched(
                    || {
                        let ctx = SessionContext::new();
                        let constraint =
                            CrossTableSumConstraint::new("orders.total", "payments.amount")
                                .tolerance(0.01)
                                .max_violations_reported(limit);
                        (ctx, constraint)
                    },
                    |(ctx, constraint)| {
                        rt.block_on(async {
                            // Use higher mismatch rate to ensure violations
                            create_cross_table_test_data(&ctx, 10_000, 0.20, None)
                                .await
                                .unwrap();

                            let result = constraint.evaluate(black_box(&ctx)).await;
                            black_box(result.unwrap());
                        })
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark CrossTableSumBuilder performance
fn bench_builder_construction(c: &mut Criterion) {
    let mut group = c.benchmark_group("builder_construction");

    group.bench_function("basic_builder", |b| {
        b.iter(|| {
            let constraint = CrossTableSumBuilder::new("orders.total", "payments.amount")
                .tolerance(0.01)
                .max_violations_reported(100)
                .build();
            black_box(constraint.unwrap());
        });
    });

    group.bench_function("complex_builder", |b| {
        b.iter(|| {
            let constraint =
                CrossTableSumBuilder::new("public.orders.total", "finance.payments.amount")
                    .group_by(vec!["customer_id", "region", "product_category"])
                    .tolerance(0.001)
                    .max_violations_reported(500)
                    .build();
            black_box(constraint.unwrap());
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_cross_table_sum_no_grouping,
    bench_cross_table_sum_with_grouping,
    bench_violation_collection,
    bench_builder_construction
);

criterion_main!(benches);
