//! Quick performance benchmarks for unified constraints API
//!
//! This is a smaller benchmark suite for quick testing

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use rand::prelude::*;
use std::sync::Arc;
use term_guard::constraints::Assertion;
use term_guard::core::builder_extensions::StatisticalOptions;
use term_guard::core::{Check, ValidationSuite};

/// Creates a test dataset with the specified number of rows
async fn create_test_data(rows: usize) -> SessionContext {
    let ctx = SessionContext::new();
    let mut rng = rand::rng();

    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("email", DataType::Utf8, true),
        Field::new("age", DataType::Float64, true),
        Field::new("salary", DataType::Float64, true),
    ]));

    // Generate data
    let mut ids = Vec::with_capacity(rows);
    let mut emails = Vec::with_capacity(rows);
    let mut ages = Vec::with_capacity(rows);
    let mut salaries = Vec::with_capacity(rows);

    for i in 0..rows {
        ids.push(i as i64);

        // 95% valid emails, 3% invalid, 2% null
        let email = if rng.random_range(0..100) < 95 {
            Some(format!("user{i}@example.com"))
        } else if rng.random_range(0..100) < 98 {
            Some("invalid-email".to_string())
        } else {
            None
        };
        emails.push(email);

        // Age between 18-80, 5% nulls
        let age = if rng.random_range(0..100) < 95 {
            Some(rng.random_range(18.0..80.0))
        } else {
            None
        };
        ages.push(age);

        // Salary between 30k-200k, 2% nulls
        let salary = if rng.random_range(0..100) < 98 {
            Some(rng.random_range(30000.0..200000.0))
        } else {
            None
        };
        salaries.push(salary);
    }

    // Create batch
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(emails)),
            Arc::new(Float64Array::from(ages)),
            Arc::new(Float64Array::from(salaries)),
        ],
    )
    .unwrap();

    // Register table
    let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("data", Arc::new(provider)).unwrap();

    ctx
}

/// Benchmark statistics constraints (old vs new)
fn bench_statistics_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("statistics_comparison");
    let runtime = tokio::runtime::Runtime::new().unwrap();

    // Test with 10k rows
    let size = 10_000;

    // Benchmark old API (multiple separate constraints)
    group.bench_function("old_api_separate", |b| {
        b.iter_batched(
            || runtime.block_on(create_test_data(size)),
            |ctx| {
                runtime.block_on(async {
                    let suite = ValidationSuite::builder("stats_old")
                        .check(
                            Check::builder("statistics")
                                .has_min("age", Assertion::GreaterThanOrEqual(18.0))
                                .has_max("age", Assertion::LessThan(100.0))
                                .has_mean("age", Assertion::Between(30.0, 50.0))
                                .build(),
                        )
                        .build();

                    std::hint::black_box(suite.run(&ctx).await.unwrap())
                })
            },
            BatchSize::SmallInput,
        );
    });

    // Benchmark new API (combined statistics)
    group.bench_function("new_api_combined", |b| {
        b.iter_batched(
            || runtime.block_on(create_test_data(size)),
            |ctx| {
                runtime.block_on(async {
                    let suite = ValidationSuite::builder("stats_new")
                        .check(
                            Check::builder("statistics")
                                .statistics(
                                    "age",
                                    StatisticalOptions::new()
                                        .min(Assertion::GreaterThanOrEqual(18.0))
                                        .max(Assertion::LessThan(100.0))
                                        .mean(Assertion::Between(30.0, 50.0)),
                                )
                                .unwrap()
                                .build(),
                        )
                        .build();

                    std::hint::black_box(suite.run(&ctx).await.unwrap())
                })
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_statistics_comparison);
criterion_main!(benches);
