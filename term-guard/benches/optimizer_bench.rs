//! Benchmarks for the query optimizer.

use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::prelude::*;
use term_guard::constraints::{
    Assertion, CompletenessConstraint, StatisticType, StatisticalConstraint,
};
use term_guard::core::{Check, Level, ValidationSuite};
use tokio::runtime::Runtime;

/// Creates a test DataFusion context with sample data
async fn create_test_context(rows: usize) -> SessionContext {
    use arrow::array::{Float64Array, Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    let ctx = SessionContext::new();

    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
        Field::new("category", DataType::Utf8, true),
        Field::new("quantity", DataType::Int32, true),
    ]));

    // Generate data
    let id_array = Int32Array::from((0..rows as i32).collect::<Vec<_>>());
    let name_array = StringArray::from((0..rows).map(|i| format!("item_{i}")).collect::<Vec<_>>());
    let value_array = Float64Array::from((0..rows).map(|i| (i as f64) * 10.5).collect::<Vec<_>>());
    let category_array = StringArray::from(
        (0..rows)
            .map(|i| {
                if i % 10 == 0 {
                    None
                } else {
                    Some(format!("cat_{}", i % 5))
                }
            })
            .collect::<Vec<_>>(),
    );
    let quantity_array = Int32Array::from(
        (0..rows)
            .map(|i| {
                if i % 20 == 0 {
                    None
                } else {
                    Some((i % 100) as i32)
                }
            })
            .collect::<Vec<_>>(),
    );

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_array),
            Arc::new(name_array),
            Arc::new(value_array),
            Arc::new(category_array),
            Arc::new(quantity_array),
        ],
    )
    .unwrap();

    // Create DataFrame and register as table
    let df = ctx.read_batch(batch).unwrap();
    ctx.register_table("data", df.into_view()).unwrap();

    ctx
}

fn benchmark_optimizer_vs_sequential(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("optimizer_comparison");

    for rows in [1000, 10000, 100000].iter() {
        let rows = *rows;

        // Benchmark with optimizer
        group.bench_function(format!("optimized_{rows}_rows"), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let ctx = create_test_context(rows).await;

                    let suite = ValidationSuite::builder("benchmark_suite")
                        .with_optimizer(true)
                        .check(
                            Check::builder("completeness_checks")
                                .level(Level::Error)
                                .constraint(CompletenessConstraint::with_threshold("id", 1.0))
                                .constraint(CompletenessConstraint::with_threshold("name", 1.0))
                                .constraint(CompletenessConstraint::with_threshold("value", 1.0))
                                .constraint(CompletenessConstraint::with_threshold("category", 0.9))
                                .constraint(CompletenessConstraint::with_threshold(
                                    "quantity", 0.95,
                                ))
                                .build(),
                        )
                        .check(
                            Check::builder("statistics_checks")
                                .level(Level::Warning)
                                .constraint(
                                    StatisticalConstraint::new(
                                        "value",
                                        StatisticType::Min,
                                        Assertion::GreaterThan(0.0),
                                    )
                                    .unwrap(),
                                )
                                .constraint(
                                    StatisticalConstraint::new(
                                        "value",
                                        StatisticType::Max,
                                        Assertion::LessThan(10000.0),
                                    )
                                    .unwrap(),
                                )
                                .constraint(
                                    StatisticalConstraint::new(
                                        "value",
                                        StatisticType::Mean,
                                        Assertion::GreaterThan(0.0),
                                    )
                                    .unwrap(),
                                )
                                .constraint(
                                    StatisticalConstraint::new(
                                        "quantity",
                                        StatisticType::Min,
                                        Assertion::GreaterThan(0.0),
                                    )
                                    .unwrap(),
                                )
                                .constraint(
                                    StatisticalConstraint::new(
                                        "quantity",
                                        StatisticType::Max,
                                        Assertion::LessThan(100.0),
                                    )
                                    .unwrap(),
                                )
                                .build(),
                        )
                        .build();

                    let result = suite.run(&ctx).await.unwrap();
                    std::hint::black_box(result);
                })
            });
        });

        // Benchmark without optimizer
        group.bench_function(format!("sequential_{rows}_rows"), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let ctx = create_test_context(rows).await;

                    let suite = ValidationSuite::builder("benchmark_suite")
                        .with_optimizer(false)
                        .check(
                            Check::builder("completeness_checks")
                                .level(Level::Error)
                                .constraint(CompletenessConstraint::with_threshold("id", 1.0))
                                .constraint(CompletenessConstraint::with_threshold("name", 1.0))
                                .constraint(CompletenessConstraint::with_threshold("value", 1.0))
                                .constraint(CompletenessConstraint::with_threshold("category", 0.9))
                                .constraint(CompletenessConstraint::with_threshold(
                                    "quantity", 0.95,
                                ))
                                .build(),
                        )
                        .check(
                            Check::builder("statistics_checks")
                                .level(Level::Warning)
                                .constraint(
                                    StatisticalConstraint::new(
                                        "value",
                                        StatisticType::Min,
                                        Assertion::GreaterThan(0.0),
                                    )
                                    .unwrap(),
                                )
                                .constraint(
                                    StatisticalConstraint::new(
                                        "value",
                                        StatisticType::Max,
                                        Assertion::LessThan(10000.0),
                                    )
                                    .unwrap(),
                                )
                                .constraint(
                                    StatisticalConstraint::new(
                                        "value",
                                        StatisticType::Mean,
                                        Assertion::GreaterThan(0.0),
                                    )
                                    .unwrap(),
                                )
                                .constraint(
                                    StatisticalConstraint::new(
                                        "quantity",
                                        StatisticType::Min,
                                        Assertion::GreaterThan(0.0),
                                    )
                                    .unwrap(),
                                )
                                .constraint(
                                    StatisticalConstraint::new(
                                        "quantity",
                                        StatisticType::Max,
                                        Assertion::LessThan(100.0),
                                    )
                                    .unwrap(),
                                )
                                .build(),
                        )
                        .build();

                    let result = suite.run(&ctx).await.unwrap();
                    std::hint::black_box(result);
                })
            });
        });
    }

    group.finish();
}

fn benchmark_cache_impact(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("cache_cold_start", |b| {
        b.iter(|| {
            rt.block_on(async {
                let ctx = create_test_context(10000).await;

                // Create a new optimizer each time (cold cache)
                let suite = ValidationSuite::builder("cache_test")
                    .with_optimizer(true)
                    .check(
                        Check::builder("stats")
                            .level(Level::Error)
                            .constraint(
                                StatisticalConstraint::new(
                                    "value",
                                    StatisticType::Min,
                                    Assertion::GreaterThan(0.0),
                                )
                                .unwrap(),
                            )
                            .constraint(
                                StatisticalConstraint::new(
                                    "value",
                                    StatisticType::Max,
                                    Assertion::LessThan(10000.0),
                                )
                                .unwrap(),
                            )
                            .constraint(
                                StatisticalConstraint::new(
                                    "value",
                                    StatisticType::Mean,
                                    Assertion::GreaterThan(0.0),
                                )
                                .unwrap(),
                            )
                            .build(),
                    )
                    .build();

                let result = suite.run(&ctx).await.unwrap();
                std::hint::black_box(result);
            })
        });
    });

    c.bench_function("cache_warm", |b| {
        // Pre-warm the cache
        rt.block_on(async {
            let ctx = create_test_context(10000).await;
            let suite = ValidationSuite::builder("cache_warmup")
                .with_optimizer(true)
                .check(
                    Check::builder("stats")
                        .level(Level::Error)
                        .constraint(
                            StatisticalConstraint::new(
                                "value",
                                StatisticType::Min,
                                Assertion::GreaterThan(0.0),
                            )
                            .unwrap(),
                        )
                        .constraint(
                            StatisticalConstraint::new(
                                "value",
                                StatisticType::Max,
                                Assertion::LessThan(10000.0),
                            )
                            .unwrap(),
                        )
                        .constraint(
                            StatisticalConstraint::new(
                                "value",
                                StatisticType::Mean,
                                Assertion::GreaterThan(0.0),
                            )
                            .unwrap(),
                        )
                        .build(),
                )
                .build();
            suite.run(&ctx).await.unwrap();
        });

        b.iter(|| {
            rt.block_on(async {
                let ctx = create_test_context(10000).await;

                // Reuse the same suite (warm cache)
                let suite = ValidationSuite::builder("cache_test")
                    .with_optimizer(true)
                    .check(
                        Check::builder("stats")
                            .level(Level::Error)
                            .constraint(
                                StatisticalConstraint::new(
                                    "value",
                                    StatisticType::Min,
                                    Assertion::GreaterThan(0.0),
                                )
                                .unwrap(),
                            )
                            .constraint(
                                StatisticalConstraint::new(
                                    "value",
                                    StatisticType::Max,
                                    Assertion::LessThan(10000.0),
                                )
                                .unwrap(),
                            )
                            .constraint(
                                StatisticalConstraint::new(
                                    "value",
                                    StatisticType::Mean,
                                    Assertion::GreaterThan(0.0),
                                )
                                .unwrap(),
                            )
                            .build(),
                    )
                    .build();

                let result = suite.run(&ctx).await.unwrap();
                std::hint::black_box(result);
            })
        });
    });
}

fn benchmark_grouping_efficiency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("grouping_efficiency");

    for num_constraints in [5, 10, 20, 50].iter() {
        let num = *num_constraints;

        group.bench_function(format!("{num}_constraints"), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let ctx = create_test_context(5000).await;

                    let mut check_builder = Check::builder("many_constraints").level(Level::Error);

                    // Add many completeness constraints
                    for col in ["id", "name", "value", "category", "quantity"].iter() {
                        for i in 0..num / 5 {
                            check_builder = check_builder
                                .constraint(CompletenessConstraint::complete(format!("{col}_{i}")));
                        }
                    }

                    let suite = ValidationSuite::builder("grouping_test")
                        .with_optimizer(true)
                        .check(check_builder.build())
                        .build();

                    let result = suite.run(&ctx).await.unwrap();
                    std::hint::black_box(result);
                })
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_optimizer_vs_sequential,
    benchmark_cache_impact,
    benchmark_grouping_efficiency
);
criterion_main!(benches);
