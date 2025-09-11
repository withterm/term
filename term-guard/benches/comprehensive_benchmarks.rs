//! Comprehensive benchmark suite for Term validation library.
//!
//! This module provides extensive performance benchmarking for all Term components,
//! helping track performance regressions and optimization opportunities.
//!
//! ## Benchmark Categories
//!
//! - **Constraint Performance**: Individual constraint evaluation times
//! - **Suite Scaling**: Validation suite performance with varying check counts
//! - **Data Scaling**: Performance with different data sizes (100 rows to 10M rows)
//! - **Analyzer Performance**: Advanced analytics (correlation, KLL, etc.)
//! - **Memory Usage**: Memory consumption patterns
//! - **Concurrent Validation**: Multi-threaded validation performance
//!
//! ## Running Benchmarks
//!
//! ```bash
//! # Run all benchmarks
//! cargo bench
//!
//! # Run specific benchmark group
//! cargo bench --bench comprehensive_benchmarks constraint_
//! cargo bench --bench comprehensive_benchmarks scaling_
//! ```

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;

use term_guard::analyzers::advanced::{CorrelationAnalyzer, KllSketch};
use term_guard::analyzers::Analyzer;
use term_guard::constraints::{
    Assertion, CompletenessConstraint, ContainmentConstraint, CustomSqlConstraint, StatisticType,
    StatisticalConstraint, UniquenessConstraint, UniquenessOptions, UniquenessType,
};
use term_guard::core::{Check, ValidationSuite};

// ============================================================================
// Test Data Generation
// ============================================================================

/// Creates a test dataset with specified number of rows.
fn create_test_data(num_rows: usize) -> (SessionContext, Runtime) {
    let rt = Runtime::new().unwrap();
    let ctx = SessionContext::new();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
        Field::new("status", DataType::Utf8, true),
        Field::new("category", DataType::Utf8, true),
    ]));

    let mut ids = Vec::with_capacity(num_rows);
    let mut names = Vec::with_capacity(num_rows);
    let mut values = Vec::with_capacity(num_rows);
    let mut statuses = Vec::with_capacity(num_rows);
    let mut categories = Vec::with_capacity(num_rows);

    let status_options = ["active", "inactive", "pending"];
    let category_options = ["A", "B", "C", "D", "E"];

    for i in 0..num_rows {
        ids.push(i as i64);
        names.push(if i % 10 == 0 {
            None
        } else {
            Some(format!("name_{i}"))
        });
        values.push(if i % 20 == 0 {
            None
        } else {
            Some((i as f64) * 1.5 + 10.0)
        });
        statuses.push(Some(status_options[i % 3]));
        categories.push(Some(category_options[i % 5]));
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(Float64Array::from(values)) as ArrayRef,
            Arc::new(StringArray::from(statuses)) as ArrayRef,
            Arc::new(StringArray::from(categories)) as ArrayRef,
        ],
    )
    .unwrap();

    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    rt.block_on(async {
        ctx.register_table("data", Arc::new(table)).unwrap();
    });

    (ctx, rt)
}

// ============================================================================
// Individual Constraint Benchmarks
// ============================================================================

fn benchmark_constraint_completeness(c: &mut Criterion) {
    let mut group = c.benchmark_group("constraint_completeness");

    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let (ctx, rt) = create_test_data(size);
            let suite = ValidationSuite::builder("completeness_bench")
                .check(
                    Check::builder("test")
                        .constraint(CompletenessConstraint::with_threshold("name", 0.9))
                        .build(),
                )
                .build();

            b.iter(|| rt.block_on(async { std::hint::black_box(suite.run(&ctx).await.unwrap()) }));
        });
    }
    group.finish();
}

fn benchmark_constraint_uniqueness(c: &mut Criterion) {
    let mut group = c.benchmark_group("constraint_uniqueness");

    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let (ctx, rt) = create_test_data(size);
            let suite = ValidationSuite::builder("uniqueness_bench")
                .check(
                    Check::builder("test")
                        .constraint(
                            UniquenessConstraint::new(
                                vec!["id"],
                                UniquenessType::FullUniqueness { threshold: 1.0 },
                                UniquenessOptions::default(),
                            )
                            .unwrap(),
                        )
                        .build(),
                )
                .build();

            b.iter(|| rt.block_on(async { std::hint::black_box(suite.run(&ctx).await.unwrap()) }));
        });
    }
    group.finish();
}

fn benchmark_constraint_statistical(c: &mut Criterion) {
    let mut group = c.benchmark_group("constraint_statistical");

    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let (ctx, rt) = create_test_data(size);
            let suite = ValidationSuite::builder("statistical_bench")
                .check(
                    Check::builder("test")
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
                                "value",
                                StatisticType::StandardDeviation,
                                Assertion::GreaterThan(0.0),
                            )
                            .unwrap(),
                        )
                        .build(),
                )
                .build();

            b.iter(|| rt.block_on(async { std::hint::black_box(suite.run(&ctx).await.unwrap()) }));
        });
    }
    group.finish();
}

fn benchmark_constraint_custom_sql(c: &mut Criterion) {
    let mut group = c.benchmark_group("constraint_custom_sql");

    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let (ctx, rt) = create_test_data(size);
            let suite = ValidationSuite::builder("custom_sql_bench")
                .check(
                    Check::builder("test")
                        .constraint(
                            CustomSqlConstraint::new(
                                "value > 10 AND value < 100000",
                                Some("Value range check"),
                            )
                            .unwrap(),
                        )
                        .build(),
                )
                .build();

            b.iter(|| rt.block_on(async { std::hint::black_box(suite.run(&ctx).await.unwrap()) }));
        });
    }
    group.finish();
}

// ============================================================================
// Suite Scaling Benchmarks
// ============================================================================

fn benchmark_suite_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("suite_scaling");
    group.measurement_time(Duration::from_secs(10));

    for num_checks in [1, 5, 10, 20, 50].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_checks),
            num_checks,
            |b, &num_checks| {
                let (ctx, rt) = create_test_data(1000);
                let mut builder = ValidationSuite::builder("scaling_bench");

                for i in 0..num_checks {
                    let check = Check::builder(format!("check_{i}"))
                        .constraint(CompletenessConstraint::with_threshold("name", 0.9))
                        .constraint(
                            StatisticalConstraint::new(
                                "value",
                                StatisticType::Mean,
                                Assertion::GreaterThan(0.0),
                            )
                            .unwrap(),
                        )
                        .build();
                    builder = builder.check(check);
                }

                let suite = builder.build();

                b.iter(|| {
                    rt.block_on(async { std::hint::black_box(suite.run(&ctx).await.unwrap()) })
                });
            },
        );
    }
    group.finish();
}

// ============================================================================
// Advanced Analytics Benchmarks
// ============================================================================

fn benchmark_kll_sketch(c: &mut Criterion) {
    let mut group = c.benchmark_group("kll_sketch");

    for size in [1000, 10000, 100000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let values: Vec<f64> = (0..size).map(|i| i as f64 * 1.5).collect();

            b.iter_batched(
                || values.clone(),
                |values| {
                    let mut sketch = KllSketch::new(200);
                    for v in values {
                        sketch.update(v);
                    }
                    std::hint::black_box(sketch.get_quantile(0.5).unwrap())
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn benchmark_correlation(c: &mut Criterion) {
    let mut group = c.benchmark_group("correlation");

    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let (ctx, rt) = create_test_data(size);
            let analyzer = CorrelationAnalyzer::pearson("id", "value");

            b.iter(|| {
                rt.block_on(async {
                    let state = analyzer.compute_state_from_data(&ctx).await.unwrap();
                    std::hint::black_box(analyzer.compute_metric_from_state(&state).unwrap())
                })
            });
        });
    }
    group.finish();
}

// ============================================================================
// Memory Usage Benchmarks
// ============================================================================

fn benchmark_memory_kll_levels(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_kll_levels");

    group.bench_function("kll_memory_growth", |b| {
        b.iter(|| {
            let mut sketch = KllSketch::new(200);
            // Add enough items to create multiple levels
            for i in 0..100000 {
                sketch.update(i as f64);
            }
            std::hint::black_box((sketch.num_levels(), sketch.memory_usage()))
        });
    });

    group.finish();
}

fn benchmark_memory_suite_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_suite_size");

    group.bench_function("suite_memory_100_checks", |b| {
        b.iter(|| {
            let mut builder = ValidationSuite::builder("memory_bench");

            for i in 0..100 {
                let check = Check::builder(format!("check_{i}"))
                    .constraint(CompletenessConstraint::with_threshold("name", 0.9))
                    .constraint(
                        StatisticalConstraint::new(
                            "value",
                            StatisticType::Mean,
                            Assertion::GreaterThan(0.0),
                        )
                        .unwrap(),
                    )
                    .build();
                builder = builder.check(check);
            }

            std::hint::black_box(builder.build())
        });
    });

    group.finish();
}

// ============================================================================
// Concurrent Validation Benchmarks
// ============================================================================

fn benchmark_concurrent_validation(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_validation");
    group.measurement_time(Duration::from_secs(15));

    group.bench_function("parallel_checks", |b| {
        let (ctx, rt) = create_test_data(10000);

        let suite = ValidationSuite::builder("concurrent_bench")
            .check(
                Check::builder("check_1")
                    .constraint(CompletenessConstraint::with_threshold("name", 0.9))
                    .build(),
            )
            .check(
                Check::builder("check_2")
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
            .check(
                Check::builder("check_3")
                    .constraint(
                        UniquenessConstraint::new(
                            vec!["id"],
                            UniquenessType::FullUniqueness { threshold: 1.0 },
                            UniquenessOptions::default(),
                        )
                        .unwrap(),
                    )
                    .build(),
            )
            .check(
                Check::builder("check_4")
                    .constraint(ContainmentConstraint::new(
                        "status",
                        vec![
                            "active".to_string(),
                            "inactive".to_string(),
                            "pending".to_string(),
                        ],
                    ))
                    .build(),
            )
            .build();

        b.iter(|| rt.block_on(async { std::hint::black_box(suite.run(&ctx).await.unwrap()) }));
    });

    group.bench_function("sequential_checks", |b| {
        let (ctx, rt) = create_test_data(10000);

        let suite = ValidationSuite::builder("sequential_bench")
            .check(
                Check::builder("check_1")
                    .constraint(CompletenessConstraint::with_threshold("name", 0.9))
                    .build(),
            )
            .check(
                Check::builder("check_2")
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
            .check(
                Check::builder("check_3")
                    .constraint(
                        UniquenessConstraint::new(
                            vec!["id"],
                            UniquenessType::FullUniqueness { threshold: 1.0 },
                            UniquenessOptions::default(),
                        )
                        .unwrap(),
                    )
                    .build(),
            )
            .check(
                Check::builder("check_4")
                    .constraint(ContainmentConstraint::new(
                        "status",
                        vec![
                            "active".to_string(),
                            "inactive".to_string(),
                            "pending".to_string(),
                        ],
                    ))
                    .build(),
            )
            .build();

        b.iter(|| rt.block_on(async { std::hint::black_box(suite.run(&ctx).await.unwrap()) }));
    });

    group.finish();
}

// ============================================================================
// Worst Case Scenario Benchmarks
// ============================================================================

fn benchmark_worst_case_scenarios(c: &mut Criterion) {
    let mut group = c.benchmark_group("worst_case");
    group.measurement_time(Duration::from_secs(20));

    // Benchmark with all nulls
    group.bench_function("all_nulls", |b| {
        let rt = Runtime::new().unwrap();
        let ctx = SessionContext::new();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "col",
            DataType::Float64,
            true,
        )]));

        let values: Vec<Option<f64>> = vec![None; 10000];
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Float64Array::from(values)) as ArrayRef],
        )
        .unwrap();

        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        rt.block_on(async {
            ctx.register_table("data", Arc::new(table)).unwrap();
        });

        let suite = ValidationSuite::builder("nulls_bench")
            .check(
                Check::builder("test")
                    .constraint(CompletenessConstraint::with_threshold("col", 0.0))
                    .build(),
            )
            .build();

        b.iter(|| rt.block_on(async { std::hint::black_box(suite.run(&ctx).await.unwrap()) }));
    });

    // Benchmark with highly skewed data
    group.bench_function("skewed_data", |b| {
        b.iter_batched(
            || {
                let rt = Runtime::new().unwrap();
                let ctx = SessionContext::new();

                let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, true)]));

                // 99% same value, 1% unique
                let mut values: Vec<String> = vec!["common".to_string(); 9900];
                for i in 0..100 {
                    values.push(format!("unique_{i}"));
                }

                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(StringArray::from(values)) as ArrayRef],
                )
                .unwrap();

                let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
                rt.block_on(async {
                    ctx.register_table("data", Arc::new(table)).unwrap();
                });

                let suite = ValidationSuite::builder("skewed_bench")
                    .check(
                        Check::builder("test")
                            .constraint(
                                UniquenessConstraint::new(
                                    vec!["col"],
                                    UniquenessType::Distinctness(Assertion::GreaterThan(0.0)),
                                    UniquenessOptions::default(),
                                )
                                .unwrap(),
                            )
                            .build(),
                    )
                    .build();

                (rt, ctx, suite)
            },
            |(rt, ctx, suite)| {
                rt.block_on(async { std::hint::black_box(suite.run(&ctx).await.unwrap()) })
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

// ============================================================================
// Benchmark Groups
// ============================================================================

criterion_group!(
    name = constraint_benches;
    config = Criterion::default().sample_size(20);
    targets = benchmark_constraint_completeness,
              benchmark_constraint_uniqueness,
              benchmark_constraint_statistical,
              benchmark_constraint_custom_sql
);

criterion_group!(
    name = scaling_benches;
    config = Criterion::default().sample_size(10);
    targets = benchmark_suite_scaling
);

criterion_group!(
    name = analytics_benches;
    config = Criterion::default().sample_size(20);
    targets = benchmark_kll_sketch,
              benchmark_correlation
);

criterion_group!(
    name = memory_benches;
    config = Criterion::default().sample_size(10);
    targets = benchmark_memory_kll_levels,
              benchmark_memory_suite_size
);

criterion_group!(
    name = concurrent_benches;
    config = Criterion::default().sample_size(10);
    targets = benchmark_concurrent_validation
);

criterion_group!(
    name = worst_case_benches;
    config = Criterion::default().sample_size(10);
    targets = benchmark_worst_case_scenarios
);

criterion_main!(
    constraint_benches,
    scaling_benches,
    analytics_benches,
    memory_benches,
    concurrent_benches,
    worst_case_benches
);
