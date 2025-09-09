//! Performance benchmarks for unified constraints API vs legacy API
//!
//! This benchmark suite compares the performance of the new unified constraint API
//! with the legacy individual constraint methods across various scenarios.

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use rand::prelude::*;
use std::sync::Arc;
use term_guard::constraints::{Assertion, CompletenessConstraint, FormatOptions, FormatType};
use term_guard::core::builder_extensions::{CompletenessOptions, StatisticalOptions};
use term_guard::core::{Check, Level, ValidationSuite};

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
        Field::new("department", DataType::Utf8, true),
    ]));

    // Generate data
    let mut ids = Vec::with_capacity(rows);
    let mut emails = Vec::with_capacity(rows);
    let mut ages = Vec::with_capacity(rows);
    let mut salaries = Vec::with_capacity(rows);
    let mut departments = Vec::with_capacity(rows);

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

        // Department from a fixed set
        let dept = ["Engineering", "Sales", "Marketing", "HR", "Finance"];
        departments.push(Some(dept[rng.random_range(0..dept.len())].to_string()));
    }

    // Create batch
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(emails)),
            Arc::new(Float64Array::from(ages)),
            Arc::new(Float64Array::from(salaries)),
            Arc::new(StringArray::from(departments)),
        ],
    )
    .unwrap();

    // Register table
    let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("data", Arc::new(provider)).unwrap();

    ctx
}

/// Benchmark completeness constraints
fn bench_completeness(c: &mut Criterion) {
    let mut group = c.benchmark_group("completeness");

    for size in [1_000, 10_000, 100_000].iter() {
        let _runtime = tokio::runtime::Runtime::new().unwrap();

        // Benchmark old API
        group.bench_with_input(BenchmarkId::new("old_api", size), size, |b, &size| {
            b.iter_batched(
                || {
                    let rt = tokio::runtime::Handle::current();
                    rt.block_on(create_test_data(size))
                },
                |ctx| async move {
                    let suite = ValidationSuite::builder("completeness_old")
                        .check(
                            Check::builder("completeness")
                                .constraint(
                                    term_guard::constraints::CompletenessConstraint::with_threshold(
                                        "id", 1.0,
                                    ),
                                )
                                .constraint(
                                    term_guard::constraints::CompletenessConstraint::with_threshold(
                                        "email", 0.9,
                                    ),
                                )
                                .constraint(
                                    term_guard::constraints::CompletenessConstraint::with_threshold(
                                        "age", 1.0,
                                    ),
                                )
                                .constraint(
                                    term_guard::constraints::CompletenessConstraint::with_threshold(
                                        "salary", 1.0,
                                    ),
                                )
                                .build(),
                        )
                        .build();

                    std::hint::black_box(suite.run(&ctx).await.unwrap())
                },
                BatchSize::SmallInput,
            );
        });

        // Benchmark new API
        group.bench_with_input(BenchmarkId::new("new_api", size), size, |b, &size| {
            b.iter_batched(
                || {
                    let rt = tokio::runtime::Handle::current();
                    rt.block_on(create_test_data(size))
                },
                |ctx| async move {
                    let suite = ValidationSuite::builder("completeness_new")
                        .check(
                            Check::builder("completeness")
                                .completeness(
                                    "id",
                                    CompletenessOptions::full().into_constraint_options(),
                                )
                                .completeness(
                                    "email",
                                    CompletenessOptions::threshold(0.9).into_constraint_options(),
                                )
                                .completeness(
                                    vec!["age", "salary"],
                                    CompletenessOptions::full().into_constraint_options(),
                                )
                                .build(),
                        )
                        .build();

                    std::hint::black_box(suite.run(&ctx).await.unwrap())
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

/// Benchmark statistical constraints
fn bench_statistics(c: &mut Criterion) {
    let mut group = c.benchmark_group("statistics");

    for size in [1_000, 10_000, 100_000].iter() {
        let _runtime = tokio::runtime::Runtime::new().unwrap();

        // Benchmark old API (multiple separate constraints)
        group.bench_with_input(
            BenchmarkId::new("old_api_separate", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let rt = tokio::runtime::Handle::current();
                        rt.block_on(create_test_data(size))
                    },
                    |ctx| async move {
                        let suite = ValidationSuite::builder("stats_old")
                            .check(
                                Check::builder("statistics")
                                    .constraint(
                                        term_guard::constraints::StatisticalConstraint::new(
                                            "age",
                                            term_guard::constraints::StatisticType::Min,
                                            Assertion::GreaterThanOrEqual(18.0),
                                        )
                                        .unwrap(),
                                    )
                                    .constraint(
                                        term_guard::constraints::StatisticalConstraint::new(
                                            "age",
                                            term_guard::constraints::StatisticType::Max,
                                            Assertion::LessThan(100.0),
                                        )
                                        .unwrap(),
                                    )
                                    .constraint(
                                        term_guard::constraints::StatisticalConstraint::new(
                                            "age",
                                            term_guard::constraints::StatisticType::Mean,
                                            Assertion::Between(30.0, 50.0),
                                        )
                                        .unwrap(),
                                    )
                                    .constraint(
                                        term_guard::constraints::StatisticalConstraint::new(
                                            "salary",
                                            term_guard::constraints::StatisticType::Min,
                                            Assertion::GreaterThan(0.0),
                                        )
                                        .unwrap(),
                                    )
                                    .constraint(
                                        term_guard::constraints::StatisticalConstraint::new(
                                            "salary",
                                            term_guard::constraints::StatisticType::Max,
                                            Assertion::LessThan(500000.0),
                                        )
                                        .unwrap(),
                                    )
                                    .constraint(
                                        term_guard::constraints::StatisticalConstraint::new(
                                            "salary",
                                            term_guard::constraints::StatisticType::Mean,
                                            Assertion::Between(50000.0, 100000.0),
                                        )
                                        .unwrap(),
                                    )
                                    .build(),
                            )
                            .build();

                        std::hint::black_box(suite.run(&ctx).await.unwrap())
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        // Benchmark new API (combined statistics)
        group.bench_with_input(
            BenchmarkId::new("new_api_combined", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let rt = tokio::runtime::Handle::current();
                        rt.block_on(create_test_data(size))
                    },
                    |ctx| async move {
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
                                    .statistics(
                                        "salary",
                                        StatisticalOptions::new()
                                            .min(Assertion::GreaterThan(0.0))
                                            .max(Assertion::LessThan(500000.0))
                                            .mean(Assertion::Between(50000.0, 100000.0)),
                                    )
                                    .unwrap()
                                    .build(),
                            )
                            .build();

                        std::hint::black_box(suite.run(&ctx).await.unwrap())
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark format validation
fn bench_format_validation(c: &mut Criterion) {
    let mut group = c.benchmark_group("format_validation");

    for size in [1_000, 10_000, 100_000].iter() {
        let _runtime = tokio::runtime::Runtime::new().unwrap();

        // Benchmark old API
        group.bench_with_input(
            BenchmarkId::new("old_api_pattern", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let rt = tokio::runtime::Handle::current();
                        rt.block_on(create_test_data(size))
                    },
                    |ctx| async move {
                        let suite = ValidationSuite::builder("format_old")
                            .check(
                                Check::builder("format")
                                    .constraint(
                                        term_guard::constraints::FormatConstraint::regex(
                                            "email",
                                            r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
                                            0.9,
                                        )
                                        .unwrap(),
                                    )
                                    .build(),
                            )
                            .build();

                        std::hint::black_box(suite.run(&ctx).await.unwrap())
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        // Benchmark new API
        group.bench_with_input(
            BenchmarkId::new("new_api_format", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let rt = tokio::runtime::Handle::current();
                        rt.block_on(create_test_data(size))
                    },
                    |ctx| async move {
                        let suite = ValidationSuite::builder("format_new")
                            .check(
                                Check::builder("format")
                                    .has_format(
                                        "email",
                                        FormatType::Email,
                                        0.9,
                                        FormatOptions::default(),
                                    )
                                    .build(),
                            )
                            .build();

                        std::hint::black_box(suite.run(&ctx).await.unwrap())
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        // Benchmark convenience method
        group.bench_with_input(
            BenchmarkId::new("convenience_email", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let rt = tokio::runtime::Handle::current();
                        rt.block_on(create_test_data(size))
                    },
                    |ctx| async move {
                        let suite = ValidationSuite::builder("format_convenience")
                            .check(Check::builder("format").email("email", 0.9).build())
                            .build();

                        std::hint::black_box(suite.run(&ctx).await.unwrap())
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark complex constraint composition
fn bench_complex_composition(c: &mut Criterion) {
    let mut group = c.benchmark_group("complex_composition");

    for size in [1_000, 10_000, 100_000].iter() {
        let _runtime = tokio::runtime::Runtime::new().unwrap();

        // Benchmark old API with many individual constraints
        group.bench_with_input(BenchmarkId::new("old_api_many", size), size, |b, &size| {
            b.iter_batched(
                || {
                    let rt = tokio::runtime::Handle::current();
                    rt.block_on(create_test_data(size))
                },
                |ctx| async move {
                    let suite = ValidationSuite::builder("complex_old")
                        .check(
                            Check::builder("validation")
                                .level(Level::Error)
                                .constraint(
                                    term_guard::constraints::CompletenessConstraint::with_threshold(
                                        "id", 1.0,
                                    ),
                                )
                                .constraint(
                                    term_guard::constraints::UniquenessConstraint::full_uniqueness(
                                        "id", 1.0,
                                    )
                                    .unwrap(),
                                )
                                .constraint(
                                    term_guard::constraints::CompletenessConstraint::with_threshold(
                                        "email", 0.95,
                                    ),
                                )
                                .constraint(
                                    term_guard::constraints::FormatConstraint::regex(
                                        "email",
                                        r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
                                        0.9,
                                    )
                                    .unwrap(),
                                )
                                .constraint(
                                    term_guard::constraints::StatisticalConstraint::new(
                                        "age",
                                        term_guard::constraints::StatisticType::Min,
                                        Assertion::GreaterThanOrEqual(18.0),
                                    )
                                    .unwrap(),
                                )
                                .constraint(
                                    term_guard::constraints::StatisticalConstraint::new(
                                        "age",
                                        term_guard::constraints::StatisticType::Max,
                                        Assertion::LessThan(100.0),
                                    )
                                    .unwrap(),
                                )
                                .constraint(
                                    term_guard::constraints::StatisticalConstraint::new(
                                        "age",
                                        term_guard::constraints::StatisticType::Mean,
                                        Assertion::Between(30.0, 50.0),
                                    )
                                    .unwrap(),
                                )
                                .constraint(
                                    term_guard::constraints::StatisticalConstraint::new(
                                        "salary",
                                        term_guard::constraints::StatisticType::Min,
                                        Assertion::GreaterThan(0.0),
                                    )
                                    .unwrap(),
                                )
                                .constraint(
                                    term_guard::constraints::StatisticalConstraint::new(
                                        "salary",
                                        term_guard::constraints::StatisticType::Max,
                                        Assertion::LessThan(500000.0),
                                    )
                                    .unwrap(),
                                )
                                .build(),
                        )
                        .build();

                    std::hint::black_box(suite.run(&ctx).await.unwrap())
                },
                BatchSize::SmallInput,
            );
        });

        // Benchmark new API with unified methods
        group.bench_with_input(
            BenchmarkId::new("new_api_unified", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let rt = tokio::runtime::Handle::current();
                        rt.block_on(create_test_data(size))
                    },
                    |ctx| async move {
                        let suite = ValidationSuite::builder("complex_new")
                            .check(
                                Check::builder("validation")
                                    .level(Level::Error)
                                    .primary_key(vec!["id"])
                                    .completeness(
                                        "email",
                                        CompletenessOptions::threshold(0.95)
                                            .into_constraint_options(),
                                    )
                                    .email("email", 0.9)
                                    .statistics(
                                        "age",
                                        StatisticalOptions::new()
                                            .min(Assertion::GreaterThanOrEqual(18.0))
                                            .max(Assertion::LessThan(100.0))
                                            .mean(Assertion::Between(30.0, 50.0)),
                                    )
                                    .unwrap()
                                    .statistics(
                                        "salary",
                                        StatisticalOptions::new()
                                            .min(Assertion::GreaterThan(0.0))
                                            .max(Assertion::LessThan(500000.0)),
                                    )
                                    .unwrap()
                                    .build(),
                            )
                            .build();

                        std::hint::black_box(suite.run(&ctx).await.unwrap())
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark multi-column completeness with logical operators
fn bench_logical_operators(c: &mut Criterion) {
    let mut group = c.benchmark_group("logical_operators");

    for size in [1_000, 10_000, 100_000].iter() {
        let _runtime = tokio::runtime::Runtime::new().unwrap();

        // Benchmark old API (separate checks)
        group.bench_with_input(
            BenchmarkId::new("old_api_separate", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let rt = tokio::runtime::Handle::current();
                        rt.block_on(create_test_data(size))
                    },
                    |ctx| async move {
                        let suite = ValidationSuite::builder("logical_old")
                            .check(
                                Check::builder("multi_column")
                                    .constraint(CompletenessConstraint::new(
                                        vec!["id", "department"],
                                        term_guard::core::ConstraintOptions::new()
                                            .with_operator(term_guard::core::LogicalOperator::All)
                                            .with_threshold(1.0),
                                    ))
                                    .constraint(
                                        term_guard::constraints::CompletenessConstraint::new(
                                            vec!["email", "age", "salary"],
                                            term_guard::core::ConstraintOptions::new()
                                                .with_operator(
                                                    term_guard::core::LogicalOperator::Any,
                                                )
                                                .with_threshold(1.0),
                                        ),
                                    )
                                    .build(),
                            )
                            .build();

                        std::hint::black_box(suite.run(&ctx).await.unwrap())
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        // Benchmark new API with logical operators
        group.bench_with_input(
            BenchmarkId::new("new_api_logical", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let rt = tokio::runtime::Handle::current();
                        rt.block_on(create_test_data(size))
                    },
                    |ctx| async move {
                        let suite = ValidationSuite::builder("logical_new")
                            .check(
                                Check::builder("multi_column")
                                    .completeness(
                                        vec!["id", "department"],
                                        CompletenessOptions::full().into_constraint_options(),
                                    )
                                    .completeness(
                                        vec!["email", "age", "salary"],
                                        CompletenessOptions::any().into_constraint_options(),
                                    )
                                    .build(),
                            )
                            .build();

                        std::hint::black_box(suite.run(&ctx).await.unwrap())
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        // Benchmark new API with at_least operator
        group.bench_with_input(
            BenchmarkId::new("new_api_at_least", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let rt = tokio::runtime::Handle::current();
                        rt.block_on(create_test_data(size))
                    },
                    |ctx| async move {
                        let suite = ValidationSuite::builder("at_least_new")
                            .check(
                                Check::builder("multi_column")
                                    .completeness(
                                        vec!["email", "age", "salary"],
                                        CompletenessOptions::at_least(2).into_constraint_options(),
                                    )
                                    .build(),
                            )
                            .build();

                        std::hint::black_box(suite.run(&ctx).await.unwrap())
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_completeness,
    bench_statistics,
    bench_format_validation,
    bench_complex_composition,
    bench_logical_operators
);

criterion_main!(benches);
