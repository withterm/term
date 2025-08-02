//! Benchmarks for ColumnProfiler performance validation.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::time::Duration;
use term_guard::analyzers::ColumnProfiler;
use term_guard::test_fixtures::create_minimal_tpc_h_context;
use tokio::runtime::Runtime;

async fn setup_context() -> datafusion::prelude::SessionContext {
    create_minimal_tpc_h_context().await.unwrap()
}

fn bench_single_column_profiling(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(setup_context());

    let mut group = c.benchmark_group("single_column_profiling");
    group.measurement_time(Duration::from_secs(10));

    // Test different column types
    let test_cases = vec![
        ("string_low_cardinality", "l_returnflag"),
        ("string_high_cardinality", "l_comment"),
        ("numeric_high_cardinality", "l_extendedprice"),
        ("integer_medium_cardinality", "l_orderkey"),
        ("date_column", "l_shipdate"),
    ];

    for (name, column) in test_cases {
        let profiler = ColumnProfiler::new();
        group.bench_with_input(
            BenchmarkId::new("default_config", name),
            &column,
            |b, &column| {
                b.iter(|| {
                    rt.block_on(profiler.profile_column(
                        black_box(&ctx),
                        black_box("lineitem"),
                        black_box(column),
                    ))
                });
            },
        );
    }

    group.finish();
}

fn bench_profiler_configurations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(setup_context());

    let mut group = c.benchmark_group("profiler_configurations");
    group.measurement_time(Duration::from_secs(8));

    let configs = vec![
        (
            "small_sample",
            ColumnProfiler::builder().sample_size(100).build(),
        ),
        (
            "medium_sample",
            ColumnProfiler::builder().sample_size(1000).build(),
        ),
        (
            "large_sample",
            ColumnProfiler::builder().sample_size(10000).build(),
        ),
        (
            "low_cardinality_threshold",
            ColumnProfiler::builder().cardinality_threshold(10).build(),
        ),
        (
            "high_cardinality_threshold",
            ColumnProfiler::builder()
                .cardinality_threshold(1000)
                .build(),
        ),
        (
            "no_parallel",
            ColumnProfiler::builder().enable_parallel(false).build(),
        ),
    ];

    for (name, profiler) in configs {
        group.bench_with_input(
            BenchmarkId::new(name, "l_extendedprice"),
            &profiler,
            |b, profiler| {
                b.iter(|| {
                    rt.block_on(profiler.profile_column(
                        black_box(&ctx),
                        black_box("lineitem"),
                        black_box("l_extendedprice"),
                    ))
                });
            },
        );
    }

    group.finish();
}

fn bench_multiple_columns(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(setup_context());

    let mut group = c.benchmark_group("multiple_columns");
    group.measurement_time(Duration::from_secs(15));

    let column_sets = vec![
        (
            "two_columns",
            vec!["l_orderkey".to_string(), "l_returnflag".to_string()],
        ),
        (
            "four_columns",
            vec![
                "l_orderkey".to_string(),
                "l_returnflag".to_string(),
                "l_linestatus".to_string(),
                "l_quantity".to_string(),
            ],
        ),
        (
            "eight_columns",
            vec![
                "l_orderkey".to_string(),
                "l_partkey".to_string(),
                "l_suppkey".to_string(),
                "l_linenumber".to_string(),
                "l_quantity".to_string(),
                "l_extendedprice".to_string(),
                "l_discount".to_string(),
                "l_tax".to_string(),
            ],
        ),
    ];

    for (name, columns) in &column_sets {
        // Sequential execution
        let profiler_seq = ColumnProfiler::builder().enable_parallel(false).build();
        group.bench_with_input(
            BenchmarkId::new("sequential", name),
            columns,
            |b, columns| {
                b.iter(|| {
                    rt.block_on(profiler_seq.profile_columns(
                        black_box(&ctx),
                        black_box("lineitem"),
                        black_box(columns),
                    ))
                });
            },
        );

        // Parallel execution
        let profiler_par = ColumnProfiler::builder().enable_parallel(true).build();
        group.bench_with_input(BenchmarkId::new("parallel", name), columns, |b, columns| {
            b.iter(|| {
                rt.block_on(profiler_par.profile_columns(
                    black_box(&ctx),
                    black_box("lineitem"),
                    black_box(columns),
                ))
            });
        });
    }

    group.finish();
}

fn bench_three_pass_algorithm(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(setup_context());

    let mut group = c.benchmark_group("three_pass_optimization");
    group.measurement_time(Duration::from_secs(10));

    // Compare different cardinality thresholds to show three-pass optimization benefit
    let thresholds = vec![
        ("always_pass2", 1000000), // Force categorical histograms
        ("always_pass3", 1),       // Force numeric distributions
        ("balanced", 100),         // Balanced threshold
    ];

    for (name, threshold) in thresholds {
        let profiler = ColumnProfiler::builder()
            .cardinality_threshold(threshold)
            .build();

        group.bench_with_input(
            BenchmarkId::new(name, "mixed_columns"),
            &profiler,
            |b, profiler| {
                let columns = vec![
                    "l_returnflag".to_string(),    // Low cardinality
                    "l_extendedprice".to_string(), // High cardinality numeric
                    "l_orderkey".to_string(),      // High cardinality integer
                ];
                b.iter(|| {
                    rt.block_on(profiler.profile_columns(
                        black_box(&ctx),
                        black_box("lineitem"),
                        black_box(&columns),
                    ))
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_single_column_profiling,
    bench_profiler_configurations,
    bench_multiple_columns,
    bench_three_pass_algorithm
);

criterion_main!(benches);
