//! Benchmarks for TypeInferenceEngine performance validation.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::time::Duration;
use term_guard::analyzers::inference::{TypeInferenceEngine, TypeStats};
use term_guard::test_fixtures::create_minimal_tpc_h_context;
use tokio::runtime::Runtime;

async fn setup_context() -> datafusion::prelude::SessionContext {
    create_minimal_tpc_h_context().await.unwrap()
}

fn bench_single_column_inference(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(setup_context());

    let mut group = c.benchmark_group("single_column_inference");
    group.measurement_time(Duration::from_secs(10));

    // Test different column types
    let test_cases = vec![
        ("integer_column", "l_orderkey"),
        ("float_column", "l_extendedprice"),
        ("categorical_column", "l_returnflag"),
        ("date_column", "l_shipdate"),
        ("text_column", "l_comment"),
    ];

    for (name, column) in test_cases {
        let engine = TypeInferenceEngine::new();
        group.bench_with_input(
            BenchmarkId::new("default_config", name),
            &column,
            |b, &column| {
                b.iter(|| {
                    rt.block_on(engine.infer_column_type(
                        std::hint::black_box(&ctx),
                        std::hint::black_box("lineitem"),
                        std::hint::black_box(column),
                    ))
                });
            },
        );
    }

    group.finish();
}

fn bench_sample_size_performance(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(setup_context());

    let mut group = c.benchmark_group("sample_size_performance");
    group.measurement_time(Duration::from_secs(8));

    let sample_sizes = vec![10, 50, 100, 500, 1000, 2000];

    for sample_size in sample_sizes {
        let engine = TypeInferenceEngine::builder()
            .sample_size(sample_size)
            .build();

        group.bench_with_input(
            BenchmarkId::new("sample_size", sample_size),
            &engine,
            |b, engine| {
                b.iter(|| {
                    rt.block_on(engine.infer_column_type(
                        std::hint::black_box(&ctx),
                        std::hint::black_box("lineitem"),
                        std::hint::black_box("l_extendedprice"),
                    ))
                });
            },
        );
    }

    group.finish();
}

fn bench_confidence_threshold_impact(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(setup_context());

    let mut group = c.benchmark_group("confidence_threshold_impact");
    group.measurement_time(Duration::from_secs(8));

    let thresholds = vec![0.1, 0.3, 0.5, 0.7, 0.9, 0.95];

    for threshold in thresholds {
        let engine = TypeInferenceEngine::builder()
            .confidence_threshold(threshold)
            .build();

        group.bench_with_input(
            BenchmarkId::new("threshold", (threshold * 100.0) as u32),
            &engine,
            |b, engine| {
                b.iter(|| {
                    rt.block_on(engine.infer_column_type(
                        std::hint::black_box(&ctx),
                        std::hint::black_box("lineitem"),
                        std::hint::black_box("l_quantity"),
                    ))
                });
            },
        );
    }

    group.finish();
}

fn bench_multiple_column_inference(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(setup_context());

    let mut group = c.benchmark_group("multiple_column_inference");
    group.measurement_time(Duration::from_secs(15));

    let column_sets = vec![
        (
            "two_columns",
            vec!["l_orderkey".to_string(), "l_quantity".to_string()],
        ),
        (
            "four_columns",
            vec![
                "l_orderkey".to_string(),
                "l_quantity".to_string(),
                "l_extendedprice".to_string(),
                "l_returnflag".to_string(),
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
        let engine = TypeInferenceEngine::new();
        group.bench_with_input(BenchmarkId::new("parallel", name), columns, |b, columns| {
            b.iter(|| {
                rt.block_on(engine.infer_multiple_columns(
                    std::hint::black_box(&ctx),
                    std::hint::black_box("lineitem"),
                    std::hint::black_box(columns),
                ))
            });
        });
    }

    group.finish();
}

fn bench_categorical_threshold_impact(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(setup_context());

    let mut group = c.benchmark_group("categorical_threshold_impact");
    group.measurement_time(Duration::from_secs(8));

    let thresholds = vec![10, 50, 100, 500, 1000];

    for threshold in thresholds {
        let engine = TypeInferenceEngine::builder()
            .categorical_threshold(threshold)
            .build();

        group.bench_with_input(
            BenchmarkId::new("threshold", threshold),
            &engine,
            |b, engine| {
                b.iter(|| {
                    rt.block_on(engine.infer_column_type(
                        std::hint::black_box(&ctx),
                        std::hint::black_box("lineitem"),
                        std::hint::black_box("l_comment"),
                    ))
                });
            },
        );
    }

    group.finish();
}

fn bench_decimal_precision_detection(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(setup_context());

    let mut group = c.benchmark_group("decimal_precision_detection");
    group.measurement_time(Duration::from_secs(8));

    // Compare with precision detection enabled vs disabled
    let configs = vec![("precision_enabled", true), ("precision_disabled", false)];

    for (name, enable_precision) in configs {
        let engine = TypeInferenceEngine::builder()
            .detect_decimal_precision(enable_precision)
            .build();

        group.bench_with_input(
            BenchmarkId::new(name, "l_extendedprice"),
            &engine,
            |b, engine| {
                b.iter(|| {
                    rt.block_on(engine.infer_column_type(
                        std::hint::black_box(&ctx),
                        std::hint::black_box("lineitem"),
                        std::hint::black_box("l_extendedprice"),
                    ))
                });
            },
        );
    }

    group.finish();
}

fn bench_international_formats(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(setup_context());

    let mut group = c.benchmark_group("international_formats");
    group.measurement_time(Duration::from_secs(8));

    // Compare with international formats enabled vs disabled
    let configs = vec![
        ("international_enabled", true),
        ("international_disabled", false),
    ];

    for (name, enable_international) in configs {
        let engine = TypeInferenceEngine::builder()
            .international_formats(enable_international)
            .build();

        group.bench_with_input(
            BenchmarkId::new(name, "l_shipdate"),
            &engine,
            |b, engine| {
                b.iter(|| {
                    rt.block_on(engine.infer_column_type(
                        std::hint::black_box(&ctx),
                        std::hint::black_box("lineitem"),
                        std::hint::black_box("l_shipdate"),
                    ))
                });
            },
        );
    }

    group.finish();
}

fn bench_pattern_matching_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("pattern_matching");
    group.measurement_time(Duration::from_secs(5));

    // Test pattern matching performance on different value types
    let test_values = vec![
        ("integer", "12345"),
        ("float", "123.45"),
        ("boolean_true", "true"),
        ("boolean_false", "false"),
        ("date_iso", "2023-12-25"),
        ("date_us", "12/25/2023"),
        ("datetime", "2023-12-25T10:30:00"),
        ("text", "hello world"),
    ];

    let engine = TypeInferenceEngine::new();

    for (name, value) in test_values {
        group.bench_with_input(
            BenchmarkId::new("pattern_test", name),
            &value,
            |b, &value| {
                b.iter(|| {
                    let mut stats = TypeStats::new();
                    engine.test_patterns(
                        std::hint::black_box(value),
                        std::hint::black_box(&mut stats),
                    );
                });
            },
        );
    }

    group.finish();
}

fn bench_type_determination(c: &mut Criterion) {
    let mut group = c.benchmark_group("type_determination");
    group.measurement_time(Duration::from_secs(5));

    let engine = TypeInferenceEngine::new();

    // Create different statistics scenarios
    let scenarios = vec![
        ("pure_integer", create_integer_stats()),
        ("pure_float", create_float_stats()),
        ("mixed_numeric", create_mixed_numeric_stats()),
        ("categorical", create_categorical_stats()),
        ("text", create_text_stats()),
    ];

    for (name, stats) in scenarios {
        group.bench_with_input(
            BenchmarkId::new("determination", name),
            &stats,
            |b, stats| {
                b.iter(|| engine.determine_type(std::hint::black_box(stats)));
            },
        );
    }

    group.finish();
}

// Helper functions to create test statistics
fn create_integer_stats() -> TypeStats {
    let mut stats = TypeStats::new();
    stats.total_samples = 100;
    stats.null_count = 0;
    stats.integer_matches = 100;
    for i in 0..100 {
        stats.unique_values.insert(i.to_string(), 1);
    }
    stats
}

fn create_float_stats() -> TypeStats {
    let mut stats = TypeStats::new();
    stats.total_samples = 100;
    stats.null_count = 0;
    stats.float_matches = 100;
    stats.decimal_info = Some((10, 2));
    for i in 0..100 {
        stats.unique_values.insert(format!("{i}.{:02}", i % 100), 1);
    }
    stats
}

fn create_mixed_numeric_stats() -> TypeStats {
    let mut stats = TypeStats::new();
    stats.total_samples = 100;
    stats.null_count = 0;
    stats.integer_matches = 50;
    stats.float_matches = 50;
    for i in 0..100 {
        if i % 2 == 0 {
            stats.unique_values.insert(i.to_string(), 1);
        } else {
            stats.unique_values.insert(format!("{i}.5"), 1);
        }
    }
    stats
}

fn create_categorical_stats() -> TypeStats {
    let mut stats = TypeStats::new();
    stats.total_samples = 100;
    stats.null_count = 0;
    stats.unique_values.insert("A".to_string(), 40);
    stats.unique_values.insert("B".to_string(), 30);
    stats.unique_values.insert("C".to_string(), 20);
    stats.unique_values.insert("D".to_string(), 10);
    stats
}

fn create_text_stats() -> TypeStats {
    let mut stats = TypeStats::new();
    stats.total_samples = 100;
    stats.null_count = 0;
    for i in 0..100 {
        stats.unique_values.insert(format!("text_value_{i}"), 1);
    }
    stats
}

criterion_group!(
    benches,
    bench_single_column_inference,
    bench_sample_size_performance,
    bench_confidence_threshold_impact,
    bench_multiple_column_inference,
    bench_categorical_threshold_impact,
    bench_decimal_precision_detection,
    bench_international_formats,
    bench_pattern_matching_performance,
    bench_type_determination
);

criterion_main!(benches);
