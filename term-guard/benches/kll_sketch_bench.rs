use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use term_guard::analyzers::advanced::kll_sketch::KllSketch;

fn benchmark_kll_sketch_update(c: &mut Criterion) {
    let mut group = c.benchmark_group("kll_sketch_update");

    for k in [100, 200, 500, 1000].iter() {
        for n in [1000, 10_000, 100_000].iter() {
            group.throughput(Throughput::Elements(*n as u64));

            group.bench_with_input(
                BenchmarkId::from_parameter(format!("k{k}_n{n}")),
                &(*k, *n),
                |b, &(k, n)| {
                    b.iter(|| {
                        let mut sketch = KllSketch::new(k);
                        for i in 0..n {
                            sketch.update(std::hint::black_box(i as f64));
                        }
                        sketch
                    });
                },
            );
        }
    }

    group.finish();
}

fn benchmark_kll_sketch_quantile(c: &mut Criterion) {
    let mut group = c.benchmark_group("kll_sketch_quantile");

    // Pre-populate sketches with data
    let k_values = [100, 200, 500, 1000];
    let n = 100_000;

    let sketches: Vec<_> = k_values
        .iter()
        .map(|&k| {
            let mut sketch = KllSketch::new(k);
            for i in 0..n {
                sketch.update(i as f64);
            }
            sketch
        })
        .collect();

    for (i, &k) in k_values.iter().enumerate() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("k{k}_quantile")),
            &i,
            |b, &sketch_idx| {
                let sketch = &sketches[sketch_idx];
                b.iter(|| {
                    // Test multiple quantiles
                    let q50 = sketch.get_quantile(std::hint::black_box(0.5)).unwrap();
                    let q90 = sketch.get_quantile(std::hint::black_box(0.9)).unwrap();
                    let q99 = sketch.get_quantile(std::hint::black_box(0.99)).unwrap();
                    (q50, q90, q99)
                });
            },
        );
    }

    group.finish();
}

fn benchmark_kll_sketch_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("kll_sketch_merge");

    for k in [200, 500, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("k{k}_merge")),
            k,
            |b, &k| {
                b.iter_batched(
                    || {
                        // Setup: create two sketches with data
                        let mut sketch1 = KllSketch::new(k);
                        let mut sketch2 = KllSketch::new(k);

                        for i in 0..50_000 {
                            sketch1.update(i as f64);
                            sketch2.update((i + 50_000) as f64);
                        }

                        (sketch1, sketch2)
                    },
                    |(mut sketch1, sketch2)| {
                        sketch1.merge(&sketch2).unwrap();
                        sketch1
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn benchmark_kll_sketch_memory_usage(c: &mut Criterion) {
    let mut group = c.benchmark_group("kll_sketch_memory");

    for k in [100, 200, 500, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("k{k}_memory")),
            k,
            |b, &k| {
                b.iter(|| {
                    let mut sketch = KllSketch::new(k);

                    // Add varying amounts of data
                    for i in 0..100_000 {
                        sketch.update(i as f64);

                        // Sample memory usage every 10k updates
                        if i % 10_000 == 0 {
                            std::hint::black_box(sketch.memory_usage());
                        }
                    }

                    sketch.memory_usage()
                });
            },
        );
    }

    group.finish();
}

fn benchmark_kll_sketch_accuracy(c: &mut Criterion) {
    let mut group = c.benchmark_group("kll_sketch_accuracy");

    // This benchmark measures the accuracy vs performance trade-off
    for k in [50, 100, 200, 500, 1000, 2000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("k{k}_accuracy")),
            k,
            |b, &k| {
                b.iter(|| {
                    let mut sketch = KllSketch::new(k);

                    // Insert data in a known distribution (0 to 100,000)
                    for i in 0..100_000 {
                        sketch.update(i as f64);
                    }

                    // Measure quantiles and compare to expected values
                    let q25 = sketch.get_quantile(0.25).unwrap();
                    let q50 = sketch.get_quantile(0.50).unwrap();
                    let q75 = sketch.get_quantile(0.75).unwrap();
                    let q95 = sketch.get_quantile(0.95).unwrap();

                    // Expected values for uniform distribution 0-99,999
                    let expected_q25 = 25_000.0;
                    let expected_q50 = 50_000.0;
                    let expected_q75 = 75_000.0;
                    let expected_q95 = 95_000.0;

                    // Calculate relative errors
                    let error_q25 = (q25 - expected_q25).abs() / expected_q25;
                    let error_q50 = (q50 - expected_q50).abs() / expected_q50;
                    let error_q75 = (q75 - expected_q75).abs() / expected_q75;
                    let error_q95 = (q95 - expected_q95).abs() / expected_q95;

                    (error_q25, error_q50, error_q75, error_q95)
                });
            },
        );
    }

    group.finish();
}

fn benchmark_kll_sketch_compaction_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("kll_sketch_compaction");

    // Test how compaction affects performance as data grows
    for k in [100, 500, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("k{k}_compaction")),
            k,
            |b, &k| {
                b.iter_batched(
                    || KllSketch::new(k),
                    |mut sketch| {
                        // Add data that will trigger multiple compactions
                        for i in 0..200_000 {
                            sketch.update(std::hint::black_box((i as f64) % 1000.0));
                        }
                        sketch
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn benchmark_kll_sketch_random_vs_deterministic(c: &mut Criterion) {
    let mut group = c.benchmark_group("kll_sketch_randomness");

    // Compare performance with/without randomness (if test-utils feature is enabled)
    let k = 200;
    let n = 100_000;

    group.bench_function("deterministic", |b| {
        b.iter(|| {
            let mut sketch = KllSketch::new(k);
            for i in 0..n {
                sketch.update(std::hint::black_box(i as f64));
            }
            sketch.get_quantile(0.5).unwrap()
        });
    });

    #[cfg(feature = "test-utils")]
    group.bench_function("random", |b| {
        b.iter(|| {
            let mut sketch = KllSketch::new(k);
            for i in 0..n {
                sketch.update(std::hint::black_box(i as f64));
            }
            sketch.get_quantile(0.5).unwrap()
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_kll_sketch_update,
    benchmark_kll_sketch_quantile,
    benchmark_kll_sketch_merge,
    benchmark_kll_sketch_memory_usage,
    benchmark_kll_sketch_accuracy,
    benchmark_kll_sketch_compaction_overhead,
    benchmark_kll_sketch_random_vs_deterministic,
);

criterion_main!(benches);
