# How to Run Benchmarks

This guide shows you how to run performance benchmarks to measure and optimize Term's validation performance.

## Prerequisites

- Term v0.2.0 or later with `bench` feature
- Rust toolchain with `cargo bench` support
- TPC-H dataset for standard benchmarks

## Running Built-in Benchmarks

### Quick Start

Run all benchmarks:

```bash
cargo bench
```

Run specific benchmark suite:

```bash
# Individual constraint benchmarks
cargo bench --bench constraints

# Suite scaling benchmarks
cargo bench --bench suite_scaling

# Data scaling benchmarks
cargo bench --bench data_scaling

# Advanced analytics benchmarks
cargo bench --bench advanced_analytics

# Memory usage benchmarks
cargo bench --bench memory_usage

# Concurrent validation benchmarks
cargo bench --bench concurrent
```

### Benchmark Output

Benchmarks produce detailed performance metrics:

```
Constraints/UnifiedCompleteness
                        time:   [1.2341 ms 1.2389 ms 1.2439 ms]
                        thrpt:  [8.0393 Melem/s 8.0719 Melem/s 8.1032 Melem/s]
                 change:
                        time:   [-2.3421% -1.9832% -1.6234%] (p = 0.00 < 0.05)
                        thrpt:  [+1.6502% +2.0233% +2.3983%]
                        Performance has improved.
```

## Benchmark Categories

### 1. Individual Constraint Performance

Tests single constraint execution speed:

```rust
// Benchmarked constraints
- UnifiedCompletenessConstraint
- UnifiedStatisticalConstraint  
- UnifiedUniquenessConstraint
- FormatConstraint
- UnifiedQuantileConstraint
- UnifiedCorrelationConstraint
```

Run with:
```bash
cargo bench --bench constraints -- --verbose
```

### 2. Suite Scaling

Measures performance with varying numbers of checks:

```rust
// Test scenarios
- 10 checks in suite
- 50 checks in suite
- 100 checks in suite
- 500 checks in suite
```

Run with:
```bash
cargo bench --bench suite_scaling
```

### 3. Data Scaling

Tests performance across different data sizes:

```rust
// Data sizes tested
- 100 rows
- 1,000 rows
- 10,000 rows
- 100,000 rows
- 1,000,000 rows
- 10,000,000 rows
```

Run with:
```bash
cargo bench --bench data_scaling
```

### 4. Advanced Analytics

Benchmarks sophisticated analyzers:

```rust
// Analyzers tested
- KllSketchAnalyzer (approximate quantiles)
- CorrelationAnalyzer (Pearson, Spearman)
- MutualInformationAnalyzer
- IncrementalAnalysisRunner
```

Run with:
```bash
cargo bench --bench advanced_analytics
```

### 5. Memory Usage

Profiles memory consumption:

```rust
// Scenarios tested
- Large dataset loading
- Multiple analyzer states
- Incremental state accumulation
- Repository storage growth
```

Run with:
```bash
cargo bench --bench memory_usage
```

### 6. Concurrent Validation

Tests parallel execution performance:

```rust
// Concurrency levels
- Single-threaded
- 2 threads
- 4 threads
- 8 threads
- 16 threads
```

Run with:
```bash
cargo bench --bench concurrent
```

## Custom Benchmarks

### Creating Your Own Benchmark

Create a new benchmark in `benches/my_benchmark.rs`:

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use term_guard::prelude::*;

async fn setup_data() -> SessionContext {
    // Create test data
    let ctx = SessionContext::new();
    // ... register data
    ctx
}

fn bench_my_validation(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let ctx = runtime.block_on(setup_data());
    
    c.bench_function("my_validation", |b| {
        b.to_async(&runtime).iter(|| async {
            let suite = ValidationSuite::new("benchmark")
                .add_check(
                    Check::new("completeness")
                        .add_constraint(UnifiedCompletenessConstraint::new("column"))
                );
            
            black_box(suite.run(&ctx).await)
        });
    });
}

criterion_group!(benches, bench_my_validation);
criterion_main!(benches);
```

Add to `Cargo.toml`:

```toml
[[bench]]
name = "my_benchmark"
harness = false
```

Run with:
```bash
cargo bench --bench my_benchmark
```

### Parameterized Benchmarks

Test across multiple configurations:

```rust
use criterion::{BenchmarkId, Criterion};

fn bench_data_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("data_sizes");
    
    for size in [100, 1000, 10000, 100000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            &size,
            |b, &size| {
                let data = generate_data(size);
                b.iter(|| validate_data(&data));
            },
        );
    }
    
    group.finish();
}
```

## Profiling and Analysis

### Generate Flame Graphs

```bash
# Install flamegraph
cargo install flamegraph

# Run with profiling
cargo flamegraph --bench constraints

# Output: flamegraph.svg
```

### CPU Profiling

```bash
# Using perf on Linux
cargo bench --bench constraints &
perf record -p $! -g
perf report

# Using Instruments on macOS
cargo bench --bench constraints
# Open Instruments.app and attach to process
```

### Memory Profiling

```bash
# Using Valgrind
valgrind --tool=massif cargo bench --bench memory_usage
ms_print massif.out.<pid>

# Using heaptrack
heaptrack cargo bench --bench memory_usage
heaptrack_gui heaptrack.cargo.*.gz
```

## Comparing Results

### Baseline Comparison

Save baseline results:

```bash
cargo bench --bench constraints -- --save-baseline main
```

Compare against baseline:

```bash
# After making changes
cargo bench --bench constraints -- --baseline main
```

### Statistical Analysis

Criterion provides statistical analysis by default:

```
Performance has regressed.
Found 3 outliers among 100 measurements (3.00%)
  2 (2.00%) low mild
  1 (1.00%) high mild
```

### Generating Reports

HTML reports are generated in `target/criterion/`:

```bash
# Open report
open target/criterion/report/index.html

# View specific benchmark
open target/criterion/Constraints/UnifiedCompleteness/report/index.html
```

## Performance Optimization Workflow

### 1. Establish Baseline

```bash
# Run full benchmark suite
cargo bench -- --save-baseline before-optimization

# Document current performance
cat target/criterion/*/base/estimates.json
```

### 2. Profile Hotspots

```bash
# Generate flamegraph
cargo flamegraph --bench constraints

# Identify bottlenecks
# Look for wide bars in flamegraph
```

### 3. Implement Optimizations

Common optimization targets:
- Query generation
- Data serialization
- Memory allocations
- Lock contention

### 4. Verify Improvements

```bash
# Run benchmarks again
cargo bench -- --baseline before-optimization

# Check for regressions
cargo bench -- --baseline before-optimization --strict
```

### 5. Document Changes

Create benchmark report:

```markdown
## Performance Improvements

### Query Optimization
- **Before**: 1.234ms per validation
- **After**: 0.987ms per validation  
- **Improvement**: 20% reduction

### Memory Usage
- **Before**: 512MB peak
- **After**: 384MB peak
- **Improvement**: 25% reduction
```

## Continuous Benchmarking

### GitHub Actions Integration

`.github/workflows/bench.yml`:

```yaml
name: Benchmarks

on:
  pull_request:
    paths:
      - 'src/**'
      - 'benches/**'
      - 'Cargo.toml'

jobs:
  benchmark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Install Rust
        uses: actions-rs/toolchain@v1
        with:
          toolchain: stable
          
      - name: Run benchmarks
        run: cargo bench -- --output-format bencher | tee output.txt
        
      - name: Compare benchmarks
        uses: benchmark-action/github-action-benchmark@v1
        with:
          tool: 'cargo'
          output-file-path: output.txt
          github-token: ${{ secrets.GITHUB_TOKEN }}
          auto-push: true
```

### Performance Regression Detection

```rust
#[test]
fn performance_regression_test() {
    let start = Instant::now();
    
    // Run performance-critical operation
    let result = expensive_operation();
    
    let duration = start.elapsed();
    
    // Fail if performance degrades
    assert!(
        duration < Duration::from_millis(100),
        "Performance regression: operation took {:?}",
        duration
    );
}
```

## Benchmark Best Practices

### 1. Warm-Up Iterations

Criterion handles warm-up automatically, but for custom benchmarks:

```rust
// Warm up caches and JIT
for _ in 0..100 {
    black_box(operation());
}

// Actual benchmark
b.iter(|| operation());
```

### 2. Avoid Benchmark Optimization

Use `black_box` to prevent compiler optimizations:

```rust
b.iter(|| {
    let result = expensive_computation();
    black_box(result); // Prevent optimization
});
```

### 3. Realistic Data

Use production-like data:

```rust
// Load TPC-H data for realistic benchmarks
let ctx = create_tpc_h_context().await?;
```

### 4. Multiple Runs

Ensure statistical significance:

```bash
# Increase sample size
cargo bench -- --sample-size 200

# Increase measurement time
cargo bench -- --measurement-time 30
```

## Interpreting Results

### Understanding Metrics

- **time**: Wall-clock time per iteration
- **thrpt**: Throughput (iterations/second)
- **change**: Percentage change from baseline
- **outliers**: Anomalous measurements

### Performance Goals

Set concrete targets:

```rust
// Goal: Process 1M rows in under 1 second
assert!(duration < Duration::from_secs(1));

// Goal: Memory usage under 100MB
assert!(memory_usage < 100 * 1024 * 1024);
```

## Troubleshooting

### Inconsistent Results

- Ensure system is idle
- Disable CPU frequency scaling
- Close unnecessary applications
- Run with `nice -n -20` for priority

### Out of Memory

- Reduce data size in benchmarks
- Increase system swap space
- Use incremental processing

### Slow Benchmarks

- Reduce iteration count: `--sample-size 10`
- Skip slow benchmarks: `--skip slow_benchmark`
- Run specific benchmark: `cargo bench specific_name`

## Next Steps

- Review [performance optimization](optimize-performance.md) guide
- Explore [incremental analysis](use-incremental-analysis.md) for large datasets
- See [analyzer architecture](../explanation/analyzer-architecture.md) for design details