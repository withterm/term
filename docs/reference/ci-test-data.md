# CI Integration for TPC-H Test Data

This document describes how to integrate the TPC-H test data generation into your CI/CD pipeline.

## Overview

The Term library provides built-in TPC-H test data generation through the `test-utils` feature. This allows you to run comprehensive data validation tests in CI without needing external data files or databases.

## Enabling Test Utils in CI

### Cargo Configuration

To use the test utilities in CI, enable the `test-utils` feature when running tests:

```bash
cargo test --features test-utils
```

### GitHub Actions Example

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Install Rust
        uses: actions-rs/toolchain@v1
        with:
          toolchain: stable
          override: true
      
      - name: Run tests with test utilities
        run: cargo test --features test-utils
      
      - name: Run specific TPC-H tests
        run: cargo test --test test_utils_test --features test-utils
```

### GitLab CI Example

```yaml
test:
  stage: test
  script:
    - cargo test --features test-utils
  cache:
    paths:
      - target/
      - Cargo.lock
```

## Using TPC-H Data in Tests

### Basic Usage

```rust
use term_core::test_utils::{create_tpc_h_context, ScaleFactor};

#[tokio::test]
async fn test_with_tpc_h_data() {
    // Create a context with small-scale TPC-H data
    let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();
    
    // Run your validation tests
    let suite = ValidationSuite::builder()
        .with_check(
            Check::builder()
                .with_constraint(Completeness::new("orders", "o_orderkey"))
                .build()
        )
        .build();
    
    let results = suite.run(&ctx).await.unwrap();
    assert!(results.is_success());
}
```

### Using Test Fixtures

The library also provides pre-configured test fixtures for common scenarios:

```rust
use term_core::test_fixtures::*;

#[tokio::test]
async fn test_null_handling() {
    let ctx = create_context_with_nulls().await.unwrap();
    // Test completeness checks on data with nulls
}

#[tokio::test]
async fn test_duplicate_detection() {
    let ctx = create_context_with_duplicates().await.unwrap();
    // Test uniqueness constraints
}

#[tokio::test]
async fn test_outlier_detection() {
    let ctx = create_context_with_outliers().await.unwrap();
    // Test statistical anomaly detection
}
```

## Performance Considerations

### Scale Factors

The test utilities support three scale factors:

- `ScaleFactor::SF01` (0.1): ~10MB data, suitable for quick unit tests
- `ScaleFactor::SF1` (1.0): ~100MB data, suitable for integration tests
- `ScaleFactor::SF10` (10.0): ~1GB data, suitable for performance tests

For CI, we recommend using `SF01` for most tests to keep build times fast.

### Row Limits

The test data is automatically limited to prevent excessive memory usage:

- Customer: max 1,000 rows
- Orders: max 1,000 rows  
- LineItem: max 5,000 rows
- Part: max 1,000 rows
- PartSupp: max 4,000 rows
- Supplier: max 100 rows
- Nation: 25 rows (fixed)
- Region: 5 rows (fixed)

## Memory Configuration

For CI environments with limited memory, configure the DataFusion memory pool:

```rust
use term_core::core::{TermContext, TermContextConfig};

let config = TermContextConfig {
    max_memory: 100 * 1024 * 1024, // 100MB limit
    ..Default::default()
};

let ctx = TermContext::with_config(config)?;
```

## Parallel Test Execution

The test data generation is deterministic and thread-safe, allowing parallel test execution:

```bash
# Run tests in parallel (default behavior)
cargo test --features test-utils

# Or explicitly set the number of threads
RUST_TEST_THREADS=4 cargo test --features test-utils
```

## Docker Integration

Example Dockerfile for running tests with TPC-H data:

```dockerfile
FROM rust:1.70 as builder

WORKDIR /app
COPY . .

# Run tests with test utilities
RUN cargo test --features test-utils

# Build release binary
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=builder /app/target/release/term /usr/local/bin/
CMD ["term"]
```

## Caching Considerations

The test data is generated in-memory and doesn't require file system caching. However, you can cache the compiled test utilities:

### GitHub Actions Cache

```yaml
- uses: actions/cache@v3
  with:
    path: |
      ~/.cargo/registry
      ~/.cargo/git
      target
    key: ${{ runner.os }}-cargo-${{ hashFiles('**/Cargo.lock') }}
```

## Debugging Test Failures

Enable detailed logging to debug test failures:

```bash
RUST_LOG=debug cargo test --features test-utils -- --nocapture
```

Or for specific modules:

```bash
RUST_LOG=term_core::test_utils=debug cargo test --features test-utils
```

## Best Practices

1. **Use the smallest scale factor** that adequately tests your validation logic
2. **Run quick tests first** using `SF01`, then comprehensive tests with larger scales
3. **Parallelize independent tests** to reduce CI time
4. **Set memory limits** appropriate for your CI environment
5. **Use test fixtures** for common scenarios instead of generating custom data
6. **Cache dependencies** but not test data (it's generated quickly)

## Example CI Configuration

Complete example for GitHub Actions:

```yaml
name: Data Validation Tests

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

env:
  CARGO_TERM_COLOR: always
  RUST_BACKTRACE: 1

jobs:
  test:
    name: Test Suite
    runs-on: ubuntu-latest
    strategy:
      matrix:
        scale: [SF01, SF1]
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Install Rust
      uses: actions-rs/toolchain@v1
      with:
        profile: minimal
        toolchain: stable
        override: true
        components: rustfmt, clippy
    
    - name: Cache dependencies
      uses: actions/cache@v3
      with:
        path: |
          ~/.cargo/registry
          ~/.cargo/git
          target
        key: ${{ runner.os }}-cargo-${{ hashFiles('**/Cargo.lock') }}
    
    - name: Check formatting
      run: cargo fmt -- --check
    
    - name: Run clippy
      run: cargo clippy --features test-utils -- -D warnings
    
    - name: Run tests with scale ${{ matrix.scale }}
      run: |
        cargo test --features test-utils -- --test-threads=2
      env:
        TEST_SCALE_FACTOR: ${{ matrix.scale }}
    
    - name: Run doc tests
      run: cargo test --doc --features test-utils
```

This configuration ensures your data validation logic is thoroughly tested with realistic TPC-H data in CI.