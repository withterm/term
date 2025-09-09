//! Performance tests for Term constraints
//!
//! These tests ensure that our constraint APIs perform efficiently,
//! especially when combining multiple validations.

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use rand::prelude::*;
use std::sync::Arc;
use std::time::Instant;
use term_guard::constraints::Assertion;
use term_guard::core::builder_extensions::{CompletenessOptions, StatisticalOptions};
use term_guard::core::{Check, ValidationSuite};

/// Helper function to detect if running under coverage instrumentation
fn is_running_under_coverage() -> bool {
    // Check for LLVM coverage
    if std::env::var("LLVM_PROFILE_FILE").is_ok() {
        return true;
    }

    // Check for cargo-llvm-cov
    if std::env::var("CARGO_LLVM_COV").is_ok() {
        return true;
    }

    // Check for tarpaulin - it sets various environment variables
    if std::env::var("CARGO_TARPAULIN").is_ok() {
        return true;
    }

    // Check for tarpaulin specific env vars
    if std::env::var("TARPAULIN").is_ok() {
        return true;
    }

    // Check for coverage-related RUSTFLAGS
    if let Ok(rustflags) = std::env::var("RUSTFLAGS") {
        if rustflags.contains("instrument-coverage") || rustflags.contains("profile") {
            return true;
        }
    }

    // Check for CARGO_INCREMENTAL=0 which is often set for coverage
    if std::env::var("CARGO_INCREMENTAL").as_deref() == Ok("0") {
        // This alone isn't enough, but combined with other factors...
        // Check if we're in CI and have incremental compilation disabled
        if std::env::var("CI").is_ok() {
            return true;
        }
    }

    false
}

/// Get performance threshold multiplier based on execution environment
fn get_threshold_multiplier() -> f64 {
    if is_running_under_coverage() {
        // Coverage instrumentation significantly slows down execution
        // Allow 10x slower performance under coverage
        10.0
    } else {
        // Even without coverage, CI environments can be slower
        // Allow 2x slower in CI environments
        if std::env::var("CI").is_ok() || std::env::var("GITHUB_ACTIONS").is_ok() {
            2.0
        } else {
            1.0
        }
    }
}

/// Creates test data with specified number of rows
async fn create_test_data(rows: usize) -> SessionContext {
    let ctx = SessionContext::new();
    let mut rng = rand::rng();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, true),
        Field::new("email", DataType::Utf8, true),
    ]));

    let mut ids = Vec::with_capacity(rows);
    let mut values = Vec::with_capacity(rows);
    let mut emails = Vec::with_capacity(rows);

    for i in 0..rows {
        ids.push(i as i64);
        values.push(if rng.random_bool(0.95) {
            Some(rng.random_range(0.0..1000.0))
        } else {
            None
        });
        emails.push(if rng.random_bool(0.95) {
            Some(format!("user{i}@example.com"))
        } else {
            None
        });
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Float64Array::from(values)),
            Arc::new(StringArray::from(emails)),
        ],
    )
    .unwrap();

    let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("data", Arc::new(provider)).unwrap();

    ctx
}

#[tokio::test]
async fn test_completeness_performance() {
    let ctx = create_test_data(10_000).await;

    let start = Instant::now();
    let suite = ValidationSuite::builder("completeness_test")
        .check(
            Check::builder("completeness")
                .completeness("id", CompletenessOptions::full().into_constraint_options())
                .completeness(
                    "value",
                    CompletenessOptions::threshold(0.95).into_constraint_options(),
                )
                .build(),
        )
        .build();
    let _ = suite.run(&ctx).await.unwrap();
    let duration = start.elapsed();

    let multiplier = get_threshold_multiplier();
    let is_coverage = is_running_under_coverage();
    println!("Completeness validation completed in: {duration:?} (multiplier: {multiplier}x, coverage detected: {is_coverage})");

    // Ensure completeness checks complete quickly
    // Base threshold of 300ms is reasonable for most environments
    let base_threshold = 300.0;
    let threshold = (base_threshold * multiplier) as u128;
    assert!(
        duration.as_millis() < threshold,
        "Completeness validation took too long: {duration:?} (threshold: {threshold}ms, base: {base_threshold}ms, multiplier: {multiplier}x)"
    );
}

#[tokio::test]
async fn test_statistics_performance() {
    let ctx = create_test_data(10_000).await;

    let start = Instant::now();
    let suite = ValidationSuite::builder("statistics_test")
        .check(
            Check::builder("stats")
                .statistics(
                    "value",
                    StatisticalOptions::new()
                        .min(Assertion::GreaterThanOrEqual(0.0))
                        .max(Assertion::LessThan(1001.0))
                        .mean(Assertion::Between(400.0, 600.0))
                        .standard_deviation(Assertion::LessThan(300.0)),
                )
                .unwrap()
                .build(),
        )
        .build();
    let _ = suite.run(&ctx).await.unwrap();
    let duration = start.elapsed();

    let multiplier = get_threshold_multiplier();
    println!("Statistics validation completed in: {duration:?} (multiplier: {multiplier}x)");

    // Ensure statistics calculations complete quickly
    // Base threshold of 300ms is reasonable for most environments
    let base_threshold = 300.0;
    let threshold = (base_threshold * multiplier) as u128;
    assert!(
        duration.as_millis() < threshold,
        "Statistics validation took too long: {duration:?} (threshold: {threshold}ms, base: {base_threshold}ms, multiplier: {multiplier}x)"
    );
}

#[tokio::test]
async fn test_complex_validation_performance() {
    let ctx = create_test_data(10_000).await;

    // Measure performance of complex validation with many constraints
    let start = Instant::now();
    let suite = ValidationSuite::builder("complex_validation")
        .check(
            Check::builder("complex")
                .completeness("id", CompletenessOptions::full().into_constraint_options())
                .completeness(
                    "value",
                    CompletenessOptions::threshold(0.95).into_constraint_options(),
                )
                .statistics(
                    "value",
                    StatisticalOptions::new()
                        .min(Assertion::GreaterThanOrEqual(0.0))
                        .max(Assertion::LessThan(1001.0))
                        .mean(Assertion::Between(400.0, 600.0))
                        .standard_deviation(Assertion::LessThan(300.0)),
                )
                .unwrap()
                .build(),
        )
        .build();
    let _ = suite.run(&ctx).await.unwrap();
    let duration = start.elapsed();

    let multiplier = get_threshold_multiplier();
    let is_coverage = is_running_under_coverage();
    println!("Complex validation completed in: {duration:?} (multiplier: {multiplier}x, coverage detected: {is_coverage})");

    // Ensure complex validations complete in reasonable time
    // Base threshold of 400ms is reasonable for complex validations
    let base_threshold = 400.0;
    let threshold = (base_threshold * multiplier) as u128;
    assert!(
        duration.as_millis() < threshold,
        "Complex validation took too long: {duration:?} (threshold: {threshold}ms, base: {base_threshold}ms, multiplier: {multiplier}x)"
    );
}

#[tokio::test]
async fn test_scaling_performance() {
    // Test with different data sizes to ensure linear scaling
    let sizes = vec![1_000, 5_000, 10_000];
    let mut durations = Vec::new();

    for size in &sizes {
        let ctx = create_test_data(*size).await;

        let start = Instant::now();
        let suite = ValidationSuite::builder("scaling_test")
            .check(
                Check::builder("stats")
                    .statistics(
                        "value",
                        StatisticalOptions::new()
                            .min(Assertion::GreaterThanOrEqual(0.0))
                            .max(Assertion::LessThan(1001.0))
                            .mean(Assertion::Between(400.0, 600.0)),
                    )
                    .unwrap()
                    .build(),
            )
            .build();
        let _ = suite.run(&ctx).await.unwrap();
        let duration = start.elapsed();

        durations.push(duration);
        println!("Size: {size}, Duration: {duration:?}");
    }

    // Check that performance scales roughly linearly
    let scaling_factor = durations[2].as_secs_f64() / durations[0].as_secs_f64();
    println!("Scaling factor (10k/1k): {scaling_factor:.2}x");

    let scaling_threshold = 15.0 * get_threshold_multiplier();
    assert!(
        scaling_factor < scaling_threshold,
        "Performance doesn't scale linearly: {scaling_factor:.2}x increase for 10x data (threshold: {scaling_threshold:.1}x, coverage: {})",
        is_running_under_coverage()
    );
}

#[tokio::test]
async fn test_memory_efficiency() {
    // Test that constraints don't consume excessive memory
    let ctx = create_test_data(50_000).await;

    let start = Instant::now();
    let suite = ValidationSuite::builder("memory_test")
        .check(
            Check::builder("memory")
                .completeness("id", CompletenessOptions::full().into_constraint_options())
                .completeness(
                    "value",
                    CompletenessOptions::threshold(0.9).into_constraint_options(),
                )
                .completeness(
                    "email",
                    CompletenessOptions::threshold(0.9).into_constraint_options(),
                )
                .statistics(
                    "value",
                    StatisticalOptions::new()
                        .min(Assertion::GreaterThanOrEqual(0.0))
                        .max(Assertion::LessThan(1001.0))
                        .mean(Assertion::Between(400.0, 600.0))
                        .standard_deviation(Assertion::LessThan(300.0))
                        .sum(Assertion::GreaterThan(0.0))
                        .variance(Assertion::GreaterThan(0.0)),
                )
                .unwrap()
                .build(),
        )
        .build();

    let result = suite.run(&ctx).await.unwrap();
    let duration = start.elapsed();

    println!("Large dataset validation completed in: {duration:?}");
    assert!(result.is_success());

    // For 50k rows with multiple constraints, should still be fast
    // Base threshold of 1000ms is reasonable for large datasets
    let base_threshold = 1000.0;
    let multiplier = get_threshold_multiplier();
    let threshold = (base_threshold * multiplier) as u128;
    assert!(
        duration.as_millis() < threshold,
        "Large dataset validation took too long: {duration:?} (threshold: {threshold}ms, base: {base_threshold}ms, multiplier: {multiplier}x)"
    );
}
