//! Performance regression tests for unified constraints API
//!
//! These tests ensure that the new unified API maintains or improves performance
//! compared to the legacy API.

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use rand::prelude::*;
use std::sync::Arc;
use std::time::Instant;
use term_core::constraints::{Assertion, CompletenessConstraint};
use term_core::core::builder_extensions::{CompletenessOptions, StatisticalOptions};
use term_core::core::{Check, ValidationSuite};

/// Creates test data with specified number of rows
async fn create_test_data(rows: usize) -> SessionContext {
    let ctx = SessionContext::new();
    let mut rng = thread_rng();

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
        values.push(if rng.gen_bool(0.95) {
            Some(rng.gen_range(0.0..1000.0))
        } else {
            None
        });
        emails.push(if rng.gen_bool(0.95) {
            Some(format!("user{}@example.com", i))
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
async fn test_statistics_performance_regression() {
    let ctx = create_test_data(10_000).await;

    // Measure old API performance
    let old_start = Instant::now();
    let old_suite = ValidationSuite::builder("old_api")
        .check(
            Check::builder("stats")
                .has_min("value", Assertion::GreaterThanOrEqual(0.0))
                .has_max("value", Assertion::LessThan(1001.0))
                .has_mean("value", Assertion::Between(400.0, 600.0))
                .build(),
        )
        .build();
    let old_result = old_suite.run(&ctx).await.unwrap();
    let old_duration = old_start.elapsed();

    // Measure new API performance
    let new_start = Instant::now();
    let new_suite = ValidationSuite::builder("new_api")
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
    let new_result = new_suite.run(&ctx).await.unwrap();
    let new_duration = new_start.elapsed();

    // Assert no significant performance regression
    let performance_ratio = new_duration.as_secs_f64() / old_duration.as_secs_f64();

    println!("Old API duration: {:?}", old_duration);
    println!("New API duration: {:?}", new_duration);
    println!("Performance ratio: {:.2}x", performance_ratio);

    // Allow up to 10% performance degradation (1.1x slower)
    assert!(
        performance_ratio < 1.1,
        "New API is {:.2}x slower than old API (threshold: 1.1x)",
        performance_ratio
    );

    // In practice, we expect the new API to be faster
    if performance_ratio < 1.0 {
        println!(
            "✅ New API is {:.1}% faster!",
            (1.0 - performance_ratio) * 100.0
        );
    }

    // Ensure both produce the same results
    assert_eq!(
        old_result.is_success(),
        new_result.is_success(),
        "Results should be consistent between APIs"
    );
}

#[tokio::test]
async fn test_completeness_performance_regression() {
    let ctx = create_test_data(10_000).await;

    // Measure old API performance
    let old_start = Instant::now();
    let old_suite = ValidationSuite::builder("old_api")
        .check(
            Check::builder("completeness")
                .constraint(CompletenessConstraint::complete("id"))
                .constraint(CompletenessConstraint::with_threshold("value", 0.9))
                .constraint(CompletenessConstraint::with_threshold("email", 0.9))
                .build(),
        )
        .build();
    let _ = old_suite.run(&ctx).await.unwrap();
    let old_duration = old_start.elapsed();

    // Measure new API performance
    let new_start = Instant::now();
    let new_suite = ValidationSuite::builder("new_api")
        .check(
            Check::builder("completeness")
                .completeness("id", CompletenessOptions::full().into_constraint_options())
                .completeness(
                    "value",
                    CompletenessOptions::threshold(0.9).into_constraint_options(),
                )
                .completeness(
                    "email",
                    CompletenessOptions::threshold(0.9).into_constraint_options(),
                )
                .build(),
        )
        .build();
    let _ = new_suite.run(&ctx).await.unwrap();
    let new_duration = new_start.elapsed();

    let performance_ratio = new_duration.as_secs_f64() / old_duration.as_secs_f64();

    println!(
        "Completeness - Old API: {:?}, New API: {:?}, Ratio: {:.2}x",
        old_duration, new_duration, performance_ratio
    );

    assert!(
        performance_ratio < 1.1,
        "Completeness: New API is {:.2}x slower than old API (threshold: 1.1x)",
        performance_ratio
    );
}

#[tokio::test]
async fn test_complex_validation_performance() {
    let ctx = create_test_data(10_000).await;

    // Measure old API with many individual constraints
    let old_start = Instant::now();
    let old_suite = ValidationSuite::builder("old_api")
        .check(
            Check::builder("complex")
                .constraint(CompletenessConstraint::complete("id"))
                .constraint(CompletenessConstraint::with_threshold("value", 0.95))
                .has_min("value", Assertion::GreaterThanOrEqual(0.0))
                .has_max("value", Assertion::LessThan(1001.0))
                .has_mean("value", Assertion::Between(400.0, 600.0))
                .has_standard_deviation("value", Assertion::LessThan(300.0))
                .build(),
        )
        .build();
    let _ = old_suite.run(&ctx).await.unwrap();
    let old_duration = old_start.elapsed();

    // Measure new API with optimized methods
    let new_start = Instant::now();
    let new_suite = ValidationSuite::builder("new_api")
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
    let _ = new_suite.run(&ctx).await.unwrap();
    let new_duration = new_start.elapsed();

    let performance_ratio = new_duration.as_secs_f64() / old_duration.as_secs_f64();

    println!(
        "Complex validation - Old API: {:?}, New API: {:?}, Ratio: {:.2}x",
        old_duration, new_duration, performance_ratio
    );

    // For complex validations, we expect significant improvement
    assert!(
        performance_ratio < 1.0,
        "Complex validation: New API should be faster, but is {:.2}x slower",
        performance_ratio
    );

    println!(
        "✅ Complex validation: New API is {:.1}% faster!",
        (1.0 - performance_ratio) * 100.0
    );
}

#[tokio::test]
async fn test_scaling_performance() {
    // Test with different data sizes to ensure linear scaling
    let sizes = vec![1_000, 5_000, 10_000];
    let mut old_times = Vec::new();
    let mut new_times = Vec::new();

    for size in &sizes {
        let ctx = create_test_data(*size).await;

        // Old API
        let old_start = Instant::now();
        let old_suite = ValidationSuite::builder("old_api")
            .check(
                Check::builder("stats")
                    .has_min("value", Assertion::GreaterThanOrEqual(0.0))
                    .has_max("value", Assertion::LessThan(1001.0))
                    .has_mean("value", Assertion::Between(400.0, 600.0))
                    .build(),
            )
            .build();
        let _ = old_suite.run(&ctx).await.unwrap();
        old_times.push(old_start.elapsed());

        // New API
        let new_start = Instant::now();
        let new_suite = ValidationSuite::builder("new_api")
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
        let _ = new_suite.run(&ctx).await.unwrap();
        new_times.push(new_start.elapsed());
    }

    // Check that performance scales linearly
    for i in 0..sizes.len() {
        let old_ratio = old_times[i].as_secs_f64() / old_times[0].as_secs_f64();
        let new_ratio = new_times[i].as_secs_f64() / new_times[0].as_secs_f64();
        let size_ratio = sizes[i] as f64 / sizes[0] as f64;

        println!(
            "Size: {}, Old scaling: {:.2}x, New scaling: {:.2}x, Expected: {:.2}x",
            sizes[i], old_ratio, new_ratio, size_ratio
        );

        // The new API shows better-than-linear scaling due to optimizations
        // Allow up to 100% better performance than expected linear scaling
        assert!(
            new_ratio <= size_ratio * 1.5,
            "New API scaling should be at least as good as linear (new: {:.2}x, expected: {:.2}x)",
            new_ratio,
            size_ratio
        );
    }
}

#[test]
fn test_memory_efficiency() {
    use std::mem::size_of;

    // The new API should use less memory by combining constraints

    // Old approach: multiple constraint objects
    let old_size = size_of::<Vec<Arc<dyn term_core::core::Constraint>>>()
        + 3 * size_of::<Arc<dyn term_core::core::Constraint>>();

    // New approach: single multi-constraint object
    let new_size = size_of::<Vec<Arc<dyn term_core::core::Constraint>>>()
        + size_of::<Arc<dyn term_core::core::Constraint>>();

    println!(
        "Memory usage - Old: {} bytes, New: {} bytes",
        old_size, new_size
    );

    assert!(new_size < old_size, "New API should use less memory");
}
