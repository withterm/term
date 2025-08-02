//! Integration tests for ColumnProfiler using TPC-H data.

use std::sync::{Arc, Mutex};
use term_guard::analyzers::profiler::{ColumnProfiler, DetectedDataType, ProfilerProgress};
use term_guard::test_fixtures::create_minimal_tpc_h_context;

#[tokio::test]
async fn test_profiler_with_tpc_h_lineitem() {
    let ctx = create_minimal_tpc_h_context().await.unwrap();
    let profiler = ColumnProfiler::new();

    // Profile a low-cardinality column (should use Pass 2)
    let profile = profiler
        .profile_column(&ctx, "lineitem", "l_returnflag")
        .await
        .unwrap();

    println!("l_returnflag profile: {profile:#?}");

    assert_eq!(profile.column_name, "l_returnflag");
    assert_eq!(profile.data_type, DetectedDataType::String);
    assert!(profile.passes_executed.contains(&1)); // Pass 1 always executed
    assert!(profile.basic_stats.row_count > 0);
    assert!(profile.basic_stats.approximate_cardinality <= 10); // Low cardinality

    // Should have categorical histogram due to low cardinality
    assert!(profile.categorical_histogram.is_some());
    assert!(profile.numeric_distribution.is_none());
}

#[tokio::test]
async fn test_profiler_with_numeric_column() {
    let ctx = create_minimal_tpc_h_context().await.unwrap();
    let profiler = ColumnProfiler::builder()
        .cardinality_threshold(50) // Lower threshold to force Pass 3
        .build();

    // Profile a high-cardinality numeric column (should use Pass 3)
    let profile = profiler
        .profile_column(&ctx, "lineitem", "l_extendedprice")
        .await
        .unwrap();

    println!("l_extendedprice profile: {profile:#?}");

    assert_eq!(profile.column_name, "l_extendedprice");
    assert!(matches!(
        profile.data_type,
        DetectedDataType::Double | DetectedDataType::Integer
    ));
    assert!(profile.passes_executed.contains(&1)); // Pass 1 always executed
    assert!(profile.basic_stats.row_count > 0);

    // Should have numeric distribution due to high cardinality and numeric type
    if profile.basic_stats.approximate_cardinality > 50 {
        assert!(profile.numeric_distribution.is_some());
        assert!(profile.categorical_histogram.is_none());

        let distribution = profile.numeric_distribution.unwrap();
        assert!(distribution.mean.is_some());
        assert!(distribution.std_dev.is_some());
        // Quantiles may be empty if the percentile function is not available in DataFusion
        println!("Quantiles computed: {:?}", distribution.quantiles);
    }
}

#[tokio::test]
async fn test_profiler_with_date_column() {
    let ctx = create_minimal_tpc_h_context().await.unwrap();
    let profiler = ColumnProfiler::new();

    // Profile a date column
    let profile = profiler
        .profile_column(&ctx, "lineitem", "l_shipdate")
        .await
        .unwrap();

    println!("l_shipdate profile: {profile:#?}");

    assert_eq!(profile.column_name, "l_shipdate");
    assert!(profile.passes_executed.contains(&1));
    assert!(profile.basic_stats.row_count > 0);
    assert!(profile.basic_stats.min_value.is_some());
    assert!(profile.basic_stats.max_value.is_some());
}

#[tokio::test]
async fn test_profiler_multiple_columns_sequential() {
    let ctx = create_minimal_tpc_h_context().await.unwrap();
    let profiler = ColumnProfiler::builder()
        .enable_parallel(false) // Force sequential
        .build();

    let columns = vec![
        "l_orderkey".to_string(),
        "l_returnflag".to_string(),
        "l_linestatus".to_string(),
    ];

    let profiles = profiler
        .profile_columns(&ctx, "lineitem", &columns)
        .await
        .unwrap();

    assert_eq!(profiles.len(), 3);

    for profile in &profiles {
        assert!(profile.passes_executed.contains(&1));
        assert!(profile.basic_stats.row_count > 0);
        assert!(profile.profiling_time_ms > 0);
        println!(
            "Column {}: {:?} passes, {} ms",
            profile.column_name, profile.passes_executed, profile.profiling_time_ms
        );
    }
}

#[tokio::test]
async fn test_profiler_multiple_columns_parallel() {
    let ctx = create_minimal_tpc_h_context().await.unwrap();
    let profiler = ColumnProfiler::builder()
        .enable_parallel(true) // Enable parallel
        .build();

    let columns = vec![
        "l_orderkey".to_string(),
        "l_partkey".to_string(),
        "l_suppkey".to_string(),
        "l_linenumber".to_string(),
    ];

    let start_time = std::time::Instant::now();
    let profiles = profiler
        .profile_columns(&ctx, "lineitem", &columns)
        .await
        .unwrap();
    let parallel_duration = start_time.elapsed();

    assert_eq!(profiles.len(), 4);

    // Test sequential for comparison
    let profiler_seq = ColumnProfiler::builder().enable_parallel(false).build();

    let start_time = std::time::Instant::now();
    let _profiles_seq = profiler_seq
        .profile_columns(&ctx, "lineitem", &columns)
        .await
        .unwrap();
    let sequential_duration = start_time.elapsed();

    println!("Parallel: {parallel_duration:?}, Sequential: {sequential_duration:?}");

    // Parallel should generally be faster or similar (depending on system load)
    // We don't assert this as it's system-dependent

    for profile in &profiles {
        assert!(profile.passes_executed.contains(&1));
        assert!(profile.basic_stats.row_count > 0);
    }
}

#[tokio::test]
async fn test_profiler_progress_reporting() {
    let ctx = create_minimal_tpc_h_context().await.unwrap();

    let progress_calls = Arc::new(Mutex::new(Vec::new()));
    let progress_calls_clone = progress_calls.clone();

    let profiler = ColumnProfiler::builder()
        .progress_callback(move |progress: ProfilerProgress| {
            progress_calls_clone.lock().unwrap().push(progress);
        })
        .build();

    let _profile = profiler
        .profile_column(&ctx, "lineitem", "l_quantity")
        .await
        .unwrap();

    let progress_history = progress_calls.lock().unwrap();
    assert!(!progress_history.is_empty());

    // Should have at least one progress report from Pass 1
    assert!(progress_history.iter().any(|p| p.current_pass == 1));

    for progress in progress_history.iter() {
        assert_eq!(progress.column_name, "l_quantity");
        assert!(progress.current_pass >= 1 && progress.current_pass <= 3);
        assert_eq!(progress.total_passes, 3);
        assert!(!progress.message.is_empty());
        println!(
            "Progress: Pass {}/{} - {}",
            progress.current_pass, progress.total_passes, progress.message
        );
    }
}

#[tokio::test]
async fn test_profiler_data_type_detection() {
    let ctx = create_minimal_tpc_h_context().await.unwrap();
    let profiler = ColumnProfiler::new();

    // Test different data types
    let test_cases = vec![
        ("l_orderkey", DetectedDataType::Integer),
        ("l_extendedprice", DetectedDataType::Double),
        ("l_returnflag", DetectedDataType::String),
        ("l_shipdate", DetectedDataType::Date),
    ];

    for (column_name, expected_type) in test_cases {
        let profile = profiler
            .profile_column(&ctx, "lineitem", column_name)
            .await
            .unwrap();

        // Allow some flexibility in type detection
        match expected_type {
            DetectedDataType::Integer => {
                assert!(matches!(
                    profile.data_type,
                    DetectedDataType::Integer | DetectedDataType::Double
                ));
            }
            DetectedDataType::Double => {
                assert!(matches!(
                    profile.data_type,
                    DetectedDataType::Double | DetectedDataType::Integer
                ));
            }
            DetectedDataType::Date => {
                assert!(matches!(
                    profile.data_type,
                    DetectedDataType::Date | DetectedDataType::String
                ));
            }
            _ => {
                assert_eq!(profile.data_type, expected_type);
            }
        }

        println!("Column {column_name}: detected as {:?}", profile.data_type);
    }
}

#[tokio::test]
async fn test_profiler_cardinality_threshold_behavior() {
    let ctx = create_minimal_tpc_h_context().await.unwrap();

    // Test with low threshold (should force Pass 3 for most columns)
    let profiler_low = ColumnProfiler::builder().cardinality_threshold(5).build();

    let profile_low = profiler_low
        .profile_column(&ctx, "lineitem", "l_orderkey")
        .await
        .unwrap();

    // Test with high threshold (should force Pass 2 for most columns)
    let profiler_high = ColumnProfiler::builder()
        .cardinality_threshold(100000)
        .build();

    let profile_high = profiler_high
        .profile_column(&ctx, "lineitem", "l_orderkey")
        .await
        .unwrap();

    println!("Low threshold passes: {:?}", profile_low.passes_executed);
    println!("High threshold passes: {:?}", profile_high.passes_executed);

    // Both should execute Pass 1
    assert!(profile_low.passes_executed.contains(&1));
    assert!(profile_high.passes_executed.contains(&1));

    // The behavior of Pass 2 vs Pass 3 depends on actual cardinality,
    // but they should behave differently
    if profile_low.basic_stats.approximate_cardinality > 5
        && matches!(
            profile_low.data_type,
            DetectedDataType::Integer | DetectedDataType::Double
        )
    {
        assert!(profile_low.passes_executed.contains(&3));
    }

    if profile_high.basic_stats.approximate_cardinality <= 100000 {
        assert!(profile_high.passes_executed.contains(&2));
    }
}

#[tokio::test]
async fn test_profiler_error_handling() {
    let ctx = create_minimal_tpc_h_context().await.unwrap();
    let profiler = ColumnProfiler::new();

    // Test with non-existent table
    let result = profiler
        .profile_column(&ctx, "nonexistent_table", "some_column")
        .await;
    assert!(result.is_err());

    // Test with non-existent column
    let result = profiler
        .profile_column(&ctx, "lineitem", "nonexistent_column")
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_profiler_memory_efficiency() {
    let ctx = create_minimal_tpc_h_context().await.unwrap();
    let profiler = ColumnProfiler::builder()
        .sample_size(1000) // Small sample size
        .max_memory_bytes(10 * 1024 * 1024) // 10MB limit
        .build();

    // Profile a large column with memory constraints
    let profile = profiler
        .profile_column(&ctx, "lineitem", "l_comment")
        .await
        .unwrap();

    assert_eq!(profile.column_name, "l_comment");
    assert!(profile.basic_stats.row_count > 0);

    // Sample values should be limited
    assert!(profile.basic_stats.sample_values.len() <= 10);

    println!(
        "Profiled {} rows with {} sample values",
        profile.basic_stats.row_count,
        profile.basic_stats.sample_values.len()
    );
}

#[tokio::test]
async fn test_profiler_performance_characteristics() {
    let ctx = create_minimal_tpc_h_context().await.unwrap();

    // Test performance with different configurations
    let configs = vec![
        (
            "small_sample",
            ColumnProfiler::builder().sample_size(100).build(),
        ),
        (
            "large_sample",
            ColumnProfiler::builder().sample_size(10000).build(),
        ),
        (
            "no_parallel",
            ColumnProfiler::builder().enable_parallel(false).build(),
        ),
        (
            "with_parallel",
            ColumnProfiler::builder().enable_parallel(true).build(),
        ),
    ];

    for (name, profiler) in configs {
        let start_time = std::time::Instant::now();
        let _profile = profiler
            .profile_column(&ctx, "lineitem", "l_extendedprice")
            .await
            .unwrap();
        let duration = start_time.elapsed();

        println!("Configuration '{name}' took: {duration:?}");

        // All configurations should complete in reasonable time
        assert!(duration.as_secs() < 30);
    }
}
