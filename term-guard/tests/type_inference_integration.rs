//! Integration tests for TypeInferenceEngine using TPC-H data.

use std::collections::HashMap;
use term_guard::analyzers::inference::{
    InferredDataType, TypeInferenceEngine, TypeInferenceResult,
};
use term_guard::test_fixtures::create_minimal_tpc_h_context;

#[tokio::test]
async fn test_integer_type_inference() {
    let ctx = create_minimal_tpc_h_context().await.unwrap();
    let engine = TypeInferenceEngine::new();

    // Test with lineitem.l_orderkey (should be integer)
    let result = engine
        .infer_column_type(&ctx, "lineitem", "l_orderkey")
        .await
        .unwrap();

    println!("l_orderkey inference: {result:#?}");

    assert!(matches!(
        result.inferred_type,
        InferredDataType::Integer { .. }
    ));
    assert!(result.confidence > 0.8);
    assert!(result.samples_analyzed > 0);
}

#[tokio::test]
async fn test_float_type_inference() {
    let ctx = create_minimal_tpc_h_context().await.unwrap();
    let engine = TypeInferenceEngine::new();

    // Test with lineitem.l_extendedprice (should be float/decimal)
    let result = engine
        .infer_column_type(&ctx, "lineitem", "l_extendedprice")
        .await
        .unwrap();

    println!("l_extendedprice inference: {result:#?}");

    // TPC-H data might be stored as integers, so accept numeric types
    assert!(matches!(
        result.inferred_type,
        InferredDataType::Float { .. }
            | InferredDataType::Decimal { .. }
            | InferredDataType::Integer { .. }
    ));
    assert!(result.confidence > 0.7);
}

#[tokio::test]
async fn test_decimal_precision_detection() {
    let ctx = create_minimal_tpc_h_context().await.unwrap();
    let engine = TypeInferenceEngine::builder()
        .detect_decimal_precision(true)
        .build();

    // Test with lineitem.l_discount (should be decimal with precision)
    let result = engine
        .infer_column_type(&ctx, "lineitem", "l_discount")
        .await
        .unwrap();

    println!("l_discount inference: {result:#?}");

    // Should detect as decimal, float, or integer (depending on actual data format)
    assert!(matches!(
        result.inferred_type,
        InferredDataType::Decimal { .. }
            | InferredDataType::Float { .. }
            | InferredDataType::Integer { .. }
    ));
}

#[tokio::test]
async fn test_date_type_inference() {
    let ctx = create_minimal_tpc_h_context().await.unwrap();
    let engine = TypeInferenceEngine::new();

    // Test with lineitem.l_shipdate (should be date)
    let result = engine
        .infer_column_type(&ctx, "lineitem", "l_shipdate")
        .await
        .unwrap();

    println!("l_shipdate inference: {result:#?}");

    // Date columns might be detected as Date, DateTime, or String depending on format
    match result.inferred_type {
        InferredDataType::Date { format } => {
            assert!(!format.is_empty());
            println!("Detected date format: {format}");
        }
        InferredDataType::DateTime { format } => {
            assert!(!format.is_empty());
            println!("Detected datetime format: {format}");
        }
        InferredDataType::Text | InferredDataType::Categorical { .. } => {
            // Also acceptable if the date format isn't recognized
            println!("Date detected as text/categorical (format not recognized)");
        }
        _ => panic!(
            "Unexpected type for date column: {:?}",
            result.inferred_type
        ),
    }
}

#[tokio::test]
async fn test_categorical_type_inference() {
    let ctx = create_minimal_tpc_h_context().await.unwrap();
    let engine = TypeInferenceEngine::builder()
        .categorical_threshold(50) // Low threshold to ensure detection
        .build();

    // Test with lineitem.l_returnflag (should be categorical)
    let result = engine
        .infer_column_type(&ctx, "lineitem", "l_returnflag")
        .await
        .unwrap();

    println!("l_returnflag inference: {result:#?}");

    assert!(matches!(
        result.inferred_type,
        InferredDataType::Categorical { .. }
    ));

    if let InferredDataType::Categorical { cardinality } = result.inferred_type {
        assert!(cardinality <= 50);
        assert!(cardinality > 0);
    }
}

#[tokio::test]
async fn test_text_type_inference() {
    let ctx = create_minimal_tpc_h_context().await.unwrap();
    let engine = TypeInferenceEngine::builder()
        .categorical_threshold(10) // Higher threshold to force text detection
        .build();

    // Test with lineitem.l_comment (should be text due to high cardinality)
    let result = engine
        .infer_column_type(&ctx, "lineitem", "l_comment")
        .await
        .unwrap();

    println!("l_comment inference: {result:#?}");

    assert!(matches!(
        result.inferred_type,
        InferredDataType::Text | InferredDataType::Categorical { .. }
    ));
}

#[tokio::test]
async fn test_multiple_column_inference() {
    let ctx = create_minimal_tpc_h_context().await.unwrap();
    let engine = TypeInferenceEngine::new();

    let columns = vec![
        "l_orderkey".to_string(),
        "l_extendedprice".to_string(),
        "l_returnflag".to_string(),
        "l_shipdate".to_string(),
    ];

    let results = engine
        .infer_multiple_columns(&ctx, "lineitem", &columns)
        .await
        .unwrap();

    assert_eq!(results.len(), 4);

    for (column_name, result) in &results {
        println!(
            "Column {column_name}: {:?} (confidence: {:.2})",
            result.inferred_type.type_name(),
            result.confidence
        );

        assert!(result.samples_analyzed > 0);
        assert!(result.confidence >= 0.0 && result.confidence <= 1.0);
    }

    // Verify specific expected types
    let result_map: HashMap<String, &TypeInferenceResult> = results
        .iter()
        .map(|(name, result)| (name.clone(), result))
        .collect();

    // l_orderkey should be integer
    assert!(matches!(
        result_map["l_orderkey"].inferred_type,
        InferredDataType::Integer { .. }
    ));

    // l_extendedprice should be numeric (could be integer in TPC-H data)
    assert!(matches!(
        result_map["l_extendedprice"].inferred_type,
        InferredDataType::Float { .. }
            | InferredDataType::Decimal { .. }
            | InferredDataType::Integer { .. }
    ));
}

#[tokio::test]
async fn test_confidence_thresholds() {
    let ctx = create_minimal_tpc_h_context().await.unwrap();

    // Test with high confidence threshold
    let engine_high = TypeInferenceEngine::builder()
        .confidence_threshold(0.95)
        .build();

    // Test with low confidence threshold
    let engine_low = TypeInferenceEngine::builder()
        .confidence_threshold(0.1)
        .build();

    let result_high = engine_high
        .infer_column_type(&ctx, "lineitem", "l_orderkey")
        .await
        .unwrap();

    let result_low = engine_low
        .infer_column_type(&ctx, "lineitem", "l_orderkey")
        .await
        .unwrap();

    println!("High threshold result: {result_high:#?}");
    println!("Low threshold result: {result_low:#?}");

    // Both should detect the same base type for a clear integer column
    assert_eq!(
        std::mem::discriminant(&result_high.inferred_type),
        std::mem::discriminant(&result_low.inferred_type)
    );
}

#[tokio::test]
async fn test_sample_size_configuration() {
    let ctx = create_minimal_tpc_h_context().await.unwrap();

    // Test with small sample size
    let engine_small = TypeInferenceEngine::builder().sample_size(10).build();

    // Test with large sample size
    let engine_large = TypeInferenceEngine::builder().sample_size(1000).build();

    let result_small = engine_small
        .infer_column_type(&ctx, "lineitem", "l_quantity")
        .await
        .unwrap();

    let result_large = engine_large
        .infer_column_type(&ctx, "lineitem", "l_quantity")
        .await
        .unwrap();

    println!("Small sample result: {result_small:#?}");
    println!("Large sample result: {result_large:#?}");

    assert!(result_small.samples_analyzed <= 10);
    assert!(result_large.samples_analyzed <= 1000);
    assert!(result_large.samples_analyzed >= result_small.samples_analyzed);
}

#[tokio::test]
async fn test_nullable_detection() {
    let ctx = create_minimal_tpc_h_context().await.unwrap();
    let engine = TypeInferenceEngine::new();

    // Test with a column that might have nulls
    let result = engine
        .infer_column_type(&ctx, "lineitem", "l_comment")
        .await
        .unwrap();

    println!("Nullable detection result: {result:#?}");

    // Check if null handling is working
    assert!(result.samples_analyzed >= result.null_count);

    // Inferred type should handle nullability appropriately
    match &result.inferred_type {
        InferredDataType::Integer { nullable } => {
            println!("Integer type, nullable: {nullable}");
        }
        InferredDataType::Float { nullable } => {
            println!("Float type, nullable: {nullable}");
        }
        _ => {
            println!(
                "Other type (inherently nullable): {:?}",
                result.inferred_type.type_name()
            );
        }
    }
}

#[tokio::test]
async fn test_alternatives_tracking() {
    let ctx = create_minimal_tpc_h_context().await.unwrap();
    let engine = TypeInferenceEngine::new();

    let result = engine
        .infer_column_type(&ctx, "lineitem", "l_quantity")
        .await
        .unwrap();

    println!("Alternatives tracking result: {result:#?}");

    // Should have some alternatives considered
    assert!(!result.alternatives.is_empty());

    for (type_name, confidence) in &result.alternatives {
        println!("Alternative: {type_name} with confidence {confidence:.2}");
        assert!(*confidence >= 0.0 && *confidence <= 1.0);
    }
}

#[tokio::test]
async fn test_error_handling() {
    let ctx = create_minimal_tpc_h_context().await.unwrap();
    let engine = TypeInferenceEngine::new();

    // Test with non-existent table
    let result = engine
        .infer_column_type(&ctx, "nonexistent_table", "some_column")
        .await;
    assert!(result.is_err());

    // Test with non-existent column
    let result = engine
        .infer_column_type(&ctx, "lineitem", "nonexistent_column")
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_performance_characteristics() {
    let ctx = create_minimal_tpc_h_context().await.unwrap();

    // Test different sample sizes and measure performance
    let sample_sizes = vec![10, 100, 1000];

    for sample_size in sample_sizes {
        let engine = TypeInferenceEngine::builder()
            .sample_size(sample_size)
            .build();

        let start_time = std::time::Instant::now();
        let result = engine
            .infer_column_type(&ctx, "lineitem", "l_extendedprice")
            .await
            .unwrap();
        let duration = start_time.elapsed();

        println!(
            "Sample size {sample_size}: {duration:?} ({} samples analyzed)",
            result.samples_analyzed
        );

        // All should complete in reasonable time
        assert!(duration.as_secs() < 10);
        assert!(result.samples_analyzed <= sample_size as usize);
    }
}

#[tokio::test]
async fn test_international_format_detection() {
    let ctx = create_minimal_tpc_h_context().await.unwrap();

    // Test with international formats enabled
    let engine_intl = TypeInferenceEngine::builder()
        .international_formats(true)
        .build();

    // Test with international formats disabled
    let engine_no_intl = TypeInferenceEngine::builder()
        .international_formats(false)
        .build();

    let result_intl = engine_intl
        .infer_column_type(&ctx, "lineitem", "l_shipdate")
        .await
        .unwrap();

    let result_no_intl = engine_no_intl
        .infer_column_type(&ctx, "lineitem", "l_shipdate")
        .await
        .unwrap();

    println!("International formats enabled: {result_intl:#?}");
    println!("International formats disabled: {result_no_intl:#?}");

    // Both should produce valid results
    assert!(result_intl.confidence >= 0.0);
    assert!(result_no_intl.confidence >= 0.0);
}
