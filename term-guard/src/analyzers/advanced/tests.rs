//! Tests for advanced analyzers.

use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use std::sync::Arc;

use crate::analyzers::{Analyzer, AnalyzerState, MetricValue};

use super::*;

/// Creates a test context with sample data.
async fn create_test_context() -> Result<SessionContext, Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();

    // Create sample data with various columns
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, true),
        Field::new("category", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
        Field::new("mixed_type", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])),
            Arc::new(Float64Array::from(vec![
                Some(1.0),
                Some(2.0),
                Some(2.0),
                Some(3.0),
                Some(3.0),
                Some(3.0),
                Some(4.0),
                Some(5.0),
                Some(10.0),
                None,
            ])),
            Arc::new(StringArray::from(vec![
                Some("A"),
                Some("B"),
                Some("A"),
                Some("C"),
                Some("B"),
                Some("A"),
                Some("D"),
                Some("E"),
                Some("A"),
                Some("F"),
            ])),
            Arc::new(Float64Array::from(vec![
                Some(10.0),
                Some(20.0),
                Some(15.0),
                Some(25.0),
                Some(30.0),
                Some(18.0),
                Some(22.0),
                Some(28.0),
                Some(35.0),
                Some(40.0),
            ])),
            Arc::new(StringArray::from(vec![
                Some("123"),
                Some("456.78"),
                Some("true"),
                Some("2023-01-01"),
                Some("hello"),
                Some("789"),
                Some("false"),
                Some("3.14"),
                Some("world"),
                Some("0"),
            ])),
        ],
    )?;

    ctx.register_batch("data", batch)?;
    Ok(ctx)
}

#[tokio::test]
async fn test_approx_count_distinct_analyzer() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = create_test_context().await?;

    // Test with category column
    let analyzer = ApproxCountDistinctAnalyzer::new("category");
    let state = analyzer.compute_state_from_data(&ctx).await?;
    assert_eq!(state.total_count, 10);
    assert_eq!(state.approx_distinct_count, 6); // A, B, C, D, E, F

    let metric = analyzer.compute_metric_from_state(&state)?;
    if let MetricValue::Long(count) = metric {
        assert_eq!(count, 6);
    } else {
        panic!("Expected Long metric value");
    }

    // Test distinctness ratio
    assert!((state.distinctness_ratio() - 0.6).abs() < 0.001);

    Ok(())
}

#[tokio::test]
async fn test_compliance_analyzer() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = create_test_context().await?;

    // Test compliance check
    let analyzer = ComplianceAnalyzer::new("score_check", "score >= 20");
    let state = analyzer.compute_state_from_data(&ctx).await?;
    assert_eq!(state.total_count, 10);
    assert_eq!(state.compliant_count, 7); // 20, 25, 30, 22, 28, 35, 40 (7 values)

    let metric = analyzer.compute_metric_from_state(&state)?;
    if let MetricValue::Double(compliance) = metric {
        assert!((compliance - 0.7).abs() < 0.001);
    } else {
        panic!("Expected Double metric value");
    }

    Ok(())
}

#[tokio::test]
async fn test_compliance_analyzer_sql_injection() {
    let ctx = create_test_context().await.unwrap();

    // Test SQL injection prevention
    let dangerous_predicates = vec![
        "1=1; DROP TABLE data; --",
        "score > 0 UNION SELECT * FROM other_table",
        "score > 0; DELETE FROM data WHERE 1=1",
        "score /* comment */ > 0",
    ];

    for predicate in dangerous_predicates {
        let analyzer = ComplianceAnalyzer::new("test", predicate);
        let result = analyzer.compute_state_from_data(&ctx).await;
        assert!(
            result.is_err(),
            "Should reject dangerous predicate: {predicate}"
        );
    }
}

#[tokio::test]
async fn test_data_type_analyzer() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = create_test_context().await?;

    // Test mixed type column
    let analyzer = DataTypeAnalyzer::new("mixed_type");
    let state = analyzer.compute_state_from_data(&ctx).await?;
    assert_eq!(state.total_count, 10);

    // Check that we detected different types
    assert!(state.type_counts.contains_key("integer"));
    assert!(state.type_counts.contains_key("double"));
    assert!(state.type_counts.contains_key("boolean"));
    assert!(state.type_counts.contains_key("string"));

    let metric = analyzer.compute_metric_from_state(&state)?;
    if let MetricValue::Map(type_map) = metric {
        assert!(!type_map.is_empty());
        // Verify all values are counts
        for (_, value) in type_map {
            assert!(matches!(value, MetricValue::Long(_)));
        }
    } else {
        panic!("Expected Map metric value");
    }

    Ok(())
}

#[tokio::test]
async fn test_histogram_analyzer() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = create_test_context().await?;

    // Test histogram with value column
    let analyzer = HistogramAnalyzer::new("value", 5);
    let state = analyzer.compute_state_from_data(&ctx).await?;

    assert_eq!(state.buckets.len(), 5);
    assert_eq!(state.total_count, 9); // 10 - 1 null
    assert!((state.min_value - 1.0).abs() < 0.001);
    assert!((state.max_value - 10.0).abs() < 0.001);

    // Check mean calculation
    let expected_mean = (1.0 + 2.0 + 2.0 + 3.0 + 3.0 + 3.0 + 4.0 + 5.0 + 10.0) / 9.0;
    assert!((state.mean().unwrap() - expected_mean).abs() < 0.001);

    let metric = analyzer.compute_metric_from_state(&state)?;
    if let MetricValue::Histogram(distribution) = metric {
        assert_eq!(distribution.buckets.len(), 5);
        assert!(distribution.min.is_some());
        assert!(distribution.max.is_some());
        assert!(distribution.mean.is_some());
    } else {
        panic!("Expected Histogram metric value");
    }

    Ok(())
}

#[tokio::test]
async fn test_standard_deviation_analyzer() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = create_test_context().await?;

    // Test with score column
    let analyzer = StandardDeviationAnalyzer::new("score");
    let state = analyzer.compute_state_from_data(&ctx).await?;

    assert_eq!(state.count, 10);
    // Mean = (10+20+15+25+30+18+22+28+35+40) / 10 = 243 / 10 = 24.3
    assert!((state.mean - 24.3).abs() < 0.001);

    // Verify standard deviation calculation
    let std_dev = state.population_std_dev().unwrap();
    assert!(std_dev > 0.0);
    assert!(std_dev < 15.0); // Reasonable range for our data

    let metric = analyzer.compute_metric_from_state(&state)?;
    if let MetricValue::Map(stats) = metric {
        assert!(stats.contains_key("std_dev"));
        assert!(stats.contains_key("variance"));
        assert!(stats.contains_key("sample_std_dev"));
        assert!(stats.contains_key("coefficient_of_variation"));
    } else {
        panic!("Expected Map metric value");
    }

    Ok(())
}

#[tokio::test]
async fn test_entropy_analyzer() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = create_test_context().await?;

    // Test with category column
    let analyzer = EntropyAnalyzer::new("category");
    let state = analyzer.compute_state_from_data(&ctx).await?;

    assert_eq!(state.total_count, 10);
    assert_eq!(state.value_counts.len(), 6); // A, B, C, D, E, F
    assert!(!state.truncated);

    // Verify entropy calculations
    let entropy = state.entropy();
    assert!(entropy > 0.0);
    assert!(entropy < 3.0); // log2(6) ≈ 2.58

    let normalized_entropy = state.normalized_entropy();
    assert!((0.0..=1.0).contains(&normalized_entropy));

    let gini = state.gini_impurity();
    assert!((0.0..=1.0).contains(&gini));

    let metric = analyzer.compute_metric_from_state(&state)?;
    if let MetricValue::Map(metrics) = metric {
        assert!(metrics.contains_key("entropy"));
        assert!(metrics.contains_key("normalized_entropy"));
        assert!(metrics.contains_key("gini_impurity"));
        assert!(metrics.contains_key("effective_values"));
        assert!(metrics.contains_key("unique_values"));
    } else {
        panic!("Expected Map metric value");
    }

    Ok(())
}

#[tokio::test]
async fn test_entropy_analyzer_uniform_distribution() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();

    // Create uniform distribution
    let schema = Arc::new(Schema::new(vec![Field::new(
        "uniform",
        DataType::Utf8,
        false,
    )]));

    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![
            Some("A"),
            Some("B"),
            Some("C"),
            Some("D"),
            Some("E"),
            Some("A"),
            Some("B"),
            Some("C"),
            Some("D"),
            Some("E"),
        ]))],
    )?;

    ctx.register_batch("data", batch)?;

    let analyzer = EntropyAnalyzer::new("uniform");
    let state = analyzer.compute_state_from_data(&ctx).await?;

    // For uniform distribution with 5 values, entropy should be log2(5)
    let expected_entropy = 5.0_f64.log2();
    assert!((state.entropy() - expected_entropy).abs() < 0.001);
    assert!((state.normalized_entropy() - 1.0).abs() < 0.001);

    Ok(())
}

#[tokio::test]
async fn test_analyzer_state_merging() -> Result<(), Box<dyn std::error::Error>> {
    // Test ApproxCountDistinctState merge
    let state1 = ApproxCountDistinctState {
        approx_distinct_count: 100,
        total_count: 1000,
    };
    let state2 = ApproxCountDistinctState {
        approx_distinct_count: 150,
        total_count: 2000,
    };
    let merged = ApproxCountDistinctState::merge(vec![state1, state2])?;
    assert_eq!(merged.approx_distinct_count, 150); // Takes max
    assert_eq!(merged.total_count, 3000);

    // Test ComplianceState merge
    let state1 = ComplianceState {
        compliant_count: 80,
        total_count: 100,
    };
    let state2 = ComplianceState {
        compliant_count: 160,
        total_count: 200,
    };
    let merged = ComplianceState::merge(vec![state1, state2])?;
    assert_eq!(merged.compliant_count, 240);
    assert_eq!(merged.total_count, 300);
    assert!((merged.compliance_fraction() - 0.8).abs() < 0.001);

    Ok(())
}

#[tokio::test]
async fn test_empty_data_handling() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();

    // Create empty table
    let schema = Arc::new(Schema::new(vec![Field::new(
        "empty_col",
        DataType::Float64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Float64Array::from(vec![None, None, None]))],
    )?;
    ctx.register_batch("data", batch)?;

    // Test each analyzer with empty data
    // Add a string column for approx_count_distinct test
    let schema2 = Arc::new(Schema::new(vec![
        Field::new("empty_col", DataType::Float64, true),
        Field::new("empty_str", DataType::Utf8, true),
    ]));
    let batch2 = RecordBatch::try_new(
        schema2,
        vec![
            Arc::new(Float64Array::from(vec![None, None, None])),
            Arc::new(StringArray::from(vec![None::<&str>, None, None])),
        ],
    )?;
    ctx.deregister_table("data")?;
    ctx.register_batch("data", batch2)?;

    let approx_analyzer = ApproxCountDistinctAnalyzer::new("empty_str");
    let state = approx_analyzer.compute_state_from_data(&ctx).await?;
    assert_eq!(state.total_count, 0);
    assert!(state.is_empty());

    let compliance_analyzer = ComplianceAnalyzer::new("test", "empty_col > 0");
    let state = compliance_analyzer.compute_state_from_data(&ctx).await?;
    assert_eq!(state.total_count, 3); // 3 rows even though all are NULL
    assert_eq!(state.compliant_count, 0); // No values satisfy > 0

    let entropy_analyzer = EntropyAnalyzer::new("empty_col");
    let state = entropy_analyzer.compute_state_from_data(&ctx).await?;
    assert_eq!(state.total_count, 0);
    assert!(state.is_empty());

    Ok(())
}
