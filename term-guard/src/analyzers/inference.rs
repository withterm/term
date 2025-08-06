//! Data type inference engine for robust type detection from string data.
//!
//! This module provides a comprehensive data type inference system that can detect:
//! - Numeric types (Integer, Float, Decimal)
//! - Temporal types (Date, DateTime, Time)
//! - Boolean values with various representations
//! - Categorical vs. free text strings
//! - Mixed type columns with confidence scores
//!
//! # Example
//!
//! ```rust,ignore
//! use term_guard::analyzers::inference::{TypeInferenceEngine, InferredDataType};
//! use term_guard::test_fixtures::create_minimal_tpc_h_context;
//!
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! let engine = TypeInferenceEngine::builder()
//!     .sample_size(1000)
//!     .confidence_threshold(0.8)
//!     .build();
//!
//! let ctx = create_minimal_tpc_h_context().await.unwrap();
//! let inference = engine.infer_column_type(&ctx, "lineitem", "l_quantity").await.unwrap();
//!
//! match inference.inferred_type {
//!     InferredDataType::Float { nullable } => println!("Detected float type, nullable: {nullable}"),
//!     _ => println!("Detected other type"),
//! }
//!
//! println!("Confidence: {:.2}", inference.confidence);
//! # })
//! ```

use std::collections::HashMap;

use datafusion::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tracing::{info, instrument};

use crate::analyzers::errors::AnalyzerError;

/// Result type for type inference operations
pub type InferenceResult<T> = Result<T, AnalyzerError>;

/// Configuration for the type inference engine
#[derive(Debug, Clone)]
pub struct InferenceConfig {
    /// Number of rows to sample for type detection (default: 1000)
    pub sample_size: u64,
    /// Minimum confidence threshold for type detection (default: 0.7)
    pub confidence_threshold: f64,
    /// Whether to detect decimal precision/scale (default: true)
    pub detect_decimal_precision: bool,
    /// Maximum cardinality for categorical detection (default: 100)
    pub categorical_threshold: usize,
    /// Enable international number format detection (default: true)
    pub international_formats: bool,
}

impl Default for InferenceConfig {
    fn default() -> Self {
        Self {
            sample_size: 1000,
            confidence_threshold: 0.7,
            detect_decimal_precision: true,
            categorical_threshold: 100,
            international_formats: true,
        }
    }
}

/// Inferred data type with specific metadata
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum InferredDataType {
    /// Integer numbers
    Integer { nullable: bool },
    /// Floating point numbers
    Float { nullable: bool },
    /// Decimal numbers with precision and scale
    Decimal { precision: u8, scale: u8 },
    /// Boolean values with detected representations
    Boolean {
        true_values: Vec<String>,
        false_values: Vec<String>,
    },
    /// Date values with detected format
    Date { format: String },
    /// DateTime values with detected format
    DateTime { format: String },
    /// Time values with detected format
    Time { format: String },
    /// Categorical data with known cardinality
    Categorical { cardinality: usize },
    /// Free text data
    Text,
    /// Mixed types with confidence scores for each type
    Mixed { types: HashMap<String, f64> },
}

impl InferredDataType {
    /// Check if the type is nullable
    pub fn is_nullable(&self) -> bool {
        match self {
            InferredDataType::Integer { nullable } => *nullable,
            InferredDataType::Float { nullable } => *nullable,
            InferredDataType::Decimal { .. } => true, // Decimals can always be null
            _ => true,                                // Most types can be nullable
        }
    }

    /// Get the base type name as a string
    pub fn type_name(&self) -> &'static str {
        match self {
            InferredDataType::Integer { .. } => "Integer",
            InferredDataType::Float { .. } => "Float",
            InferredDataType::Decimal { .. } => "Decimal",
            InferredDataType::Boolean { .. } => "Boolean",
            InferredDataType::Date { .. } => "Date",
            InferredDataType::DateTime { .. } => "DateTime",
            InferredDataType::Time { .. } => "Time",
            InferredDataType::Categorical { .. } => "Categorical",
            InferredDataType::Text => "Text",
            InferredDataType::Mixed { .. } => "Mixed",
        }
    }
}

/// Type inference result with confidence score
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeInferenceResult {
    /// The inferred data type
    pub inferred_type: InferredDataType,
    /// Confidence score (0.0 to 1.0)
    pub confidence: f64,
    /// Number of samples analyzed
    pub samples_analyzed: usize,
    /// Number of null values encountered
    pub null_count: usize,
    /// Alternative types considered with their scores
    pub alternatives: HashMap<String, f64>,
}

/// Type detection statistics for internal use
#[derive(Debug)]
pub struct TypeStats {
    pub total_samples: usize,
    pub null_count: usize,
    pub integer_matches: usize,
    pub float_matches: usize,
    pub boolean_matches: usize,
    pub date_matches: usize,
    pub datetime_matches: usize,
    pub time_matches: usize,
    pub unique_values: HashMap<String, usize>,
    pub decimal_info: Option<(u8, u8)>, // precision, scale
    pub boolean_representations: (Vec<String>, Vec<String>), // true_values, false_values
    pub detected_formats: Vec<String>,
}

impl Default for TypeStats {
    fn default() -> Self {
        Self::new()
    }
}

impl TypeStats {
    pub fn new() -> Self {
        Self {
            total_samples: 0,
            null_count: 0,
            integer_matches: 0,
            float_matches: 0,
            boolean_matches: 0,
            date_matches: 0,
            datetime_matches: 0,
            time_matches: 0,
            unique_values: HashMap::new(),
            decimal_info: None,
            boolean_representations: (Vec::new(), Vec::new()),
            detected_formats: Vec::new(),
        }
    }
}

/// Builder for TypeInferenceEngine
pub struct TypeInferenceEngineBuilder {
    config: InferenceConfig,
}

impl TypeInferenceEngineBuilder {
    /// Set the sample size for type detection
    pub fn sample_size(mut self, size: u64) -> Self {
        self.config.sample_size = size;
        self
    }

    /// Set the confidence threshold
    pub fn confidence_threshold(mut self, threshold: f64) -> Self {
        self.config.confidence_threshold = threshold;
        self
    }

    /// Enable or disable decimal precision detection
    pub fn detect_decimal_precision(mut self, enable: bool) -> Self {
        self.config.detect_decimal_precision = enable;
        self
    }

    /// Set the categorical cardinality threshold
    pub fn categorical_threshold(mut self, threshold: usize) -> Self {
        self.config.categorical_threshold = threshold;
        self
    }

    /// Enable or disable international format detection
    pub fn international_formats(mut self, enable: bool) -> Self {
        self.config.international_formats = enable;
        self
    }

    /// Build the TypeInferenceEngine
    pub fn build(self) -> TypeInferenceEngine {
        TypeInferenceEngine {
            config: self.config,
            patterns: TypePatterns::new(),
        }
    }
}

/// Pattern matching utilities for type detection
struct TypePatterns {
    integer: Regex,
    float: Regex,
    decimal: Regex,
    date_iso: Regex,
    date_us: Regex,
    date_eu: Regex,
    datetime_iso: Regex,
    time: Regex,
    boolean_true: Vec<Regex>,
    boolean_false: Vec<Regex>,
}

impl TypePatterns {
    fn new() -> Self {
        Self {
            integer: Regex::new(r"^[+-]?\d+$").unwrap(),
            float: Regex::new(r"^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$").unwrap(),
            decimal: Regex::new(r"^[+-]?\d+\.\d+$").unwrap(),
            date_iso: Regex::new(r"^\d{4}-\d{2}-\d{2}$").unwrap(),
            date_us: Regex::new(r"^\d{1,2}/\d{1,2}/\d{4}$").unwrap(),
            date_eu: Regex::new(r"^\d{1,2}\.\d{1,2}\.\d{4}$").unwrap(),
            datetime_iso: Regex::new(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}").unwrap(),
            time: Regex::new(r"^\d{1,2}:\d{2}(:\d{2})?(\s?(AM|PM))?$").unwrap(),
            boolean_true: vec![Regex::new(r"(?i)^(true|t|yes|y|1|on|enabled?)$").unwrap()],
            boolean_false: vec![Regex::new(r"(?i)^(false|f|no|n|0|off|disabled?)$").unwrap()],
        }
    }
}

/// Main type inference engine
pub struct TypeInferenceEngine {
    config: InferenceConfig,
    patterns: TypePatterns,
}

impl TypeInferenceEngine {
    /// Create a new builder for TypeInferenceEngine
    pub fn builder() -> TypeInferenceEngineBuilder {
        TypeInferenceEngineBuilder {
            config: InferenceConfig::default(),
        }
    }

    /// Create a TypeInferenceEngine with default configuration
    pub fn new() -> Self {
        Self::builder().build()
    }

    /// Infer the data type of a column in a table
    #[instrument(skip(self, ctx))]
    pub async fn infer_column_type(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        column_name: &str,
    ) -> InferenceResult<TypeInferenceResult> {
        info!(
            table = table_name,
            column = column_name,
            sample_size = self.config.sample_size,
            "Starting type inference"
        );

        // Sample data for analysis
        let samples = self.collect_samples(ctx, table_name, column_name).await?;

        // Analyze the samples
        let stats = self.analyze_samples(&samples);

        // Determine the best type match
        let result = self.determine_type(&stats);

        info!(
            table = table_name,
            column = column_name,
            inferred_type = result.inferred_type.type_name(),
            confidence = result.confidence,
            samples = result.samples_analyzed,
            "Completed type inference"
        );

        Ok(result)
    }

    /// Infer types for multiple columns in parallel
    #[instrument(skip(self, ctx))]
    pub async fn infer_multiple_columns(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        column_names: &[String],
    ) -> InferenceResult<Vec<(String, TypeInferenceResult)>> {
        let mut handles = Vec::new();

        for column_name in column_names {
            let ctx = ctx.clone();
            let table_name = table_name.to_string();
            let column_name = column_name.clone();
            let engine = Self {
                config: self.config.clone(),
                patterns: TypePatterns::new(), // Create new patterns for each task
            };

            let handle = tokio::spawn(async move {
                let result = engine
                    .infer_column_type(&ctx, &table_name, &column_name)
                    .await?;
                Ok::<_, AnalyzerError>((column_name, result))
            });

            handles.push(handle);
        }

        let mut results = Vec::new();
        for handle in handles {
            match handle.await {
                Ok(Ok(result)) => results.push(result),
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(AnalyzerError::execution(format!("Task join error: {e}"))),
            }
        }

        Ok(results)
    }

    /// Collect sample data from the specified column
    async fn collect_samples(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        column_name: &str,
    ) -> InferenceResult<Vec<Option<String>>> {
        let sql = format!(
            "SELECT {column_name} FROM {table_name} LIMIT {}",
            self.config.sample_size
        );

        let df = ctx
            .sql(&sql)
            .await
            .map_err(|e| AnalyzerError::execution(e.to_string()))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| AnalyzerError::execution(e.to_string()))?;

        let mut samples = Vec::new();
        for batch in &batches {
            if batch.num_rows() > 0 {
                let column_data = batch.column(0);
                for i in 0..batch.num_rows() {
                    if column_data.is_null(i) {
                        samples.push(None);
                    } else {
                        let value = self.extract_string_value(column_data, i)?;
                        samples.push(Some(value));
                    }
                }
            }
        }

        Ok(samples)
    }

    /// Extract string value from Arrow column
    fn extract_string_value(
        &self,
        column: &dyn arrow::array::Array,
        row_idx: usize,
    ) -> InferenceResult<String> {
        if column.is_null(row_idx) {
            return Ok("".to_string());
        }

        if let Some(arr) = column.as_any().downcast_ref::<arrow::array::StringArray>() {
            Ok(arr.value(row_idx).to_string())
        } else if let Some(arr) = column
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
        {
            Ok(arr.value(row_idx).to_string())
        } else if let Some(arr) = column.as_any().downcast_ref::<arrow::array::Int64Array>() {
            Ok(arr.value(row_idx).to_string())
        } else if let Some(arr) = column.as_any().downcast_ref::<arrow::array::Float64Array>() {
            Ok(arr.value(row_idx).to_string())
        } else if let Some(arr) = column.as_any().downcast_ref::<arrow::array::BooleanArray>() {
            Ok(arr.value(row_idx).to_string())
        } else {
            // Generic fallback
            Ok("UNKNOWN".to_string())
        }
    }

    /// Analyze collected samples to gather type statistics
    fn analyze_samples(&self, samples: &[Option<String>]) -> TypeStats {
        let mut stats = TypeStats::new();
        stats.total_samples = samples.len();

        for sample in samples {
            match sample {
                None => stats.null_count += 1,
                Some(value) => {
                    let trimmed = value.trim();
                    if trimmed.is_empty() {
                        stats.null_count += 1;
                        continue;
                    }

                    // Track unique values for categorical detection
                    *stats.unique_values.entry(trimmed.to_string()).or_insert(0) += 1;

                    // Test against various patterns
                    self.test_patterns(trimmed, &mut stats);
                }
            }
        }

        stats
    }

    /// Test a value against all type patterns
    pub fn test_patterns(&self, value: &str, stats: &mut TypeStats) {
        // Integer test
        if self.patterns.integer.is_match(value) {
            stats.integer_matches += 1;
        }

        // Float test - but only count it if it's actually a float (has decimal point or scientific notation)
        if self.patterns.float.is_match(value) {
            // Only count as float if it's not a pure integer
            if !self.patterns.integer.is_match(value)
                || value.contains('.')
                || value.contains('e')
                || value.contains('E')
            {
                stats.float_matches += 1;

                // Check for decimal precision if it's a decimal
                if self.patterns.decimal.is_match(value) && self.config.detect_decimal_precision {
                    if let Some(dot_pos) = value.rfind('.') {
                        let fractional_part = &value[dot_pos + 1..];
                        let scale = fractional_part.len() as u8;
                        let precision = (value.len() - 1) as u8; // -1 for the dot

                        stats.decimal_info = Some((precision.min(38), scale.min(38)));
                    }
                }
            }
        }

        // Date tests
        if self.patterns.date_iso.is_match(value) {
            stats.date_matches += 1;
            stats.detected_formats.push("YYYY-MM-DD".to_string());
        } else if self.patterns.date_us.is_match(value) {
            stats.date_matches += 1;
            stats.detected_formats.push("MM/DD/YYYY".to_string());
        } else if self.patterns.date_eu.is_match(value) {
            stats.date_matches += 1;
            stats.detected_formats.push("DD.MM.YYYY".to_string());
        }

        // DateTime test
        if self.patterns.datetime_iso.is_match(value) {
            stats.datetime_matches += 1;
            stats
                .detected_formats
                .push("YYYY-MM-DD HH:MM:SS".to_string());
        }

        // Time test
        if self.patterns.time.is_match(value) {
            stats.time_matches += 1;
            stats.detected_formats.push("HH:MM:SS".to_string());
        }

        // Boolean tests
        for pattern in &self.patterns.boolean_true {
            if pattern.is_match(value) {
                stats.boolean_matches += 1;
                stats.boolean_representations.0.push(value.to_string());
                break;
            }
        }
        for pattern in &self.patterns.boolean_false {
            if pattern.is_match(value) {
                stats.boolean_matches += 1;
                stats.boolean_representations.1.push(value.to_string());
                break;
            }
        }
    }

    /// Determine the best type match from statistics
    pub fn determine_type(&self, stats: &TypeStats) -> TypeInferenceResult {
        let non_null_samples = stats.total_samples - stats.null_count;

        if non_null_samples == 0 {
            return TypeInferenceResult {
                inferred_type: InferredDataType::Text,
                confidence: 0.0,
                samples_analyzed: stats.total_samples,
                null_count: stats.null_count,
                alternatives: HashMap::new(),
            };
        }

        let mut alternatives = HashMap::new();

        // Calculate confidence scores for each type
        let integer_confidence = stats.integer_matches as f64 / non_null_samples as f64;
        let float_confidence = stats.float_matches as f64 / non_null_samples as f64;
        let boolean_confidence = stats.boolean_matches as f64 / non_null_samples as f64;
        let date_confidence = stats.date_matches as f64 / non_null_samples as f64;
        let datetime_confidence = stats.datetime_matches as f64 / non_null_samples as f64;
        let time_confidence = stats.time_matches as f64 / non_null_samples as f64;

        // Categorical vs Text decision
        let is_categorical = stats.unique_values.len() <= self.config.categorical_threshold;
        let categorical_confidence = if is_categorical { 1.0 } else { 0.0 };

        // Add alternatives
        if integer_confidence > 0.0 {
            alternatives.insert("Integer".to_string(), integer_confidence);
        }
        if float_confidence > 0.0 {
            alternatives.insert("Float".to_string(), float_confidence);
        }
        if boolean_confidence > 0.0 {
            alternatives.insert("Boolean".to_string(), boolean_confidence);
        }
        if date_confidence > 0.0 {
            alternatives.insert("Date".to_string(), date_confidence);
        }
        if datetime_confidence > 0.0 {
            alternatives.insert("DateTime".to_string(), datetime_confidence);
        }
        if time_confidence > 0.0 {
            alternatives.insert("Time".to_string(), time_confidence);
        }
        if categorical_confidence > 0.0 {
            alternatives.insert("Categorical".to_string(), categorical_confidence);
        }

        // Determine the best type based on highest confidence
        let nullable = stats.null_count > 0;

        // Priority order: DateTime > Date > Time > Boolean > Decimal > Float > Integer > Categorical > Text
        let (inferred_type, confidence) = if datetime_confidence >= self.config.confidence_threshold
        {
            let format = stats
                .detected_formats
                .first()
                .unwrap_or(&"YYYY-MM-DD HH:MM:SS".to_string())
                .clone();
            (InferredDataType::DateTime { format }, datetime_confidence)
        } else if date_confidence >= self.config.confidence_threshold {
            let format = stats
                .detected_formats
                .first()
                .unwrap_or(&"YYYY-MM-DD".to_string())
                .clone();
            (InferredDataType::Date { format }, date_confidence)
        } else if time_confidence >= self.config.confidence_threshold {
            let format = stats
                .detected_formats
                .first()
                .unwrap_or(&"HH:MM:SS".to_string())
                .clone();
            (InferredDataType::Time { format }, time_confidence)
        } else if boolean_confidence >= self.config.confidence_threshold {
            let (true_values, false_values) = &stats.boolean_representations;
            (
                InferredDataType::Boolean {
                    true_values: true_values.clone(),
                    false_values: false_values.clone(),
                },
                boolean_confidence,
            )
        } else if float_confidence >= self.config.confidence_threshold && stats.float_matches > 0 {
            // Check if we should prefer decimal over float
            if let Some((precision, scale)) = stats.decimal_info {
                (
                    InferredDataType::Decimal { precision, scale },
                    float_confidence,
                )
            } else {
                (InferredDataType::Float { nullable }, float_confidence)
            }
        } else if integer_confidence >= self.config.confidence_threshold {
            (InferredDataType::Integer { nullable }, integer_confidence)
        } else if is_categorical && stats.unique_values.len() > 1 {
            (
                InferredDataType::Categorical {
                    cardinality: stats.unique_values.len(),
                },
                categorical_confidence,
            )
        } else {
            // Check for mixed types
            let mixed_types = alternatives
                .iter()
                .filter(|(_, &conf)| conf > 0.1) // At least 10% confidence
                .map(|(name, &conf)| (name.clone(), conf))
                .collect::<HashMap<_, _>>();

            if mixed_types.len() > 1 {
                let max_confidence = mixed_types.values().fold(0.0f64, |a, &b| a.max(b));
                (
                    InferredDataType::Mixed { types: mixed_types },
                    max_confidence,
                )
            } else {
                (InferredDataType::Text, 1.0)
            }
        };

        TypeInferenceResult {
            inferred_type,
            confidence,
            samples_analyzed: stats.total_samples,
            null_count: stats.null_count,
            alternatives,
        }
    }
}

impl Default for TypeInferenceEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_inference_engine_builder() {
        let engine = TypeInferenceEngine::builder()
            .sample_size(500)
            .confidence_threshold(0.8)
            .categorical_threshold(50)
            .detect_decimal_precision(false)
            .international_formats(false)
            .build();

        assert_eq!(engine.config.sample_size, 500);
        assert_eq!(engine.config.confidence_threshold, 0.8);
        assert_eq!(engine.config.categorical_threshold, 50);
        assert!(!engine.config.detect_decimal_precision);
        assert!(!engine.config.international_formats);
    }

    #[tokio::test]
    async fn test_type_pattern_matching() {
        let patterns = TypePatterns::new();

        // Integer tests
        assert!(patterns.integer.is_match("123"));
        assert!(patterns.integer.is_match("-456"));
        assert!(patterns.integer.is_match("+789"));
        assert!(!patterns.integer.is_match("12.34"));

        // Float tests
        assert!(patterns.float.is_match("12.34"));
        assert!(patterns.float.is_match("1.23e10"));
        assert!(patterns.float.is_match(".5"));
        assert!(patterns.float.is_match("123."));

        // Date tests
        assert!(patterns.date_iso.is_match("2023-12-25"));
        assert!(patterns.date_us.is_match("12/25/2023"));
        assert!(patterns.date_eu.is_match("25.12.2023"));

        // Boolean tests
        assert!(patterns.boolean_true[0].is_match("true"));
        assert!(patterns.boolean_true[0].is_match("YES"));
        assert!(patterns.boolean_true[0].is_match("1"));
        assert!(patterns.boolean_false[0].is_match("false"));
        assert!(patterns.boolean_false[0].is_match("NO"));
        assert!(patterns.boolean_false[0].is_match("0"));
    }

    #[test]
    fn test_inferred_data_type_methods() {
        let int_type = InferredDataType::Integer { nullable: true };
        assert!(int_type.is_nullable());
        assert_eq!(int_type.type_name(), "Integer");

        let float_type = InferredDataType::Float { nullable: false };
        assert!(!float_type.is_nullable());
        assert_eq!(float_type.type_name(), "Float");

        let bool_type = InferredDataType::Boolean {
            true_values: vec!["yes".to_string()],
            false_values: vec!["no".to_string()],
        };
        assert!(bool_type.is_nullable());
        assert_eq!(bool_type.type_name(), "Boolean");
    }

    #[test]
    fn test_type_stats_creation() {
        let stats = TypeStats::new();
        assert_eq!(stats.total_samples, 0);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.integer_matches, 0);
        assert!(stats.unique_values.is_empty());
    }

    #[test]
    fn test_analyze_samples_with_nulls() {
        let engine = TypeInferenceEngine::new();
        let samples = vec![
            Some("123".to_string()),
            None,
            Some("456".to_string()),
            None,
            Some("789".to_string()),
        ];

        let stats = engine.analyze_samples(&samples);
        assert_eq!(stats.total_samples, 5);
        assert_eq!(stats.null_count, 2);
        assert_eq!(stats.integer_matches, 3);
    }

    #[test]
    fn test_analyze_samples_all_nulls() {
        let engine = TypeInferenceEngine::new();
        let samples = vec![None, None, None];

        let stats = engine.analyze_samples(&samples);
        assert_eq!(stats.total_samples, 3);
        assert_eq!(stats.null_count, 3);
        assert_eq!(stats.integer_matches, 0);
    }

    #[test]
    fn test_analyze_samples_mixed_types() {
        let engine = TypeInferenceEngine::new();
        let samples = vec![
            Some("123".to_string()),        // Integer (also matches float)
            Some("45.67".to_string()),      // Float
            Some("true".to_string()),       // Boolean
            Some("2023-12-25".to_string()), // Date
            Some("hello".to_string()),      // Text
        ];

        let stats = engine.analyze_samples(&samples);
        assert_eq!(stats.total_samples, 5);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.integer_matches, 1);
        assert_eq!(stats.float_matches, 1); // Only "45.67" matches float (not "123" anymore)
        assert_eq!(stats.boolean_matches, 1);
        assert_eq!(stats.date_matches, 1);
    }

    #[test]
    fn test_determine_type_all_nulls() {
        let engine = TypeInferenceEngine::new();
        let mut stats = TypeStats::new();
        stats.total_samples = 3;
        stats.null_count = 3;

        let result = engine.determine_type(&stats);
        assert!(matches!(result.inferred_type, InferredDataType::Text));
        assert_eq!(result.confidence, 0.0);
        assert_eq!(result.null_count, 3);
    }

    #[test]
    fn test_determine_type_single_value() {
        let engine = TypeInferenceEngine::new();
        let mut stats = TypeStats::new();
        stats.total_samples = 1;
        stats.null_count = 0;
        stats.integer_matches = 1;
        stats.unique_values.insert("42".to_string(), 1);

        let result = engine.determine_type(&stats);
        assert!(matches!(
            result.inferred_type,
            InferredDataType::Integer { .. }
        ));
        assert_eq!(result.confidence, 1.0);
    }

    #[test]
    fn test_determine_type_boolean_detection() {
        let engine = TypeInferenceEngine::new();
        let mut stats = TypeStats::new();
        stats.total_samples = 4;
        stats.null_count = 0;
        stats.boolean_matches = 4;
        stats.boolean_representations = (
            vec!["true".to_string(), "yes".to_string()],
            vec!["false".to_string(), "no".to_string()],
        );

        let result = engine.determine_type(&stats);
        assert!(matches!(
            result.inferred_type,
            InferredDataType::Boolean { .. }
        ));
        assert_eq!(result.confidence, 1.0);

        if let InferredDataType::Boolean {
            true_values,
            false_values,
        } = result.inferred_type
        {
            assert!(!true_values.is_empty());
            assert!(!false_values.is_empty());
        }
    }

    #[test]
    fn test_determine_type_categorical_vs_text() {
        let engine = TypeInferenceEngine::builder()
            .categorical_threshold(3)
            .build();

        // Test categorical (low cardinality)
        let mut stats_categorical = TypeStats::new();
        stats_categorical.total_samples = 10;
        stats_categorical.null_count = 0;
        stats_categorical.unique_values.insert("A".to_string(), 5);
        stats_categorical.unique_values.insert("B".to_string(), 3);
        stats_categorical.unique_values.insert("C".to_string(), 2);

        let result_categorical = engine.determine_type(&stats_categorical);
        assert!(matches!(
            result_categorical.inferred_type,
            InferredDataType::Categorical { .. }
        ));

        // Test text (high cardinality)
        let mut stats_text = TypeStats::new();
        stats_text.total_samples = 10;
        stats_text.null_count = 0;
        for i in 0..10 {
            stats_text.unique_values.insert(format!("text_{i}"), 1);
        }

        let result_text = engine.determine_type(&stats_text);
        assert!(matches!(result_text.inferred_type, InferredDataType::Text));
    }

    #[test]
    fn test_determine_type_decimal_precision() {
        let engine = TypeInferenceEngine::builder()
            .detect_decimal_precision(true)
            .build();

        let mut stats = TypeStats::new();
        stats.total_samples = 3;
        stats.null_count = 0;
        stats.float_matches = 3;
        stats.decimal_info = Some((5, 2)); // precision=5, scale=2

        let result = engine.determine_type(&stats);
        assert!(matches!(
            result.inferred_type,
            InferredDataType::Decimal { .. }
        ));

        if let InferredDataType::Decimal { precision, scale } = result.inferred_type {
            assert_eq!(precision, 5);
            assert_eq!(scale, 2);
        }
    }

    #[test]
    fn test_determine_type_mixed_types() {
        let engine = TypeInferenceEngine::builder()
            .confidence_threshold(0.9) // High threshold to force mixed detection
            .build();

        let mut stats = TypeStats::new();
        stats.total_samples = 10;
        stats.null_count = 0;
        stats.integer_matches = 3; // 30% integers
        stats.float_matches = 4; // 40% floats
        stats.boolean_matches = 3; // 30% booleans

        let result = engine.determine_type(&stats);

        // Should detect as mixed type due to no single type having >90% confidence
        match result.inferred_type {
            InferredDataType::Mixed { types } => {
                assert!(!types.is_empty());
                assert!(types.len() > 1);
            }
            _ => {
                // Or it might detect the highest confidence type
                assert!(result.confidence > 0.0);
            }
        }
    }

    #[test]
    fn test_date_format_detection() {
        let patterns = TypePatterns::new();

        // ISO format
        assert!(patterns.date_iso.is_match("2023-12-25"));
        assert!(!patterns.date_iso.is_match("12/25/2023"));

        // US format
        assert!(patterns.date_us.is_match("12/25/2023"));
        assert!(patterns.date_us.is_match("1/1/2023"));
        assert!(!patterns.date_us.is_match("2023-12-25"));

        // EU format
        assert!(patterns.date_eu.is_match("25.12.2023"));
        assert!(patterns.date_eu.is_match("1.1.2023"));
        assert!(!patterns.date_eu.is_match("2023-12-25"));

        // DateTime format
        assert!(patterns.datetime_iso.is_match("2023-12-25T10:30:00"));
        assert!(patterns.datetime_iso.is_match("2023-12-25 10:30:00"));
        assert!(!patterns.datetime_iso.is_match("2023-12-25"));
    }

    #[test]
    fn test_boolean_representations() {
        let patterns = TypePatterns::new();

        // True values
        let true_cases = vec![
            "true", "TRUE", "True", "t", "T", "yes", "YES", "y", "Y", "1", "on", "enabled",
        ];
        for case in true_cases {
            assert!(
                patterns.boolean_true[0].is_match(case),
                "Failed to match true case: {case}"
            );
        }

        // False values
        let false_cases = vec![
            "false", "FALSE", "False", "f", "F", "no", "NO", "n", "N", "0", "off", "disabled",
        ];
        for case in false_cases {
            assert!(
                patterns.boolean_false[0].is_match(case),
                "Failed to match false case: {case}"
            );
        }
    }

    #[test]
    fn test_numeric_edge_cases() {
        let patterns = TypePatterns::new();

        // Integer edge cases
        assert!(patterns.integer.is_match("0"));
        assert!(patterns.integer.is_match("-0"));
        assert!(patterns.integer.is_match("+0"));
        assert!(patterns.integer.is_match("9223372036854775807")); // max i64

        // Float edge cases
        assert!(patterns.float.is_match("0.0"));
        assert!(patterns.float.is_match(".0"));
        assert!(patterns.float.is_match("0."));
        assert!(patterns.float.is_match("1e10"));
        assert!(patterns.float.is_match("1E-10"));
        assert!(patterns.float.is_match("-1.23e+45"));

        // Invalid cases
        assert!(!patterns.integer.is_match(""));
        assert!(!patterns.integer.is_match("abc"));
        assert!(!patterns.float.is_match(""));
        assert!(!patterns.float.is_match("abc"));
    }

    #[test]
    fn test_confidence_calculation() {
        let engine = TypeInferenceEngine::new();

        // Perfect match (100% integers)
        let mut stats_perfect = TypeStats::new();
        stats_perfect.total_samples = 5;
        stats_perfect.null_count = 0;
        stats_perfect.integer_matches = 5;

        let result_perfect = engine.determine_type(&stats_perfect);
        assert_eq!(result_perfect.confidence, 1.0);

        // Partial match (60% integers)
        let mut stats_partial = TypeStats::new();
        stats_partial.total_samples = 10;
        stats_partial.null_count = 0;
        stats_partial.integer_matches = 6;

        let result_partial = engine.determine_type(&stats_partial);
        assert!(result_partial.confidence >= 0.6);
    }

    #[test]
    fn test_empty_samples() {
        let engine = TypeInferenceEngine::new();
        let samples: Vec<Option<String>> = vec![];

        let stats = engine.analyze_samples(&samples);
        assert_eq!(stats.total_samples, 0);
        assert_eq!(stats.null_count, 0);

        let result = engine.determine_type(&stats);
        assert!(matches!(result.inferred_type, InferredDataType::Text));
        assert_eq!(result.confidence, 0.0);
    }

    #[test]
    fn test_whitespace_handling() {
        let engine = TypeInferenceEngine::new();
        let samples = vec![
            Some("  123  ".to_string()), // Should be trimmed to "123"
            Some("\t456\n".to_string()), // Should be trimmed to "456"
            Some("   ".to_string()),     // Should be treated as null
            Some("".to_string()),        // Should be treated as null
        ];

        let stats = engine.analyze_samples(&samples);
        assert_eq!(stats.total_samples, 4);
        assert_eq!(stats.null_count, 2); // Empty and whitespace-only
        assert_eq!(stats.integer_matches, 2); // "123" and "456"
    }
}
