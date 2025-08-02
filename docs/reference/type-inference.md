# TypeInferenceEngine API Reference

<!-- 
This is a REFERENCE document following Diátaxis principles.
It provides complete technical information about the TypeInferenceEngine API.
-->

## Overview

The `TypeInferenceEngine` analyzes string values to detect semantic data types beyond basic SQL types.

## TypeInferenceEngine

```rust
pub struct TypeInferenceEngine {
    sample_size: usize,
    confidence_threshold: f64,
    patterns: TypePatterns,
}
```

### Constructor

```rust
pub fn new() -> Self
```

Creates an engine with default settings:
- `sample_size`: 1000
- `confidence_threshold`: 0.9
- `patterns`: Default pattern set

### Builder

```rust
pub fn builder() -> TypeInferenceEngineBuilder
```

Returns a builder for configuration.

### Methods

#### infer_type

```rust
pub fn infer_type(&self, samples: &[&str]) -> Result<InferenceResult>
```

**Parameters:**
- `samples`: Slice of string values to analyze

**Returns:** `InferenceResult` with detected type and confidence

**Errors:**
- `InferenceError::NoSamples` - Empty input
- `InferenceError::InsufficientData` - Too few non-null samples

#### infer_type_from_values

```rust
pub fn infer_type_from_values(&self, values: &[String]) -> Result<InferenceResult>
```

**Parameters:**
- `values`: Vector of owned strings

**Returns:** `InferenceResult` with detected type

#### detect_patterns

```rust
pub fn detect_patterns(&self, samples: &[&str]) -> Vec<DetectedPattern>
```

**Parameters:**
- `samples`: String values to analyze

**Returns:** Vector of detected patterns with match percentages

## TypeInferenceEngineBuilder

```rust
pub struct TypeInferenceEngineBuilder {
    sample_size: usize,
    confidence_threshold: f64,
    custom_patterns: Vec<(String, Regex)>,
}
```

### Methods

#### sample_size

```rust
pub fn sample_size(mut self, size: usize) -> Self
```

Maximum samples to analyze. Default: 1000.

#### confidence_threshold

```rust
pub fn confidence_threshold(mut self, threshold: f64) -> Self
```

Minimum confidence for type detection (0.0-1.0). Default: 0.9.

#### add_pattern

```rust
pub fn add_pattern(mut self, name: String, pattern: Regex) -> Self
```

Adds a custom pattern for detection.

#### build

```rust
pub fn build(self) -> TypeInferenceEngine
```

Creates the configured engine.

## InferenceResult

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceResult {
    pub data_type: InferredDataType,
    pub confidence: f64,
    pub sample_count: usize,
    pub null_count: usize,
    pub parse_errors: Vec<String>,
}
```

### Fields

- `data_type`: Detected semantic type
- `confidence`: Detection confidence (0.0-1.0)
- `sample_count`: Number of samples analyzed
- `null_count`: Number of null/empty values
- `parse_errors`: Sample values that failed parsing

## InferredDataType

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum InferredDataType {
    // Numeric types
    Boolean { nullable: bool },
    Integer { nullable: bool },
    Decimal { precision: u8, scale: u8, nullable: bool },
    
    // Temporal types
    Date { format: String, nullable: bool },
    Time { format: String, nullable: bool },
    Timestamp { format: String, nullable: bool },
    
    // String types
    String { max_length: usize, nullable: bool },
    Email { nullable: bool },
    Url { nullable: bool },
    Json { nullable: bool },
    
    // Special types
    Mixed { types: Vec<InferredDataType> },
    Unknown,
}
```

### Variants

#### Boolean
- `nullable`: Whether nulls were found

#### Integer
- `nullable`: Whether nulls were found

#### Decimal
- `precision`: Total digits
- `scale`: Decimal places
- `nullable`: Whether nulls were found

#### Date
- `format`: Detected date format (e.g., "YYYY-MM-DD")
- `nullable`: Whether nulls were found

#### Time
- `format`: Detected time format (e.g., "HH:MM:SS")
- `nullable`: Whether nulls were found

#### Timestamp
- `format`: ISO 8601 or RFC format
- `nullable`: Whether nulls were found

#### String
- `max_length`: Maximum string length found
- `nullable`: Whether nulls were found

#### Email
- `nullable`: Whether nulls were found

#### Url
- `nullable`: Whether nulls were found

#### Json
- `nullable`: Whether nulls were found

#### Mixed
- `types`: List of detected types when multiple apply

#### Unknown
No recognizable type pattern

## DetectedDataType

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectedDataType {
    pub arrow_type: DataType,
    pub semantic_type: Option<String>,
    pub confidence: f64,
    pub sample_values: Vec<String>,
}
```

### Fields

- `arrow_type`: Recommended Arrow/DataFusion type
- `semantic_type`: Semantic type name (e.g., "email", "phone")
- `confidence`: Detection confidence
- `sample_values`: Example values of this type

## TypePatterns

Internal pattern registry with pre-defined patterns:

### Boolean Patterns
- `true`, `false` (case-insensitive)
- `yes`, `no`
- `y`, `n`
- `1`, `0`
- `on`, `off`

### Numeric Patterns
- **Integer**: `^[+-]?\d+$`
- **Decimal**: `^[+-]?\d+\.\d+$`
- **Scientific**: `^[+-]?\d+\.?\d*[eE][+-]?\d+$`
- **Formatted**: `^[+-]?\d{1,3}(,\d{3})*(\.\d+)?$`

### Temporal Patterns
- **ISO Date**: `^\d{4}-\d{2}-\d{2}$`
- **US Date**: `^\d{2}/\d{2}/\d{4}$`
- **EU Date**: `^\d{2}\.\d{2}\.\d{4}$`
- **ISO Time**: `^\d{2}:\d{2}:\d{2}$`
- **ISO Timestamp**: `^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}`

### String Patterns
- **Email**: RFC 5322 compliant regex
- **URL**: `^https?://[^\s/$.?#].[^\s]*$`
- **UUID**: `^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`
- **Phone US**: `^\+?1?\d{10,14}$`
- **SSN**: `^\d{3}-\d{2}-\d{4}$`
- **Credit Card**: Luhn-compatible patterns

### Data Format Patterns
- **JSON Object**: `^\{.*\}$`
- **JSON Array**: `^\[.*\]$`
- **XML**: `^<[^>]+>.*</[^>]+>$`

## Usage Patterns

### Basic Type Detection

```rust
let engine = TypeInferenceEngine::new();
let samples = vec!["2023-01-01", "2023-12-31", "2023-06-15"];
let result = engine.infer_type(&samples)?;

match result.data_type {
    InferredDataType::Date { format, .. } => {
        println!("Date column with format: {}", format);
    }
    _ => println!("Other type detected"),
}
```

### Custom Pattern Detection

```rust
use regex::Regex;

let engine = TypeInferenceEngine::builder()
    .add_pattern(
        "product_code".to_string(),
        Regex::new(r"^PROD-\d{6}$").unwrap()
    )
    .build();
```

### Confidence Tuning

```rust
let strict_engine = TypeInferenceEngine::builder()
    .confidence_threshold(0.99)  // Very high confidence
    .sample_size(10000)          // Large sample
    .build();

let lenient_engine = TypeInferenceEngine::builder()
    .confidence_threshold(0.8)   // Lower threshold
    .sample_size(100)            // Smaller sample
    .build();
```

## Performance Characteristics

| Operation | Time Complexity | Memory Complexity |
|-----------|----------------|-------------------|
| Type inference | O(n × p) | O(1) |
| Pattern matching | O(n × p) | O(p) |
| Boolean detection | O(n) | O(1) |
| Numeric parsing | O(n) | O(1) |
| Date parsing | O(n × f) | O(f) |

Where:
- n = number of samples
- p = number of patterns
- f = number of date formats

## Thread Safety

- `TypeInferenceEngine` is `Send + Sync`
- All result types are `Send + Sync`
- Regex patterns are compiled once and reused

## Error Types

```rust
pub enum InferenceError {
    NoSamples,
    InsufficientData { 
        required: usize, 
        found: usize 
    },
    InvalidPattern(String),
    ParseError { 
        value: String, 
        expected_type: String 
    },
}
```

## Best Practices

1. **Sample Size**: Use at least 100 samples for reliable detection
2. **Null Handling**: Engine automatically handles null/empty values
3. **Mixed Types**: Returns `Mixed` when multiple types detected
4. **Custom Patterns**: Add domain-specific patterns for better accuracy
5. **Performance**: Reuse engine instances for multiple inferences

## Limitations

1. **String Input**: Only analyzes string representations
2. **Pattern Priority**: Earlier patterns take precedence
3. **Locale**: Currently uses US/ISO formats primarily
4. **Memory**: Patterns are kept in memory

## See Also

- [`ColumnProfiler`](profiler.md) - Column profiling reference
- [`InferredDataType`](../explanation/type-system.md) - Type system design
- [Column Profiling Tutorial](../tutorials/04-column-profiling.md) - Usage examples
- [Profiling Algorithm](../explanation/profiling-algorithm.md) - Implementation details