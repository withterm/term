# Anomaly Detection Reference

Complete API reference for Term's anomaly detection framework.

## Core Types

### `Anomaly`

Represents a detected anomaly in a metric.

```rust
pub struct Anomaly {
    pub metric_name: String,
    pub current_value: MetricValue,
    pub expected_value: Option<MetricValue>,
    pub detection_strategy: String,
    pub confidence: f64,
    pub description: String,
    pub detected_at: DateTime<Utc>,
    pub metadata: HashMap<String, String>,
}
```

### `MetricDataPoint`

Historical data point for a metric.

```rust
pub struct MetricDataPoint {
    pub value: MetricValue,
    pub timestamp: DateTime<Utc>,
    pub metadata: HashMap<String, String>,
}
```

## Traits

### `AnomalyDetector`

Core trait for implementing anomaly detection strategies.

```rust
#[async_trait]
pub trait AnomalyDetector: Send + Sync {
    async fn detect(
        &self,
        metric_name: &str,
        current_value: &MetricValue,
        history: &[MetricDataPoint],
    ) -> AnalyzerResult<Option<Anomaly>>;
    
    fn name(&self) -> &str;
    fn description(&self) -> &str;
}
```

### `MetricsRepository`

Storage abstraction for historical metrics.

```rust
#[async_trait]
pub trait MetricsRepository: Send + Sync {
    async fn store_metric(
        &self,
        metric_name: &str,
        value: MetricValue,
        timestamp: DateTime<Utc>,
    ) -> AnalyzerResult<()>;
    
    async fn get_metric_history(
        &self,
        metric_name: &str,
        since: Option<DateTime<Utc>>,
        until: Option<DateTime<Utc>>,
        limit: Option<usize>,
    ) -> AnalyzerResult<Vec<MetricDataPoint>>;
    
    async fn store_context(&self, context: &AnalyzerContext) -> AnalyzerResult<()>;
}
```

## Built-in Detectors

### `RelativeRateOfChangeDetector`

Detects anomalies based on relative percentage changes.

```rust
pub struct RelativeRateOfChangeDetector {
    pub max_rate_of_change: f64,
    pub min_history_size: usize,
}

impl RelativeRateOfChangeDetector {
    pub fn new(max_rate_of_change: f64) -> Self;
    pub fn with_min_history_size(self, size: usize) -> Self;
}
```

**Parameters:**
- `max_rate_of_change`: Maximum allowed relative change (e.g., 0.1 for 10%)
- `min_history_size`: Minimum data points required (default: 2)

**Detection Logic:**
- Calculates: `|(current - previous) / previous|`
- Triggers when rate exceeds threshold
- Confidence: `rate_of_change / max_rate_of_change`

### `AbsoluteChangeDetector`

Detects anomalies based on absolute value changes.

```rust
pub struct AbsoluteChangeDetector {
    pub max_absolute_change: f64,
    pub min_history_size: usize,
}

impl AbsoluteChangeDetector {
    pub fn new(max_absolute_change: f64) -> Self;
    pub fn with_min_history_size(self, size: usize) -> Self;
}
```

**Parameters:**
- `max_absolute_change`: Maximum allowed absolute change
- `min_history_size`: Minimum data points required (default: 1)

**Detection Logic:**
- Calculates: `|current - previous|`
- Triggers when change exceeds threshold
- Confidence: `absolute_change / max_absolute_change`

### `ZScoreDetector`

Detects statistical outliers using Z-score analysis.

```rust
pub struct ZScoreDetector {
    pub z_score_threshold: f64,
    pub min_history_size: usize,
}

impl ZScoreDetector {
    pub fn new(z_score_threshold: f64) -> Self;
    pub fn with_min_history_size(self, size: usize) -> Self;
}
```

**Parameters:**
- `z_score_threshold`: Number of standard deviations (e.g., 3.0)
- `min_history_size`: Minimum data points for statistics (default: 10)

**Detection Logic:**
- Calculates: `|current - mean| / std_dev`
- Triggers when Z-score exceeds threshold
- Confidence: `min(z_score / threshold, 1.0)`

## Repositories

### `InMemoryMetricsRepository`

Testing implementation with in-memory storage.

```rust
pub struct InMemoryMetricsRepository { /* ... */ }

impl InMemoryMetricsRepository {
    pub fn new() -> Self;
}
```

**Features:**
- Thread-safe with async RwLock
- Automatic timestamp sorting
- Time-based filtering support
- No persistence

## Runner

### `AnomalyDetectionRunner`

Orchestrates anomaly detection across metrics.

```rust
pub struct AnomalyDetectionRunner { /* ... */ }

impl AnomalyDetectionRunner {
    pub fn builder() -> AnomalyDetectionRunnerBuilder;
    
    pub async fn detect_anomalies(
        &self,
        context: &AnalyzerContext,
    ) -> AnalyzerResult<Vec<Anomaly>>;
}
```

### `AnomalyDetectionRunnerBuilder`

Builder for configuring the detection runner.

```rust
impl AnomalyDetectionRunnerBuilder {
    pub fn repository(self, repository: Box<dyn MetricsRepository>) -> Self;
    
    pub fn add_detector(
        self,
        pattern: &str,
        detector: Box<dyn AnomalyDetector>,
    ) -> Self;
    
    pub fn config(self, config: AnomalyDetectionConfig) -> Self;
    
    pub fn build(self) -> AnalyzerResult<AnomalyDetectionRunner>;
}
```

### `AnomalyDetectionConfig`

Configuration options for anomaly detection.

```rust
pub struct AnomalyDetectionConfig {
    pub min_confidence: f64,
    pub store_current_metrics: bool,
    pub default_history_window: Duration,
}

impl Default for AnomalyDetectionConfig {
    fn default() -> Self {
        Self {
            min_confidence: 0.7,
            store_current_metrics: true,
            default_history_window: Duration::days(30),
        }
    }
}
```

## Pattern Matching

The runner supports pattern matching for metric names:

| Pattern | Matches | Example |
|---------|---------|---------|
| `*` | All metrics | `*` → `size`, `mean_revenue`, etc. |
| `prefix*` | Metrics starting with prefix | `completeness.*` → `completeness.email`, `completeness.user_id` |
| `exact` | Exact metric name | `size` → only `size` |

Patterns are evaluated in order, first match wins.

## Example Usage

```rust
use term_guard::analyzers::*;
use chrono::Duration;

// Configure detection
let config = AnomalyDetectionConfig {
    min_confidence: 0.8,
    store_current_metrics: true,
    default_history_window: Duration::days(14),
};

// Build detector
let detector = AnomalyDetectionRunner::builder()
    .repository(Box::new(InMemoryMetricsRepository::new()))
    .config(config)
    .add_detector(
        "completeness.*",
        Box::new(RelativeRateOfChangeDetector::new(0.1))
    )
    .add_detector(
        "size",
        Box::new(
            ZScoreDetector::new(3.0)
                .with_min_history_size(20)
        )
    )
    .build()?;

// Detect anomalies
let anomalies = detector.detect_anomalies(&metrics).await?;

// Process results
for anomaly in anomalies {
    if anomaly.confidence > 0.9 {
        println!("Critical: {}", anomaly.description);
    }
}
```