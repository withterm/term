# Metrics Repository Reference

Complete API reference for Term's metrics repository framework for storing and querying validation results.

## Overview

The metrics repository provides a flexible storage and query interface for validation metrics with support for:
- Time-series metric storage
- Tag-based organization
- Flexible query capabilities
- DataFusion integration for optimized queries
- Thread-safe concurrent access

## Core Trait

### MetricsRepository

Defines the interface for metric storage and retrieval.

```rust
#[async_trait]
pub trait MetricsRepository: Send + Sync {
    async fn save(
        &self,
        key: ResultKey,
        value: MetricValue,
    ) -> Result<()>;
    
    async fn load(
        &self,
        key: &ResultKey,
    ) -> Result<Option<MetricValue>>;
    
    async fn query(
        &self,
        query: MetricsQuery,
    ) -> Result<Vec<(ResultKey, MetricValue)>>;
    
    async fn delete(
        &self,
        key: &ResultKey,
    ) -> Result<()>;
    
    async fn list_keys(
        &self,
        prefix: Option<&str>,
    ) -> Result<Vec<ResultKey>>;
    
    fn metadata(&self) -> RepositoryMetadata;
}
```

**Module:** `term_guard::repository`

## Key Components

### ResultKey

Collision-resistant key for metric identification.

```rust
pub struct ResultKey {
    pub metric_name: String,
    pub timestamp: DateTime<Utc>,
    pub tags: HashMap<String, String>,
    hash: String,  // SHA256 hash for complex keys
}
```

**Constructor:**
```rust
pub fn new(
    metric_name: impl Into<String>,
    timestamp: DateTime<Utc>,
    tags: HashMap<String, String>,
) -> Self

pub fn with_current_time(
    metric_name: impl Into<String>,
    tags: HashMap<String, String>,
) -> Self
```

**Methods:**
```rust
pub fn hash(&self) -> &str
pub fn matches_prefix(&self, prefix: &str) -> bool
pub fn matches_tags(&self, required_tags: &HashMap<String, String>) -> bool
```

**Module:** `term_guard::repository::result_key`

### MetricsQuery

Flexible query builder for metric retrieval.

```rust
pub struct MetricsQuery {
    pub metric_names: Option<Vec<String>>,
    pub time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    pub tags: Option<HashMap<String, String>>,
    pub limit: Option<usize>,
    pub order_by: QueryOrder,
}
```

**Builder Methods:**
```rust
pub fn new() -> Self
pub fn with_metric_names(mut self, names: Vec<String>) -> Self
pub fn with_time_range(mut self, start: DateTime<Utc>, end: DateTime<Utc>) -> Self
pub fn with_tags(mut self, tags: HashMap<String, String>) -> Self
pub fn with_limit(mut self, limit: usize) -> Self
pub fn with_order(mut self, order: QueryOrder) -> Self
```

**Query Orders:**
```rust
pub enum QueryOrder {
    TimestampAsc,
    TimestampDesc,
    MetricNameAsc,
    MetricNameDesc,
}
```

**Module:** `term_guard::repository::query`

### RepositoryMetadata

Repository capabilities and configuration.

```rust
pub struct RepositoryMetadata {
    pub name: String,
    pub version: String,
    pub supports_queries: bool,
    pub supports_tags: bool,
    pub supports_time_range: bool,
    pub is_persistent: bool,
}
```

## Implementations

### InMemoryRepository

Thread-safe in-memory implementation using Arc<RwLock>.

```rust
pub struct InMemoryRepository {
    storage: Arc<RwLock<HashMap<String, (ResultKey, MetricValue)>>>,
}
```

**Constructor:**
```rust
pub fn new() -> Self
```

**Characteristics:**
- Thread-safe concurrent access
- O(1) save/load operations
- O(n) query operations
- Non-persistent (data lost on restart)

**Module:** `term_guard::repository::in_memory`

### DataFusion Integration

Query executor using DataFusion for optimized metric queries.

```rust
pub trait DataFusionQueryExecutorExt: MetricsRepository {
    async fn execute_sql(&self, sql: &str) -> Result<Vec<RecordBatch>>;
    
    async fn to_datafusion_context(&self) -> Result<SessionContext>;
}
```

**Features:**
- Arrow columnar format for efficient queries
- SQL support for complex analytics
- Optimized aggregations and filtering
- Integration with DataFusion ecosystem

**Module:** `term_guard::repository::datafusion_executor`

## Usage Patterns

### Basic Storage and Retrieval

```rust
let repo = InMemoryRepository::new();

// Save metric
let key = ResultKey::with_current_time(
    "row_count",
    HashMap::from([("table".to_string(), "users".to_string())]),
);
repo.save(key.clone(), MetricValue::Long(1000)).await?;

// Load metric
if let Some(value) = repo.load(&key).await? {
    println!("Metric value: {:?}", value);
}
```

### Time-Series Queries

```rust
let query = MetricsQuery::new()
    .with_time_range(
        Utc::now() - Duration::days(7),
        Utc::now(),
    )
    .with_metric_names(vec!["completeness".to_string()])
    .with_order(QueryOrder::TimestampDesc);

let results = repo.query(query).await?;
```

### Tag-Based Filtering

```rust
let query = MetricsQuery::new()
    .with_tags(HashMap::from([
        ("environment".to_string(), "production".to_string()),
        ("region".to_string(), "us-west".to_string()),
    ]));

let results = repo.query(query).await?;
```

### Batch Operations

```rust
// Save multiple metrics
for metric in metrics {
    let key = ResultKey::with_current_time(&metric.name, metric.tags);
    repo.save(key, metric.value).await?;
}

// Query and aggregate
let all_metrics = repo.query(MetricsQuery::new()).await?;
let aggregated = aggregate_metrics(all_metrics);
```

## Performance Optimization

### Query Strategies

```rust
pub trait QueryExecutor: MetricsRepository {
    async fn optimize_query(&self, query: &MetricsQuery) -> Result<ExecutionPlan>;
    
    async fn estimate_cost(&self, query: &MetricsQuery) -> Result<QueryCost>;
}
```

### Caching

The repository supports result caching for frequently accessed metrics:

```rust
// Cached query execution
let results = repo.query_cached(query, Duration::minutes(5)).await?;
```

## Error Handling

Repository-specific error types:

```rust
pub enum RepositoryError {
    SaveFailed(String),
    LoadFailed(String),
    QueryFailed(String),
    InvalidKey(String),
    StorageFull,
    ConcurrencyConflict,
}
```

## OpenTelemetry Integration

All repository operations are instrumented with OpenTelemetry:

```rust
#[instrument(skip(self, value))]
async fn save(&self, key: ResultKey, value: MetricValue) -> Result<()> {
    // Automatic tracing and metrics
}
```

**Collected Metrics:**
- `repository.save.duration` - Save operation latency
- `repository.query.duration` - Query execution time
- `repository.size` - Current repository size
- `repository.query.results` - Number of results returned

## Configuration

### Environment Variables

- `TERM_REPOSITORY_TYPE`: Repository implementation (memory, postgres, etc.)
- `TERM_REPOSITORY_CACHE_SIZE`: Query cache size in MB
- `TERM_REPOSITORY_MAX_CONNECTIONS`: Connection pool size

### Repository Selection

```rust
pub fn create_repository(config: &Config) -> Box<dyn MetricsRepository> {
    match config.repository_type {
        RepositoryType::InMemory => Box::new(InMemoryRepository::new()),
        RepositoryType::Postgres => Box::new(PostgresRepository::new(config)),
        // Additional implementations
    }
}
```

## Best Practices

1. **Key Design**: Use consistent tag naming conventions
2. **Query Optimization**: Use time ranges and tags to limit result sets
3. **Batch Operations**: Group related saves for better performance
4. **Cleanup**: Implement retention policies for time-series data
5. **Monitoring**: Track repository size and query performance

## Thread Safety

All repository implementations must be thread-safe:

```rust
// Safe to share across threads
let repo = Arc::new(InMemoryRepository::new());
let repo_clone = repo.clone();

tokio::spawn(async move {
    repo_clone.save(key, value).await.unwrap();
});
```

## Future Extensions

Planned repository implementations:
- PostgreSQL with TimescaleDB
- Redis for distributed caching
- S3 for long-term storage
- ClickHouse for analytics