# Incremental Analysis Reference

Complete API reference for Term's incremental computation framework for efficient append-only and partitioned dataset analysis.

## Overview

The incremental analysis framework enables stateful metrics computation that efficiently handles:
- Append-only data streams
- Partitioned datasets
- Incremental updates without reprocessing historical data
- State persistence and recovery

## Core Components

### IncrementalAnalysisRunner

Orchestrates incremental analysis across partitions with state management.

```rust
pub struct IncrementalAnalysisRunner {
    analyzers: Vec<Box<dyn TypeErasedAnalyzer>>,
    state_store: Box<dyn StateStore>,
    batch_size: usize,
}
```

**Constructor:**
```rust
pub fn new(
    analyzers: Vec<Box<dyn TypeErasedAnalyzer>>,
    state_store: Box<dyn StateStore>,
) -> Self

pub fn with_batch_size(mut self, batch_size: usize) -> Self
```

**Methods:**
```rust
pub async fn run_incremental(
    &self,
    ctx: &SessionContext,
    partitions: Vec<String>,
) -> AnalyzerResult<HashMap<String, MetricValue>>

pub async fn run_on_partition(
    &self,
    ctx: &SessionContext,
    partition: &str,
) -> AnalyzerResult<HashMap<String, Box<dyn Any>>>

pub async fn compute_metrics_from_states(
    &self,
    states: HashMap<String, Box<dyn Any>>,
) -> AnalyzerResult<HashMap<String, MetricValue>>
```

**Module:** `term_guard::analyzers::incremental::runner`

### StateStore Trait

Defines interface for state persistence across analysis runs.

```rust
#[async_trait]
pub trait StateStore: Send + Sync {
    async fn load_state(
        &self,
        analyzer_name: &str,
        partition: &str,
    ) -> AnalyzerResult<Option<Vec<u8>>>;
    
    async fn save_state(
        &self,
        analyzer_name: &str,
        partition: &str,
        state: &[u8],
    ) -> AnalyzerResult<()>;
    
    async fn list_partitions(
        &self,
        analyzer_name: &str,
    ) -> AnalyzerResult<Vec<String>>;
    
    async fn delete_state(
        &self,
        analyzer_name: &str,
        partition: &str,
    ) -> AnalyzerResult<()>;
}
```

### FilesystemStateStore

File-based implementation of StateStore for persistent state management.

```rust
pub struct FilesystemStateStore {
    base_path: PathBuf,
}
```

**Constructor:**
```rust
pub fn new(base_path: impl Into<PathBuf>) -> Self
```

**Storage Layout:**
```
{base_path}/
├── {analyzer_name}/
│   ├── {partition_1}.state
│   ├── {partition_2}.state
│   └── {partition_n}.state
```

**Module:** `term_guard::analyzers::incremental::state_store`

### TypeErasedAnalyzer

Wrapper for dynamic analyzer dispatch in incremental computations.

```rust
pub trait TypeErasedAnalyzer: Send + Sync {
    fn name(&self) -> &str;
    
    async fn compute_state(
        &self,
        ctx: &SessionContext,
    ) -> AnalyzerResult<Box<dyn Any>>;
    
    fn compute_metric(
        &self,
        state: &dyn Any,
    ) -> AnalyzerResult<MetricValue>;
    
    fn merge_states(
        &self,
        states: Vec<Box<dyn Any>>,
    ) -> AnalyzerResult<Box<dyn Any>>;
    
    fn serialize_state(
        &self,
        state: &dyn Any,
    ) -> AnalyzerResult<Vec<u8>>;
    
    fn deserialize_state(
        &self,
        bytes: &[u8],
    ) -> AnalyzerResult<Box<dyn Any>>;
}
```

## State Management

### State Serialization

States are serialized using bincode for efficient binary representation:

```rust
// Serialization
let bytes = bincode::serialize(&state)?;

// Deserialization  
let state: AnalyzerState = bincode::deserialize(&bytes)?;
```

### State Merging

States from multiple partitions are combined using the `AnalyzerState::merge` method:

```rust
impl AnalyzerState for MyState {
    fn merge(states: Vec<Self>) -> AnalyzerResult<Self> {
        // Combine states (e.g., sum counts, merge histograms)
    }
}
```

## Usage Patterns

### Incremental Updates

Process new partitions without recomputing historical data:

```rust
let runner = IncrementalAnalysisRunner::new(
    vec![Box::new(analyzer)],
    Box::new(FilesystemStateStore::new("./state")),
);

// Process initial partitions
let metrics = runner.run_incremental(
    &ctx,
    vec!["2024-01".to_string(), "2024-02".to_string()],
).await?;

// Later, process only new partition
let updated_metrics = runner.run_incremental(
    &ctx, 
    vec!["2024-01".to_string(), "2024-02".to_string(), "2024-03".to_string()],
).await?;
```

### Batch Processing

Handle large partition sets efficiently:

```rust
let runner = IncrementalAnalysisRunner::new(analyzers, state_store)
    .with_batch_size(100); // Process 100 partitions at a time

let metrics = runner.run_incremental(&ctx, large_partition_list).await?;
```

### State Recovery

Recover from failures using persisted state:

```rust
// State automatically loaded from store
let runner = IncrementalAnalysisRunner::new(
    analyzers,
    Box::new(FilesystemStateStore::new("./state")),
);

// Continues from last saved state
let metrics = runner.run_incremental(&ctx, partitions).await?;
```

## Performance Characteristics

### Time Complexity
- State loading: O(1) per partition
- State merging: O(p) where p = number of partitions
- Batch processing: O(n/b) where n = partitions, b = batch size

### Memory Usage
- State store: O(analyzers × partitions × state_size)
- Runtime: O(batch_size × state_size)
- Merge buffer: O(partitions × state_size) per batch

### Optimization Tips

1. **Batch Size**: Tune based on available memory and partition count
2. **State Cleanup**: Periodically clean old partition states
3. **Compression**: Consider compressing serialized states for large datasets
4. **Parallel Processing**: Use batch processing for concurrent partition analysis

## Error Handling

The framework provides specific error types:

- `StateLoadError`: Failed to load persisted state
- `StateSaveError`: Failed to persist state
- `SerializationError`: State serialization/deserialization failed
- `MergeError`: State merging failed
- `PartitionError`: Partition processing failed

## Supported Analyzers

All standard analyzers support incremental computation:

| Analyzer | State Size | Merge Complexity |
|----------|-----------|------------------|
| SizeAnalyzer | O(1) | O(p) |
| CompletenessAnalyzer | O(1) | O(p) |
| MeanAnalyzer | O(1) | O(p) |
| HistogramAnalyzer | O(buckets) | O(p × buckets) |
| KllSketchAnalyzer | O(k) | O(p × k log k) |
| CorrelationAnalyzer | O(1) | O(p) |

## Configuration

### Environment Variables

- `TERM_STATE_STORE_PATH`: Default path for filesystem state store
- `TERM_INCREMENTAL_BATCH_SIZE`: Default batch size for processing

### Best Practices

1. **Partition Design**: Use time-based or logical partitions
2. **State Lifecycle**: Implement cleanup for obsolete states
3. **Error Recovery**: Handle partial state corruption gracefully
4. **Monitoring**: Track state size growth over time
5. **Testing**: Verify state merge logic with property tests