# Understanding Term's Analyzer Architecture

<!-- 
This is an EXPLANATION document following Diátaxis principles.
It provides conceptual understanding and design rationale.
-->

## Introduction

Term's analyzer architecture is designed around two key principles: **composability** and **distributed computation readiness**. This document explains why analyzers work the way they do and the design decisions behind the architecture.

## The Two-Phase Computation Model

### Why Two Phases?

Term analyzers use a two-phase computation model:

1. **State Computation Phase**: Extract intermediate results from data
2. **Metric Computation Phase**: Transform state into final metrics

This separation serves several purposes:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Partition 1 │     │  Partition 2 │     │  Partition 3 │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       ▼                   ▼                   ▼
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   State 1    │     │   State 2    │     │   State 3    │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       └───────────────────┴───────────────────┘
                           │
                           ▼
                    ┌─────────────┐
                    │ Merged State │
                    └──────┬──────┘
                           │
                           ▼
                    ┌─────────────┐
                    │   Metric     │
                    └─────────────┘
```

### Distributed Computation

The primary reason for this design is to support distributed computation. Consider calculating the mean of a column across multiple data partitions:

```rust
// State for each partition
struct MeanState {
    sum: f64,    // Sum of values
    count: u64,  // Count of values
}

// Can be computed independently on each partition
let state1 = MeanState { sum: 100.0, count: 10 };
let state2 = MeanState { sum: 200.0, count: 20 };

// States can be merged
let merged = MeanState {
    sum: state1.sum + state2.sum,
    count: state1.count + state2.count,
};

// Final metric computation
let mean = merged.sum / merged.count as f64; // 10.0
```

This pattern enables:
- **Parallel Processing**: Each partition can be analyzed independently
- **Incremental Updates**: New data can be incorporated without reprocessing
- **Memory Efficiency**: Only state needs to be held in memory, not raw data

### State Design Principles

Analyzer states must be:

1. **Mergeable**: States from different partitions can be combined
2. **Serializable**: States can be transmitted over network or stored
3. **Sufficient**: State contains all information needed for metric computation

## The Analyzer Trait Design

### Core Methods

The `Analyzer` trait defines three essential methods:

```rust
#[async_trait]
pub trait Analyzer {
    type State: AnalyzerState;
    type Metric: Into<MetricValue>;
    
    async fn compute_state_from_data(&self, ctx: &SessionContext) -> Result<Self::State>;
    fn compute_metric_from_state(&self, state: &Self::State) -> Result<Self::Metric>;
    fn name(&self) -> &str;
}
```

### Async Computation

The `compute_state_from_data` method is async because:
- **I/O Operations**: Reading data from disk/network is inherently async
- **DataFusion Integration**: DataFusion's query engine is async
- **Scalability**: Async allows handling many concurrent analyses

The `compute_metric_from_state` is synchronous because:
- **CPU-only**: Metric computation is pure calculation
- **Fast**: No I/O involved, completes quickly
- **Simplicity**: Easier to test and reason about

### Type Safety

The associated types provide compile-time guarantees:

```rust
impl Analyzer for CompletenessAnalyzer {
    type State = CompletenessState;  // Specific state type
    type Metric = MetricValue;        // Always produces MetricValue
    
    // Compiler ensures state and metric types match
}
```

## The AnalysisRunner Architecture

### Problem: Heterogeneous Types

Analyzers have different state types, making it challenging to store them in a collection:

```rust
// This doesn't work - different types!
let analyzers = vec![
    SizeAnalyzer::new(),           // State = SizeState
    MeanAnalyzer::new("column"),    // State = MeanState
];
```

### Solution: Type Erasure with Closures

The AnalysisRunner uses closure-based type erasure:

```rust
pub type AnalyzerExecution = Box<dyn Fn(&SessionContext) -> BoxFuture<'_, Result<(String, MetricValue)>>>;

// Each analyzer is wrapped in a closure that erases its specific types
let execution: AnalyzerExecution = Box::new(move |ctx| {
    async move {
        let state = analyzer.compute_state_from_data(ctx).await?;
        let metric = analyzer.compute_metric_from_state(&state)?;
        Ok((analyzer.name().to_string(), metric.into()))
    }.boxed()
});
```

This design:
- **Preserves Type Safety**: Each analyzer maintains its types internally
- **Enables Collection**: All executions have the same type signature
- **Avoids Trait Objects**: No need for `Box<dyn Analyzer>` complexity

### Progress Tracking

Progress callbacks use a simple float (0.0 to 1.0) because:
- **Universal**: Works regardless of analyzer types or counts
- **Composable**: Easy to aggregate progress from multiple sources
- **UI-Friendly**: Maps directly to progress bars

## Metric Key Design

### Hierarchical Naming

Metric keys follow a hierarchical pattern:

```
analyzer_name[.qualifier]*

Examples:
- "size"                    # Simple metric
- "completeness.user_id"    # Column-specific metric
- "compliance.rule1"        # Rule-specific metric
```

This enables:
- **Namespace Isolation**: No conflicts between analyzers
- **Logical Grouping**: Related metrics share prefixes
- **Query Patterns**: Easy to find all metrics for an analyzer

### Column-Based Metrics

Many analyzers operate on columns, so the default pattern is:

```rust
fn metric_key(&self) -> String {
    format!("{}.{}", self.name(), self.column)
}
```

This convention:
- **Scales**: Supports analyzing hundreds of columns
- **Self-Documents**: Key indicates both analyzer and column
- **Queryable**: Can filter metrics by analyzer or column

## Error Handling Philosophy

### Continue-on-Error Default

The AnalysisRunner defaults to `continue_on_error = true` because:

1. **Partial Results Are Valuable**: 90% success is better than 0%
2. **Data Quality Varies**: Real-world data has surprises
3. **Exploratory Analysis**: Users often don't know what will work

### Error Collection

Errors are collected rather than thrown because:
- **Batch Processing**: See all problems at once
- **Root Cause Analysis**: Patterns in failures reveal issues
- **Audit Trail**: Record what was attempted

## Performance Considerations

### Sequential Execution

Current implementation executes analyzers sequentially. This is intentional:

1. **Simplicity**: Easy to understand and debug
2. **Memory Predictable**: One analyzer at a time = bounded memory
3. **Future Optimization**: Can add grouping without breaking API

### Future: Computation Grouping

The architecture supports future optimization through analyzer grouping:

```rust
// Future optimization - group compatible analyzers
let groups = group_by_computation_pattern(analyzers);
for group in groups {
    // Single scan computes multiple metrics
    let sql = build_combined_query(group);
    // ...
}
```

## Design Trade-offs

### Table Name Convention

Requiring tables to be named "data" is a deliberate trade-off:

**Benefits:**
- **Simplicity**: No configuration needed
- **Consistency**: All analyzers work the same way
- **Optimization**: Can optimize for known table name

**Drawbacks:**
- **Flexibility**: Can't analyze multiple tables simultaneously
- **Convention**: Users must remember this requirement

**Mitigation:**
```sql
-- Users can create views for flexibility
CREATE VIEW data AS SELECT * FROM my_actual_table;
```

### Synchronous Metric Computation

Keeping metric computation synchronous trades flexibility for simplicity:

**Benefits:**
- **Testing**: Easy to unit test
- **Debugging**: Stack traces are clear
- **Performance**: No async overhead for CPU work

**Limitations:**
- **No I/O**: Can't fetch additional data during metric computation
- **No Parallelism**: Can't parallelize metric computation

## Comparison with Other Approaches

### vs. Single-Phase Analysis

Some systems compute metrics directly from data:

```rust
// Single-phase approach
async fn compute_mean(ctx: &SessionContext) -> f64 {
    let df = ctx.sql("SELECT AVG(column) FROM data").await?;
    // Extract result
}
```

**Term's Approach Benefits:**
- **Composable**: States can be merged
- **Resumable**: Can save/restore analysis progress
- **Distributable**: Natural fit for distributed systems

### vs. Streaming Analytics

Stream processing systems update metrics incrementally:

```rust
// Streaming approach
metric.update(new_value);
```

**Term's Approach Benefits:**
- **Batch Efficient**: Optimized for large batch processing
- **SQL Integration**: Leverages DataFusion's optimizer
- **Complex Metrics**: Supports arbitrarily complex computations

## Future Directions

The architecture is designed to support:

1. **Automatic Grouping**: Detect compatible analyzers and combine
2. **Incremental Updates**: Update metrics as new data arrives
3. **Distributed Execution**: Run on compute clusters
4. **Custom Schedulers**: Pluggable execution strategies

These can be added without breaking the existing API, demonstrating the architecture's extensibility.

## Conclusion

Term's analyzer architecture balances several concerns:
- **Usability**: Simple API that's hard to misuse
- **Performance**: Efficient batch processing with DataFusion
- **Flexibility**: Supports diverse analysis patterns
- **Future-Proof**: Can evolve without breaking changes

The two-phase computation model, combined with the AnalysisRunner's orchestration, provides a foundation for both simple single-machine analysis and future distributed processing scenarios.