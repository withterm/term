# Term Architecture

This document explains Term's architecture, design decisions, and how the various components work together to provide fast, reliable data validation.

## Overview

Term is built on a layered architecture that separates concerns while maintaining high performance:

```
┌─────────────────────────────────────────────────────┐
│              User API Layer                         │
│  (ValidationSuite, Check, Builder APIs)            │
├─────────────────────────────────────────────────────┤
│           Constraint Engine                         │
│  (Unified Constraints, Logical Operators)          │
├─────────────────────────────────────────────────────┤
│            Query Optimizer                          │
│  (Batching, Caching, Query Combination)           │
├─────────────────────────────────────────────────────┤
│           Execution Engine                          │
│  (DataFusion SQL Engine, Arrow Processing)        │
├─────────────────────────────────────────────────────┤
│            Data Sources                             │
│  (Files, Cloud Storage, Databases)                │
└─────────────────────────────────────────────────────┘
```

## Core Components

### 1. Validation Engine Architecture

The validation engine is the heart of Term, orchestrating the entire validation process:

**ValidationSuite**: The top-level container that groups related checks
- Manages execution context and configuration
- Coordinates parallel check execution
- Aggregates results into a unified report

**Check**: Groups related constraints that share a common purpose
- Provides a fluent builder API for constraint configuration
- Manages constraint dependencies and execution order
- Associates metadata (name, level) with validation results

**Constraint**: The atomic unit of validation logic
- Implements the `Constraint` trait with async `evaluate()` method
- Generates optimized SQL queries for data validation
- Returns typed results with success/failure status and metrics

### 2. Constraint Evaluation Pipeline

Term's constraint evaluation follows a sophisticated pipeline:

```
User Input → Parse → Analyze → Optimize → Execute → Aggregate → Report
```

1. **Parse**: Convert builder API calls into constraint objects
2. **Analyze**: Identify constraint dependencies and optimization opportunities
3. **Optimize**: Batch similar constraints, cache shared computations
4. **Execute**: Run optimized queries on DataFusion engine
5. **Aggregate**: Combine individual constraint results
6. **Report**: Format results for user consumption

### 3. Query Optimization Strategy

Term employs several optimization strategies to maximize performance:

**Query Batching**: Constraints that scan the same columns are combined into single queries
```rust
// These constraints are batched into one query:
.is_complete("user_id")
.is_unique("user_id")
.has_min("user_id", 1)
```

**Statistics Caching**: Common statistics (count, min, max, mean) are computed once and reused
```rust
// Statistics computed once, used by multiple constraints:
.has_mean("price", Between(10.0, 100.0))
.has_std_dev("price", LessThan(50.0))
.has_sum("price", GreaterThan(1000.0))
```

**Predicate Pushdown**: Filters are pushed down to the data source level when possible

**Column Pruning**: Only required columns are loaded into memory

### 4. Result Aggregation

Results are aggregated hierarchically:

```
ValidationResult
├── Check Results
│   ├── Constraint Results
│   │   ├── Success/Failure Status
│   │   ├── Metrics (rows checked, failures found)
│   │   └── Error Details
│   └── Check-level Summary
└── Suite-level Summary
```

## Design Decisions

### Why DataFusion over Spark?

We chose Apache DataFusion as our query engine for several reasons:

1. **Zero Infrastructure**: DataFusion is an embedded engine - no cluster setup required
2. **Native Rust**: Seamless integration with Rust's safety and performance guarantees
3. **Columnar Processing**: Built on Apache Arrow for efficient analytical queries
4. **Lower Overhead**: No JVM, no serialization overhead, direct memory access
5. **Single-Node Optimization**: Optimized for single-node performance vs distributed overhead

For datasets that fit on a single machine (up to ~100GB), DataFusion often outperforms Spark due to lower coordination overhead.

### Async-First Design

Term is built on async Rust using Tokio:

```rust
// All constraint evaluation is async
async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult>
```

Benefits:
- **I/O Efficiency**: Non-blocking file and network operations
- **Concurrent Execution**: Multiple constraints evaluated in parallel
- **Resource Efficiency**: Thousands of concurrent validations with low memory overhead
- **Integration**: Works naturally with async data sources and services

### Builder Pattern Usage

We use the builder pattern extensively for ergonomic APIs:

```rust
ValidationSuite::builder("name")
    .with_check(
        Check::builder("check")
            .is_complete("col")
            .is_unique("col")
            .build()
    )
    .build()
```

Benefits:
- **Discoverability**: IDE autocomplete guides users
- **Type Safety**: Compile-time validation of configuration
- **Flexibility**: Optional parameters without combinatorial explosion
- **Readability**: Chainable methods create self-documenting code

### Error Handling Philosophy

Term follows Rust's explicit error handling:

1. **No Panics**: All errors are returned as `Result<T, TermError>`
2. **Detailed Context**: Errors include constraint name, column, and failure details
3. **Actionable Messages**: Error messages guide users toward solutions
4. **Error Categories**: Errors are categorized (Data, Configuration, IO, etc.)

## Performance Architecture

### Columnar Processing with Arrow

Term leverages Apache Arrow's columnar format:

```
Row Format (Traditional):     Columnar Format (Arrow):
┌──────┬──────┬──────┐      ┌──────────────┐
│ ID=1 │Name=A│Val=10│      │ ID: [1,2,3]  │
├──────┼──────┼──────┤      ├──────────────┤
│ ID=2 │Name=B│Val=20│      │Name:[A,B,C]  │
├──────┼──────┼──────┤      ├──────────────┤
│ ID=3 │Name=C│Val=30│      │Val:[10,20,30]│
└──────┴──────┴──────┘      └──────────────┘
```

Benefits:
- **Cache Efficiency**: Better CPU cache utilization for analytical queries
- **Vectorization**: SIMD operations on entire columns
- **Compression**: Better compression ratios for similar data
- **Zero-Copy**: Data can be shared between processes without copying

### Query Batching and Optimization

The optimizer identifies opportunities to combine queries:

```sql
-- Before optimization (3 queries):
SELECT COUNT(*) FROM data WHERE col1 IS NOT NULL;
SELECT COUNT(DISTINCT col1) FROM data;
SELECT MIN(col1), MAX(col1) FROM data;

-- After optimization (1 query):
SELECT 
    COUNT(*) as total,
    COUNT(col1) as non_null,
    COUNT(DISTINCT col1) as unique,
    MIN(col1) as min,
    MAX(col1) as max
FROM data;
```

### Memory Management

Term employs several strategies for efficient memory use:

1. **Streaming**: Large files are processed in chunks
2. **Spilling**: Intermediate results can spill to disk
3. **Reference Counting**: Arrow arrays use reference counting for zero-copy
4. **Memory Pools**: DataFusion manages memory pools for query execution

### Parallel Execution

Parallelism happens at multiple levels:

1. **Check Level**: Independent checks run concurrently
2. **Constraint Level**: Non-dependent constraints within a check run in parallel
3. **Query Level**: DataFusion parallelizes query execution across CPU cores
4. **I/O Level**: Async I/O allows concurrent data loading

## Extensibility

### Plugin Architecture

Term is designed for extensibility:

```rust
// Custom constraint implementation
pub struct MyConstraint { ... }

#[async_trait]
impl Constraint for MyConstraint {
    async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult> {
        // Custom validation logic
    }
}
```

### Custom Constraints

Creating custom constraints involves:

1. Implementing the `Constraint` trait
2. Defining evaluation logic
3. Returning appropriate results
4. Optionally implementing optimization hints

### Data Source Abstraction

New data sources can be added by implementing DataFusion's `TableProvider`:

```rust
pub trait TableProvider: Sync + Send {
    fn schema(&self) -> SchemaRef;
    async fn scan(...) -> Result<Arc<dyn ExecutionPlan>>;
}
```

### Result Formatters

Results can be formatted in various ways:

```rust
// Built-in formatters
result.to_json()?;
result.to_human_readable()?;
result.to_junit_xml()?;

// Custom formatter
impl ResultFormatter for MyFormatter {
    fn format(&self, result: &ValidationResult) -> String {
        // Custom formatting logic
    }
}
```

## Integration Points

### OpenTelemetry Integration

Term integrates with OpenTelemetry for observability:

```rust
// Automatic tracing
#[instrument]
async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult>

// Metrics collection
meter.u64_counter("term.constraints.evaluated").add(1);
meter.f64_histogram("term.constraint.duration").record(duration);
```

### Error Propagation

Errors flow through the system with context:

```
DataFusion Error
    → TermError::QueryExecution
        → ConstraintResult::Error
            → CheckResult with context
                → ValidationResult with full trace
```

## Future Architecture Considerations

### Distributed Execution

While Term currently focuses on single-node performance, the architecture allows for future distributed execution:

- Constraints could be distributed across nodes
- DataFusion's query plans are serializable
- Results aggregation already handles partial results

### Streaming Support

The architecture supports future streaming validations:

- Constraints can operate on bounded windows
- Results can be incrementally updated
- State management for streaming aggregations

### GPU Acceleration

Potential for GPU acceleration of certain operations:

- Regex matching on GPUs
- Statistical computations
- Large-scale sorting and grouping

## Conclusion

Term's architecture prioritizes:

1. **Performance**: Through intelligent optimization and efficient execution
2. **Usability**: Through intuitive APIs and clear error messages
3. **Extensibility**: Through well-defined interfaces and plugin points
4. **Reliability**: Through Rust's safety guarantees and comprehensive testing

This architecture enables Term to provide enterprise-grade data validation without the complexity of distributed systems, making it accessible to teams of all sizes.