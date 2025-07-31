# AnalysisRunner API Reference

<!-- 
This is a REFERENCE document following Diátaxis principles.
It provides complete technical information without tutorials or explanations.
-->

## Overview

The `AnalysisRunner` orchestrates the execution of multiple analyzers on a dataset.

**Module:** `term_guard::analyzers::runner`

## Struct: AnalysisRunner

```rust
pub struct AnalysisRunner
```

Orchestrates the execution of multiple analyzers on a dataset, providing progress tracking and error handling.

### Methods

#### new

```rust
pub fn new() -> Self
```

Creates a new empty `AnalysisRunner`.

**Returns:** A new `AnalysisRunner` instance.

#### add

```rust
pub fn add<A>(self, analyzer: A) -> Self
where
    A: Analyzer + 'static,
    A::Metric: Into<MetricValue> + 'static,
```

Adds an analyzer to the runner.

**Parameters:**
- `analyzer`: The analyzer to add

**Returns:** Self for method chaining

**Example:**
```rust
let runner = AnalysisRunner::new()
    .add(SizeAnalyzer::new())
    .add(CompletenessAnalyzer::new("column_name"));
```

#### on_progress

```rust
pub fn on_progress<F>(self, callback: F) -> Self
where
    F: Fn(f64) + Send + Sync + 'static,
```

Sets a progress callback that will be called during execution.

**Parameters:**
- `callback`: Function that receives progress as a float between 0.0 and 1.0

**Returns:** Self for method chaining

**Example:**
```rust
let runner = runner.on_progress(|progress| {
    println!("Progress: {:.1}%", progress * 100.0);
});
```

#### continue_on_error

```rust
pub fn continue_on_error(self, continue_on_error: bool) -> Self
```

Sets whether to continue execution when individual analyzers fail.

**Parameters:**
- `continue_on_error`: If true, continues execution on analyzer failure. If false, stops on first error.

**Default:** `true`

**Returns:** Self for method chaining

#### run

```rust
pub async fn run(&self, ctx: &SessionContext) -> AnalyzerResult<AnalyzerContext>
```

Executes all analyzers on the given data context.

**Parameters:**
- `ctx`: The DataFusion session context with registered data table named "data"

**Returns:** `Result<AnalyzerContext>` containing all computed metrics and any errors

**Errors:**
- `AnalyzerError::execution` if `continue_on_error` is false and an analyzer fails

#### analyzer_count

```rust
pub fn analyzer_count(&self) -> usize
```

Returns the number of analyzers configured.

**Returns:** Number of analyzers

## Type Aliases

### ProgressCallback

```rust
pub type ProgressCallback = Arc<dyn Fn(f64) + Send + Sync>;
```

Type alias for progress callback function.

### AnalyzerExecution

```rust
pub type AnalyzerExecution = Box<dyn Fn(&SessionContext) -> BoxFuture<'_, AnalyzerResult<(String, MetricValue)>> + Send + Sync>;
```

Type alias for a boxed analyzer execution function.

## Usage Patterns

### Basic Usage

```rust
use term_guard::analyzers::{AnalysisRunner, basic::*};
use datafusion::prelude::*;

let ctx = SessionContext::new();
// ... register data table ...

let runner = AnalysisRunner::new()
    .add(SizeAnalyzer::new())
    .add(CompletenessAnalyzer::new("user_id"));

let context = runner.run(&ctx).await?;
```

### With Progress Tracking

```rust
let runner = AnalysisRunner::new()
    .add(SizeAnalyzer::new())
    .add(CompletenessAnalyzer::new("user_id"))
    .add(MeanAnalyzer::new("amount"))
    .on_progress(|p| eprintln!("Progress: {:.0}%", p * 100.0));
```

### With Error Handling

```rust
let runner = AnalysisRunner::new()
    .add(SizeAnalyzer::new())
    .add(CompletenessAnalyzer::new("invalid_column"))
    .continue_on_error(true); // Will not fail despite invalid column

let context = runner.run(&ctx).await?;
if context.has_errors() {
    for error in context.errors() {
        eprintln!("Analyzer {} failed: {}", error.analyzer_name, error.error);
    }
}
```

### Fail-Fast Mode

```rust
let runner = AnalysisRunner::new()
    .add(SizeAnalyzer::new())
    .add(CompletenessAnalyzer::new("invalid_column"))
    .continue_on_error(false); // Will fail immediately

match runner.run(&ctx).await {
    Ok(context) => { /* all analyzers succeeded */ },
    Err(e) => { /* first analyzer failed */ }
}
```

## Performance Characteristics

- **Execution Model**: Sequential execution of analyzers
- **Memory Usage**: O(n) where n is the number of analyzers
- **Thread Safety**: The runner itself is Send + Sync

## Limitations

- Requires data table to be named "data"
- Currently executes analyzers sequentially (no automatic grouping)
- Progress callbacks are called from async context

## See Also

- [`Analyzer`](analyzers.md#trait-analyzer) - The analyzer trait
- [`AnalyzerContext`](analyzer-context.md) - Result storage
- [`MetricValue`](metric-value.md) - Metric value types