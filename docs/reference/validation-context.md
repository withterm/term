# ValidationContext Reference

<!-- 
This is a REFERENCE document following Diátaxis principles.
Information-oriented, comprehensive technical description.
-->

## Overview

ValidationContext provides runtime information to constraints during evaluation, enabling dynamic table name specification and future extensibility for validation parameters.

## Synopsis

```rust
use term_guard::core::{ValidationContext, validation_context::CURRENT_CONTEXT};

// Create context
let ctx = ValidationContext::new("my_table");

// Use with task-local storage
CURRENT_CONTEXT.scope(ctx, async {
    // Constraints can access the context here
    let result = suite.run(&datafusion_ctx).await?;
}).await?;

// Get current context
let current = current_validation_context();
```

## Description

ValidationContext enables constraints to work with any table name rather than being hardcoded to "data". It uses Tokio's task-local storage to provide context information to constraints without requiring changes to the Constraint trait interface.

## API Reference

### Types

#### `ValidationContext`

```rust
pub struct ValidationContext {
    table_name: Arc<str>,
}
```

Runtime context for validation operations containing information that constraints need during evaluation.

##### Fields

- **`table_name`**: `Arc<str>` - The name of the table being validated (reference-counted for efficient cloning)

##### Example

```rust
let ctx = ValidationContext::new("customer_data");
assert_eq!(ctx.table_name(), "customer_data");
```

### Functions

#### `ValidationContext::new`

```rust
pub fn new(table_name: impl Into<Arc<str>>) -> Self
```

Creates a new validation context with the specified table name.

##### Parameters

- **`table_name`**: `impl Into<Arc<str>>` - The name of the table to validate

##### Returns

- `ValidationContext` - New validation context instance

##### Example

```rust
let ctx = ValidationContext::new("orders");
let ctx2 = ValidationContext::new(String::from("products"));
let ctx3 = ValidationContext::new(Arc::from("users"));
```

#### `ValidationContext::default`

```rust
pub fn default() -> Self
```

Creates a validation context for the default table name "data".

This is provided for backward compatibility with existing code that expects the table to be named "data".

##### Returns

- `ValidationContext` - Validation context with table name "data"

##### Example

```rust
let ctx = ValidationContext::default();
assert_eq!(ctx.table_name(), "data");
```

#### `ValidationContext::table_name`

```rust
pub fn table_name(&self) -> &str
```

Returns the name of the table being validated.

##### Returns

- `&str` - The table name

##### Example

```rust
let ctx = ValidationContext::new("my_table");
assert_eq!(ctx.table_name(), "my_table");
```

#### `current_validation_context`

```rust
pub fn current_validation_context() -> ValidationContext
```

Gets the current validation context from task-local storage.

Returns the default context (table name "data") if no context has been set in the current task.

##### Returns

- `ValidationContext` - The current validation context or default context

##### Example

```rust
// Outside of any context scope
let ctx = current_validation_context();
assert_eq!(ctx.table_name(), "data");

// Inside a context scope
let custom_ctx = ValidationContext::new("custom_table");
CURRENT_CONTEXT.scope(custom_ctx, async {
    let ctx = current_validation_context();
    assert_eq!(ctx.table_name(), "custom_table");
}).await;
```

### Task-Local Storage

#### `CURRENT_CONTEXT`

```rust
tokio::task_local! {
    pub static CURRENT_CONTEXT: ValidationContext;
}
```

Task-local storage for the current validation context.

This allows constraints to access the validation context without requiring changes to the Constraint trait interface. Each async task has its own independent context.

##### Methods

- **`scope`** - Execute a future with a specific validation context
- **`try_with`** - Attempt to access the current context (returns `Err` if not set)
- **`with`** - Access the current context (panics if not set)

##### Example

```rust
use term_guard::core::validation_context::CURRENT_CONTEXT;

// Set context for an async operation
let ctx = ValidationContext::new("my_table");
let result = CURRENT_CONTEXT.scope(ctx, async {
    // All validation operations in this scope use "my_table"
    suite.run(&datafusion_ctx).await
}).await?;

// Nested contexts
let outer_ctx = ValidationContext::new("outer_table");
CURRENT_CONTEXT.scope(outer_ctx, async {
    assert_eq!(current_validation_context().table_name(), "outer_table");
    
    let inner_ctx = ValidationContext::new("inner_table");
    CURRENT_CONTEXT.scope(inner_ctx, async {
        assert_eq!(current_validation_context().table_name(), "inner_table");
    }).await;
    
    // Back to outer context
    assert_eq!(current_validation_context().table_name(), "outer_table");
}).await;
```

### Traits

#### `Default`

```rust
impl Default for ValidationContext {
    fn default() -> Self
```

Standard trait implementation that creates a context with table name "data".

##### Example

```rust
let ctx: ValidationContext = Default::default();
assert_eq!(ctx.table_name(), "data");
```

#### `Debug`

```rust
impl Debug for ValidationContext
```

Debug formatting for ValidationContext showing the table name.

##### Example

```rust
let ctx = ValidationContext::new("test_table");
println!("{:?}", ctx); // ValidationContext { table_name: "test_table" }
```

#### `Clone`

```rust
impl Clone for ValidationContext
```

Efficient cloning using Arc for the table name.

##### Example

```rust
let ctx1 = ValidationContext::new("shared_table");
let ctx2 = ctx1.clone(); // Efficient - shares the Arc<str>
assert_eq!(ctx1.table_name(), ctx2.table_name());
```

## Behavior

### Task-Local Isolation

Each async task has its own independent ValidationContext:

```rust
// Task 1
tokio::spawn(async {
    let ctx = ValidationContext::new("task1_table");
    CURRENT_CONTEXT.scope(ctx, async {
        // This context only affects this task
        validate_data().await
    }).await
});

// Task 2 (independent)
tokio::spawn(async {
    let ctx = ValidationContext::new("task2_table");
    CURRENT_CONTEXT.scope(ctx, async {
        // This context is separate from task 1
        validate_data().await
    }).await
});
```

### Context Inheritance

Child tasks inherit the parent's context:

```rust
let parent_ctx = ValidationContext::new("parent_table");
CURRENT_CONTEXT.scope(parent_ctx, async {
    // Spawn a child task
    let handle = tokio::spawn(async {
        // This task inherits "parent_table" context
        current_validation_context().table_name()
    });
    
    let table_name = handle.await?;
    assert_eq!(table_name, "parent_table");
}).await;
```

### Context Nesting

Contexts can be nested with inner contexts shadowing outer ones:

```rust
let outer = ValidationContext::new("outer");
CURRENT_CONTEXT.scope(outer, async {
    let inner = ValidationContext::new("inner");
    CURRENT_CONTEXT.scope(inner, async {
        // Inner context shadows outer
        assert_eq!(current_validation_context().table_name(), "inner");
    }).await;
    
    // Back to outer context
    assert_eq!(current_validation_context().table_name(), "outer");
}).await;
```

### Default Fallback

When no context is set, the default context is returned:

```rust
// No context set
let ctx = current_validation_context();
assert_eq!(ctx.table_name(), "data");

// Even after a context scope ends
let custom_ctx = ValidationContext::new("custom");
CURRENT_CONTEXT.scope(custom_ctx, async {
    assert_eq!(current_validation_context().table_name(), "custom");
}).await;

// Back to default
let ctx = current_validation_context();
assert_eq!(ctx.table_name(), "data");
```

## Performance Characteristics

- **Memory**: O(1) - Uses Arc<str> for efficient string sharing
- **Context Access**: O(1) - Task-local storage provides constant-time access
- **Context Creation**: O(1) - Minimal allocation overhead
- **Cloning**: O(1) - Arc-based cloning is reference counting only

## Thread Safety

ValidationContext is thread-safe:
- **Immutable**: Once created, contexts cannot be modified
- **Arc-based**: Table names use Arc<str> for safe sharing
- **Task-local**: Each task has independent context storage

## Version History

| Version | Changes |
|---------|---------|
| 0.0.1   | Initial implementation with table name support |
| 0.0.2   | Added task-local storage integration |
| 0.0.3   | Added context inheritance for spawned tasks |

## Examples

### Basic Example

```rust
use term_guard::core::ValidationContext;

let ctx = ValidationContext::new("my_table");
assert_eq!(ctx.table_name(), "my_table");

// Clone is efficient
let ctx2 = ctx.clone();
assert_eq!(ctx2.table_name(), "my_table");
```

### ValidationSuite Integration

```rust
use term_guard::core::{ValidationContext, validation_context::CURRENT_CONTEXT};
use term_guard::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = ValidationContext::new("customer_orders");
    
    let result = CURRENT_CONTEXT.scope(ctx, async {
        let suite = ValidationSuite::builder("order_validation")
            .table_name("customer_orders")
            .add_check(Check::new("Order checks").is_complete("order_id"))
            .build();
        
        suite.run(&datafusion_ctx).await
    }).await?;
    
    Ok(())
}
```

### Multiple Tables with Context Switching

```rust
use term_guard::core::{ValidationContext, validation_context::CURRENT_CONTEXT};

async fn validate_multiple_tables(ctx: &SessionContext) -> Result<()> {
    let tables = vec!["customers", "orders", "products"];
    
    for table_name in tables {
        let validation_ctx = ValidationContext::new(table_name);
        
        let result = CURRENT_CONTEXT.scope(validation_ctx, async {
            let suite = ValidationSuite::builder(&format!("{}_validation", table_name))
                .table_name(table_name)
                .add_check(Check::new("Basic validation").is_complete("id"))
                .build();
            
            suite.run(ctx).await
        }).await?;
        
        println!("{}: {}", table_name, if result.is_success() { "✅" } else { "❌" });
    }
    
    Ok(())
}
```

### Context in Custom Constraints

```rust
use term_guard::core::{ValidationContext, validation_context::current_validation_context};
use term_guard::prelude::*;
use async_trait::async_trait;

struct CustomConstraint {
    column: String,
}

#[async_trait]
impl Constraint for CustomConstraint {
    async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult> {
        // Access the current table name from context
        let validation_ctx = current_validation_context();
        let table_name = validation_ctx.table_name();
        
        // Use the dynamic table name in SQL
        let sql = format!(
            "SELECT COUNT(*) as count FROM {} WHERE {} IS NOT NULL",
            table_name, self.column
        );
        
        let df = ctx.sql(&sql).await?;
        // ... rest of constraint logic
        
        Ok(ConstraintResult::new("custom_check", true, None))
    }
}
```

## See Also

- [`DatabaseSource`](database-sources.md) - Database sources that work with ValidationContext
- [`ValidationSuite`](../reference/constraints.md#validationsuite) - Validation suite builder that accepts table names
- [How to Validate Data Across Multiple Tables](../how-to/validate-multiple-tables.md)
- [Understanding Dynamic Table Names](../explanation/dynamic-table-names.md)

---