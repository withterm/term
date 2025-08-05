# Understanding Dynamic Table Names

<!-- 
This is an EXPLANATION document following Diátaxis principles.
Understanding-oriented, providing context for ValidationContext design.
-->

## Introduction

Dynamic table names in Term solve a fundamental limitation in data validation systems: the assumption that all data lives in a table called "data". This assumption breaks down in real-world scenarios where you need to validate multiple tables, work with existing database schemas, or build reusable validation logic. ValidationContext provides the foundation for flexible, runtime-configurable table validation.

## Background

Traditional data validation tools emerged from the big data ecosystem where the common pattern was to load data into a generic table (often called "data" or "df") and then run validation checks. This pattern worked well for:
- Single-file validations (CSV, JSON, Parquet)
- ETL pipelines with consistent staging table names
- Analytical workflows with temporary datasets

However, this approach creates significant limitations when validating operational databases:
- **Schema conflicts**: Production tables rarely follow generic naming conventions
- **Multi-table scenarios**: Validating related tables requires separate validation runs
- **Reusability issues**: Validation logic becomes coupled to specific table names
- **Dynamic workflows**: Runtime table selection becomes impossible

Term's ValidationContext addresses these limitations by decoupling validation logic from table names, enabling truly flexible data validation.

## Core Concepts

### Table Name Abstraction

The fundamental insight behind ValidationContext is that validation logic should be independent of table names. Consider this constraint:

```sql
-- Hardcoded table name
SELECT COUNT(*) FROM data WHERE email IS NOT NULL
```

This constraint only works if your table is named "data". ValidationContext enables this instead:

```sql
-- Dynamic table name  
SELECT COUNT(*) FROM {table_name} WHERE email IS NOT NULL
```

Now the same constraint logic works with any table name, determined at runtime.

### Task-Local Context Storage

ValidationContext uses Tokio's task-local storage to provide context information without requiring changes to existing APIs. This is a sophisticated solution to a complex problem.

Consider the alternative approaches:
1. **Parameter passing**: Every constraint method would need table_name parameters
2. **Global state**: Thread-unsafe and conflict-prone
3. **Context objects**: Would require redesigning all existing constraint APIs

Task-local storage provides the benefits of parameter passing (correct isolation) without the API complexity, and the benefits of global state (easy access) without the safety issues.

```
Task 1: ValidationContext("customers")
├── Constraint evaluation uses "customers"
└── Nested operations inherit "customers"

Task 2: ValidationContext("orders")  
├── Constraint evaluation uses "orders"
└── Independent from Task 1
```

### Backward Compatibility

ValidationContext maintains perfect backward compatibility with existing Term code. When no context is explicitly set, constraints receive the default table name "data". This ensures that:
- Existing validation suites continue working unchanged
- Migration to dynamic table names is opt-in
- Learning curve for new users remains gentle

## How Dynamic Table Names Work in Term

The ValidationContext system operates through three key components:

### 1. Context Creation and Management

ValidationContext provides a simple API for creating and managing table name contexts:

```rust
// Create context for specific table
let ctx = ValidationContext::new("customer_profiles");

// Use default table name ("data")
let default_ctx = ValidationContext::default();

// Context is immutable once created
assert_eq!(ctx.table_name(), "customer_profiles");
```

### 2. Task-Local Storage Integration

Tokio's task-local storage provides isolation between concurrent validation operations:

```rust
// Each async task gets independent context
CURRENT_CONTEXT.scope(ctx, async {
    // All constraints in this scope use the specified table name
    let result = suite.run(&datafusion_ctx).await?;
}).await
```

This design enables:
- **Concurrent validations** on different tables without interference
- **Nested contexts** where inner contexts shadow outer ones
- **Automatic cleanup** when async tasks complete

### 3. Constraint Integration

Constraints access the current table name through a simple function call:

```rust
let validation_ctx = current_validation_context();
let table_name = validation_ctx.table_name();

// Use in SQL generation
let sql = format!("SELECT COUNT(*) FROM {} WHERE {}", table_name, column);
```

This integration is transparent to constraint implementers—they simply call a function to get the current table name.

## Design Decisions

### Why Task-Local Storage?

We chose task-local storage over several alternatives after careful consideration:

**Alternatives Considered:**
1. **Thread-local storage**: Doesn't work with async/await where tasks can move between threads
2. **Context parameters**: Would require changing every constraint API
3. **Global static variables**: Not thread-safe and would cause conflicts
4. **Arc<Mutex<Context>>**: Overhead and complexity for simple read-only access

**Task-local storage provides:**
- **Async-aware isolation**: Each async task has independent context
- **Zero API changes**: Existing constraint APIs remain unchanged  
- **Automatic inheritance**: Spawned tasks inherit parent context
- **Performance**: No locking or synchronization overhead

### Why Immutable Contexts?

ValidationContext instances are immutable after creation. This design prevents entire classes of bugs:

```rust
// This is not allowed - contexts are immutable
let mut ctx = ValidationContext::new("table1");
ctx.set_table_name("table2"); // ❌ Doesn't exist

// Instead, create new contexts
let ctx1 = ValidationContext::new("table1");
let ctx2 = ValidationContext::new("table2"); // ✅ Clear and safe
```

Immutability ensures that:
- Constraint evaluation is predictable and deterministic
- Concurrent access is always safe
- Context changes are explicit and visible

### Why Arc<str> for Table Names?

Table names use `Arc<str>` instead of `String` for efficiency:

```rust
pub struct ValidationContext {
    table_name: Arc<str>, // Reference-counted string
}
```

This choice provides:
- **Efficient cloning**: Cloning ValidationContext only increments a reference count
- **Memory sharing**: Multiple contexts with the same table name share memory
- **Immutability**: Arc<str> is inherently immutable

For validation workloads that process many tables, this can significantly reduce memory allocation overhead.

## Comparison with Other Systems

### Term's Approach vs Parameter Passing

**Parameter Passing Approach:**
```rust
// Every method needs table_name parameter
impl Constraint for MyConstraint {
    fn evaluate(&self, ctx: &SessionContext, table_name: &str) -> Result<ConstraintResult>
}

// Usage becomes verbose
constraint.evaluate(&ctx, "my_table")?;
```

**Term's Task-Local Approach:**
```rust
// Clean API - no parameter changes needed
impl Constraint for MyConstraint {
    fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult>
}

// Context is set once for multiple operations
CURRENT_CONTEXT.scope(validation_ctx, async {
    constraint.evaluate(&ctx)?; // Table name is automatically available
}).await
```

### Term vs Global State Systems

**Global State Approach:**
```rust
// Not thread-safe, causes conflicts
static mut CURRENT_TABLE: &str = "data";

// Race conditions in concurrent scenarios
set_current_table("table1");
constraint1.evaluate(); // Might use "table1" or "table2"
set_current_table("table2");  
constraint2.evaluate();
```

**Term's Task-Local Approach:**
```rust
// Each task has independent context
let task1 = tokio::spawn(async {
    let ctx = ValidationContext::new("table1");
    CURRENT_CONTEXT.scope(ctx, async {
        // Always uses "table1" - no race conditions
        constraint.evaluate()
    }).await
});

let task2 = tokio::spawn(async {
    let ctx = ValidationContext::new("table2"); 
    CURRENT_CONTEXT.scope(ctx, async {
        // Always uses "table2" - independent from task1
        constraint.evaluate()
    }).await
});
```

## Benefits and Trade-offs

### Benefits

1. **Flexibility**: Validate any table without hardcoded names
2. **Reusability**: Validation logic works across different table schemas
3. **Concurrency**: Safe concurrent validation of multiple tables
4. **Backward Compatibility**: Existing code continues working unchanged
5. **Performance**: Efficient context management with minimal overhead

### Trade-offs

1. **Complexity**: Additional abstraction layer that developers must understand
2. **Debugging**: Context inheritance can sometimes make debugging more complex
3. **Task-Local Limitations**: Context doesn't cross task boundaries unless explicitly handled

The trade-offs are minimal because the complexity is largely hidden from daily usage, while the benefits enable powerful validation scenarios that would otherwise be impossible.

## Common Misconceptions

### Misconception: ValidationContext Is Only for Multi-Table Scenarios

**Reality**: ValidationContext improves single-table scenarios too by eliminating table naming constraints

Even if you only validate one table, ValidationContext provides value:
- No need to rename tables to "data"
- Validation logic matches your actual schema
- Future flexibility if you need to validate additional tables

### Misconception: Task-Local Storage Is Complex and Error-Prone

**Reality**: Task-local storage is simpler and safer than alternatives

The task-local pattern might seem complex initially, but it's actually simpler than alternatives:
- No manual parameter passing through multiple layers
- No risk of global state conflicts
- Automatic cleanup when tasks complete
- Built-in isolation between concurrent operations

## Real-World Implications

Understanding ValidationContext helps you:

### Design Better Validation Architecture

- **Create reusable validation suites** that work across different environments (dev/staging/prod)
- **Build table-agnostic constraints** that can be applied to similar schemas
- **Implement dynamic validation workflows** where table names are determined at runtime

### Optimize Performance

- **Batch validations efficiently** by reusing ValidationContext across multiple constraints
- **Minimize context switching overhead** by grouping operations within context scopes
- **Leverage async concurrency** to validate multiple tables simultaneously

### Avoid Common Pitfalls

- **Context inheritance**: Understand how spawned tasks inherit parent contexts
- **Scope boundaries**: Ensure context scopes cover all related validation operations
- **Default fallback**: Remember that constraints use "data" as default when no context is set

## Advanced Topics

### Context Inheritance Patterns

Understanding how contexts inherit across task boundaries:

```rust
// Parent context
let parent_ctx = ValidationContext::new("parent_table");
CURRENT_CONTEXT.scope(parent_ctx, async {
    // Direct operations use parent context
    constraint.evaluate(&ctx)?; // Uses "parent_table"
    
    // Spawned tasks inherit parent context
    let handle = tokio::spawn(async {
        constraint.evaluate(&ctx) // Also uses "parent_table"
    });
    
    // But can be overridden in child tasks
    let override_handle = tokio::spawn(async {
        let child_ctx = ValidationContext::new("child_table");
        CURRENT_CONTEXT.scope(child_ctx, async {
            constraint.evaluate(&ctx) // Uses "child_table"
        }).await
    });
}).await
```

### Integration with ValidationSuite

ValidationSuite provides a high-level API that automatically manages ValidationContext:

```rust
// ValidationSuite handles context management
let suite = ValidationSuite::builder("my_validation")
    .table_name("actual_table_name")  // Sets context automatically
    .add_check(Check::new("checks").is_complete("id"))
    .build();

// All constraints in the suite use "actual_table_name"
let result = suite.run(&ctx).await?;
```

This integration provides the best of both worlds:
- High-level convenience for common scenarios
- Low-level control when needed for advanced use cases

## Summary

Dynamic table names through ValidationContext represent Term's commitment to real-world flexibility over academic purity. By decoupling validation logic from table naming assumptions, Term enables validation workflows that scale from simple single-table checks to complex multi-database validation orchestration.

The essential understanding is: **Validation logic should be independent of table naming conventions.**

## Further Reading

### Internal Documentation
- [Tutorial: Working with Multiple Databases](../tutorials/06-multi-database-validation.md)
- [How to Validate Data Across Multiple Tables](../how-to/validate-multiple-tables.md)
- [ValidationContext Reference](../reference/validation-context.md)

### External Resources
- [Tokio Task-Local Storage Documentation](https://docs.rs/tokio/latest/tokio/task_local/index.html)
- [Rust Arc and Reference Counting](https://doc.rust-lang.org/std/sync/struct.Arc.html)
- [Database Schema Design Best Practices](https://www.postgresql.org/docs/current/ddl-schemas.html)

---