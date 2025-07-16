# GitHub Copilot Instructions for Term Project

## Security Requirements

### NEVER generate:
- Hardcoded credentials or API keys
- Direct SQL queries without parameterization
- Shell commands with user input
- Eval or dynamic code execution
- Weak random number generation
- Unvalidated user input handling

### ALWAYS:
- Use parameterized queries for database access
- Validate and sanitize all inputs
- Use cryptographically secure random functions
- Handle errors without exposing sensitive information
- Follow the principle of least privilege
- Use type-safe patterns

## Code Patterns

### Async/Await Pattern
All I/O operations must use async/await:
```rust
#[instrument]
pub async fn my_function() -> Result<T> {
    // Implementation
}
```

### Error Handling
Use the project's Result type:
```rust
use crate::Result;  // This is std::result::Result<T, TermError>
```

### Builder Pattern
Public APIs should use builder pattern:
```rust
impl MyStruct {
    pub fn builder() -> MyStructBuilder {
        MyStructBuilder::default()
    }
}
```

## Project-Specific Guidelines

1. **Constraints**: Implement in `src/constraints/` with `Constraint` trait
2. **Testing**: Use `create_tpc_h_context()` for integration tests
3. **Documentation**: Include examples in all public functions
4. **Performance**: Consider DataFusion memory pool configuration
5. **Tracing**: Add `#[instrument]` to public async functions

## Forbidden Patterns

1. **No raw SQL execution** - Use DataFusion's API
2. **No unwrap() in production code** - Use proper error handling
3. **No blocking I/O** - Everything must be async
4. **No mutable globals** - Use proper state management
5. **No println!** - Use tracing macros instead
