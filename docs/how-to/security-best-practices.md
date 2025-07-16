# Security Best Practices for Term

This guide provides security best practices for developers using and contributing to the Term data validation library.

## For Library Users

### 1. Input Validation

**Always validate user inputs before passing to Term constraints:**

```rust
// ❌ Bad: Direct user input
let column = user_input.get("column").unwrap();
let constraint = CompletenessConstraint::new(column, 0.95)?;

// ✅ Good: Validate first
let column = user_input.get("column")
    .ok_or("Missing column")?;
    
// Additional application-level validation
if !is_allowed_column(column) {
    return Err("Column not allowed");
}

let constraint = CompletenessConstraint::new(column, 0.95)?;
```

### 2. Credential Management

**Use secure credential storage:**

```rust
// ❌ Bad: Hardcoded credentials
let source = PostgresSource::new(
    "localhost", 5432, "mydb", "user", "password123", "table"
)?;

// ✅ Good: Environment variables or secure vault
let password = std::env::var("DB_PASSWORD")
    .expect("DB_PASSWORD not set");
let source = PostgresSource::new(
    "localhost", 5432, "mydb", "user", password, "table"
)?;

// ✅ Better: Use managed identity when possible
let source = PostgresSource::from_managed_identity(
    "localhost", 5432, "mydb", "table"
).await?;
```

### 3. Error Handling

**Don't expose sensitive information in errors:**

```rust
// ❌ Bad: Exposing internal details
match validation_result {
    Err(e) => {
        eprintln!("Validation failed: {:?}", e);
        return Err(format!("Failed with: {}", e));
    }
    Ok(result) => result,
}

// ✅ Good: Sanitized error messages
match validation_result {
    Err(e) => {
        // Log full error internally
        tracing::error!("Validation error: {:?}", e);
        
        // Return generic error to user
        return Err("Validation failed. Check logs for details.");
    }
    Ok(result) => result,
}
```

### 4. Custom SQL Constraints

**Be extremely careful with custom SQL:**

```rust
// ❌ Bad: Dynamic SQL from user input
let sql = format!("price > {}", user_threshold);
let constraint = CustomSqlConstraint::new(sql, None)?;

// ✅ Good: Parameterized expressions
let constraint = CustomSqlConstraint::new(
    "price > 100 AND category IN ('A', 'B', 'C')",
    Some("Price and category validation")
)?;

// ✅ Better: Use built-in constraints when possible
let constraint = MinConstraint::new("price", Assertion::GreaterThan(100.0));
```

### 5. Logging and Monitoring

**Configure logging appropriately:**

```rust
// Production configuration
let log_config = LogConfig {
    level: tracing::Level::INFO,
    format: LogFormat::Json,
    field_truncation: Some(1000), // Prevent log injection
    mask_sensitive: true,         // Mask sensitive data
};

// Enable security monitoring
let telemetry = TermTelemetry::builder()
    .with_metrics()
    .with_traces()
    .with_security_events() // Track security-relevant events
    .build();
```

## For Library Contributors

### 1. SQL Generation

**Always escape identifiers and validate inputs:**

```rust
// ❌ Bad: Direct interpolation
let sql = format!("SELECT COUNT({}) FROM data", column);

// ✅ Good: Use security utilities
let column_id = SqlSecurity::escape_identifier(&column)?;
let sql = format!("SELECT COUNT({}) FROM data", column_id);
```

### 2. Adding New Constraints

**Follow the security pattern:**

```rust
impl MyConstraint {
    pub fn new(column: impl Into<String>, threshold: f64) -> Result<Self> {
        let column = column.into();
        
        // Validate identifier
        SqlSecurity::validate_identifier(&column)?;
        
        // Validate numeric inputs
        InputValidator::validate_threshold(threshold, "threshold")?;
        
        Ok(Self { column, threshold })
    }
}

#[async_trait]
impl Constraint for MyConstraint {
    async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult> {
        // Always escape identifiers in SQL
        let column_id = SqlSecurity::escape_identifier(&self.column)?;
        let sql = format!("SELECT ... FROM data WHERE {} ...", column_id);
        
        // Execute safely
        let df = ctx.sql(&sql).await?;
        // ...
    }
}
```

### 3. Error Messages

**Sanitize error messages:**

```rust
// ❌ Bad: Exposing SQL in errors
return Err(TermError::Internal(
    format!("Query failed: {}", sql)
));

// ✅ Good: Generic message with context
return Err(TermError::constraint_evaluation(
    "my_constraint",
    "Failed to evaluate constraint"
));
```

### 4. Testing Security

**Add security tests for new features:**

```rust
#[test]
fn test_sql_injection_prevention() {
    let malicious_inputs = vec![
        "col; DROP TABLE users",
        "col' OR '1'='1",
    ];
    
    for input in malicious_inputs {
        assert!(MyConstraint::new(input, 0.5).is_err());
    }
}

#[test]
fn test_invalid_inputs() {
    assert!(MyConstraint::new("valid_col", f64::NAN).is_err());
    assert!(MyConstraint::new("valid_col", f64::INFINITY).is_err());
}
```

### 5. Dependencies

**Keep dependencies minimal and updated:**

```toml
[dependencies]
# Pin versions for security
regex = "=1.10.2"
datafusion = "=48.0"

# Use security features
zeroize = { version = "1.8", features = ["derive"] }
```

## Security Checklist for PRs

Before submitting a PR:

- [ ] All user inputs are validated
- [ ] SQL identifiers are properly escaped
- [ ] No sensitive data in error messages
- [ ] No sensitive data in logs
- [ ] Security tests added for new features
- [ ] Dependencies reviewed and minimal
- [ ] Documentation updated for security considerations

## Reporting Security Issues

If you discover a security vulnerability:

1. **DO NOT** open a public GitHub issue
2. Email security details to: [security@term.dev]
3. Include:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Suggested fix (if any)

## Security Response Process

1. **Acknowledgment**: Within 48 hours
2. **Assessment**: Severity and impact analysis
3. **Fix Development**: Priority based on severity
4. **Disclosure**: Coordinated disclosure after fix
5. **Credit**: Security researchers acknowledged

## Additional Resources

- [OWASP Secure Coding Practices](https://owasp.org/www-project-secure-coding-practices-quick-reference-guide/)
- [Rust Security Guidelines](https://anssi-fr.github.io/rust-guide/)
- [SQL Injection Prevention Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/SQL_Injection_Prevention_Cheat_Sheet.html)

## Version History

- v0.1.0 - Initial security implementation
  - SQL injection prevention
  - Secure credential handling
  - Input validation framework
  - Security test suite