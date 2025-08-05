# How to Connect to PostgreSQL

<!-- 
This is a HOW-TO GUIDE following Diátaxis principles.
Task-oriented guide for PostgreSQL connections.
-->

## Goal

Connect Term to a PostgreSQL database and run validation checks on your tables.

## Prerequisites

Before you begin, ensure you have:
- [ ] Term v0.0.1 or later with PostgreSQL feature: `term-guard = { features = ["postgres"] }`
- [ ] PostgreSQL server running and accessible
- [ ] Database credentials (host, port, database name, username, password)
- [ ] Basic understanding of Term validation from tutorials

## Quick Solution

```rust
use term_guard::sources::{DatabaseConfig, PostgresSource};
use term_guard::prelude::*;
use datafusion::prelude::SessionContext;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Method 1: Using PostgresSource (recommended)
    let source = PostgresSource::new(
        "localhost",
        5432,
        "mydb",
        "username",
        "password",
        "my_table"
    )?;
    
    let ctx = SessionContext::new();
    source.register(&ctx, "my_table").await?;
    
    let suite = ValidationSuite::builder("postgres_validation")
        .table_name("my_table")
        .add_check(Check::new("Data quality").is_complete("id"))
        .build();
    
    let result = suite.run(&ctx).await?;
    println!("Validation success: {}", result.is_success());
    
    Ok(())
}
```

## Step-by-Step Guide

### Step 1: Add PostgreSQL Dependencies

Add Term with PostgreSQL support to your `Cargo.toml`:

```toml
[dependencies]
term-guard = { version = "0.0.1", features = ["postgres"] }
datafusion = "48.0"
tokio = { version = "1.0", features = ["full"] }
```

### Step 2: Choose Your Connection Method

Term provides two ways to connect to PostgreSQL:

**Option A: PostgresSource (Recommended for simple cases)**
```rust
use term_guard::sources::PostgresSource;

let source = PostgresSource::new(
    "localhost",    // host
    5432,          // port
    "mydb",        // database
    "user",        // username
    "password",    // password
    "customers"    // table name
)?;
```

**Option B: DatabaseSource (More flexible)**
```rust
use term_guard::sources::{DatabaseConfig, DatabaseSource};
use term_guard::security::SecureString;

let config = DatabaseConfig::PostgreSQL {
    host: "localhost".to_string(),
    port: 5432,
    database: "mydb".to_string(),
    username: "user".to_string(),
    password: SecureString::new("password"),
    sslmode: Some("require".to_string()),
};
let source = DatabaseSource::new(config, "customers")?;
```

### Step 3: Register and Validate

```rust
use datafusion::prelude::SessionContext;
use term_guard::prelude::*;

let ctx = SessionContext::new();
source.register(&ctx, "customers").await?;

let suite = ValidationSuite::builder("customer_validation")
    .table_name("customers")
    .add_check(
        Check::new("Customer data quality")
            .is_complete("customer_id")
            .has_completeness("email", 0.9)
            .is_positive("age")
    )
    .build();

let result = suite.run(&ctx).await?;
```

## Complete Example

Here's a complete example with error handling and SSL:

```rust
use std::error::Error;
use term_guard::sources::PostgresSource;
use term_guard::prelude::*;
use datafusion::prelude::SessionContext;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Connect with SSL
    let source = PostgresSource::new_with_ssl(
        "localhost",
        5432,
        "production_db",
        "app_user",
        "secure_password",
        "customer_profiles",
        "require"  // SSL mode
    )?;
    
    let ctx = SessionContext::new();
    
    // Register table
    match source.register(&ctx, "customer_profiles").await {
        Ok(_) => println!("✅ Connected to PostgreSQL successfully"),
        Err(e) => {
            eprintln!("❌ Failed to connect: {}", e);
            return Err(e);
        }
    }
    
    // Run comprehensive validation
    let suite = ValidationSuite::builder("production_customer_validation")
        .table_name("customer_profiles")
        .add_check(
            Check::new("Identity validation")
                .is_complete("customer_id")
                .is_unique("customer_id")
                .is_complete("email")
                .is_unique("email")
        )
        .add_check(
            Check::new("Data quality validation")
                .has_completeness("phone_number", 0.8)
                .has_completeness("address", 0.9)
                .is_non_negative("account_balance")
        )
        .build();
    
    let result = suite.run(&ctx).await?;
    
    // Report results
    if result.is_success() {
        println!("🎉 All {} validation checks passed!", result.total_constraints());
    } else {
        println!("⚠️  {} of {} checks failed", 
                result.total_constraints() - result.passed_constraints(),
                result.total_constraints());
        
        // Show failed constraints
        for check_result in result.check_results() {
            for constraint_result in check_result.constraint_results() {
                if !constraint_result.is_success() {
                    println!("  ❌ {}", constraint_result.name());
                }
            }
        }
    }
    
    Ok(())
}
```

## Variations

### Connecting with Environment Variables

```rust
use std::env;

let source = PostgresSource::new(
    env::var("POSTGRES_HOST").unwrap_or_else(|_| "localhost".to_string()),
    env::var("POSTGRES_PORT").unwrap_or_else(|_| "5432".to_string()).parse()?,
    env::var("POSTGRES_DB")?,
    env::var("POSTGRES_USER")?,
    env::var("POSTGRES_PASSWORD")?,
    "my_table"
)?;
```

### Connection Pooling Configuration

```rust
use term_guard::sources::{DatabaseConfig, DatabaseSource};

// DatabaseSource automatically handles connection pooling
let config = DatabaseConfig::PostgreSQL {
    host: "localhost".to_string(),
    port: 5432,
    database: "mydb".to_string(),
    username: "user".to_string(),
    password: SecureString::new("password"),
    sslmode: Some("prefer".to_string()),
};
let source = DatabaseSource::new(config, "large_table")?;

// The connection pool is managed automatically
// No additional configuration needed for basic use cases
```

### Multiple Tables from Same Database

```rust
// Connect once, register multiple tables
let ctx = SessionContext::new();

let customers_source = PostgresSource::new("localhost", 5432, "mydb", "user", "pass", "customers")?;
let orders_source = PostgresSource::new("localhost", 5432, "mydb", "user", "pass", "orders")?;

customers_source.register(&ctx, "customers").await?;
orders_source.register(&ctx, "orders").await?;

// Validate both tables
let customer_suite = ValidationSuite::builder("customer_validation")
    .table_name("customers")
    .add_check(Check::new("Customer checks").is_complete("id"))
    .build();

let order_suite = ValidationSuite::builder("order_validation")
    .table_name("orders")
    .add_check(Check::new("Order checks").is_complete("order_id"))
    .build();
```

## Verification

To verify that your PostgreSQL connection worked correctly:

1. Check that registration succeeds without errors
2. Run a simple query to confirm data access: `ctx.sql("SELECT COUNT(*) FROM my_table").await?`
3. Ensure validation results make sense for your data

## Troubleshooting

### Problem: Connection refused or timeout
**Solution:** 
- Verify PostgreSQL server is running: `pg_isready -h localhost -p 5432`
- Check firewall settings and network connectivity
- Confirm host and port are correct

### Problem: Authentication failed
**Solution:**
- Verify username and password are correct
- Check if user has necessary permissions: `GRANT SELECT ON table_name TO username;`
- Ensure password is not expired

### Problem: SSL/TLS errors
**Solution:**
- For development, try `sslmode: Some("disable".to_string())`
- For production, ensure server has valid SSL certificate
- Use `"prefer"` or `"require"` SSL modes appropriately

### Problem: Table not found
**Solution:**
- Verify table exists: `\dt` in psql
- Check schema permissions: `GRANT USAGE ON SCHEMA schema_name TO username;`
- Use fully qualified table names if needed: `schema.table_name`

### Problem: Out of memory or connection pool exhausted
**Solution:**
- Term uses connection pooling automatically
- For very large datasets, see [How to Optimize Database Performance](optimize-database-performance.md)
- Monitor connection usage in PostgreSQL: `SELECT * FROM pg_stat_activity;`

## Performance Considerations

- **Connection Pooling**: Term automatically manages connection pools for PostgreSQL
- **Query Pushdown**: Validation queries are pushed down to PostgreSQL for better performance
- **Large Tables**: For tables with millions of rows, consider using sampling or chunked validation
- **Network Latency**: Place Term close to your PostgreSQL server to minimize network overhead

## Security Considerations

- **Use SSL**: Always use `sslmode: "require"` or higher in production
- **Least Privilege**: Create dedicated read-only users for Term validation
- **Password Management**: Use environment variables or secret management systems
- **Network Security**: Restrict PostgreSQL access to necessary IP addresses only

## Related Guides

- [How to Connect to MySQL](connect-mysql.md)
- [How to Secure Database Connections](secure-database-connections.md)
- [How to Optimize Database Validation Performance](optimize-database-performance.md)
- [Tutorial: Connect to Your First Database](../tutorials/05-database-connections.md) (for understanding)
- [Database Sources Reference](../reference/database-sources.md) (for API details)

---