# How to Connect to MySQL

<!-- 
This is a HOW-TO GUIDE following Diátaxis principles.
Task-oriented guide for MySQL connections.
-->

## Goal

Connect Term to a MySQL database and run validation checks on your tables.

## Prerequisites

Before you begin, ensure you have:
- [ ] Term v0.0.1 or later with MySQL feature: `term-guard = { features = ["mysql"] }`
- [ ] MySQL server running and accessible
- [ ] Database credentials (host, port, database name, username, password)
- [ ] Basic understanding of Term validation from tutorials

## Quick Solution

```rust
use term_guard::sources::{DatabaseConfig, MySqlSource};
use term_guard::prelude::*;
use datafusion::prelude::SessionContext;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Using MySqlSource (recommended)
    let source = MySqlSource::new(
        "localhost",
        3306,
        "mydb",
        "username",
        "password",
        "my_table"
    )?;
    
    let ctx = SessionContext::new();
    source.register(&ctx, "my_table").await?;
    
    let suite = ValidationSuite::builder("mysql_validation")
        .table_name("my_table")
        .add_check(Check::new("Data quality").is_complete("id"))
        .build();
    
    let result = suite.run(&ctx).await?;
    println!("Validation success: {}", result.is_success());
    
    Ok(())
}
```

## Step-by-Step Guide

### Step 1: Add MySQL Dependencies

Add Term with MySQL support to your `Cargo.toml`:

```toml
[dependencies]
term-guard = { version = "0.0.1", features = ["mysql"] }
datafusion = "48.0"
tokio = { version = "1.0", features = ["full"] }
```

### Step 2: Choose Your Connection Method

Term provides two ways to connect to MySQL:

**Option A: MySqlSource (Recommended for simple cases)**
```rust
use term_guard::sources::MySqlSource;

let source = MySqlSource::new(
    "localhost",    // host
    3306,          // port
    "mydb",        // database
    "user",        // username
    "password",    // password
    "products"     // table name
)?;
```

**Option B: DatabaseSource (More flexible)**
```rust
use term_guard::sources::{DatabaseConfig, DatabaseSource};
use term_guard::security::SecureString;

let config = DatabaseConfig::MySQL {
    host: "localhost".to_string(),
    port: 3306,
    database: "mydb".to_string(),
    username: "user".to_string(),
    password: SecureString::new("password"),
};
let source = DatabaseSource::new(config, "products")?;
```

### Step 3: Register and Validate

```rust
use datafusion::prelude::SessionContext;
use term_guard::prelude::*;

let ctx = SessionContext::new();
source.register(&ctx, "products").await?;

let suite = ValidationSuite::builder("product_validation")
    .table_name("products")
    .add_check(
        Check::new("Product data quality")
            .is_complete("product_id")
            .is_complete("name")
            .is_positive("price")
            .is_non_negative("stock_quantity")
    )
    .build();

let result = suite.run(&ctx).await?;
```

## Complete Example

Here's a complete example with comprehensive validation:

```rust
use std::error::Error;
use term_guard::sources::MySqlSource;
use term_guard::prelude::*;
use datafusion::prelude::SessionContext;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Connect to MySQL
    let source = MySqlSource::new(
        "localhost",
        3306,
        "ecommerce_db",
        "app_user",
        "secure_password",
        "order_items"
    )?;
    
    let ctx = SessionContext::new();
    
    // Register table
    match source.register(&ctx, "order_items").await {
        Ok(_) => println!("✅ Connected to MySQL successfully"),
        Err(e) => {
            eprintln!("❌ Failed to connect: {}", e);
            return Err(e);
        }
    }
    
    // Verify connection with a quick count
    let count_result = ctx.sql("SELECT COUNT(*) as total_orders FROM order_items").await?;
    count_result.show().await?;
    
    // Run comprehensive validation
    let suite = ValidationSuite::builder("ecommerce_order_validation")
        .table_name("order_items")
        .add_check(
            Check::new("Order item identity")
                .is_complete("order_item_id")
                .is_unique("order_item_id")
                .is_complete("order_id")
                .is_complete("product_id")
        )
        .add_check(
            Check::new("Order item business rules")
                .is_positive("quantity")
                .is_positive("unit_price")
                .is_non_negative("discount_amount")
                .has_max("quantity", 1000.0)  // Reasonable max quantity
        )
        .add_check(
            Check::new("Data completeness")
                .has_completeness("created_at", 1.0)  // Must have timestamps
                .has_completeness("updated_at", 0.95)  // Allow some missing
        )
        .build();
    
    let result = suite.run(&ctx).await?;
    
    // Detailed reporting
    println!("\n=== MySQL Validation Results ===");
    if result.is_success() {
        println!("🎉 All {} validation checks passed!", result.total_constraints());
    } else {
        println!("⚠️  {} of {} checks failed", 
                result.total_constraints() - result.passed_constraints(),
                result.total_constraints());
        
        // Group failures by check
        for check_result in result.check_results() {
            let failed_constraints: Vec<_> = check_result.constraint_results()
                .iter()
                .filter(|c| !c.is_success())
                .collect();
            
            if !failed_constraints.is_empty() {
                println!("\n❌ {}: {} failures", check_result.check_name(), failed_constraints.len());
                for constraint in failed_constraints {
                    println!("  • {}", constraint.name());
                }
            }
        }
    }
    
    Ok(())
}
```

## Variations

### Connecting with Connection String

```rust
use std::env;

// Using environment variables for security
let source = MySqlSource::new(
    &env::var("MYSQL_HOST").unwrap_or_else(|_| "localhost".to_string()),
    env::var("MYSQL_PORT").unwrap_or_else(|_| "3306".to_string()).parse()?,
    &env::var("MYSQL_DATABASE")?,
    &env::var("MYSQL_USER")?,
    &env::var("MYSQL_PASSWORD")?, 
    "user_profiles"
)?;
```

### Cloud MySQL (AWS RDS, Google Cloud SQL)

```rust
// Example for AWS RDS MySQL
let source = MySqlSource::new(
    "mydb.cluster-xyz.us-west-2.rds.amazonaws.com",
    3306,
    "production",
    "app_readonly",
    &env::var("RDS_PASSWORD")?,
    "analytics_events"
)?;

// For Google Cloud SQL with SSL
let config = DatabaseConfig::MySQL {
    host: "127.0.0.1".to_string(),  // Using Cloud SQL Proxy
    port: 3306,
    database: "production".to_string(),
    username: "app-service".to_string(),
    password: SecureString::new(env::var("DB_PASSWORD")?),
};
```

### Multiple Databases/Tables

```rust
// Validate across multiple MySQL databases
let ctx = SessionContext::new();

// Database 1: User data
let users_source = MySqlSource::new("localhost", 3306, "users_db", "reader", "pass", "profiles")?;
users_source.register(&ctx, "user_profiles").await?;

// Database 2: Orders data  
let orders_source = MySqlSource::new("localhost", 3306, "orders_db", "reader", "pass", "orders")?;
orders_source.register(&ctx, "customer_orders").await?;

// Cross-database validation
let user_suite = ValidationSuite::builder("user_validation")
    .table_name("user_profiles")
    .add_check(Check::new("User data").is_complete("user_id").is_unique("email"))
    .build();

let order_suite = ValidationSuite::builder("order_validation")
    .table_name("customer_orders")
    .add_check(Check::new("Order data").is_complete("order_id").is_positive("total_amount"))
    .build();
```

### Character Set and Collation Handling

```rust
// MySQL handles character sets automatically through DataFusion
// But you can verify character set compatibility
let source = MySqlSource::new("localhost", 3306, "utf8_db", "user", "pass", "international_customers")?;

// Validate text data that might have encoding issues
let suite = ValidationSuite::builder("encoding_validation")
    .table_name("international_customers")
    .add_check(
        Check::new("Text data validation")
            .is_complete("customer_name")
            .has_completeness("address", 0.9)
            // Term automatically handles UTF-8 text validation
    )
    .build();
```

## Verification

To verify that your MySQL connection worked correctly:

1. Check that registration succeeds without errors
2. Run a simple query: `ctx.sql("SELECT COUNT(*) FROM my_table").await?`
3. Verify table schema: `ctx.sql("DESCRIBE my_table").await?` (if needed)
4. Ensure validation results align with your data expectations

## Troubleshooting

### Problem: Connection refused (Error 2003)
**Solution:**
- Verify MySQL server is running: `systemctl status mysql` or `brew services list | grep mysql`
- Check if MySQL is listening on the correct port: `netstat -an | grep 3306`
- Confirm host and port in connection string

### Problem: Access denied (Error 1045)
**Solution:**
- Verify username/password: `mysql -u username -p -h localhost`
- Check user permissions: `SHOW GRANTS FOR 'username'@'localhost';`
- Ensure user has SELECT privileges: `GRANT SELECT ON database.* TO 'username'@'%';`

### Problem: Unknown database (Error 1049)
**Solution:**
- Verify database exists: `SHOW DATABASES;`
- Check database name spelling and case sensitivity
- Ensure user has access to the specific database

### Problem: Table doesn't exist (Error 1146)
**Solution:**
- Verify table exists: `SHOW TABLES FROM database_name;`
- Check if table is in a different schema/database
- Confirm table name spelling and case sensitivity

### Problem: Too many connections (Error 1040)
**Solution:**
- Term uses connection pooling, but check MySQL's `max_connections` setting
- Monitor active connections: `SHOW PROCESSLIST;`
- Consider connection limits in your MySQL configuration

### Problem: Charset/encoding issues
**Solution:**
- Ensure MySQL database uses UTF-8: `ALTER DATABASE mydb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;`
- Check table charset: `SHOW CREATE TABLE table_name;`
- Term handles UTF-8 automatically through DataFusion

## Performance Considerations

- **Connection Pooling**: Term automatically manages MySQL connection pools
- **Query Optimization**: Validation queries leverage MySQL's query optimizer
- **Large Tables**: Use appropriate indexes on columns being validated
- **Network Latency**: Place Term close to MySQL server for better performance
- **Memory Usage**: For very large tables, consider sampling strategies

## Security Considerations

- **User Privileges**: Create read-only users for Term validation
- **SSL Connections**: MySQL connections through Term use secure connections by default
- **Password Management**: Store passwords in environment variables or secret managers
- **Network Security**: Use firewalls to restrict MySQL access to authorized hosts
- **Audit Logging**: Enable MySQL audit logging to track Term's data access

## MySQL-Specific Features

### Working with JSON Columns

```rust
// MySQL JSON column validation
let suite = ValidationSuite::builder("json_validation")
    .table_name("user_preferences")
    .add_check(
        Check::new("JSON data validation")
            .is_complete("user_id")
            .is_complete("preferences")  // JSON column
            // Term validates JSON columns as text by default
    )
    .build();
```

### Time Zone Handling

```rust
// MySQL automatically handles time zones through DataFusion
// Validate timestamp columns normally
let suite = ValidationSuite::builder("timestamp_validation")
    .table_name("events")
    .add_check(
        Check::new("Timestamp validation")
            .is_complete("created_at")
            .is_complete("updated_at")
            // Timestamps are handled correctly regardless of MySQL time zone settings
    )
    .build();
```

## Related Guides

- [How to Connect to PostgreSQL](connect-postgresql.md)
- [How to Connect to SQLite](connect-sqlite.md)
- [How to Secure Database Connections](secure-database-connections.md)
- [How to Optimize Database Validation Performance](optimize-database-performance.md)
- [Tutorial: Connect to Your First Database](../tutorials/05-database-connections.md) (for understanding)
- [Database Sources Reference](../reference/database-sources.md) (for API details)

---