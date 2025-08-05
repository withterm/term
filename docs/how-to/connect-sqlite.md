# How to Connect to SQLite

<!-- 
This is a HOW-TO GUIDE following Diátaxis principles.
Task-oriented guide for SQLite connections.
-->

## Goal

Connect Term to a SQLite database and run validation checks on your tables.

## Prerequisites

Before you begin, ensure you have:
- [ ] Term v0.0.1 or later with SQLite feature: `term-guard = { features = ["sqlite"] }`
- [ ] SQLite database file or ability to create one
- [ ] Basic understanding of Term validation from tutorials

## Quick Solution

```rust
use term_guard::sources::{DatabaseConfig, SqliteSource};
use term_guard::prelude::*;
use datafusion::prelude::SessionContext;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Using SqliteSource (recommended)
    let source = SqliteSource::new(
        "path/to/database.db",
        "my_table"
    )?;
    
    let ctx = SessionContext::new();
    source.register(&ctx, "my_table").await?;
    
    let suite = ValidationSuite::builder("sqlite_validation")
        .table_name("my_table")
        .add_check(Check::new("Data quality").is_complete("id"))
        .build();
    
    let result = suite.run(&ctx).await?;
    println!("Validation success: {}", result.is_success());
    
    Ok(())
}
```

## Step-by-Step Guide

### Step 1: Add SQLite Dependencies

Add Term with SQLite support to your `Cargo.toml`:

```toml
[dependencies]
term-guard = { version = "0.0.1", features = ["sqlite"] }
datafusion = "48.0"
tokio = { version = "1.0", features = ["full"] }
```

### Step 2: Choose Your Connection Method

Term provides two ways to connect to SQLite:

**Option A: SqliteSource (Recommended for simple cases)**
```rust
use term_guard::sources::SqliteSource;

let source = SqliteSource::new(
    "data/customers.db",  // path to SQLite file
    "customers"           // table name
)?;
```

**Option B: DatabaseSource (More flexible)**
```rust
use term_guard::sources::{DatabaseConfig, DatabaseSource};

let config = DatabaseConfig::SQLite("data/customers.db".to_string());
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

Here's a complete example with file handling and comprehensive validation:

```rust
use std::error::Error;
use std::path::Path;
use term_guard::sources::SqliteSource;
use term_guard::prelude::*;
use datafusion::prelude::SessionContext;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let db_path = "data/inventory.db";
    
    // Check if database file exists
    if !Path::new(db_path).exists() {
        eprintln!("❌ Database file not found: {}", db_path);
        eprintln!("   Please ensure the SQLite file exists and is readable");
        return Err("Database file not found".into());
    }
    
    // Connect to SQLite
    let source = SqliteSource::new(db_path, "products")?;
    let ctx = SessionContext::new();
    
    // Register table
    match source.register(&ctx, "products").await {
        Ok(_) => println!("✅ Connected to SQLite database: {}", db_path),
        Err(e) => {
            eprintln!("❌ Failed to register table: {}", e);
            return Err(e);
        }
    }
    
    // Verify table structure
    println!("📋 Verifying table structure...");
    let schema_query = ctx.sql("SELECT name FROM pragma_table_info('products')").await?;
    schema_query.show().await?;
    
    // Count records
    let count_result = ctx.sql("SELECT COUNT(*) as product_count FROM products").await?;
    count_result.show().await?;
    
    // Run comprehensive validation
    let suite = ValidationSuite::builder("inventory_validation")
        .table_name("products")
        .add_check(
            Check::new("Product identity")
                .is_complete("product_id")
                .is_unique("product_id")
                .is_complete("name")
        )
        .add_check(
            Check::new("Inventory business rules")
                .is_non_negative("stock_quantity")
                .is_positive("unit_price")
                .has_min("unit_price", 0.01)  // Minimum price
                .has_max("stock_quantity", 10000.0)  // Reasonable max stock
        )
        .add_check(
            Check::new("Data completeness")
                .has_completeness("category", 0.95)
                .has_completeness("supplier_id", 0.8)
                .is_complete("created_date")
        )
        .build();
    
    println!("\n🔍 Running inventory validation...");
    let result = suite.run(&ctx).await?;
    
    // Report results with SQLite-specific details
    println!("\n=== SQLite Validation Results ===");
    println!("Database: {}", db_path);
    println!("Table: products");
    
    if result.is_success() {
        println!("🎉 All {} validation checks passed!", result.total_constraints());
    } else {
        println!("⚠️  {} of {} checks failed", 
                result.total_constraints() - result.passed_constraints(),
                result.total_constraints());
        
        // Show detailed failures
        for check_result in result.check_results() {
            if !check_result.is_success() {
                println!("\n❌ Check: {}", check_result.check_name());
                for constraint_result in check_result.constraint_results() {
                    if !constraint_result.is_success() {
                        println!("  • {}", constraint_result.name());
                    }
                }
            }
        }
    }
    
    // SQLite database info
    let db_size = std::fs::metadata(db_path)?.len();
    println!("\n📊 Database Info:");
    println!("  File size: {:.2} MB", db_size as f64 / 1_048_576.0);
    
    Ok(())
}
```

## Variations

### In-Memory SQLite Database

```rust
// For testing or temporary data processing
let source = SqliteSource::new(":memory:", "temp_data")?;

// Note: In-memory databases are lost when the connection closes
// Useful for testing or data transformation pipelines
```

### Relative and Absolute Paths

```rust
use std::env;
use std::path::PathBuf;

// Using relative path (relative to current working directory)
let source = SqliteSource::new("./data/local.db", "events")?;

// Using absolute path
let mut db_path = env::current_dir()?;
db_path.push("databases");
db_path.push("production.db");
let source = SqliteSource::new(db_path.to_str().unwrap(), "logs")?;

// Using environment variable
let db_path = env::var("SQLITE_DB_PATH").unwrap_or_else(|_| "default.db".to_string());
let source = SqliteSource::new(&db_path, "metrics")?;
```

### Multiple Tables from Same Database

```rust
// Register multiple tables from the same SQLite file
let ctx = SessionContext::new();
let db_path = "app_data.db";

let users_source = SqliteSource::new(db_path, "users")?;
let posts_source = SqliteSource::new(db_path, "posts")?;
let comments_source = SqliteSource::new(db_path, "comments")?;

users_source.register(&ctx, "users").await?;
posts_source.register(&ctx, "posts").await?;
comments_source.register(&ctx, "comments").await?;

// Validate related tables
let user_suite = ValidationSuite::builder("user_validation")
    .table_name("users")
    .add_check(Check::new("User integrity").is_unique("user_id").is_complete("username"))
    .build();

let content_suite = ValidationSuite::builder("content_validation")
    .table_name("posts")
    .add_check(Check::new("Post integrity").is_complete("post_id").is_complete("author_id"))
    .build();
```

### Read-Only Database Access

```rust
// SQLite databases opened through Term are read-only by default
// This is ideal for validation without risk of data modification

let source = SqliteSource::new("readonly_data.db", "analytics")?;
// Term will not modify the database - only read for validation
```

### WAL Mode Databases

```rust
// Term works with SQLite databases in WAL (Write-Ahead Logging) mode
// No special configuration needed - DataFusion handles this automatically

let source = SqliteSource::new("wal_database.db", "transactions")?;
// Works the same regardless of SQLite journal mode
```

## Verification

To verify that your SQLite connection worked correctly:

1. Check that the database file exists and is readable
2. Confirm table registration succeeds without errors
3. Run a simple query: `ctx.sql("SELECT COUNT(*) FROM my_table").await?`
4. Verify table schema if needed: `ctx.sql("PRAGMA table_info(my_table)").await?`

## Troubleshooting

### Problem: Database file not found
**Solution:**
- Verify the file path is correct and file exists
- Check file permissions (Term needs read access)  
- Use absolute paths to avoid working directory issues
- Ensure the file is a valid SQLite database: `file database.db`

### Problem: Database is locked (SQLITE_BUSY)
**Solution:**
- Close other applications that might have the database open
- Check for long-running transactions in other connections
- SQLite allows multiple readers but only one writer
- Term only reads, so this usually indicates another process is writing

### Problem: Table not found
**Solution:**
- List available tables: `ctx.sql("SELECT name FROM sqlite_master WHERE type='table'").await?`
- Check table name spelling and case sensitivity
- Verify you're connecting to the correct database file

### Problem: Corrupted database
**Solution:**
- Check database integrity: `sqlite3 database.db "PRAGMA integrity_check;"`
- Try to repair if possible: `sqlite3 database.db ".recover" | sqlite3 repaired.db`
- Restore from backup if available

### Problem: Permission denied
**Solution:**
- Check file permissions: `ls -la database.db`
- Ensure Term process has read access to the file
- Verify directory permissions (Term needs to access the containing directory)

### Problem: Unsupported SQLite version
**Solution:**
- Term supports SQLite 3.x databases
- Check SQLite version: `sqlite3 --version`
- Upgrade old SQLite 2.x databases if necessary

## Performance Considerations

- **File Location**: Keep SQLite files on fast storage (SSD) for better performance
- **Database Size**: SQLite performs well up to several GB; very large databases may benefit from PostgreSQL/MySQL
- **Concurrent Access**: SQLite allows multiple readers, perfect for Term's read-only validation
- **Indexing**: Ensure appropriate indexes exist on columns being validated for better query performance
- **Memory Usage**: Term automatically manages memory usage for SQLite connections

## Security Considerations

- **File Permissions**: Set appropriate file permissions (e.g., 644 for read-only access)
- **Encryption**: For sensitive data, consider using SQLite encryption extensions
- **Access Control**: SQLite relies on filesystem permissions for access control
- **Backup Security**: Secure backup files with appropriate permissions
- **Data Privacy**: SQLite files contain all data in a single file - secure accordingly

## SQLite-Specific Features

### Working with SQLite Data Types

```rust
// SQLite dynamic typing works seamlessly with Term
let suite = ValidationSuite::builder("type_validation")
    .table_name("mixed_data")
    .add_check(
        Check::new("Type consistency")
            .is_complete("id")        // INTEGER
            .is_complete("name")      // TEXT
            .is_complete("score")     // REAL
            .is_complete("data")      // BLOB (validated as complete/incomplete)
    )
    .build();
```

### JSON Support in Modern SQLite

```rust
// SQLite 3.38+ JSON functions work through DataFusion
let suite = ValidationSuite::builder("json_validation")
    .table_name("json_data")
    .add_check(
        Check::new("JSON column validation")
            .is_complete("id")
            .is_complete("json_column")  // Validates JSON as text column
    )
    .build();
```

### Date and Time Handling

```rust
// SQLite stores dates as text, integers, or real numbers
// Term validates them based on their stored representation
let suite = ValidationSuite::builder("datetime_validation")
    .table_name("events")
    .add_check(
        Check::new("Timestamp validation")
            .is_complete("created_at")    // Text format: '2024-01-01 12:00:00'
            .is_complete("timestamp")     // Integer format: Unix timestamp
    )
    .build();
```

## Related Guides

- [How to Connect to PostgreSQL](connect-postgresql.md)
- [How to Connect to MySQL](connect-mysql.md)
- [How to Validate Data Across Multiple Tables](validate-multiple-tables.md)
- [How to Optimize Database Validation Performance](optimize-database-performance.md)
- [Tutorial: Connect to Your First Database](../tutorials/05-database-connections.md) (for understanding)
- [Database Sources Reference](../reference/database-sources.md) (for API details)

---