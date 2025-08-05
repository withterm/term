# Tutorial: Working with Multiple Databases

<!-- 
This is a TUTORIAL following Diátaxis principles.
Advanced tutorial building on database connection basics.
-->

## What You'll Learn

In this tutorial, you will:
- Connect to multiple database types (PostgreSQL, MySQL, SQLite) simultaneously
- Validate data consistency across different databases
- Use ValidationContext for dynamic table switching
- Handle connection pooling and resource management
- Compare validation results between databases

## What You'll Need

Before starting this tutorial, make sure you have:
- Completed the [Connect to Your First Database](05-database-connections.md) tutorial
- Term with database features: `term-guard = { features = ["postgres", "mysql", "sqlite"] }`
- Docker installed (for PostgreSQL and MySQL containers)
- Understanding of SQL and basic database concepts

**Time to complete:** ~50 minutes

## Getting Started

We'll create a scenario where customer data is distributed across different databases - maybe due to a migration process or microservices architecture. You'll learn to validate data consistency across these systems and ensure your distributed data maintains quality standards.

## Step 1: Set Up Multi-Database Environment

First, let's set up our development environment with multiple databases:

```toml
# Add to Cargo.toml
[dependencies]
term-guard = { version = "0.0.1", features = ["postgres", "mysql", "sqlite"] }
datafusion = "48.0"
tokio = { version = "1.0", features = ["full"] }
rusqlite = "0.31"
tempfile = "3.8"
```

Create `docker-compose.yml` for our test databases:

```yaml
version: '3.8'
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: testdb
      POSTGRES_USER: testuser
      POSTGRES_PASSWORD: testpass
    ports:
      - "5432:5432"
    
  mysql:
    image: mysql:8.0
    environment:
      MYSQL_DATABASE: testdb
      MYSQL_USER: testuser
      MYSQL_PASSWORD: testpass
      MYSQL_ROOT_PASSWORD: rootpass
    ports:
      - "3306:3306"
```

Start the databases:
```bash
docker-compose up -d
```

> 💡 **What's happening here?** We're creating isolated database environments for testing. In production, you'd connect to existing databases, but this setup lets us experiment safely.

## Step 2: Create Sample Data Across Databases

Now let's create similar data in all three databases to simulate a real-world scenario:

```rust
use std::error::Error;
use term_guard::sources::{DatabaseConfig, DatabaseSource, PostgresSource, MySqlSource, SqliteSource};
use term_guard::prelude::*;
use datafusion::prelude::SessionContext;
use rusqlite::Connection;
use tempfile::NamedTempFile;

async fn setup_sqlite_data() -> Result<String, Box<dyn Error>> {
    let temp_file = NamedTempFile::new()?;
    let db_path = temp_file.path().to_str().unwrap().to_string();
    let conn = Connection::open(&db_path)?;
    
    // Create customers table with some specific data quality issues
    conn.execute(
        "CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            region TEXT,
            created_at TEXT
        )",
        [],
    )?;
    
    conn.execute_batch(
        "INSERT INTO customers (id, name, email, region, created_at) VALUES
        (1, 'Alice Johnson', 'alice@example.com', 'US', '2024-01-01'),
        (2, 'Bob Smith', 'bob@example.com', 'US', '2024-01-02'),
        (3, 'Carol Brown', NULL, 'CA', '2024-01-03'),
        (4, 'David Wilson', 'david@example.com', 'UK', '2024-01-04')"
    )?;
    
    println!("SQLite database created at: {}", db_path);
    Ok(db_path)
}

async fn setup_postgres_data() -> Result<(), Box<dyn Error>> {
    // In a real scenario, you'd use a proper PostgreSQL client
    // For this tutorial, we'll assume the data exists
    println!("PostgreSQL setup would happen here...");
    println!("Assuming customers table exists with similar structure");
    Ok(())
}

async fn setup_mysql_data() -> Result<(), Box<dyn Error>> {
    // Similar to PostgreSQL
    println!("MySQL setup would happen here...");
    println!("Assuming customers table exists with similar structure");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let sqlite_path = setup_sqlite_data().await?;
    setup_postgres_data().await?;
    setup_mysql_data().await?;
    
    println!("All databases prepared for multi-database validation!");
    Ok(())
}
```

> ⚠️ **Note:** For this tutorial, we'll focus on SQLite with the understanding that PostgreSQL and MySQL follow the same patterns. In production, you'd set up all three with actual data.

## Step 3: Connect to Multiple Databases

Let's see how to work with multiple database connections simultaneously:

```rust
use std::error::Error;
use term_guard::sources::{DatabaseConfig, DatabaseSource};
use term_guard::core::ValidationContext;
use term_guard::prelude::*;
use datafusion::prelude::SessionContext;
use rusqlite::Connection;
use tempfile::NamedTempFile;

async fn create_multi_database_setup() -> Result<(String, String), Box<dyn Error>> {
    // Create two SQLite databases to simulate different systems
    
    // Database 1: "Legacy" system
    let legacy_file = NamedTempFile::new()?;
    let legacy_path = legacy_file.path().to_str().unwrap().to_string();
    let legacy_conn = Connection::open(&legacy_path)?;
    
    legacy_conn.execute(
        "CREATE TABLE customer_data (
            customer_id INTEGER PRIMARY KEY,
            full_name TEXT,
            email_address TEXT,
            country TEXT
        )",
        [],
    )?;
    
    legacy_conn.execute_batch(
        "INSERT INTO customer_data (customer_id, full_name, email_address, country) VALUES
        (1, 'Alice Johnson', 'alice@example.com', 'USA'),
        (2, 'Bob Smith', NULL, 'USA'),
        (3, 'Carol Brown', 'carol@example.com', 'Canada')"
    )?;
    
    // Database 2: "Modern" system
    let modern_file = NamedTempFile::new()?;
    let modern_path = modern_file.path().to_str().unwrap().to_string();
    let modern_conn = Connection::open(&modern_path)?;
    
    modern_conn.execute(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT,
            region TEXT,
            status TEXT
        )",
        [],
    )?;
    
    modern_conn.execute_batch(
        "INSERT INTO users (id, name, email, region, status) VALUES
        (1, 'Alice Johnson', 'alice@example.com', 'US', 'active'),
        (2, 'Bob Smith', 'bob@example.com', 'US', 'active'),
        (3, 'Carol Brown', 'carol@example.com', 'CA', 'inactive'),
        (4, 'David Wilson', 'david@example.com', 'UK', 'active')"
    )?;
    
    Ok((legacy_path, modern_path))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (legacy_path, modern_path) = create_multi_database_setup().await?;
    
    // Create database sources for each system
    let legacy_config = DatabaseConfig::SQLite(legacy_path);
    let legacy_source = DatabaseSource::new(legacy_config, "customer_data")?;
    
    let modern_config = DatabaseConfig::SQLite(modern_path);
    let modern_source = DatabaseSource::new(modern_config, "users")?;
    
    // Create separate contexts for each database
    let legacy_ctx = SessionContext::new();
    let modern_ctx = SessionContext::new();
    
    // Register tables with their respective contexts
    legacy_source.register(&legacy_ctx, "customer_data").await?;
    modern_source.register(&modern_ctx, "users").await?;
    
    println!("✅ Connected to legacy system: customer_data table");
    println!("✅ Connected to modern system: users table");
    
    // Verify connections by counting records
    let legacy_count = legacy_ctx.sql("SELECT COUNT(*) as count FROM customer_data").await?;
    let modern_count = modern_ctx.sql("SELECT COUNT(*) as count FROM users").await?;
    
    println!("\nRecord counts:");
    println!("Legacy system:");
    legacy_count.show().await?;
    println!("Modern system:");
    modern_count.show().await?;
    
    Ok(())
}
```

Run this code:
```bash
cargo run
```

You should see:
```
✅ Connected to legacy system: customer_data table
✅ Connected to modern system: users table

Record counts:
Legacy system:
+-------+
| count |
+-------+
| 3     |
+-------+
Modern system:
+-------+
| count |
+-------+
| 4     |
+-------+
```

> 💡 **What's happening here?** We created two separate databases with different table structures and names, simulating real-world scenarios where legacy and modern systems coexist.

## Step 4: Cross-Database Validation

Now let's validate data quality across both systems and compare results:

```rust
use std::error::Error;
use term_guard::sources::{DatabaseConfig, DatabaseSource};
use term_guard::prelude::*;
use datafusion::prelude::SessionContext;
use rusqlite::Connection;
use tempfile::NamedTempFile;

// ... (previous setup functions remain the same)

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (legacy_path, modern_path) = create_multi_database_setup().await?;
    
    let legacy_config = DatabaseConfig::SQLite(legacy_path);
    let legacy_source = DatabaseSource::new(legacy_config, "customer_data")?;
    
    let modern_config = DatabaseConfig::SQLite(modern_path);
    let modern_source = DatabaseSource::new(modern_config, "users")?;
    
    let legacy_ctx = SessionContext::new();
    let modern_ctx = SessionContext::new();
    
    legacy_source.register(&legacy_ctx, "customer_data").await?;
    modern_source.register(&modern_ctx, "users").await?;
    
    // Step 4: Create validation suites for each database
    let legacy_suite = ValidationSuite::builder("legacy_data_quality")
        .table_name("customer_data")
        .add_check(
            Check::new("Legacy system validation")
                .is_complete("customer_id")
                .is_complete("full_name")
                .has_completeness("email_address", 0.8)  // Expect 80% email completeness
        )
        .build();
    
    let modern_suite = ValidationSuite::builder("modern_data_quality")
        .table_name("users")
        .add_check(
            Check::new("Modern system validation")
                .is_complete("id")
                .is_complete("name")
                .has_completeness("email", 0.9)  // Higher standard for modern system
                .is_complete("status")
        )
        .build();
    
    // Run validations on both systems
    println!("🔍 Running validation on legacy system...");
    let legacy_result = legacy_suite.run(&legacy_ctx).await?;
    
    println!("🔍 Running validation on modern system...");
    let modern_result = modern_suite.run(&modern_ctx).await?;
    
    // Compare results
    println!("\n=== VALIDATION COMPARISON ===");
    
    println!("\n📊 Legacy System Results:");
    println!("  Success: {}", legacy_result.is_success());
    println!("  Passed: {}/{}", legacy_result.passed_constraints(), legacy_result.total_constraints());
    
    for check_result in legacy_result.check_results() {
        for constraint_result in check_result.constraint_results() {
            let status = if constraint_result.is_success() { "✅" } else { "❌" };
            println!("  {} {}", status, constraint_result.name());
        }
    }
    
    println!("\n📊 Modern System Results:");
    println!("  Success: {}", modern_result.is_success());
    println!("  Passed: {}/{}", modern_result.passed_constraints(), modern_result.total_constraints());
    
    for check_result in modern_result.check_results() {
        for constraint_result in check_result.constraint_results() {
            let status = if constraint_result.is_success() { "✅" } else { "❌" };
            println!("  {} {}", status, constraint_result.name());
        }
    }
    
    // Summary comparison
    println!("\n📈 SUMMARY:");
    if modern_result.is_success() && !legacy_result.is_success() {
        println!("🎯 Modern system has better data quality than legacy system");
        println!("💡 Consider migrating remaining data and fixing legacy issues");
    } else if legacy_result.is_success() && modern_result.is_success() {
        println!("🎉 Both systems meet data quality standards!");
    } else {
        println!("⚠️  Both systems need data quality improvements");
    }
    
    Ok(())
}
```

Run the comparison:
```bash
cargo run
```

You should see output like:
```
🔍 Running validation on legacy system...
🔍 Running validation on modern system...

=== VALIDATION COMPARISON ===

📊 Legacy System Results:
  Success: false
  Passed: 2/3
  ✅ customer_id is complete
  ✅ full_name is complete
  ❌ email_address has completeness >= 0.8

📊 Modern System Results:
  Success: true
  Passed: 4/4
  ✅ id is complete
  ✅ name is complete
  ✅ email has completeness >= 0.9
  ✅ status is complete

📈 SUMMARY:
🎯 Modern system has better data quality than legacy system
💡 Consider migrating remaining data and fixing legacy issues
```

> 💡 **What's happening here?** The legacy system fails email completeness (only 67% vs required 80%), while the modern system passes all checks. This shows how Term helps you compare data quality across systems!

### Try It Yourself

Try adjusting the completeness requirements. What happens if you set the legacy system requirement to 0.6 (60%)?

<details>
<summary>💡 Hint</summary>

Change `.has_completeness("email_address", 0.6)` and rerun. The legacy system should now pass all checks.

</details>

## Step 5: Dynamic Table Switching with ValidationContext

Let's learn how to use ValidationContext for dynamic table name handling:

```rust
use std::error::Error;
use term_guard::sources::{DatabaseConfig, DatabaseSource};
use term_guard::core::{ValidationContext, validation_context::CURRENT_CONTEXT};
use term_guard::prelude::*;
use datafusion::prelude::SessionContext;
use rusqlite::Connection;
use tempfile::NamedTempFile;

async fn setup_shared_context_database() -> Result<String, Box<dyn Error>> {
    let temp_file = NamedTempFile::new()?;
    let db_path = temp_file.path().to_str().unwrap().to_string();
    let conn = Connection::open(&db_path)?;
    
    // Create multiple tables in the same database
    conn.execute(
        "CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT
        )",
        [],
    )?;
    
    conn.execute(
        "CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            amount REAL,
            status TEXT
        )",
        [],
    )?;
    
    // Insert data
    conn.execute_batch(
        "INSERT INTO customers (id, name, email) VALUES
        (1, 'Alice', 'alice@example.com'),
        (2, 'Bob', 'bob@example.com'),
        (3, 'Carol', NULL);
        
        INSERT INTO orders (order_id, customer_id, amount, status) VALUES
        (101, 1, 99.99, 'completed'),
        (102, 2, 149.50, 'completed'),
        (103, 3, 75.00, NULL),
        (104, 1, 200.00, 'pending')"
    )?;
    
    Ok(db_path)
}

async fn validate_table_with_context(
    ctx: &SessionContext,
    table_name: &str,
    validation_name: &str,
) -> Result<ValidationResult, Box<dyn Error>> {
    // Create validation context for the specific table
    let validation_ctx = ValidationContext::new(table_name);
    
    // Use task-local context to set the table name
    let result = CURRENT_CONTEXT.scope(validation_ctx, async {
        let suite = ValidationSuite::builder(validation_name)
            .table_name(table_name)  // Explicitly set table name
            .add_check(
                match table_name {
                    "customers" => Check::new("Customer validation")
                        .is_complete("id")
                        .is_complete("name")
                        .has_completeness("email", 0.6),
                    "orders" => Check::new("Order validation")
                        .is_complete("order_id")
                        .is_complete("customer_id")
                        .is_positive("amount")
                        .has_completeness("status", 0.7),
                    _ => Check::new("Generic validation")
                        .has_size(1)  // At least one record
                }
            )
            .build();
        
        suite.run(ctx).await
    }).await?;
    
    Ok(result)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let db_path = setup_shared_context_database().await?;
    
    // Set up single context with multiple tables
    let db_config = DatabaseConfig::SQLite(db_path);
    let ctx = SessionContext::new();
    
    // Register both tables in the same context
    let customers_source = DatabaseSource::new(db_config.clone(), "customers")?;
    let orders_source = DatabaseSource::new(db_config, "orders")?;
    
    customers_source.register(&ctx, "customers").await?;
    orders_source.register(&ctx, "orders").await?;
    
    println!("📋 Registered multiple tables in shared context");
    
    // Step 5: Validate different tables dynamically
    println!("\n🔍 Running dynamic table validation...");
    
    let tables = vec!["customers", "orders"];
    
    for table_name in tables {
        println!("\n--- Validating {} ---", table_name);
        
        let result = validate_table_with_context(
            &ctx,
            table_name,
            &format!("{}_quality", table_name)
        ).await?;
        
        println!("Success: {}", result.is_success());
        println!("Passed: {}/{}", result.passed_constraints(), result.total_constraints());
        
        if !result.is_success() {
            println!("Failed constraints:");
            for check_result in result.check_results() {
                for constraint_result in check_result.constraint_results() {
                    if !constraint_result.is_success() {
                        println!("  ❌ {}", constraint_result.name());
                    }
                }
            }
        }
    }
    
    println!("\n✅ Dynamic table validation complete!");
    Ok(())
}
```

Run the dynamic validation:
```bash
cargo run
```

You should see:
```
📋 Registered multiple tables in shared context

🔍 Running dynamic table validation...

--- Validating customers ---
Success: true
Passed: 3/3

--- Validating orders ---
Success: false
Passed: 3/4
Failed constraints:
  ❌ status has completeness >= 0.7

✅ Dynamic table validation complete!
```

> 💡 **What's happening here?** ValidationContext allows you to dynamically specify which table to validate at runtime. This is powerful for scenarios where you need to validate many tables with similar logic or where table names are determined at runtime.

## What You've Learned

Congratulations! You've learned how to:

✅ **Set up multiple database connections** simultaneously  
✅ **Compare data quality across different systems** using separate ValidationSuites  
✅ **Use ValidationContext for dynamic table names** at runtime  
✅ **Handle different table schemas and structures** in validation logic  
✅ **Manage connection contexts and resource sharing** efficiently

### Key Takeaways

- **Multi-Database Strategy**: Each database gets its own SessionContext for isolation
- **Cross-System Comparison**: Compare validation results to identify data quality differences
- **Dynamic Table Handling**: ValidationContext enables runtime table name specification
- **Resource Management**: Proper context management prevents connection leaks
- **Flexible Validation**: Different validation rules can apply to different systems

## Next Steps

Now that you understand multi-database validation, you're ready to:

1. **Apply to Production**: See [How to Optimize Database Validation Performance](../how-to/optimize-database-performance.md)
2. **Secure Connections**: Read [How to Secure Database Connections](../how-to/secure-database-connections.md)
3. **Understand Architecture**: Learn [Understanding Database Connectors](../explanation/database-connectors.md)

## Exercises

1. **Exercise 1**: Create a validation suite that compares the same logical data across PostgreSQL and MySQL
2. **Exercise 2**: Build a system that validates data consistency during a database migration
3. **Challenge**: Implement cross-database referential integrity checks using Term's validation framework

---