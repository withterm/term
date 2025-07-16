# Tutorial: Connect to Your First Database

<!-- 
This is a TUTORIAL following Diátaxis principles.
Tutorials are LEARNING-oriented, focused on hands-on experience.
-->

## What You'll Learn

In this tutorial, you will:
- Connect Term to a SQLite database for the first time
- Register database tables with Term's validation engine
- Run your first database validation checks
- Understand how database sources work with Term's architecture

## What You'll Need

Before starting this tutorial, make sure you have:
- Rust 1.70 or later installed
- Term added to your project dependencies with the `sqlite` feature enabled
- Basic understanding of Term constraints from previous tutorials
- SQLite installed on your system (for creating sample data)

**Time to complete:** ~35 minutes

## Getting Started

Let's start by creating a simple SQLite database with customer data that we'll validate using Term. This will help you understand how Term connects to real databases and performs validation on live data.

We'll build a customer database validation system step by step, learning about database sources, table registration, and validation contexts along the way.

## Step 1: Set Up Your Project

First, let's create a new Rust project and add Term with database support.

```bash
cargo new --bin database_tutorial
cd database_tutorial
```

Add Term to your `Cargo.toml`:

```toml
[dependencies]
term-guard = { version = "0.0.1", features = ["sqlite"] }
datafusion = "48.0"
tokio = { version = "1.0", features = ["full"] }
rusqlite = "0.31" # For creating sample data
tempfile = "3.8"  # For temporary database files
```

> 💡 **What's happening here?** The `sqlite` feature enables Term's SQLite connector. We also include `datafusion` (Term's query engine) and `rusqlite` to create our sample database.

## Step 2: Create Sample Data

Now let's create a simple database with customer data. Create `src/main.rs`:

```rust
use std::error::Error;
use rusqlite::Connection;
use tempfile::NamedTempFile;

fn create_sample_database() -> Result<String, Box<dyn Error>> {
    // Create a temporary SQLite file
    let temp_file = NamedTempFile::new()?;
    let db_path = temp_file.path().to_str().unwrap().to_string();
    
    // Connect and create our sample table
    let conn = Connection::open(&db_path)?;
    
    // Create customers table
    conn.execute(
        "CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            age INTEGER,
            signup_date TEXT NOT NULL
        )",
        [],
    )?;
    
    // Insert sample data (some with missing emails to test validation)
    conn.execute_batch(
        "INSERT INTO customers (id, name, email, age, signup_date) VALUES
        (1, 'Alice Johnson', 'alice@example.com', 28, '2024-01-15'),
        (2, 'Bob Smith', 'bob@example.com', 35, '2024-01-20'),
        (3, 'Carol Brown', NULL, 42, '2024-01-25'),
        (4, 'David Wilson', 'david@example.com', 29, '2024-02-01'),
        (5, 'Eve Davis', NULL, 33, '2024-02-05')"
    )?;
    
    // Don't close the connection yet - return the path
    println!("Created sample database at: {}", db_path);
    Ok(db_path)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let db_path = create_sample_database()?;
    println!("Database ready for validation!");
    Ok(())
}
```

Run this code:
```bash
cargo run
```

You should see:
```
Created sample database at: /tmp/.tmpXXXXXX
Database ready for validation!
```

> 💡 **What's happening here?** We created a SQLite database with a `customers` table containing 5 records. Notice that some customers don't have email addresses - we'll use Term to validate data completeness.

## Step 3: Connect Term to the Database

Now let's connect Term to our database. Update your `main.rs`:

```rust
use std::error::Error;
use rusqlite::Connection;
use tempfile::NamedTempFile;
use term_guard::sources::{DatabaseConfig, DatabaseSource};
use term_guard::prelude::*;
use datafusion::prelude::SessionContext;

fn create_sample_database() -> Result<String, Box<dyn Error>> {
    // ... (same as before)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let db_path = create_sample_database()?;
    
    // Step 3: Create a database source
    let db_config = DatabaseConfig::SQLite(db_path);
    let source = DatabaseSource::new(db_config, "customers")?;
    
    // Create DataFusion context and register our table
    let ctx = SessionContext::new();
    source.register(&ctx, "customers").await?;
    
    println!("Database connected and table registered!");
    
    // Let's verify the connection by running a simple query
    let df = ctx.sql("SELECT COUNT(*) as customer_count FROM customers").await?;
    df.show().await?;
    
    Ok(())
}
```

Run the updated code:
```bash
cargo run
```

You should see:
```
Database connected and table registered!
+----------------+
| customer_count |
+----------------+
| 5              |
+----------------+
```

> 💡 **What's happening here?** We created a `DatabaseSource` from our SQLite file and registered it with DataFusion. Term can now query this table for validation. The `register` method makes the database table available as "customers" in our validation context.

### Try It Yourself

Before moving on, try modifying the code to show all customer names. What SQL query would you use?

<details>
<summary>💡 Hint</summary>

Try: `SELECT name FROM customers`

</details>

## Step 4: Run Your First Database Validation

Let's see what happens when we run actual validation checks on our database. This is where Term's power really shows:

```rust
use std::error::Error;
use rusqlite::Connection;
use tempfile::NamedTempFile;
use term_guard::sources::{DatabaseConfig, DatabaseSource};
use term_guard::prelude::*;
use datafusion::prelude::SessionContext;

fn create_sample_database() -> Result<String, Box<dyn Error>> {
    // ... (same as before)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let db_path = create_sample_database()?;
    
    // Connect to database
    let db_config = DatabaseConfig::SQLite(db_path);
    let source = DatabaseSource::new(db_config, "customers")?;
    let ctx = SessionContext::new();
    source.register(&ctx, "customers").await?;
    
    // Step 4: Create and run validation checks
    let suite = ValidationSuite::builder("customer_data_quality")
        .table_name("customers")
        .add_check(
            Check::new("Customer data validation")
                .is_complete("id")           // All customers must have IDs
                .is_complete("name")         // All customers must have names
                .has_completeness("email", 0.7)  // At least 70% should have emails
                .is_positive("age")          // Ages must be positive
                .has_min("age", 18)          // Customers must be adults
        )
        .build();
    
    println!("Running validation checks...");
    let result = suite.run(&ctx).await?;
    
    // Display results
    println!("\n=== Validation Results ===");
    println!("Overall success: {}", result.is_success());
    println!("Passed: {}/{}", result.passed_constraints(), result.total_constraints());
    
    // Show detailed results
    for check_result in result.check_results() {
        println!("\nCheck: {}", check_result.check_name());
        for constraint_result in check_result.constraint_results() {
            let status = if constraint_result.is_success() { "✅ PASS" } else { "❌ FAIL" };
            println!("  {} - {}", status, constraint_result.name());
        }
    }
    
    Ok(())
}
```

Run this version:
```bash
cargo run
```

You should see output like:
```
Running validation checks...

=== Validation Results ===
Overall success: false
Passed: 4/5

Check: Customer data validation
  ✅ PASS - id is complete
  ✅ PASS - name is complete
  ❌ FAIL - email has completeness >= 0.7
  ✅ PASS - age is positive
  ✅ PASS - age has minimum 18
```

> ⚠️ **Expected Failure:** The email completeness check fails because only 3 out of 5 customers (60%) have email addresses, but we required 70%. This shows Term working correctly!

## Step 5: Fix and Revalidate

Now let's see what happens when we fix our data quality issue. We'll add email addresses for the missing customers:

```rust
use std::error::Error;
use rusqlite::Connection;
use tempfile::NamedTempFile;
use term_guard::sources::{DatabaseConfig, DatabaseSource};
use term_guard::prelude::*;
use datafusion::prelude::SessionContext;

fn create_sample_database() -> Result<String, Box<dyn Error>> {
    let temp_file = NamedTempFile::new()?;
    let db_path = temp_file.path().to_str().unwrap().to_string();
    let conn = Connection::open(&db_path)?;
    
    conn.execute(
        "CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            age INTEGER,
            signup_date TEXT NOT NULL
        )",
        [],
    )?;
    
    // Step 5: Add email addresses to meet our 70% completeness requirement
    conn.execute_batch(
        "INSERT INTO customers (id, name, email, age, signup_date) VALUES
        (1, 'Alice Johnson', 'alice@example.com', 28, '2024-01-15'),
        (2, 'Bob Smith', 'bob@example.com', 35, '2024-01-20'),
        (3, 'Carol Brown', 'carol@example.com', 42, '2024-01-25'),
        (4, 'David Wilson', 'david@example.com', 29, '2024-02-01'),
        (5, 'Eve Davis', NULL, 33, '2024-02-05')"
    )?;
    
    println!("Created improved sample database at: {}", db_path);
    Ok(db_path)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let db_path = create_sample_database()?;
    
    let db_config = DatabaseConfig::SQLite(db_path);
    let source = DatabaseSource::new(db_config, "customers")?;
    let ctx = SessionContext::new();
    source.register(&ctx, "customers").await?;
    
    let suite = ValidationSuite::builder("customer_data_quality")
        .table_name("customers")
        .add_check(
            Check::new("Customer data validation")
                .is_complete("id")
                .is_complete("name")
                .has_completeness("email", 0.7)  // Now we have 80% (4/5)
                .is_positive("age")
                .has_min("age", 18)
        )
        .build();
    
    println!("Running validation on improved data...");
    let result = suite.run(&ctx).await?;
    
    println!("\n=== Validation Results ===");
    println!("Overall success: {}", result.is_success());
    println!("Passed: {}/{}", result.passed_constraints(), result.total_constraints());
    
    if result.is_success() {
        println!("🎉 All validation checks passed!");
    }
    
    Ok(())
}
```

Run the final version:
```bash
cargo run
```

You should now see:
```
Running validation on improved data...

=== Validation Results ===
Overall success: true
Passed: 5/5
🎉 All validation checks passed!
```

> 💡 **What's happening here?** Now 4 out of 5 customers (80%) have email addresses, which exceeds our 70% requirement. All validations pass!

## What You've Learned

Congratulations! You've learned how to:

✅ **Connect Term to a SQLite database** using DatabaseConfig and DatabaseSource  
✅ **Register database tables** with DataFusion for validation  
✅ **Run validation checks on live database data** using ValidationSuite  
✅ **Interpret validation results** and understand pass/fail criteria  
✅ **Use table names dynamically** with the table_name() method

### Key Takeaways

- **Database Sources**: Term connects to databases through DataSource implementations like DatabaseSource
- **Table Registration**: Database tables must be registered with DataFusion before validation
- **Dynamic Table Names**: Use `.table_name()` to specify which table to validate (not hardcoded to "data")
- **Live Data Validation**: Term validates actual database content, not just file exports
- **Immediate Feedback**: Validation results show exactly which constraints pass or fail

## Next Steps

Now that you understand database connections, you're ready to:

1. **Continue Learning**: Try the [Working with Multiple Databases](06-multi-database-validation.md) tutorial
2. **Apply Your Knowledge**: See [How to Connect to PostgreSQL](../how-to/connect-postgresql.md) for production databases
3. **Go Deeper**: Read [Understanding Database Connectors](../explanation/database-connectors.md) to understand the architecture

## Exercises

1. **Exercise 1**: Add a validation check that ensures all customer ages are between 18 and 80
2. **Exercise 2**: Create a second table called `orders` and validate relationships between customers and orders
3. **Challenge**: Set up validation for a multi-table database with foreign key constraints

---