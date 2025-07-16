# Tutorial: Getting Started with Term

## What You'll Learn

In this tutorial, you will:
- Install Term and set up your first Rust project
- Create your first data validation constraint
- Run a validation and interpret the results
- Understand the basic concepts of constraints and validation suites

## What You'll Need

Before starting this tutorial, make sure you have:
- Rust 1.70 or later installed ([install Rust](https://rustup.rs/))
- A text editor or IDE with Rust support
- Basic familiarity with Rust syntax

**Time to complete:** ~20 minutes

## Getting Started

Let's create a new Rust project and add Term as a dependency. We'll build a simple data quality validator that checks if a CSV file meets our quality requirements.

## Step 1: Create a New Project

First, let's create a new Rust project:

```bash
cargo new my-validator
cd my-validator
```

Now add Term to your `Cargo.toml`:

```toml
[dependencies]
term-core = "0.0.1"
tokio = { version = "1.0", features = ["full"] }
```

Run `cargo build` to download the dependencies:

```bash
cargo build
```

You should see Cargo downloading Term and its dependencies.

> 💡 **What's happening here?** We're adding Term (the data validation library) and Tokio (for async runtime) to our project. Term uses async/await for efficient data processing.

## Step 2: Create Your First Constraint

Now let's write our first validation. Open `src/main.rs` and replace its contents with:

```rust
use term_core::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    // Create a validation context from a CSV file
    let ctx = SessionContext::new();
    
    // Create a simple CSV for demonstration
    let csv_data = "name,age,email
Alice,25,alice@example.com
Bob,30,bob@example.com
Charlie,35,charlie@example.com";
    
    // Register the CSV data
    ctx.register_csv_from_string("users", csv_data).await?;
    
    println!("Data loaded successfully!");
    Ok(())
}
```

Run this code:
```bash
cargo run
```

You should see:
```
Data loaded successfully!
```

> 💡 **What's happening here?** We created a SessionContext (Term's data container) and loaded CSV data into it. The data is now ready for validation.

## Step 3: Add a Completeness Check

Let's check if our data has any missing values. Modify your code:

```rust
use term_core::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    // Create a validation context
    let ctx = SessionContext::new();
    
    // Create sample data with a missing value
    let csv_data = "name,age,email
Alice,25,alice@example.com
Bob,,bob@example.com
Charlie,35,charlie@example.com";
    
    // Register the CSV data
    ctx.register_csv_from_string("users", csv_data).await?;
    
    // Create a completeness constraint
    let constraint = UnifiedCompletenessConstraint::complete("age");
    
    // Run the validation
    let result = constraint.evaluate(&ctx).await?;
    
    println!("Validation result: {:?}", result.status);
    println!("Message: {}", result.message.unwrap_or_default());
    
    Ok(())
}
```

### Try It Yourself

Before running, predict what will happen. We have one missing age value - will the validation pass or fail?

<details>
<summary>💡 Hint</summary>

The completeness constraint checks if ALL values in a column are non-null. With one missing value, what do you think will happen?

</details>

Run the code:
```bash
cargo run
```

You should see that the validation failed because of the missing age value.

## Step 4: Building a Validation Suite

Real-world validation involves checking multiple constraints. Let's create a validation suite:

```rust
use term_core::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    // Create a validation context
    let ctx = SessionContext::new();
    
    // Create sample data
    let csv_data = "name,age,email
Alice,25,alice@example.com
Bob,130,bob@example.com
Charlie,35,charlie@example.com";
    
    // Register the CSV data
    ctx.register_csv_from_string("users", csv_data).await?;
    
    // Create a validation suite with multiple checks
    let suite = ValidationSuite::builder("User Data Quality")
        .check(
            Check::builder("Data Completeness")
                .is_complete("name")
                .is_complete("age")
                .is_complete("email")
                .build()
        )
        .check(
            Check::builder("Data Validity")
                .has_max("age", Assertion::LessThan(120.0))
                .build()
        )
        .build();
    
    // Run the validation suite
    let report = suite.run(&ctx).await?;
    
    // Display results
    println!("Validation Report: {}", report.name);
    println!("Status: {}", if report.is_success() { "✅ PASSED" } else { "❌ FAILED" });
    
    for check_result in &report.check_results {
        println!("\nCheck: {}", check_result.check_name);
        for constraint_result in &check_result.constraint_results {
            println!("  - {}: {}", 
                constraint_result.constraint_name,
                constraint_result.result.status
            );
        }
    }
    
    Ok(())
}
```

> ⚠️ **Common Mistake:** Notice how Bob's age is 130? This will cause the maximum age constraint to fail. This demonstrates how Term catches data quality issues.

## Step 5: Understanding the Results

Run the complete example:

```bash
cargo run
```

You'll see a detailed report showing which validations passed and which failed. The suite checks:
1. All fields are complete (no missing values)
2. Age values are reasonable (less than 120)

## What You've Learned

Congratulations! You've learned how to:

✅ Set up a Rust project with Term  
✅ Load data into a validation context  
✅ Create and run individual constraints  
✅ Build validation suites with multiple checks  
✅ Interpret validation results

### Key Takeaways

- **Constraints**: Rules that check specific properties of your data
- **Validation Context**: Container for your data (SessionContext)
- **Validation Suite**: Collection of related checks
- **Checks**: Groups of related constraints

## Next Steps

Now that you understand the basics, you're ready to:

1. **Continue Learning**: Try the [Working with Constraints](./02-basic-constraints.md) tutorial
2. **Apply Your Knowledge**: See [How to Validate CSV Files](../how-to/validate-csv-files.md)
3. **Go Deeper**: Read [Understanding Constraints](../explanation/constraint-design.md)

## Exercises

1. **Exercise 1**: Modify the age constraint to check that ages are between 0 and 120
2. **Exercise 2**: Add an email validation constraint using `PatternConstraint`
3. **Challenge**: Create a suite that validates a product catalog with price, name, and SKU fields

---