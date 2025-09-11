# Tutorial: Getting Started with Term

> **Type**: Tutorial (Learning-oriented)
> **Audience**: Newcomers to Term
> **Goal**: Learn Term basics through hands-on validation

## What You'll Learn

In this tutorial, you will:
- Set up your first Term validation suite
- Create and run basic data quality checks
- Understand validation results and metrics
- Handle validation failures gracefully

## What You'll Need

Before starting this tutorial, make sure you have:
- Rust 1.70 or later installed
- A new Rust project created
- Basic familiarity with async Rust

**Time to complete:** ~20 minutes

## Getting Started

Let's build a simple data validation pipeline that checks the quality of customer data. This will introduce you to Term's core concepts: validation suites, checks, and constraints.

## Step 1: Adding Term to Your Project

First, let's add Term to your project dependencies.

```toml
[dependencies]
term = "0.0.1"
tokio = { version = "1.0", features = ["full"] }
```

Run this command to download the dependencies:
```bash
cargo build
```

You should see Cargo downloading and compiling Term and its dependencies.

> 💡 **What's happening here?** Term uses Tokio for async operations and DataFusion for efficient data processing. These dependencies enable Term to handle large datasets efficiently.

## Step 2: Creating Your First Validation Suite

Now let's create a validation suite to check data quality. A validation suite is a collection of checks that run together.

```rust
use term::core::{ValidationSuite, Check};
use term::sources::DataSource;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a new validation suite
    let suite = ValidationSuite::builder("customer_validation")
        .description("Validate customer data quality")
        .build();
    
    println!("Created validation suite: {}", suite.name());
    Ok(())
}
```

Run this code:
```bash
cargo run
```

You should see:
```
Created validation suite: customer_validation
```

> 💡 **What's happening here?** The ValidationSuite uses the builder pattern, which is common throughout Term. This pattern makes it easy to configure complex objects step by step.

## Step 3: Adding Data and Checks

Let's expand our validation to actually check some data. We'll add a CSV data source and create checks for it.

```rust
use term::core::{ValidationSuite, Check};
use term::sources::{DataSource, CsvSource};
use datafusion::prelude::SessionContext;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a DataFusion context
    let ctx = SessionContext::new();
    
    // Register a CSV data source
    let source = CsvSource::builder()
        .path("examples/data/customers.csv")
        .has_header(true)
        .build()?;
    
    source.register(&ctx, "customers").await?;
    
    // Create checks for the data
    let completeness_check = Check::new("email_completeness")
        .is_complete("email")
        .with_threshold(0.95);
    
    // Create validation suite with checks
    let suite = ValidationSuite::builder("customer_validation")
        .description("Validate customer data quality")
        .add_check(completeness_check)
        .build();
    
    println!("Suite has {} check(s)", suite.checks().len());
    Ok(())
}
```

### Try It Yourself

Before moving on, try modifying the threshold value from 0.95 to 0.99. What do you think will happen when we run the validation?

<details>
<summary>💡 Hint</summary>

A higher threshold means stricter validation. With 0.99, the check will fail if more than 1% of email values are missing.

</details>

## Step 4: Running Validations

Now let's actually run the validation and see the results.

```rust
use term::core::{ValidationSuite, Check, ValidationRunner};
use term::sources::{DataSource, CsvSource};
use datafusion::prelude::SessionContext;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    
    // Register data source
    let source = CsvSource::builder()
        .path("examples/data/customers.csv")
        .has_header(true)
        .build()?;
    
    source.register(&ctx, "customers").await?;
    
    // Create checks
    let email_check = Check::new("email_completeness")
        .is_complete("email")
        .with_threshold(0.95);
    
    let age_check = Check::new("age_validity")
        .is_non_negative("age");
    
    // Build suite with multiple checks
    let suite = ValidationSuite::builder("customer_validation")
        .add_check(email_check)
        .add_check(age_check)
        .build();
    
    // Run the validation
    let runner = ValidationRunner::new();
    let results = runner.run(&suite, &ctx).await?;
    
    // Display results
    for result in results.check_results() {
        println!("Check '{}': {:?}", 
            result.check_name(), 
            result.status()
        );
    }
    
    Ok(())
}
```

> ⚠️ **Common Mistake:** Forgetting to register the data source with the SessionContext before running validations. Always ensure your data is registered before creating checks.

## Step 5: Handling Validation Results

Let's learn how to properly handle validation results and take action based on them.

```rust
use term::core::{ValidationSuite, Check, ValidationRunner, ConstraintStatus};
use term::sources::{DataSource, CsvSource};
use datafusion::prelude::SessionContext;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    
    // Set up data source
    let source = CsvSource::builder()
        .path("examples/data/customers.csv")
        .has_header(true)
        .build()?;
    
    source.register(&ctx, "customers").await?;
    
    // Create comprehensive checks
    let suite = ValidationSuite::builder("customer_validation")
        .add_check(
            Check::new("email_completeness")
                .is_complete("email")
                .with_threshold(0.95)
        )
        .add_check(
            Check::new("age_validity")
                .is_non_negative("age")
        )
        .add_check(
            Check::new("id_uniqueness")
                .is_unique("customer_id")
        )
        .build();
    
    // Run validation
    let runner = ValidationRunner::new();
    let results = runner.run(&suite, &ctx).await?;
    
    // Process results
    let failed_checks: Vec<_> = results
        .check_results()
        .iter()
        .filter(|r| r.status() == &ConstraintStatus::Failure)
        .collect();
    
    if failed_checks.is_empty() {
        println!("✅ All validations passed!");
    } else {
        println!("❌ {} validation(s) failed:", failed_checks.len());
        for check in failed_checks {
            println!("  - {}: {}", 
                check.check_name(),
                check.message().unwrap_or("No details")
            );
        }
    }
    
    // Access metrics
    for result in results.check_results() {
        if let Some(metric) = result.metric() {
            println!("Metric for '{}': {:.2}", 
                result.check_name(), 
                metric
            );
        }
    }
    
    Ok(())
}
```

## What You've Learned

Congratulations! You've learned how to:

✅ Set up Term in a Rust project and configure dependencies  
✅ Create validation suites using the builder pattern  
✅ Add multiple types of checks to validate data quality  
✅ Run validations and interpret the results  
✅ Handle validation failures and extract metrics

### Key Takeaways

- **ValidationSuite**: Container for organizing related data quality checks
- **Check**: Defines what to validate with configurable thresholds
- **SessionContext**: DataFusion's execution context for data operations
- **ConstraintStatus**: Indicates whether a check passed or failed

## Next Steps

Now that you understand Term's basics, you're ready to:

1. **Continue Learning**: Try the [Building Complex Validations](complex-validations.md) tutorial
2. **Apply Your Knowledge**: See [How to Validate CSV Files](../how-to/validate-csv.md) for production use
3. **Go Deeper**: Read [Understanding Constraints](../explanation/constraints.md) to learn how Term works internally

## Exercises

1. **Exercise 1**: Add a check to ensure all email addresses contain an '@' symbol
2. **Exercise 2**: Create a validation suite for a different dataset with numeric constraints
3. **Challenge**: Implement a custom threshold that changes based on the day of the week

---

*This tutorial focused on learning Term's core concepts through hands-on experience. For specific task completion, see our [How-To Guides](../how-to/).*