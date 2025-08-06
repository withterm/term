# How to Build Complex Data Validation Suites

## Goal

You'll learn to build comprehensive data validation suites that combine multiple constraint types for robust data quality checking in production systems.

## Prerequisites

Before you begin, ensure you have:
- [ ] Term v0.0.1 or later installed
- [ ] Rust 1.70 or later
- [ ] DataFusion dependency added to your project
- [ ] Basic understanding of Term's Check and ValidationSuite concepts

## Quick Solution

```rust
use term_guard::core::{ValidationSuite, Check, Level};
use term_guard::constraints::*;

// Complete validation suite for e-commerce data
let suite = ValidationSuite::builder("ecommerce_validation")
    .with_optimizer(true)
    .check(Check::builder("customer_data")
        .level(Level::Error)
        .constraint(FormatConstraint::email("email", 0.99)?)
        .constraint(UnifiedUniquenessConstraint::single("customer_id", UniquenessOptions::strict())?)
        .build())
    .check(Check::builder("order_data")
        .level(Level::Error)
        .constraint(StatisticalConstraint::min("order_total", Assertion::GreaterThan(0.0))?)
        .constraint(FormatConstraint::iso8601_datetime("order_date", 0.99)?)
        .build())
    .build();
```

## Step-by-Step Guide

### Step 1: Plan Your Validation Strategy

Start by identifying the different types of data quality issues you need to catch:

```rust
use term_guard::core::{ValidationSuite, Check, Level};
use term_guard::constraints::*;

// Group related validations into logical checks
let customer_check = Check::builder("customer_data_quality")
    .level(Level::Error)
    .description("Validate customer information integrity")
    .build();
```

### Step 2: Add Format Validations

Add format constraints for structured data like emails, phones, and dates:

```rust
let customer_check = Check::builder("customer_data_quality")
    .level(Level::Error)
    .description("Validate customer information integrity")
    // Email validation with high threshold
    .constraint(FormatConstraint::email("email", 0.99)?)
    // Phone validation for US numbers
    .constraint(FormatConstraint::phone("phone", 0.85, Some("US".to_string()))?)
    .build();
```

### Step 3: Add Uniqueness and Completeness Constraints

Ensure data integrity with uniqueness and completeness checks:

```rust
let customer_check = Check::builder("customer_data_quality")
    .level(Level::Error)
    .description("Validate customer information integrity")
    // Format validations
    .constraint(FormatConstraint::email("email", 0.99)?)
    .constraint(FormatConstraint::phone("phone", 0.85, Some("US".to_string()))?)
    // Uniqueness validations
    .constraint(UnifiedUniquenessConstraint::single("customer_id", UniquenessOptions::strict())?)
    // Completeness validations
    .constraint(UnifiedCompletenessConstraint::single("customer_id", 1.0)?)
    .constraint(UnifiedCompletenessConstraint::single("email", 0.95)?)
    .build();
```

### Step 4: Add Statistical Constraints

Include statistical validations for numeric data:

```rust
let order_check = Check::builder("order_validation")
    .level(Level::Error)
    .description("Validate order transaction data")
    // Order amounts must be positive and reasonable
    .constraint(StatisticalConstraint::min("order_total", Assertion::GreaterThan(0.0))?)
    .constraint(StatisticalConstraint::max("order_total", Assertion::LessThan(10000.0))?)
    // Order dates must be valid
    .constraint(FormatConstraint::iso8601_datetime("order_date", 0.99)?)
    .build();
```

### Step 5: Combine Into Validation Suite

Create the complete validation suite with optimization enabled:

```rust
let suite = ValidationSuite::builder("comprehensive_validation")
    .with_optimizer(true)  // Enable query optimization
    .check(customer_check)
    .check(order_check)
    .build();
```

## Complete Example

Here's the complete code for a comprehensive e-commerce validation suite:

```rust
use term_guard::core::{ValidationSuite, Check, Level};
use term_guard::constraints::*;
use datafusion::prelude::SessionContext;

async fn create_ecommerce_validation_suite() -> Result<ValidationSuite, Box<dyn std::error::Error>> {
    // Customer data validation
    let customer_check = Check::builder("customer_data")
        .level(Level::Error)
        .description("Validate customer information integrity")
        // Identity validation
        .constraint(UnifiedUniquenessConstraint::single("customer_id", UniquenessOptions::strict())?)
        .constraint(FormatConstraint::email("email", 0.99)?)
        .constraint(FormatConstraint::phone("phone", 0.85, Some("US".to_string()))?)
        // Completeness requirements
        .constraint(UnifiedCompletenessConstraint::single("customer_id", 1.0)?)
        .constraint(UnifiedCompletenessConstraint::single("email", 0.95)?)
        .build();

    // Order data validation
    let order_check = Check::builder("order_data")
        .level(Level::Error)
        .description("Validate order transaction data")
        // Order integrity
        .constraint(UnifiedUniquenessConstraint::single("order_id", UniquenessOptions::strict())?)
        .constraint(StatisticalConstraint::min("order_total", Assertion::GreaterThan(0.0))?)
        .constraint(StatisticalConstraint::max("order_total", Assertion::LessThan(10000.0))?)
        // Date validation
        .constraint(FormatConstraint::iso8601_datetime("order_date", 0.99)?)
        .build();

    // Product data validation
    let product_check = Check::builder("product_data")
        .level(Level::Warning)
        .description("Validate product catalog data")
        // Product codes
        .constraint(FormatConstraint::new(
            "product_sku",
            FormatType::Regex(r"^[A-Z]{3}-\d{6}$".to_string()),
            0.95,
            FormatOptions::strict()
        )?)
        // Pricing validation
        .constraint(StatisticalConstraint::min("price", Assertion::GreaterThan(0.01))?)
        .constraint(StatisticalConstraint::mean("price", Assertion::Between(10.0, 500.0))?)
        .build();

    let suite = ValidationSuite::builder("ecommerce_validation")
        .with_optimizer(true)  // Enable query optimization
        .check(customer_check)
        .check(order_check)
        .check(product_check)
        .build();

    Ok(suite)
}

async fn run_validation() -> Result<(), Box<dyn std::error::Error>> {
    let suite = create_ecommerce_validation_suite().await?;
    
    let ctx = SessionContext::new();
    // Register your data tables here
    
    let results = suite.run(&ctx).await?;
    
    match results {
        ValidationResult::Success { report, metrics } => {
            println!("✅ All validations passed!");
            println!("Checks run: {}", metrics.total_checks);
        }
        ValidationResult::Failure { report } => {
            println!("❌ Validation failed!");
            for issue in &report.issues {
                println!("{}: {}", issue.check_name, issue.message);
            }
        }
    }
    
    Ok(())
}
```

## Variations

### Financial Data Validation

For financial data with stricter requirements:

```rust
let financial_check = Check::builder("financial_validation")
    .level(Level::Error)
    // No credit card numbers in descriptions (PII detection)
    .constraint(FormatConstraint::credit_card("description", 0.0, true)?)
    // Account numbers must be unique and properly formatted
    .constraint(UnifiedUniquenessConstraint::single("account_number", UniquenessOptions::strict())?)
    .constraint(FormatConstraint::new(
        "account_number",
        FormatType::Regex(r"^\d{8,12}$".to_string()),
        1.0,
        FormatOptions::strict()
    )?)
    .build();
```

### API Input Validation

For validating single records from API inputs:

```rust
let api_validation = Check::builder("api_input_validation")
    .level(Level::Error)
    // Strict validation for single records
    .constraint(FormatConstraint::new(
        "email",
        FormatType::Email,
        1.0,  // Must be 100% valid for single record
        FormatOptions::strict()
    )?)
    .constraint(StatisticalConstraint::min("age", Assertion::GreaterThanOrEqual(13.0))?)
    .constraint(StatisticalConstraint::max("age", Assertion::LessThanOrEqual(120.0))?)
    .build();
```

## Verification

To verify that your validation suite works correctly:

1. Check that all constraints compiled without errors
2. Confirm the suite runs without panics on sample data
3. You should see appropriate ValidationResult::Success or ValidationResult::Failure
4. Error messages should be descriptive and actionable

## Troubleshooting

### Problem: Validation Suite Runs Too Slowly
**Solution:** Enable the query optimizer with `.with_optimizer(true)` and group related constraints in the same check.

### Problem: Too Many False Positives
**Solution:** Adjust thresholds (e.g., 0.95 instead of 1.0) and use appropriate FormatOptions for your data quality.

### Problem: Missing Validation Results
**Solution:** Ensure all data tables are properly registered with the SessionContext before running the suite.

## Performance Considerations

- Group related constraints in the same check to reduce table scans
- Use the query optimizer for better performance with multiple constraints
- Consider using approximate statistics for large datasets
- Cache validation suites when possible to reuse compiled constraints

## Related Guides

- [How to Validate Specific Data Types](./format-validation.md) (for format-specific validation)
- [How to Handle Validation Errors](./error-handling.md) (for error processing)
- [Tutorial: Building Your First Validation Suite] (for learning basics)
- [Reference: Constraint API] (for detailed parameter information)