# Term Unified Constraints - Comprehensive Examples

This document provides comprehensive examples for using Term's unified constraint API, covering common patterns and advanced use cases.

## Table of Contents

1. [Basic Format Validation](#basic-format-validation)
2. [Advanced Format Options](#advanced-format-options)
3. [Uniqueness Validation](#uniqueness-validation)
4. [Statistical Validation](#statistical-validation)
5. [Completeness Validation](#completeness-validation)
6. [Complex Validation Suites](#complex-validation-suites)
7. [Performance Optimization](#performance-optimization)
8. [Error Handling Patterns](#error-handling-patterns)
9. [Real-World Use Cases](#real-world-use-cases)

## Basic Format Validation

### Email Validation

```rust
use term_core::core::{ValidationSuite, Check, Level};
use term_core::constraints::FormatConstraint;
use datafusion::prelude::*;

async fn validate_emails() -> Result<(), Box<dyn std::error::Error>> {
    // Basic email validation
    let email_check = Check::builder("email_validation")
        .level(Level::Error)
        .description("Ensure email addresses are valid")
        // Require 95% of emails to be valid
        .constraint(FormatConstraint::email("email", 0.95)?)
        // Multiple email columns
        .constraint(FormatConstraint::email("primary_email", 0.99)?)
        .constraint(FormatConstraint::email("secondary_email", 0.80)?)
        .build();

    let suite = ValidationSuite::builder("email_suite")
        .check(email_check)
        .build();

    // Setup DataFusion context with your data
    let ctx = SessionContext::new();
    // ... register your data as "data" table ...

    let results = suite.run(&ctx).await?;
    println!("Email validation results: {:?}", results);
    
    Ok(())
}
```

### Phone Number Validation

```rust
use term_core::constraints::FormatConstraint;

async fn validate_phones() -> Result<(), Box<dyn std::error::Error>> {
    let phone_checks = vec![
        // US phone numbers
        FormatConstraint::phone("us_phone", 0.95, Some("US".to_string()))?,
        // UK phone numbers  
        FormatConstraint::phone("uk_phone", 0.95, Some("UK".to_string()))?,
        // International E.164 format
        FormatConstraint::phone("intl_phone", 0.90, None)?,
    ];

    let check = Check::builder("phone_validation")
        .level(Level::Warning)
        .constraints(phone_checks.into_iter().map(|c| std::sync::Arc::new(c)).collect())
        .build();

    Ok(())
}
```

### URL and Web Address Validation

```rust
use term_core::constraints::FormatConstraint;

async fn validate_urls() -> Result<(), Box<dyn std::error::Error>> {
    let url_checks = vec![
        // Production URLs (no localhost)
        FormatConstraint::url("website", 0.95, false)?,
        // Development URLs (localhost allowed)
        FormatConstraint::url("dev_url", 0.80, true)?,
        // API endpoints
        FormatConstraint::url("api_endpoint", 0.99, false)?,
    ];

    Ok(())
}
```

## Advanced Format Options

### Case-Insensitive and Flexible Validation

```rust
use term_core::constraints::{FormatConstraint, FormatType, FormatOptions};

async fn flexible_validation() -> Result<(), Box<dyn std::error::Error>> {
    // Lenient email validation (case insensitive, trimming, nulls allowed)
    let flexible_email = FormatConstraint::new(
        "customer_email",
        FormatType::Email,
        0.90,
        FormatOptions::lenient()
    )?;

    // Strict validation (case sensitive, no nulls, no trimming)
    let strict_email = FormatConstraint::new(
        "admin_email",
        FormatType::Email,
        0.99,
        FormatOptions::strict()
    )?;

    // Custom configuration
    let custom_validation = FormatConstraint::new(
        "product_code",
        FormatType::Regex(r"^[A-Z]{2}\d{4}$".to_string()),
        0.98,
        FormatOptions::new()
            .case_sensitive(false)      // Allow lowercase
            .trim_before_check(true)    // Remove whitespace
            .null_is_valid(false)       // Nulls are invalid
    )?;

    Ok(())
}
```

### Enhanced Check Builder Methods

```rust
use term_core::core::Check;
use term_core::constraints::FormatOptions;

fn enhanced_builder_example() -> Check {
    Check::builder("enhanced_validation")
        .level(Level::Error)
        // Basic format methods (backward compatible)
        .validates_email("email", 0.95)
        .validates_url("website", 0.90, false)
        .validates_phone("phone", 0.85, Some("US".to_string()))
        // Enhanced methods with options
        .validates_email_with_options(
            "backup_email",
            0.80,
            FormatOptions::lenient()
        )
        .validates_url_with_options(
            "dev_server",
            0.70,
            true,  // allow localhost
            FormatOptions::case_insensitive()
        )
        .validates_phone_with_options(
            "mobile",
            0.95,
            Some("US".to_string()),
            FormatOptions::strict()
        )
        .validates_regex_with_options(
            "order_id",
            r"^ORD-\d{8}$",
            0.99,
            FormatOptions::with_trimming()
        )
        .build()
}
```

## Uniqueness Validation

### Single Column Uniqueness

```rust
use term_core::constraints::{UnifiedUniquenessConstraint, UniquenessOptions, NullHandling};

async fn single_column_uniqueness() -> Result<(), Box<dyn std::error::Error>> {
    // Basic uniqueness (100% unique)
    let basic_unique = UnifiedUniquenessConstraint::single(
        "user_id",
        UniquenessOptions::default()
    )?;

    // Uniqueness with null handling
    let unique_with_nulls = UnifiedUniquenessConstraint::single(
        "email",
        UniquenessOptions::new()
            .null_handling(NullHandling::Exclude)  // Don't count nulls
            .threshold(0.99)  // 99% of non-null values must be unique
    )?;

    Ok(())
}
```

### Multi-Column Uniqueness (Composite Keys)

```rust
use term_core::constraints::{UnifiedUniquenessConstraint, UniquenessOptions};

async fn composite_key_uniqueness() -> Result<(), Box<dyn std::error::Error>> {
    // Composite primary key
    let composite_key = UnifiedUniquenessConstraint::multiple(
        vec!["customer_id", "order_date", "product_id"],
        UniquenessOptions::new()
            .threshold(1.0)  // Must be 100% unique
            .null_handling(NullHandling::Fail)  // Any nulls cause failure
    )?;

    // Flexible composite key
    let flexible_key = UnifiedUniquenessConstraint::multiple(
        vec!["region", "category", "subcategory"],
        UniquenessOptions::new()
            .threshold(0.95)  // Allow some duplicates
            .null_handling(NullHandling::Exclude)
    )?;

    Ok(())
}
```

## Statistical Validation

### Basic Statistical Constraints

```rust
use term_core::constraints::{StatisticalConstraint, StatisticType, Assertion};

async fn statistical_validation() -> Result<(), Box<dyn std::error::Error>> {
    let stat_checks = vec![
        // Age validation
        StatisticalConstraint::min("age", Assertion::GreaterThanOrEqual(0.0))?,
        StatisticalConstraint::max("age", Assertion::LessThanOrEqual(120.0))?,
        StatisticalConstraint::mean("age", Assertion::Between(25.0, 65.0))?,
        
        // Order value validation
        StatisticalConstraint::sum("order_total", Assertion::GreaterThan(10000.0))?,
        StatisticalConstraint::std_dev("price", Assertion::LessThan(50.0))?,
    ];

    Ok(())
}
```

### Advanced Statistical Analysis

```rust
use term_core::constraints::{UnifiedStatisticalConstraint, StatisticType};

async fn advanced_statistics() -> Result<(), Box<dyn std::error::Error>> {
    // Multiple statistics on same column
    let comprehensive_stats = UnifiedStatisticalConstraint::new(
        "response_time_ms",
        vec![
            (StatisticType::Min, Assertion::GreaterThanOrEqual(0.0)),
            (StatisticType::Max, Assertion::LessThan(5000.0)),
            (StatisticType::Mean, Assertion::Between(100.0, 1000.0)),
            (StatisticType::StandardDeviation, Assertion::LessThan(500.0)),
        ]
    )?;

    Ok(())
}
```

## Completeness Validation

### Single Column Completeness

```rust
use term_core::constraints::UnifiedCompletenessConstraint;

async fn completeness_validation() -> Result<(), Box<dyn std::error::Error>> {
    let completeness_checks = vec![
        // Critical fields must be 100% complete
        UnifiedCompletenessConstraint::single("user_id", 1.0)?,
        UnifiedCompletenessConstraint::single("order_id", 1.0)?,
        
        // Optional fields can have some nulls
        UnifiedCompletenessConstraint::single("phone", 0.80)?,
        UnifiedCompletenessConstraint::single("secondary_email", 0.60)?,
    ];

    Ok(())
}
```

### Multi-Column Completeness Strategies

```rust
use term_core::constraints::{UnifiedCompletenessConstraint, CompletenessStrategy};

async fn multi_column_completeness() -> Result<(), Box<dyn std::error::Error>> {
    // At least one contact method required
    let contact_completeness = UnifiedCompletenessConstraint::multiple(
        vec!["email", "phone", "address"],
        0.95,
        CompletenessStrategy::AtLeastOne
    )?;

    // All address fields required together
    let address_completeness = UnifiedCompletenessConstraint::multiple(
        vec!["street", "city", "state", "zip"],
        0.90,
        CompletenessStrategy::AllOrNone
    )?;

    Ok(())
}
```

## Complex Validation Suites

### E-commerce Data Validation

```rust
use term_core::core::{ValidationSuite, Check, Level};
use term_core::constraints::*;

async fn ecommerce_validation_suite() -> Result<ValidationSuite, Box<dyn std::error::Error>> {
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
```

### Financial Data Validation

```rust
async fn financial_validation_suite() -> Result<ValidationSuite, Box<dyn std::error::Error>> {
    // Account validation
    let account_check = Check::builder("account_validation")
        .level(Level::Error)
        .description("Validate financial account data")
        // Account numbers
        .constraint(UnifiedUniquenessConstraint::single("account_number", UniquenessOptions::strict())?)
        .constraint(FormatConstraint::new(
            "account_number",
            FormatType::Regex(r"^\d{8,12}$".to_string()),
            1.0,
            FormatOptions::strict()
        )?)
        // No credit card numbers in descriptions (PII detection)
        .constraint(FormatConstraint::credit_card("description", 0.0, true)?)
        .build();

    // Transaction validation
    let transaction_check = Check::builder("transaction_validation")
        .level(Level::Error)
        .description("Validate financial transactions")
        // Transaction amounts
        .constraint(StatisticalConstraint::min("amount", Assertion::GreaterThan(-1000000.0))?)
        .constraint(StatisticalConstraint::max("amount", Assertion::LessThan(1000000.0))?)
        // Transaction IDs must be unique
        .constraint(UnifiedUniquenessConstraint::single("transaction_id", UniquenessOptions::strict())?)
        // Timestamps
        .constraint(FormatConstraint::iso8601_datetime("timestamp", 1.0)?)
        .build();

    let suite = ValidationSuite::builder("financial_validation")
        .with_optimizer(true)
        .check(account_check)
        .check(transaction_check)
        .build();

    Ok(suite)
}
```

## Performance Optimization

### Optimized Validation Suite

```rust
use term_core::core::ValidationSuite;
use term_core::constraints::*;

async fn optimized_validation() -> Result<(), Box<dyn std::error::Error>> {
    // Group related constraints in same check for optimization
    let grouped_check = Check::builder("grouped_validation")
        .level(Level::Error)
        // Group all format validations together
        .constraint(FormatConstraint::email("email", 0.95)?)
        .constraint(FormatConstraint::phone("phone", 0.90, Some("US".to_string()))?)
        .constraint(FormatConstraint::url("website", 0.85, false)?)
        // Group all statistical validations together  
        .constraint(StatisticalConstraint::min("age", Assertion::GreaterThanOrEqual(0.0))?)
        .constraint(StatisticalConstraint::max("age", Assertion::LessThanOrEqual(120.0))?)
        .constraint(StatisticalConstraint::mean("income", Assertion::Between(20000.0, 200000.0))?)
        .build();

    let suite = ValidationSuite::builder("optimized_suite")
        .with_optimizer(true)  // Enable query optimizer
        .check(grouped_check)
        .build();

    // The optimizer will combine compatible constraints into single queries
    // reducing table scans and improving performance significantly

    Ok(())
}
```

### Reusing Format Constraints for Better Performance

```rust
use std::sync::Arc;
use term_core::constraints::FormatConstraint;

async fn reuse_constraints() -> Result<(), Box<dyn std::error::Error>> {
    // Create reusable constraints (patterns are cached)
    let email_constraint = Arc::new(FormatConstraint::email("email", 0.95)?);
    let phone_constraint = Arc::new(FormatConstraint::phone("phone", 0.90, Some("US".to_string()))?);

    // Use in multiple checks
    let check1 = Check::builder("user_validation")
        .constraint(email_constraint.clone())
        .constraint(phone_constraint.clone())
        .build();

    let check2 = Check::builder("admin_validation")
        .constraint(email_constraint.clone())
        .constraint(phone_constraint.clone())
        .build();

    // Regex patterns are compiled once and cached for reuse
    
    Ok(())
}
```

## Error Handling Patterns

### Comprehensive Error Handling

```rust
use term_core::core::{ValidationResult, ValidationSuite};
use term_core::error::TermError;

async fn handle_validation_errors() -> Result<(), Box<dyn std::error::Error>> {
    let suite = ValidationSuite::builder("error_handling_example")
        // ... add checks ...
        .build();

    let ctx = SessionContext::new();
    // ... setup data ...

    match suite.run(&ctx).await {
        Ok(ValidationResult::Success { report, metrics }) => {
            println!("✅ Validation passed!");
            println!("Checks run: {}", metrics.total_checks);
            println!("Constraints evaluated: {}", metrics.total_constraints);
            
            // Process successful results
            for check_result in &report.check_results {
                println!("✅ {} passed", check_result.check_name);
            }
        }
        
        Ok(ValidationResult::Failure { report }) => {
            println!("❌ Validation failed!");
            
            // Handle validation failures
            for issue in &report.issues {
                match issue.level {
                    Level::Error => println!("❌ ERROR in {}: {}", issue.check_name, issue.message),
                    Level::Warning => println!("⚠️  WARNING in {}: {}", issue.check_name, issue.message),
                    Level::Info => println!("ℹ️  INFO in {}: {}", issue.check_name, issue.message),
                }
            }
            
            // Decide whether to continue processing based on severity
            let has_errors = report.issues.iter().any(|i| i.level == Level::Error);
            if has_errors {
                return Err("Critical validation errors found".into());
            }
        }
        
        Err(TermError::DataSource(msg)) => {
            println!("❌ Data source error: {}", msg);
            return Err("Failed to access data".into());
        }
        
        Err(TermError::SecurityError(msg)) => {
            println!("❌ Security validation error: {}", msg);
            return Err("Security constraint violated".into());
        }
        
        Err(other) => {
            println!("❌ Unexpected error: {}", other);
            return Err(other.into());
        }
    }

    Ok(())
}
```

### Graceful Degradation

```rust
async fn graceful_degradation() -> Result<(), Box<dyn std::error::Error>> {
    // Build validation suite with fallbacks
    let mut checks = Vec::new();
    
    // Try to add advanced constraints, fall back to basic ones
    match FormatConstraint::new(
        "email", 
        FormatType::Email, 
        0.95, 
        FormatOptions::strict()
    ) {
        Ok(constraint) => {
            checks.push(Check::builder("advanced_email")
                .constraint(constraint)
                .build());
        }
        Err(_) => {
            // Fallback to basic email validation
            checks.push(Check::builder("basic_email") 
                .validates_email("email", 0.95)
                .build());
        }
    }

    let suite = ValidationSuite::builder("graceful_suite")
        .checks(checks)
        .build();

    Ok(())
}
```

## Real-World Use Cases

### Data Pipeline Validation

```rust
use term_core::core::{ValidationSuite, Check, Level};
use term_core::sources::CsvDataSource;

async fn data_pipeline_validation() -> Result<(), Box<dyn std::error::Error>> {
    // Setup data source
    let data_source = CsvDataSource::new("data/user_exports.csv");
    let ctx = SessionContext::new();
    data_source.register(&ctx, "data").await?;

    // Create comprehensive validation suite for data pipeline
    let suite = ValidationSuite::builder("pipeline_validation")
        .with_optimizer(true)
        
        // Data completeness checks
        .check(Check::builder("data_completeness")
            .level(Level::Error)
            .description("Ensure critical data is present")
            .constraint(UnifiedCompletenessConstraint::single("user_id", 1.0)?)
            .constraint(UnifiedCompletenessConstraint::single("created_at", 1.0)?)
            .constraint(UnifiedCompletenessConstraint::single("email", 0.95)?)
            .build())
            
        // Data quality checks
        .check(Check::builder("data_quality")
            .level(Level::Warning)
            .description("Validate data formats and ranges")
            .constraint(FormatConstraint::email("email", 0.98)?)
            .constraint(FormatConstraint::iso8601_datetime("created_at", 0.99)?)
            .constraint(StatisticalConstraint::min("age", Assertion::GreaterThanOrEqual(0.0))?)
            .constraint(StatisticalConstraint::max("age", Assertion::LessThanOrEqual(150.0))?)
            .build())
            
        // Business rule validation
        .check(Check::builder("business_rules")
            .level(Level::Error)
            .description("Validate business constraints")
            .constraint(UnifiedUniquenessConstraint::single("user_id", UniquenessOptions::strict())?)
            .constraint(StatisticalConstraint::min("account_balance", Assertion::GreaterThanOrEqual(-1000.0))?)
            .build())
            
        .build();

    // Run validation
    let results = suite.run(&ctx).await?;
    
    match results {
        ValidationResult::Success { .. } => {
            println!("✅ Data pipeline validation passed - proceeding with processing");
        }
        ValidationResult::Failure { report } => {
            println!("❌ Data pipeline validation failed");
            
            // Log detailed failure information
            for issue in &report.issues {
                if issue.level == Level::Error {
                    eprintln!("CRITICAL: {}", issue.message);
                }
            }
            
            // Stop pipeline on critical errors
            let critical_errors = report.issues.iter()
                .filter(|i| i.level == Level::Error)
                .count();
                
            if critical_errors > 0 {
                return Err(format!("Pipeline stopped: {} critical validation errors", critical_errors).into());
            }
        }
    }

    Ok(())
}
```

### API Input Validation

```rust
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
struct UserRegistration {
    email: String,
    phone: Option<String>,
    age: u32,
    website: Option<String>,
}

async fn validate_api_input(registration: &UserRegistration) -> Result<(), Box<dyn std::error::Error>> {
    // Create in-memory validation data
    let ctx = SessionContext::new();
    
    // Convert API input to DataFrame for validation
    // (implementation details omitted for brevity)
    
    let validation_check = Check::builder("api_input_validation")
        .level(Level::Error)
        .description("Validate user registration data")
        
        // Email validation with strict requirements
        .constraint(FormatConstraint::new(
            "email",
            FormatType::Email,
            1.0,  // Must be 100% valid for single record
            FormatOptions::strict()
        )?)
        
        // Phone validation if provided
        .constraint(FormatConstraint::new(
            "phone",
            FormatType::Phone { country: Some("US".to_string()) },
            1.0,
            FormatOptions::new().null_is_valid(true)  // Optional field
        )?)
        
        // Age validation
        .constraint(StatisticalConstraint::min("age", Assertion::GreaterThanOrEqual(13.0))?)
        .constraint(StatisticalConstraint::max("age", Assertion::LessThanOrEqual(120.0))?)
        
        // Website validation if provided
        .constraint(FormatConstraint::new(
            "website",
            FormatType::Url { allow_localhost: false },
            1.0,
            FormatOptions::new().null_is_valid(true)
        )?)
        
        .build();

    let suite = ValidationSuite::builder("api_validation")
        .check(validation_check)
        .build();

    let results = suite.run(&ctx).await?;
    
    match results {
        ValidationResult::Success { .. } => {
            println!("✅ User registration data is valid");
        }
        ValidationResult::Failure { report } => {
            let errors: Vec<String> = report.issues
                .into_iter()
                .map(|issue| issue.message)
                .collect();
            
            return Err(format!("Invalid registration data: {}", errors.join(", ")).into());
        }
    }

    Ok(())
}
```

This comprehensive examples document covers the most common use cases and patterns for Term's unified constraint API, providing developers with practical guidance for implementing robust data validation in their applications.