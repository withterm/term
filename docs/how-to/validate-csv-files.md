# How to Validate CSV Files

## Goal

You'll learn to validate CSV file data quality using Term's file-based data sources and comprehensive constraint validation.

## Prerequisites

Before you begin, ensure you have:
- [ ] Term v0.0.1 or later installed
- [ ] Rust 1.70 or later
- [ ] A CSV file to validate (we'll provide sample data)
- [ ] Basic understanding of Term's Check and ValidationSuite concepts

## Quick Solution

```rust
use term_guard::core::{ValidationSuite, Check, Level};
use term_guard::constraints::*;
use term_guard::sources::CsvDataSource;
use datafusion::prelude::SessionContext;

// Complete CSV validation example
async fn validate_csv() -> Result<(), Box<dyn std::error::Error>> {
    let data_source = CsvDataSource::new("data/customers.csv");
    let ctx = SessionContext::new();
    data_source.register(&ctx, "data").await?;

    let suite = ValidationSuite::builder("csv_validation")
        .check(Check::builder("data_quality")
            .constraint(CompletenessConstraint::new("email", 0.95)?)
            .constraint(FormatConstraint::email("email", 0.99)?)
            .constraint(UniquenessConstraint::new("customer_id")?)
            .build())
        .build();

    let results = suite.run(&ctx).await?;
    println!("Validation results: {:?}", results);
    Ok(())
}
```

## Step-by-Step Guide

### Step 1: Set Up Your CSV Data Source

First, configure Term to read from your CSV file:

```rust
use term_guard::sources::CsvDataSource;
use datafusion::prelude::SessionContext;

let data_source = CsvDataSource::new("path/to/your/file.csv")
    .with_header(true)  // CSV has header row
    .with_delimiter(b',');  // Use comma delimiter

let ctx = SessionContext::new();
data_source.register(&ctx, "data").await?;
```

### Step 2: Create Data Quality Checks

Define validation checks appropriate for CSV data:

```rust
use term_guard::core::{ValidationSuite, Check, Level};
use term_guard::constraints::*;

let completeness_check = Check::builder("completeness")
    .level(Level::Error)
    .description("Ensure required fields are present")
    .constraint(CompletenessConstraint::new("customer_id", 1.0)?)
    .constraint(CompletenessConstraint::new("email", 0.95)?)
    .build();

let format_check = Check::builder("format_validation")
    .level(Level::Warning)
    .description("Validate data formats")
    .constraint(FormatConstraint::email("email", 0.98)?)
    .constraint(FormatConstraint::phone("phone", 0.80, Some("US".to_string()))?)
    .build();
```

### Step 3: Add Business Rule Validation

Include domain-specific validation rules:

```rust
let business_check = Check::builder("business_rules")
    .level(Level::Error)
    .description("Validate business constraints")
    .constraint(UniquenessConstraint::new("customer_id")?)
    .constraint(StatisticalConstraint::min("age", Assertion::GreaterThanOrEqual(0.0))?)
    .constraint(StatisticalConstraint::max("age", Assertion::LessThanOrEqual(150.0))?)
    .build();
```

### Step 4: Run the Complete Validation

Combine all checks and execute validation:

```rust
let suite = ValidationSuite::builder("csv_validation")
    .with_optimizer(true)
    .check(completeness_check)
    .check(format_check)
    .check(business_check)
    .build();

let results = suite.run(&ctx).await?;

match results {
    ValidationResult::Success { report, metrics } => {
        println!("✅ CSV validation passed!");
        println!("Rows processed: {}", metrics.rows_processed);
    }
    ValidationResult::Failure { report } => {
        println!("❌ CSV validation failed:");
        for issue in &report.issues {
            println!("  - {}: {}", issue.check_name, issue.message);
        }
    }
}
```

## Complete Example

Here's the complete code for CSV file validation:

```rust
use term_guard::core::{ValidationSuite, Check, Level, ValidationResult};
use term_guard::constraints::*;
use term_guard::sources::CsvDataSource;
use datafusion::prelude::SessionContext;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set up CSV data source
    let data_source = CsvDataSource::new("data/customers.csv")
        .with_header(true)
        .with_delimiter(b',');

    let ctx = SessionContext::new();
    data_source.register(&ctx, "data").await?;

    // Create comprehensive validation suite
    let suite = ValidationSuite::builder("csv_validation")
        .with_optimizer(true)
        
        // Data completeness
        .check(Check::builder("completeness")
            .level(Level::Error)
            .constraint(CompletenessConstraint::new("customer_id", 1.0)?)
            .constraint(CompletenessConstraint::new("email", 0.95)?)
            .constraint(CompletenessConstraint::new("first_name", 0.98)?)
            .build())
            
        // Format validation
        .check(Check::builder("formats")
            .level(Level::Warning)
            .constraint(FormatConstraint::email("email", 0.98)?)
            .constraint(FormatConstraint::phone("phone", 0.80, Some("US".to_string()))?)
            .build())
            
        // Business rules
        .check(Check::builder("business_rules")
            .level(Level::Error)
            .constraint(UniquenessConstraint::new("customer_id")?)
            .constraint(StatisticalConstraint::min("age", Assertion::GreaterThanOrEqual(0.0))?)
            .constraint(StatisticalConstraint::max("age", Assertion::LessThanOrEqual(150.0))?)
            .build())
            
        .build();

    // Run validation
    let results = suite.run(&ctx).await?;

    // Process results
    match results {
        ValidationResult::Success { report, metrics } => {
            println!("✅ CSV validation passed!");
            println!("Processed {} rows across {} checks", 
                     metrics.rows_processed, metrics.total_checks);
            
            for check_result in &report.check_results {
                println!("  ✅ {} - {} constraints passed", 
                         check_result.check_name, 
                         check_result.constraint_results.len());
            }
        }
        ValidationResult::Failure { report } => {
            println!("❌ CSV validation failed!");
            
            for issue in &report.issues {
                match issue.level {
                    Level::Error => println!("  ❌ ERROR: {} - {}", issue.check_name, issue.message),
                    Level::Warning => println!("  ⚠️  WARNING: {} - {}", issue.check_name, issue.message),
                    Level::Info => println!("  ℹ️  INFO: {} - {}", issue.check_name, issue.message),
                }
            }
            
            // Exit with error code for critical failures
            let has_errors = report.issues.iter().any(|i| i.level == Level::Error);
            if has_errors {
                std::process::exit(1);
            }
        }
    }

    Ok(())
}
```

## Variations

### Large CSV Files

For large CSV files, enable streaming and configure memory limits:

```rust
let data_source = CsvDataSource::new("large_file.csv")
    .with_header(true)
    .with_batch_size(10000)  // Process in 10k row batches
    .with_schema_inference(true);
```

### Custom Delimiters

For non-standard CSV formats:

```rust
// Tab-separated values
let data_source = CsvDataSource::new("data.tsv")
    .with_delimiter(b'\t')
    .with_quote(b'"')
    .with_escape(b'\\');

// Pipe-delimited
let data_source = CsvDataSource::new("data.txt")
    .with_delimiter(b'|');
```

### Validation with Custom Schema

When you need explicit column types:

```rust
use datafusion::arrow::datatypes::{Schema, Field, DataType};

let schema = Schema::new(vec![
    Field::new("customer_id", DataType::Int64, false),
    Field::new("email", DataType::Utf8, true),
    Field::new("age", DataType::Int32, true),
]);

let data_source = CsvDataSource::new("data.csv")
    .with_schema(Arc::new(schema));
```

## Verification

To verify that your CSV validation works correctly:

1. Check that the CSV file loads without DataFusion errors
2. Confirm all defined constraints execute successfully
3. You should see appropriate success/failure messages
4. Error messages should clearly identify data quality issues

## Troubleshooting

### Problem: "No such file or directory" Error
**Solution:** Ensure the CSV file path is correct and the file exists. Use absolute paths if needed.

### Problem: CSV Parsing Errors
**Solution:** Check delimiter, quote character, and encoding settings. Use `.with_has_header(false)` if no header row.

### Problem: Memory Issues with Large Files
**Solution:** Reduce batch size with `.with_batch_size()` and enable streaming processing.

### Problem: Schema Inference Issues
**Solution:** Explicitly define schema with `.with_schema()` for complex data types.

## Performance Considerations

- Use appropriate batch sizes for memory efficiency
- Enable the query optimizer for multiple constraints
- Consider sampling large files for initial validation runs
- Use approximate constraints for very large datasets

## Security Considerations

- Validate file paths to prevent path traversal attacks
- Sanitize CSV content if processing user-uploaded files
- Use appropriate file permissions for sensitive data
- Consider encrypting CSV files at rest

## Related Guides

- [How to Optimize Performance](./optimize-performance.md) (for large CSV files)
- [How to Use Cloud Storage](./use-cloud-storage.md) (for S3/GCS CSV files)
- [Tutorial: Getting Started](../tutorials/01-getting-started.md) (for learning basics)
- [Reference: Data Sources](../reference/data-sources.md) (for CSV configuration options)