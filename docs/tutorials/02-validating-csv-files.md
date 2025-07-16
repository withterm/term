# Tutorial: Validating CSV Files

In this tutorial, you'll learn how to validate CSV files using Term. We'll start with basic CSV loading and progress to handling complex validation scenarios commonly found in real-world data.

## What You'll Learn

- Loading CSV files with different options
- Handling headers and data types  
- Common CSV validation patterns
- Error handling and reporting
- Performance optimization for large CSV files

## Prerequisites

- Completed the [Getting Started](01-getting-started.md) tutorial
- Basic understanding of CSV format
- A Rust project with Term dependencies

## Step 1: Basic CSV Loading

Let's start by creating a simple CSV file to validate:

```csv
user_id,name,email,age,signup_date
1,Alice Johnson,alice@example.com,28,2024-01-15
2,Bob Smith,bob@example.com,35,2024-01-16
3,Charlie Brown,charlie@invalid-email,42,2024-01-17
4,,missing@example.com,25,2024-01-18
5,Eve Davis,eve@example.com,-5,2024-01-19
```

Save this as `users.csv` in your project directory.

Now let's write code to load and validate this CSV:

```rust
use term_guard::prelude::*;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    // Create a DataFusion context
    let ctx = SessionContext::new();
    
    // Load the CSV file
    ctx.register_csv("users", "users.csv", CsvReadOptions::new()).await?;
    
    // Quick test to ensure data loaded
    let df = ctx.sql("SELECT COUNT(*) FROM users").await?;
    df.show().await?;
    
    Ok(())
}
```

## Step 2: Configuring CSV Options

CSV files come in many formats. Let's configure the CSV reader for different scenarios:

```rust
use datafusion::dataframe::DataFrameWriteOptions;

// Configure CSV options for different formats
let csv_options = CsvReadOptions::new()
    .has_header(true)           // First row contains headers
    .delimiter(b',')            // Comma-separated (default)
    .quote(b'"')               // Quote character for fields
    .escape(b'\\')             // Escape character
    .schema_infer_max_records(1000); // Rows to scan for schema

// Load CSV with custom options
ctx.register_csv("users", "users.csv", csv_options).await?;

// For tab-separated files
let tsv_options = CsvReadOptions::new()
    .delimiter(b'\t')
    .has_header(true);

// For files without headers
let no_header_options = CsvReadOptions::new()
    .has_header(false)
    .schema(&Schema::new(vec![
        Field::new("col1", DataType::Utf8, true),
        Field::new("col2", DataType::Int64, true),
        Field::new("col3", DataType::Float64, true),
    ]));
```

## Step 3: Basic Validations

Now let's add some basic validations for our user data:

```rust
let validation_suite = ValidationSuite::builder("User CSV Validation")
    .with_check(
        Check::builder("Critical Fields")
            .is_complete("user_id")    // No missing user IDs
            .is_unique("user_id")       // No duplicate users
            .is_complete("email")       // Email is required
            .build()
    )
    .with_check(
        Check::builder("Data Quality")
            .has_pattern("email", r"^[^@]+@[^@]+\.[^@]+$", 0.95) // 95% valid emails
            .has_min("age", 0)          // Age must be positive
            .has_max("age", 120)        // Reasonable age limit
            .is_not_null("name", 0.90)  // 90% should have names
            .build()
    )
    .build();

// Run validation
let results = validation_suite.run(&ctx).await?;

// Display results
println!("{}", results.to_string());
```

## Step 4: Handling Common CSV Issues

### Missing Values

CSV files often have missing or empty values. Here's how to handle them:

```rust
let missing_value_check = Check::builder("Missing Value Handling")
    // Check completeness with thresholds
    .completeness("name", CompletenessOptions::threshold(0.90).into())
    .completeness("email", CompletenessOptions::full().into())
    
    // Check for empty strings (different from NULL)
    .custom_sql(
        "no_empty_strings",
        "SELECT COUNT(*) as empty_count FROM users WHERE name = ''",
        |df| async move {
            let empty_count = df.collect().await?[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0);
            
            if empty_count > 0 {
                ValidationResult::new(
                    Status::Failure,
                    Some(format!("{} empty name fields found", empty_count))
                )
            } else {
                ValidationResult::success()
            }
        }
    )
    .build();
```

### Date Format Validation

Dates in CSV files can have various formats:

```rust
let date_validation = Check::builder("Date Validation")
    // Check date format consistency
    .has_pattern("signup_date", r"^\d{4}-\d{2}-\d{2}$", 1.0) // YYYY-MM-DD format
    
    // Validate date ranges
    .custom_sql(
        "reasonable_signup_dates",
        "SELECT COUNT(*) FROM users 
         WHERE signup_date < '2020-01-01' 
            OR signup_date > CURRENT_DATE",
        |df| async move {
            let count = get_count_from_df(df).await?;
            if count > 0 {
                ValidationResult::new(
                    Status::Warning,
                    Some(format!("{} records with suspicious dates", count))
                )
            } else {
                ValidationResult::success()
            }
        }
    )
    .build();
```

### Encoding Issues

Handle different character encodings:

```rust
// For UTF-8 with BOM or different encodings
use encoding_rs::UTF_8;
use std::fs;

// Read file and convert encoding if needed
let file_contents = fs::read("users_utf16.csv")?;
let (cow, encoding_used, had_errors) = UTF_8.decode(&file_contents);

if had_errors {
    eprintln!("Warning: Encoding errors detected");
}

// Write to temporary UTF-8 file
fs::write("temp_utf8.csv", cow.as_bytes())?;

// Now load the UTF-8 version
ctx.register_csv("users", "temp_utf8.csv", CsvReadOptions::new()).await?;
```

## Step 5: Performance Optimization

For large CSV files, optimize your validation approach:

### Sampling Large Files

```rust
// For files > 1GB, validate a sample first
let sample_validation = Check::builder("Sample Validation")
    .custom_sql(
        "validate_sample",
        "SELECT * FROM users TABLESAMPLE (10 PERCENT)",
        |df| async move {
            // Run validations on 10% sample
            let row_count = df.count().await?;
            println!("Validating {} sample rows", row_count);
            ValidationResult::success()
        }
    )
    .build();
```

### Chunked Processing

```rust
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};

async fn validate_csv_in_chunks(file_path: &str, chunk_size: usize) -> Result<()> {
    let file = File::open(file_path).await?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    
    let mut chunk_num = 0;
    let mut current_chunk = Vec::new();
    
    while let Some(line) = lines.next_line().await? {
        current_chunk.push(line);
        
        if current_chunk.len() >= chunk_size {
            // Process chunk
            process_chunk(&current_chunk, chunk_num).await?;
            current_chunk.clear();
            chunk_num += 1;
        }
    }
    
    // Process remaining lines
    if !current_chunk.is_empty() {
        process_chunk(&current_chunk, chunk_num).await?;
    }
    
    Ok(())
}
```

### Memory-Efficient Configuration

```rust
// Configure DataFusion for large files
let config = SessionConfig::new()
    .with_batch_size(8192)              // Larger batches
    .with_target_partitions(4)          // Parallel processing
    .with_repartition_file_scans(true); // Optimize file scanning

let ctx = SessionContext::with_config(config);
```

## Step 6: Advanced CSV Validation Patterns

### Cross-Column Validation

```rust
let cross_column_check = Check::builder("Cross-Column Validation")
    // Email should contain part of the name
    .custom_sql(
        "email_name_consistency",
        "SELECT COUNT(*) FROM users 
         WHERE LOWER(email) NOT LIKE CONCAT('%', LOWER(SPLIT_PART(name, ' ', 1)), '%')",
        |df| async move {
            let count = get_count_from_df(df).await?;
            if count > 0 {
                ValidationResult::new(
                    Status::Warning,
                    Some(format!("{} emails don't match user names", count))
                )
            } else {
                ValidationResult::success()
            }
        }
    )
    .build();
```

### Duplicate Detection with Fuzzy Matching

```rust
let duplicate_check = Check::builder("Fuzzy Duplicate Detection")
    // Exact duplicates
    .is_unique("email")
    
    // Similar names (potential duplicates)
    .custom_sql(
        "similar_names",
        "SELECT a.name as name1, b.name as name2
         FROM users a
         JOIN users b ON a.user_id < b.user_id
         WHERE levenshtein(LOWER(a.name), LOWER(b.name)) <= 2",
        |df| async move {
            let similar_pairs = df.count().await?;
            if similar_pairs > 0 {
                ValidationResult::new(
                    Status::Warning,
                    Some(format!("{} potential duplicate names found", similar_pairs))
                )
            } else {
                ValidationResult::success()
            }
        }
    )
    .build();
```

## Step 7: Creating a Reusable CSV Validator

Let's create a reusable function for CSV validation:

```rust
use std::path::Path;

pub struct CsvValidator {
    validations: ValidationSuite,
}

impl CsvValidator {
    pub fn new() -> Self {
        let validations = ValidationSuite::builder("CSV Validator")
            .with_check(
                Check::builder("Schema Validation")
                    .has_columns(vec!["user_id", "name", "email", "age", "signup_date"])
                    .build()
            )
            .with_check(
                Check::builder("Data Integrity")
                    .is_complete("user_id")
                    .is_unique("user_id")
                    .is_not_null("email", 0.95)
                    .build()
            )
            .with_check(
                Check::builder("Business Rules")
                    .has_pattern("email", r"^[^@]+@[^@]+\.[^@]+$", 0.95)
                    .has_min("age", 0)
                    .has_max("age", 120)
                    .build()
            )
            .build();
            
        Self { validations }
    }
    
    pub async fn validate_file(&self, file_path: &Path) -> Result<ValidationReport> {
        // Create context and load CSV
        let ctx = SessionContext::new();
        let options = CsvReadOptions::new().has_header(true);
        
        ctx.register_csv("data", file_path.to_str().unwrap(), options).await?;
        
        // Run validations
        let results = self.validations.run(&ctx).await?;
        
        // Generate report
        Ok(ValidationReport {
            file_path: file_path.to_string_lossy().to_string(),
            total_rows: self.get_row_count(&ctx).await?,
            results,
        })
    }
    
    async fn get_row_count(&self, ctx: &SessionContext) -> Result<usize> {
        let df = ctx.sql("SELECT COUNT(*) FROM data").await?;
        let batches = df.collect().await?;
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0) as usize;
        Ok(count)
    }
}

pub struct ValidationReport {
    pub file_path: String,
    pub total_rows: usize,
    pub results: ValidationResult,
}

impl ValidationReport {
    pub fn print_summary(&self) {
        println!("CSV Validation Report");
        println!("====================");
        println!("File: {}", self.file_path);
        println!("Total Rows: {}", self.total_rows);
        println!("Status: {:?}", self.results.status());
        println!("\nDetails:");
        println!("{}", self.results.to_string());
    }
}
```

## Step 8: Error Handling Best Practices

Handle common CSV errors gracefully:

```rust
use term_guard::error::TermError;

async fn safe_csv_validation(file_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    
    // Handle file not found
    if !Path::new(file_path).exists() {
        return Err(format!("File not found: {}", file_path).into());
    }
    
    // Try to load CSV with error handling
    match ctx.register_csv("data", file_path, CsvReadOptions::new()).await {
        Ok(_) => println!("CSV loaded successfully"),
        Err(e) => {
            eprintln!("Failed to load CSV: {}", e);
            
            // Try with different options
            println!("Attempting with semicolon delimiter...");
            let alt_options = CsvReadOptions::new().delimiter(b';');
            ctx.register_csv("data", file_path, alt_options).await?;
        }
    }
    
    // Run validations with timeout
    let validation_result = tokio::time::timeout(
        std::time::Duration::from_secs(300), // 5 minute timeout
        run_validations(&ctx)
    ).await??;
    
    Ok(())
}

async fn run_validations(ctx: &SessionContext) -> Result<ValidationResult> {
    // Your validation logic here
    let suite = ValidationSuite::builder("CSV Validation")
        .with_check(Check::builder("Basic").is_not_empty("data").build())
        .build();
        
    suite.run(ctx).await
}
```

## Exercises

1. **Exercise 1**: Modify the validator to handle pipe-delimited (|) files
2. **Exercise 2**: Add validation for phone numbers in international format
3. **Exercise 3**: Create a custom constraint that checks if dates are business days
4. **Exercise 4**: Implement a progress bar for large file validation
5. **Exercise 5**: Add support for compressed CSV files (.csv.gz)

## Summary

In this tutorial, you learned how to:

- Load CSV files with various configurations
- Handle common CSV data quality issues
- Optimize performance for large files
- Create reusable validation components
- Handle errors gracefully

## Next Steps

- Learn about [validating cloud storage data](../how-to/use-cloud-storage.md)
- Explore [custom constraint creation](../how-to/write-custom-constraints.md)
- Understand [performance optimization](../how-to/optimize-performance.md)

## Additional Resources

- [DataFusion CSV Documentation](https://arrow.apache.org/datafusion/user-guide/dataframe.html)
- [CSV Format RFC 4180](https://tools.ietf.org/html/rfc4180)
- [Term Constraints Reference](../reference/constraints.md)