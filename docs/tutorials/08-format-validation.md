# Tutorial: Data Format Validation

## Introduction

In this tutorial, you'll learn how to use Term's comprehensive format validators to ensure data quality and consistency. We'll explore validators for emails, URLs, credit cards, phone numbers, and more, understanding how to apply them effectively to your datasets.

## What You'll Learn

- How to validate common data formats (email, URL, phone, etc.)
- How to set validation thresholds for partial compliance
- How to create custom format validators
- How to combine multiple format validations

## Prerequisites

Before starting, you should:
- Have Term installed and configured
- Understand basic Term validation concepts
- Have sample data with various format types

## Step 1: Understanding Format Validation

Format validation ensures data follows expected patterns. Term provides both built-in validators and custom pattern matching:

```rust
use term_guard::prelude::*;

// Built-in validators use optimized patterns
check.validates_email("email_column", 1.0)  // 100% must be valid

// Custom validators use regex patterns  
check.validates_regex("product_code", r"^PRD-\d{6}$", 0.95)  // 95% threshold
```

## Step 2: Email Validation

Let's start by validating email addresses in a customer dataset:

```rust
use term_guard::prelude::*;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    // Load customer data
    let ctx = SessionContext::new();
    ctx.register_csv(
        "customers",
        "data/customers.csv",
        CsvReadOptions::default()
    ).await?;

    // Create validation suite for emails
    let suite = ValidationSuite::builder("customer_email_validation")
        .add_check(
            Check::builder("validate_primary_emails")
                .description("Ensure all primary emails are valid")
                .validates_email("primary_email", 1.0)  // 100% must be valid
                .build()
        )
        .add_check(
            Check::builder("validate_secondary_emails")
                .description("Check secondary emails (allowing some invalid)")
                .validates_email("secondary_email", 0.95)  // 95% threshold
                .build()
        )
        .build();

    // Run validation
    let results = suite.run(&ctx).await?;
    
    // Check results
    for check_result in results.check_results() {
        match check_result.status {
            CheckStatus::Success => {
                println!("✓ {}: All emails valid", check_result.check.name());
            }
            CheckStatus::Warning(msg) => {
                println!("⚠ {}: {}", check_result.check.name(), msg);
            }
            CheckStatus::Error(msg) => {
                println!("✗ {}: {}", check_result.check.name(), msg);
                
                // Get specific validation metrics
                if let Some(metrics) = check_result.constraint_results.first() {
                    println!("  Valid: {}/{} ({:.1}%)", 
                             metrics.num_passed,
                             metrics.num_validated,
                             metrics.compliance_rate * 100.0);
                }
            }
        }
    }

    Ok(())
}
```

## Step 3: URL and Web Link Validation

Validate URLs with different strictness levels:

```rust
async fn validate_web_links(ctx: &SessionContext) -> Result<()> {
    let suite = ValidationSuite::builder("web_link_validation")
        .add_check(
            Check::builder("product_urls")
                .description("Validate product page URLs")
                .validates_url_with_options(
                    "product_url",
                    0.99,  // 99% must be valid
                    UrlValidationOptions {
                        require_protocol: true,    // Must have http/https
                        require_tld: true,         // Must have .com, .org, etc
                        allow_localhost: false,    // No localhost URLs
                        allow_ip_address: false,   // No IP addresses
                        allowed_protocols: vec!["https".to_string()],  // HTTPS only
                    }
                )
                .build()
        )
        .add_check(
            Check::builder("api_endpoints")
                .description("Validate API endpoint URLs")
                .validates_url_with_options(
                    "api_endpoint",
                    1.0,
                    UrlValidationOptions {
                        require_protocol: true,
                        require_tld: false,        // Allow internal domains
                        allow_localhost: true,     // Allow localhost for dev
                        allow_ip_address: true,    // Allow IP addresses
                        allowed_protocols: vec!["http".to_string(), "https".to_string()],
                    }
                )
                .build()
        )
        .build();

    let results = suite.run(ctx).await?;
    results.print_summary();
    
    Ok(())
}
```

## Step 4: Financial Data Validation

Validate credit cards and financial identifiers:

```rust
async fn validate_payment_data(ctx: &SessionContext) -> Result<()> {
    let suite = ValidationSuite::builder("payment_validation")
        .add_check(
            Check::builder("credit_card_numbers")
                .description("Validate credit card numbers with Luhn algorithm")
                .validates_credit_card("card_number", 1.0)  // All must be valid
                .build()
        )
        .add_check(
            Check::builder("card_type_consistency")
                .description("Ensure card numbers match their types")
                .satisfies(
                    "CASE 
                        WHEN card_type = 'VISA' THEN card_number LIKE '4%'
                        WHEN card_type = 'MASTERCARD' THEN 
                            card_number LIKE '51%' OR 
                            card_number LIKE '52%' OR 
                            card_number LIKE '53%' OR 
                            card_number LIKE '54%' OR 
                            card_number LIKE '55%'
                        WHEN card_type = 'AMEX' THEN 
                            card_number LIKE '34%' OR 
                            card_number LIKE '37%'
                        ELSE true 
                    END",
                    "Card numbers must match their type prefix",
                    1.0
                )
                .build()
        )
        .build();

    let results = suite.run(ctx).await?;
    
    Ok(())
}
```

## Step 5: Phone Number Validation

Validate phone numbers with regional options:

```rust
async fn validate_contact_numbers(ctx: &SessionContext) -> Result<()> {
    let suite = ValidationSuite::builder("phone_validation")
        .add_check(
            Check::builder("us_phone_numbers")
                .description("Validate US phone numbers")
                .validates_phone_with_options(
                    "phone",
                    0.98,  // 98% threshold
                    PhoneValidationOptions {
                        country: Some("US".to_string()),
                        allow_international: false,
                        require_country_code: false,
                        formats: vec![
                            r"^\d{3}-\d{3}-\d{4}$".to_string(),      // 555-123-4567
                            r"^\(\d{3}\) \d{3}-\d{4}$".to_string(),  // (555) 123-4567
                            r"^\d{10}$".to_string(),                 // 5551234567
                        ],
                    }
                )
                .build()
        )
        .add_check(
            Check::builder("international_phones")
                .description("Validate international phone numbers")
                .validates_phone_with_options(
                    "intl_phone",
                    0.95,
                    PhoneValidationOptions {
                        country: None,  // Any country
                        allow_international: true,
                        require_country_code: true,
                        formats: vec![],  // Use default international formats
                    }
                )
                .build()
        )
        .build();

    let results = suite.run(ctx).await?;
    
    Ok(())
}
```

## Step 6: Identifier Validation

Validate various types of identifiers:

```rust
async fn validate_identifiers(ctx: &SessionContext) -> Result<()> {
    let suite = ValidationSuite::builder("identifier_validation")
        .add_check(
            Check::builder("uuid_validation")
                .description("Validate UUID formats")
                .validates_uuid("transaction_id", 1.0)
                .build()
        )
        .add_check(
            Check::builder("ipv4_addresses")
                .description("Validate IPv4 addresses")
                .validates_ipv4("client_ip", 0.99)
                .build()
        )
        .add_check(
            Check::builder("ipv6_addresses")
                .description("Validate IPv6 addresses")
                .validates_ipv6("server_ipv6", 0.95)
                .build()
        )
        .add_check(
            Check::builder("postal_codes")
                .description("Validate US postal codes")
                .validates_postal_code("zip_code", "US", 0.99)
                .build()
        )
        .build();

    let results = suite.run(ctx).await?;
    
    Ok(())
}
```

## Step 7: Structured Data Validation

Validate JSON and datetime formats:

```rust
async fn validate_structured_data(ctx: &SessionContext) -> Result<()> {
    let suite = ValidationSuite::builder("structured_data_validation")
        .add_check(
            Check::builder("json_payloads")
                .description("Validate JSON request/response payloads")
                .validates_json("request_body", 1.0)
                .validates_json("response_body", 0.99)  // Allow 1% errors
                .build()
        )
        .add_check(
            Check::builder("iso_timestamps")
                .description("Validate ISO 8601 datetime formats")
                .validates_iso8601_datetime("created_at", 1.0)
                .validates_iso8601_datetime("updated_at", 1.0)
                .build()
        )
        .build();

    let results = suite.run(ctx).await?;
    
    // Advanced: Parse and validate JSON structure
    if results.is_success() {
        validate_json_schema(ctx).await?;
    }
    
    Ok(())
}

async fn validate_json_schema(ctx: &SessionContext) -> Result<()> {
    // Validate specific JSON structure
    let suite = ValidationSuite::builder("json_schema_validation")
        .add_check(
            Check::builder("api_response_structure")
                .satisfies(
                    r#"
                    json_extract_path_text(response_body, '$.status') IS NOT NULL
                    AND json_extract_path_text(response_body, '$.data') IS NOT NULL
                    AND json_extract_path_text(response_body, '$.timestamp') IS NOT NULL
                    "#,
                    "API responses must have status, data, and timestamp fields",
                    1.0
                )
                .build()
        )
        .build();
    
    suite.run(ctx).await
}
```

## Step 8: Custom Format Validation

Create custom validators for domain-specific formats:

```rust
async fn validate_custom_formats(ctx: &SessionContext) -> Result<()> {
    let suite = ValidationSuite::builder("custom_format_validation")
        .add_check(
            Check::builder("product_codes")
                .description("Validate product code format")
                .validates_regex(
                    "product_code",
                    r"^[A-Z]{3}-\d{4}-[A-Z0-9]{2}$",  // e.g., ABC-1234-X1
                    1.0
                )
                .build()
        )
        .add_check(
            Check::builder("employee_ids")
                .description("Validate employee ID format")
                .validates_regex_with_options(
                    "employee_id",
                    r"^(EMP|CTR|TMP)\d{6}$",  // EMP123456, CTR123456, TMP123456
                    0.99,
                    RegexValidationOptions {
                        case_sensitive: true,
                        multiline: false,
                        dot_matches_newline: false,
                    }
                )
                .build()
        )
        .add_check(
            Check::builder("version_numbers")
                .description("Validate semantic version numbers")
                .validates_regex(
                    "version",
                    r"^\d+\.\d+\.\d+(-[a-zA-Z0-9]+)?$",  // 1.2.3 or 1.2.3-beta
                    0.95
                )
                .build()
        )
        .build();

    let results = suite.run(ctx).await?;
    
    Ok(())
}
```

## Step 9: Combining Format Validations

Create comprehensive validation suites:

```rust
async fn comprehensive_validation(ctx: &SessionContext) -> Result<()> {
    let suite = ValidationSuite::builder("user_data_validation")
        .add_check(
            Check::builder("user_contact_info")
                .description("Validate all user contact information")
                .is_complete("email")  // No nulls
                .validates_email("email", 1.0)  // Valid emails
                .is_unique("email")  // No duplicates
                .validates_phone("phone", 0.95)  // Valid phone (95% threshold)
                .has_length_between("username", 3, 20)  // Username length
                .validates_regex("username", r"^[a-zA-Z0-9_]+$", 1.0)  // Alphanumeric
                .build()
        )
        .add_check(
            Check::builder("user_profile_urls")
                .description("Validate user profile links")
                .validates_url("website", 0.90)  // Personal websites
                .validates_url("linkedin_url", 0.95)  // LinkedIn profiles
                .validates_regex(
                    "twitter_handle",
                    r"^@[a-zA-Z0-9_]{1,15}$",
                    0.95
                )
                .build()
        )
        .build();

    let results = suite.run(ctx).await?;
    
    // Generate detailed report
    generate_validation_report(&results);
    
    Ok(())
}

fn generate_validation_report(results: &ValidationResult) {
    println!("\n╔══════════════════════════════════════╗");
    println!("║     Format Validation Report         ║");
    println!("╚══════════════════════════════════════╝");
    
    for check_result in results.check_results() {
        println!("\n► {}", check_result.check.name());
        println!("  Status: {:?}", check_result.status);
        
        for constraint_result in &check_result.constraint_results {
            if let Some(name) = &constraint_result.constraint_name {
                println!("  - {}: {:.1}% compliant ({}/{})",
                         name,
                         constraint_result.compliance_rate * 100.0,
                         constraint_result.num_passed,
                         constraint_result.num_validated);
            }
        }
    }
}
```

## Step 10: Exercise - Building a Data Quality Pipeline

Now let's build a complete data quality pipeline with format validation:

```rust
async fn data_quality_pipeline(ctx: &SessionContext) -> Result<()> {
    // Step 1: Profile the data
    let profiler = ColumnProfiler::builder().build();
    let email_profile = profiler.profile_column(ctx, "users", "email").await?;
    
    // Step 2: Generate suggested validations
    let suggestions = SuggestionEngine::new()
        .add_rule(Box::new(FormatDetectionRule::new()))
        .suggest_constraints(&email_profile);
    
    // Step 3: Build validation suite from suggestions
    let mut builder = ValidationSuite::builder("auto_generated_validations");
    
    for suggestion in suggestions {
        if suggestion.confidence > 0.8 {  // High confidence suggestions
            builder = builder.add_suggested_check(suggestion);
        }
    }
    
    // Step 4: Add custom business rules
    builder = builder.add_check(
        Check::builder("business_rules")
            .satisfies(
                "email NOT LIKE '%@competitor.com'",
                "No competitor email addresses",
                1.0
            )
            .build()
    );
    
    // Step 5: Run validation
    let suite = builder.build();
    let results = suite.run(ctx).await?;
    
    // Step 6: Store results for monitoring
    let repository = InMemoryRepository::new();
    repository.save(
        ResultKey::new()
            .with_timestamp(Utc::now())
            .with_tag("pipeline", "format_validation"),
        results.to_analyzer_context()
    ).await?;
    
    Ok(())
}
```

## Summary

You've learned how to:
- ✅ Use built-in format validators for common data types
- ✅ Configure validation thresholds for partial compliance
- ✅ Create custom format validators with regex
- ✅ Combine multiple format validations
- ✅ Build comprehensive data quality pipelines

## Next Steps

- Explore [Format Validator Reference](../reference/format-validators.md) for all available validators
- Learn about [Custom Constraint Development](../how-to/write-custom-constraints.md)
- See [Performance Optimization](../how-to/optimize-performance.md) for large-scale validation

## Troubleshooting

**Q: Why are valid emails being marked as invalid?**
A: Check for leading/trailing whitespace. Use string trimming in your data pipeline.

**Q: How do I handle international formats?**
A: Use the `_with_options` variants to specify country codes and formats.

**Q: Can I validate multiple formats in one column?**
A: Use custom regex with alternation: `(pattern1|pattern2|pattern3)`

## Exercises

1. Create a validator for social security numbers (with appropriate masking)
2. Build a validator for IBAN (International Bank Account Numbers)
3. Implement a validator for scientific notation numbers
4. Create a comprehensive validation suite for an e-commerce dataset