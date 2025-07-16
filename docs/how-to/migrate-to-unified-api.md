# Term Unified Constraints Migration Guide

This guide helps you migrate from the deprecated individual constraint types to the new unified constraint API in Term.

## Why Migrate?

The unified constraint system provides:

- **Better Performance**: Single constraints can handle multiple related validations
- **Consistent APIs**: Uniform builder patterns across all constraint types  
- **Enhanced Features**: Rich configuration options and built-in caching
- **Future-Proof**: Active development focuses on unified constraints
- **Better Security**: Enhanced validation and ReDoS protection

## Migration Timeline

- **v0.2.0**: Unified constraints introduced, old constraints deprecated
- **v0.3.0**: Deprecation warnings added to old constraint types
- **v0.4.0** (planned): Old constraint types will be removed

## Quick Migration Reference

### Format/Pattern Constraints

| Old Constraint | New Unified API | Notes |
|----------------|-----------------|--------|
| `PatternConstraint` | `FormatConstraint::regex()` | Custom patterns |
| `EmailConstraint` | `FormatConstraint::email()` | Built-in email validation |
| `UrlConstraint` | `FormatConstraint::url()` | Localhost support option |
| `CreditCardConstraint` | `FormatConstraint::credit_card()` | Detection vs validation modes |

### Uniqueness Constraints

| Old Constraint | New Unified API | Notes |
|----------------|-----------------|--------|
| `UniquenessConstraint::single()` | `UnifiedUniquenessConstraint::single()` | Enhanced null handling |
| `UniquenessConstraint::multiple()` | `UnifiedUniquenessConstraint::multiple()` | Better composite key support |
| `PrimaryKeyConstraint` | `UnifiedUniquenessConstraint::primary_key()` | Combines uniqueness + completeness |

### Statistical Constraints  

| Old Constraint | New Unified API | Notes |
|----------------|-----------------|--------|
| `MinConstraint` | `StatisticalConstraint::min()` | Single method for all statistics |
| `MaxConstraint` | `StatisticalConstraint::max()` | |
| `MeanConstraint` | `StatisticalConstraint::mean()` | |
| `SumConstraint` | `StatisticalConstraint::sum()` | |
| `StandardDeviationConstraint` | `StatisticalConstraint::std_dev()` | |

### Completeness Constraints

| Old Constraint | New Unified API | Notes |
|----------------|-----------------|--------|
| `CompletenessConstraint` | `UnifiedCompletenessConstraint::single()` | Single column completeness |
| `MultipleCompletenessConstraint` | `UnifiedCompletenessConstraint::multiple()` | Multi-column strategies |

### Length Constraints

| Old Constraint | New Unified API | Notes |
|----------------|-----------------|--------|
| `MinLengthConstraint` | `LengthConstraint::min()` | Unified length validation |
| `MaxLengthConstraint` | `LengthConstraint::max()` | |

## Step-by-Step Migration

### 1. Format Constraints Migration

**Before:**
```rust
use term_guard::constraints::{PatternConstraint, EmailConstraint, UrlConstraint};

// Old individual constraints
let email_check = EmailConstraint::new("email", 0.95);
let phone_check = PatternConstraint::new("phone", r"^\d{3}-\d{3}-\d{4}$", 0.90)?;
let url_check = UrlConstraint::new("website", 0.85);
```

**After:**
```rust
use term_guard::constraints::FormatConstraint;

// New unified constraints
let email_check = FormatConstraint::email("email", 0.95)?;
let phone_check = FormatConstraint::phone("phone", 0.90, Some("US".to_string()))?;
let url_check = FormatConstraint::url("website", 0.85, false)?;
```

**With Advanced Options:**
```rust
use term_guard::constraints::{FormatConstraint, FormatOptions};

// Advanced configuration
let flexible_email = FormatConstraint::new(
    "email",
    term_guard::constraints::FormatType::Email,
    0.95,
    FormatOptions::lenient()  // case insensitive + trimming + nulls OK
)?;
```

### 2. Uniqueness Constraints Migration

**Before:**
```rust
use term_guard::constraints::UniquenessConstraint;

// Old uniqueness constraints
let unique_id = UniquenessConstraint::single("user_id")?;
let unique_combo = UniquenessConstraint::multiple(vec!["region", "customer_id"])?;
```

**After:**
```rust
use term_guard::constraints::{UnifiedUniquenessConstraint, UniquenessOptions};

// New unified uniqueness
let unique_id = UnifiedUniquenessConstraint::single("user_id", UniquenessOptions::default())?;
let unique_combo = UnifiedUniquenessConstraint::multiple(
    vec!["region", "customer_id"],
    UniquenessOptions::new().null_handling(
        term_guard::constraints::NullHandling::Exclude
    )
)?;
```

### 3. Statistical Constraints Migration

**Before:**
```rust
use term_guard::constraints::{MinConstraint, MaxConstraint, MeanConstraint};
use term_guard::constraints::Assertion;

// Old statistical constraints
let min_age = MinConstraint::new("age", Assertion::GreaterThanOrEqual(0.0))?;
let max_age = MaxConstraint::new("age", Assertion::LessThanOrEqual(120.0))?;
let avg_order = MeanConstraint::new("order_value", Assertion::Between(50.0, 500.0))?;
```

**After:**
```rust
use term_guard::constraints::{StatisticalConstraint, StatisticType, Assertion};

// New unified statistical constraints
let min_age = StatisticalConstraint::new("age", StatisticType::Min, Assertion::GreaterThanOrEqual(0.0))?;
let max_age = StatisticalConstraint::new("age", StatisticType::Max, Assertion::LessThanOrEqual(120.0))?;
let avg_order = StatisticalConstraint::new("order_value", StatisticType::Mean, Assertion::Between(50.0, 500.0))?;

// Or use convenience methods
let min_age = StatisticalConstraint::min("age", Assertion::GreaterThanOrEqual(0.0))?;
let max_age = StatisticalConstraint::max("age", Assertion::LessThanOrEqual(120.0))?;
let avg_order = StatisticalConstraint::mean("order_value", Assertion::Between(50.0, 500.0))?;
```

### 4. Check Builder Migration

**Before:**
```rust
use term_guard::core::Check;

let check = Check::builder("validation")
    .validates_email("email", 0.95)
    .validates_url("website", 0.90)
    .validates_phone("phone", 0.85)
    .build();
```

**After (Enhanced API):**
```rust
use term_guard::core::Check;
use term_guard::constraints::FormatOptions;

let check = Check::builder("validation")
    .validates_email_with_options("email", 0.95, FormatOptions::strict())
    .validates_url_with_options("website", 0.90, FormatOptions::default())
    .validates_phone_with_options("phone", 0.85, FormatOptions::lenient())
    .build();
```

## Automated Migration Script

Here's a sed-based script to help with basic migrations:

```bash
#!/bin/bash
# migrate_constraints.sh - Basic constraint migration helper

# Format constraints
sed -i 's/EmailConstraint::new(/FormatConstraint::email(/g' **/*.rs
sed -i 's/UrlConstraint::new(/FormatConstraint::url(/g' **/*.rs
sed -i 's/PatternConstraint::new(\([^,]*\), \([^,]*\), \([^)]*\))/FormatConstraint::regex(\1, \3, \2)/g' **/*.rs

# Add error handling where needed
echo "Manual review required for:"
echo "1. PatternConstraint parameter order changes"
echo "2. FormatOptions configuration"
echo "3. Error handling updates"
echo "4. Import statement updates"
```

## Common Migration Issues

### Issue 1: Parameter Order Changes

**Problem**: `PatternConstraint::new(column, pattern, threshold)` vs `FormatConstraint::regex(column, threshold, pattern)`

**Solution**: Update parameter order manually:
```rust
// Old
PatternConstraint::new("phone", r"^\d{3}-\d{3}-\d{4}$", 0.90)?

// New  
FormatConstraint::regex("phone", 0.90, r"^\d{3}-\d{3}-\d{4}$")?
```

### Issue 2: Error Handling Changes

**Problem**: Some old constraints didn't return `Result`, new ones do.

**Solution**: Add `?` operator and update error handling:
```rust
// Old
let constraint = EmailConstraint::new("email", 0.95);

// New
let constraint = FormatConstraint::email("email", 0.95)?;
```

### Issue 3: Import Statement Updates

**Problem**: Old import statements reference deprecated types.

**Solution**: Update imports:
```rust
// Old
use term_guard::constraints::{EmailConstraint, PatternConstraint, UrlConstraint};

// New
use term_guard::constraints::{FormatConstraint, FormatOptions};
```

### Issue 4: Configuration Options

**Problem**: Old constraints had limited configuration options.

**Solution**: Use FormatOptions for advanced configuration:
```rust
// Old (limited options)
let constraint = EmailConstraint::new("email", 0.95);

// New (with options)
let constraint = FormatConstraint::new(
    "email",
    term_guard::constraints::FormatType::Email,
    0.95,
    FormatOptions::new()
        .case_sensitive(false)
        .trim_before_check(true)
        .null_is_valid(true)
)?;
```

## Testing Your Migration

### 1. Compile-time Validation

```bash
# Check for deprecation warnings
cargo build 2>&1 | grep -i deprecated

# Check for compilation errors  
cargo check --all-targets
```

### 2. Runtime Validation

```rust
#[cfg(test)]
mod migration_tests {
    use super::*;
    use term_guard::test_utils::*;

    #[tokio::test]
    async fn test_migrated_constraints() {
        let ctx = create_test_context().await;
        
        // Test that migrated constraints produce same results
        let old_result = /* ... run old constraint ... */;
        let new_result = /* ... run new constraint ... */;
        
        assert_eq!(old_result.status, new_result.status);
        assert_eq!(old_result.metric, new_result.metric);
    }
}
```

### 3. Performance Comparison

```rust
use std::time::Instant;

fn benchmark_migration() {
    let start = Instant::now();
    // ... run old constraints ...
    let old_time = start.elapsed();

    let start = Instant::now();  
    // ... run new constraints ...
    let new_time = start.elapsed();

    println!("Old: {:?}, New: {:?}, Improvement: {:.2}x", 
             old_time, new_time, old_time.as_secs_f64() / new_time.as_secs_f64());
}
```

## Rollback Instructions

If you need to rollback to the old API:

### 1. Version Pinning

```toml
# Cargo.toml - pin to last version with old API
[dependencies]
term-guard = "=0.1.9"  # Last version before unified constraints
```

### 2. Feature Flags (if available)

```toml
[dependencies]
term-guard = { version = "0.2.0", features = ["legacy-constraints"] }
```

### 3. Conditional Compilation

```rust
#[cfg(feature = "legacy-constraints")]
use term_guard::constraints::EmailConstraint;

#[cfg(not(feature = "legacy-constraints"))]
use term_guard::constraints::FormatConstraint;
```

## FAQ

### Q: Will the old constraints be completely removed?

A: Yes, but not until v0.4.0. You have at least 2 major versions to migrate.

### Q: Are there any breaking changes in the unified API?

A: Minimal breaking changes. Main differences are:
- Some methods now return `Result` (error handling)
- Parameter order changes in a few cases  
- Import statement updates required

### Q: How do I handle FormatOptions migration?

A: Start with these convenience methods:
- `FormatOptions::default()` - same as old behavior
- `FormatOptions::strict()` - stricter validation  
- `FormatOptions::lenient()` - more flexible

### Q: What if I have custom constraints?

A: Custom constraints implementing the `Constraint` trait don't need changes. Consider if they could benefit from the unified approach.

### Q: How do I migrate complex validation suites?

A: Migrate incrementally:
1. Start with format constraints (easiest)
2. Move to statistical constraints  
3. Handle uniqueness constraints last (most complex)
4. Test thoroughly at each step

### Q: Are there performance differences?

A: Yes, unified constraints are generally faster due to:
- Pattern caching
- Better query optimization
- Reduced overhead

### Q: Can I mix old and new constraint types?

A: Yes, during the migration period. However, you won't get the full performance benefits until you fully migrate.

## Getting Help

- **Documentation**: See rustdoc for detailed API documentation
- **Examples**: Check the `examples/` directory for complete examples  
- **Issues**: Report migration problems on GitHub
- **Community**: Join discussions in GitHub Discussions

## Migration Checklist

- [ ] Update import statements
- [ ] Replace deprecated constraint types
- [ ] Add error handling (`?` operators)
- [ ] Update parameter orders where changed
- [ ] Add FormatOptions configuration where needed
- [ ] Run tests to verify functionality
- [ ] Check for deprecation warnings  
- [ ] Benchmark performance improvements
- [ ] Update documentation/comments
- [ ] Remove unused imports

Happy migrating! 🚀