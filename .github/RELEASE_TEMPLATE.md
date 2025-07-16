# Term v0.0.1 - Initial Release

We're excited to announce the first release of Term, a Rust data validation library providing Deequ-like capabilities without Spark dependencies!

## 🎉 Highlights

- **Zero Spark Dependencies**: Native Rust implementation built on Apache DataFusion
- **Unified Constraint Architecture**: 8 powerful constraint families replacing 30+ individual types
- **Performance Optimized**: 14-27% faster than individual constraints with 44% less memory usage
- **Async-First Design**: Built on Tokio for concurrent validation
- **Rich Data Source Support**: CSV, Parquet, JSON, databases, and cloud storage

## 📦 Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
term-guard = "0.0.1"
```

## 🚀 Quick Start

```rust
use term_guard::prelude::*;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let suite = ValidationSuite::builder("customer_validation")
        .check(
            Check::builder("required_fields")
                .is_complete("customer_id")
                .has_completeness("email", 0.95)
                .build()
        )
        .build();

    let ctx = SessionContext::new();
    ctx.register_csv("data", "customers.csv", CsvReadOptions::new()).await?;

    let results = suite.run(&ctx).await?;
    println!("Validation Status: {:?}", results.overall_status);

    Ok(())
}
```

## ✨ Key Features

### Unified Constraints

- **UnifiedCompletenessConstraint**: Data completeness with logical operators
- **UnifiedStatisticalConstraint**: Min, max, mean, sum, and more in optimized queries
- **FormatConstraint**: Email, URL, phone, regex validation with caching
- **UnifiedUniquenessConstraint**: Primary keys, uniqueness with null handling
- **LengthConstraint**: String length validation
- **UnifiedDataTypeConstraint**: Type checking and consistency
- **UnifiedQuantileConstraint**: Percentiles and distribution analysis
- **UnifiedCorrelationConstraint**: Column correlation analysis

### Data Sources

- Local files: CSV, Parquet, JSON
- Cloud storage: S3, GCS, Azure (with feature flags)
- Databases: PostgreSQL, MySQL, SQLite (with feature flags)
- In-memory data

### Performance

- Query optimizer batches similar constraints
- Pattern caching for regex validations
- Efficient DataFusion execution engine
- OpenTelemetry instrumentation

## 📊 Migration from Deequ

Term provides equivalent functionality to common Deequ checks:

| Deequ           | Term                  |
| --------------- | --------------------- |
| `hasSize`       | `has_size()`          |
| `isComplete`    | `is_complete()`       |
| `isUnique`      | `is_unique()`         |
| `hasMin/hasMax` | `has_min()/has_max()` |
| `hasPattern`    | `has_pattern()`       |
| `isContainedIn` | `is_contained_in()`   |

## 🔧 Technical Details

- **Rust**: 1.70+ with 2021 edition
- **DataFusion**: 48.0
- **Arrow**: 55.0
- **Async Runtime**: Tokio
- **Error Handling**: thiserror
- **Observability**: OpenTelemetry

## 📚 Documentation

- [API Documentation](https://docs.rs/term-guard)
- [Examples](https://github.com/withterm/term/tree/main/examples)
- [Migration Guide](https://github.com/withterm/term/blob/main/MIGRATION_GUIDE.md)

## 🙏 Acknowledgments

Special thanks to the Apache DataFusion and Arrow communities for providing the excellent foundation for this library.

## 📝 Full Changelog

See [CHANGELOG.md](https://github.com/withterm/term/blob/main/CHANGELOG.md) for detailed changes.

---

**Full Changelog**: https://github.com/withterm/term/commits/v0.0.1
