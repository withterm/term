# term-guard

Core library for the Term data validation framework.

## Overview

`term-guard` is the main library crate that provides all the data validation functionality for Term. It includes:

- **Constraints** - Over 30 built-in validation rules
- **Validation Engine** - High-performance constraint evaluation
- **Data Sources** - Support for files, cloud storage, and databases
- **Query Optimization** - Intelligent constraint batching
- **Telemetry** - Built-in observability with OpenTelemetry

## Documentation

- [API Documentation](https://docs.rs/term-guard)
- [Constraints Reference](../docs/reference/constraints.md)
- [Getting Started Tutorial](../docs/tutorials/01-getting-started.md)

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
term-guard = "0.0.1"
```

Basic example:

```rust
use term_guard::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_csv("data", "data.csv", CsvReadOptions::new()).await?;
    
    let suite = ValidationSuite::builder("validation")
        .check(
            Check::builder("quality")
                .is_complete("id")
                .is_unique("email")
                .build()
        )
        .build();
    
    let results = suite.run(&ctx).await?;
    Ok(())
}
```

## Features

- `default` - Core functionality with file support
- `cloud-storage` - AWS S3, Google Cloud Storage, Azure Blob support
- `telemetry` - OpenTelemetry integration
- `test-utils` - Utilities for testing (TPC-H data generation)

## Project Structure

```
term-guard/
├── src/
│   ├── constraints/    # Validation constraints
│   ├── core/          # Core types (Suite, Check, Result)
│   ├── formatters/    # Result formatting
│   ├── sources/       # Data source connectors
│   └── lib.rs         # Library entry point
├── tests/             # Integration tests
└── benches/          # Performance benchmarks
```

## Development

Run tests:
```bash
cargo test
```

Run benchmarks:
```bash
cargo bench
```

## License

MIT License - see [LICENSE](../LICENSE) for details.