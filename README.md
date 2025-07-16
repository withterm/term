# Term - Lightning-Fast Data Validation for Rust

<div align="center">

[![CI](https://github.com/withterm/term/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/withterm/term/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/withterm/term/branch/main/graph/badge.svg)](https://codecov.io/gh/withterm/term)
[![Crates.io](https://img.shields.io/crates/v/term-guard.svg)](https://crates.io/crates/term-guard)
[![Documentation](https://docs.rs/term-guard/badge.svg)](https://docs.rs/term-guard)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Bulletproof data validation without the infrastructure headache.**

[Get Started](#-5-minute-quickstart) • [Documentation](docs/) • [Examples](examples/) • [API Reference](https://docs.rs/term-guard)

</div>

## Why Term?

**Every data pipeline is a ticking time bomb.** Null values crash production. Duplicate IDs corrupt databases. Format changes break downstream systems. Yet most teams discover these issues only after the damage is done.

Traditional data validation tools assume you have a data team, a Spark cluster, and weeks to implement. **Term takes a different approach:**

- **🚀 5-minute setup** - From install to first validation. No clusters, no configs, no complexity
- **⚡ 100MB/s single-core performance** - Validate millions of rows in seconds, not hours
- **🛡️ Fail fast, fail safe** - Catch data issues before they hit production
- **📊 See everything** - Built-in OpenTelemetry means you're never debugging blind
- **🔧 Zero infrastructure** - Single binary runs on your laptop, in CI/CD, or in the cloud

**Term is data validation for the 99% of engineering teams who just want their data to work.**

## 🎯 5-Minute Quickstart

```bash
# Add to your Cargo.toml
cargo add term-guard tokio --features tokio/full
```

```rust
use term_guard::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    // Load your data
    let ctx = SessionContext::new();
    ctx.register_csv("users", "users.csv", CsvReadOptions::new()).await?;

    // Define what good data looks like
    let checks = ValidationSuite::builder("User Data Quality")
        .check(
            Check::builder("No broken data")
                .is_complete("user_id")          // No missing IDs
                .is_unique("email")              // No duplicate emails
                .has_pattern("email", r"@", 1.0) // All emails have @
                .build()
        )
        .build();

    // Validate and get instant feedback
    let report = checks.run(&ctx).await?;
    println!("{}", report);  // ✅ All 3 checks passed!

    Ok(())
}
```

**That's it!** No clusters to manage, no JVMs to tune, no YAML to write.

## 🔥 Real-World Example: Validate 1M Rows in Under 1 Second

```rust
// Validate a production dataset with multiple quality checks
let suite = ValidationSuite::builder("Production Pipeline")
    .check(
        Check::builder("Data Freshness")
            .satisfies("created_at > now() - interval '1 day'")
            .has_size(Assertion::GreaterThan(1000))
            .build()
    )
    .check(
        Check::builder("Business Rules")
            .has_min("revenue", Assertion::GreaterThan(0.0))
            .has_mean("conversion_rate", Assertion::Between(0.01, 0.10))
            .has_correlation("ad_spend", "revenue", Assertion::GreaterThan(0.5))
            .build()
    )
    .build();

// Runs all checks in a single optimized pass
let report = suite.run(&ctx).await?;
```

## 🎨 What Can You Validate?

<table>
<tr>
<td>

### 🔍 Data Quality

- Completeness checks
- Uniqueness validation
- Pattern matching
- Type consistency

</td>
<td>

### 📊 Statistical

- Min/max boundaries
- Mean/median checks
- Standard deviation
- Correlation analysis

</td>
<td>

### 🛡️ Security

- Email validation
- Credit card detection
- URL format checks
- PII detection

</td>
<td>

### 🚀 Performance

- Row count assertions
- Query optimization
- Batch processing
- Memory efficiency

</td>
</tr>
</table>

## 📈 Benchmarks: 15x Faster with Smart Optimization

```
Dataset: 1M rows, 20 constraints
Without optimizer: 3.2s (20 full scans)
With Term:         0.21s (2 optimized scans)
```

## 🚀 Getting Started

### Installation

```toml
[dependencies]
term-guard = "0.0.1"
tokio = { version = "1", features = ["full"] }

# Optional features
term-guard = { version = "0.0.1", features = ["cloud-storage"] }  # S3, GCS, Azure support
```

### Learn Term in 30 Minutes

1. **[Quick Start Tutorial](docs/tutorials/01-getting-started.md)** - Your first validation in 5 minutes
2. **[From Deequ to Term](docs/explanation/deequ-comparison.md)** - Migration guide with examples
3. **[Production Best Practices](docs/how-to/optimize-performance.md)** - Scaling and monitoring

### Example Projects

Check out the [`examples/`](examples/) directory for real-world scenarios:

- [`basic_validation.rs`](examples/src/basic_validation.rs) - Simple CSV validation
- [`cloud_storage_example.rs`](examples/src/cloud_storage_example.rs) - Validate S3/GCS data
- [`telemetry_example.rs`](examples/src/telemetry_example.rs) - Production monitoring
- [`tpc_h_validation.rs`](examples/src/tpc_h_validation.rs) - Complex business rules

## 📚 Documentation

Our documentation is organized using the [Diátaxis](https://diataxis.fr/) framework:

- **[Tutorials](docs/tutorials/)** - Learn Term step-by-step
- **[How-To Guides](docs/how-to/)** - Solve specific problems
- **[Reference](docs/reference/)** - Technical specifications
- **[Explanation](docs/explanation/)** - Understand the concepts
- **[API Docs](https://docs.rs/term-guard)** - Complete Rust documentation

## 🛠️ Contributing

We love contributions! Term is built by the community, for the community.

```bash
# Get started in 3 steps
git clone https://github.com/withterm/term.git
cd term
cargo test
```

- **[Contributing Guide](CONTRIBUTING.md)** - How to contribute
- **[Good First Issues](https://github.com/withterm/term/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22)** - Perfect for newcomers
- **[Architecture Docs](docs/explanation/architecture.md)** - Understand the internals

## 🗺️ Roadmap

### Now (v0.0.1)

- ✅ Core validation engine
- ✅ File format support (CSV, JSON, Parquet)
- ✅ Cloud storage integration
- ✅ OpenTelemetry support

### Next (v0.0.2)

- 🔜 Database connectivity (PostgreSQL, MySQL, SQLite)
- 🔜 Streaming data support
- 🔜 Python bindings
- 🔜 Web UI for validation reports

### Future

- 🎯 Data profiling and suggestions
- 🎯 Incremental validation
- 🎯 Distributed execution
- 🎯 More language bindings

## 📄 License

Term is MIT licensed. See [LICENSE](LICENSE) for details.

## 🙏 Acknowledgments

Term stands on the shoulders of giants:

- [Apache Arrow DataFusion](https://arrow.apache.org/datafusion/) for the query engine
- [AWS Deequ](https://github.com/awslabs/deequ) for the inspiration
- The amazing Rust community for invaluable feedback

---

<div align="center">

**Ready to bulletproof your data pipelines?**

[⚡ Get Started](docs/tutorials/01-getting-started.md) • [📖 Read the Docs](docs/) • [💬 Join Community](https://github.com/withterm/term/discussions)

</div>
