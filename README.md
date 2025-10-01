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

## 🆕 What's New in v0.0.2

### 🎯 Incremental Analysis - Process Only What's Changed

```rust
use term_guard::analyzers::{IncrementalAnalysisRunner, FilesystemStateStore};

// Initialize with state persistence
let store = FilesystemStateStore::new("./metrics_state");
let runner = IncrementalAnalysisRunner::new(store);

// Process daily partitions incrementally
let state = runner.analyze_partition(
    &ctx,
    "2025-09-30",  // Today's partition
    vec![analyzer],
).await?;

// Only new data is processed, previous results are reused!
```

### 📊 Advanced Analytics - KLL Sketches & Correlation

```rust
use term_guard::analyzers::{KllSketchAnalyzer, CorrelationAnalyzer};

// Approximate quantiles with minimal memory
let kll = KllSketchAnalyzer::new("response_time")
    .with_k(256)  // Higher k = better accuracy
    .with_quantiles(vec![0.5, 0.95, 0.99]);

// Detect relationships between metrics
let correlation = CorrelationAnalyzer::new("ad_spend", "revenue")
    .with_method(CorrelationMethod::Spearman);  // Handles non-linear

let results = runner.run_analyzers(vec![kll, correlation]).await?;
```

### 🔍 Multi-Table Validation - Foreign Keys & Joins

```rust
// Validate relationships across tables with fluent API
let suite = ValidationSuite::builder("Cross-table integrity")
    .check(
        Check::builder("Referential integrity")
            .foreign_key("orders.customer_id", "customers.id")
            .temporal_consistency("orders", "created_at", "updated_at")
            .build()
    )
    .build();
```

### 🛡️ Enhanced Security - SSN & PII Detection

```rust
// New format validators including SSN detection
let check = Check::builder("PII Protection")
    .contains_ssn("ssn_field")         // Validates SSN format
    .contains_credit_card("cc_field")  // Credit card detection
    .contains_email("email_field")     // Email validation
    .build();
```

### 🚨 Anomaly Detection - Catch Outliers Automatically

```rust
use term_guard::analyzers::{AnomalyDetector, RelativeRateOfChangeStrategy};

// Detect sudden metric changes
let detector = AnomalyDetector::new()
    .with_strategy(RelativeRateOfChangeStrategy::new()
        .max_rate_increase(0.5)  // Flag 50%+ increases
        .max_rate_decrease(0.3)  // Flag 30%+ decreases
    );

let anomalies = detector.detect(&historical_metrics, &current_metric)?;
```

### 📈 Grouped Metrics - Segment-Level Analysis

```rust
use term_guard::analyzers::{GroupedCompletenessAnalyzer};

// Analyze data quality by segment
let analyzer = GroupedCompletenessAnalyzer::new()
    .group_by(vec!["region", "product_category"])
    .analyze_column("revenue");

// Get metrics for each group combination
let results = analyzer.compute(&ctx).await?;
// e.g., completeness for region=US & category=Electronics
```

### 🔧 Improved Developer Experience

- **Fluent Builder API**: Natural language-like validation rules
- **Auto Schema Detection**: Automatic foreign key and temporal column discovery
- **Debug Context**: Detailed error reporting with actionable suggestions
- **Dependency Updates**: Updated to latest stable versions (criterion 0.7, rand 0.9, thiserror 2.0)
- **Test Infrastructure**: >95% test coverage with TPC-H integration tests

## 🤖 Smart Constraint Suggestions

**Don't know what to validate? Term can analyze your data and suggest constraints automatically:**

```rust
use term_guard::analyzers::{ColumnProfiler, SuggestionEngine};
use term_guard::analyzers::{CompletenessRule, UniquenessRule, PatternRule, RangeRule};

// Profile your data
let profiler = ColumnProfiler::new();
let profile = profiler.profile_column(&ctx, "users", "email").await?;

// Get intelligent suggestions
let engine = SuggestionEngine::new()
    .add_rule(Box::new(CompletenessRule::new()))    // Suggests null checks
    .add_rule(Box::new(UniquenessRule::new()))      // Finds potential keys
    .add_rule(Box::new(PatternRule::new()))         // Detects email/phone patterns
    .add_rule(Box::new(RangeRule::new()))           // Recommends numeric bounds
    .confidence_threshold(0.8);

let suggestions = engine.suggest_constraints(&profile);

// Example output:
// ✓ Suggested: is_complete (confidence: 0.90)
//   Rationale: Column is 99.8% complete, suggesting completeness constraint
// ✓ Suggested: is_unique (confidence: 0.95)
//   Rationale: Column has 99.9% unique values, suggesting uniqueness constraint
// ✓ Suggested: matches_email_pattern (confidence: 0.85)
//   Rationale: Sample values suggest email format
```

Term analyzes your actual data patterns to recommend the most relevant quality checks!

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

v0.0.2 Performance Improvements:
- 30-50% faster CI/CD with cargo-nextest
- Memory-efficient KLL sketches for quantile computation
- SQL window functions for correlation analysis  
- Cached SessionContext for test speedup
- Comprehensive benchmark suite for regression detection
```

## 🚀 Getting Started

### Installation

```toml
[dependencies]
term-guard = "0.0.2"
tokio = { version = "1", features = ["full"] }

# Optional features
term-guard = { version = "0.0.2", features = ["cloud-storage"] }  # S3, GCS, Azure support
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
- [`incremental_analysis.rs`](examples/src/incremental_analysis.rs) - Incremental computation
- [`anomaly_detection_strategy.rs`](examples/src/anomaly_detection_strategy.rs) - Anomaly detection
- [`grouped_metrics.rs`](examples/grouped_metrics.rs) - Segment-level analysis

## 📚 Documentation

Our documentation is organized using the [Diátaxis](https://diataxis.fr/) framework:

- **[Tutorials](docs/tutorials/)** - Learn Term step-by-step
  - [Getting Started](docs/tutorials/01-getting-started.md)
  - [Format Validation](docs/tutorials/08-format-validation.md)
  - [Metrics Repository](docs/tutorials/09-metrics-repository.md)
  - [Anomaly Detection](docs/tutorials/10-anomaly-detection.md)
  - [Incremental Analysis](docs/tutorials/12-incremental-analysis.md)
- **[How-To Guides](docs/how-to/)** - Solve specific problems
  - [Use Analyzers](docs/how-to/use-analyzers.md)
  - [Migrate from Deequ](docs/how-to/migrate-from-deequ.md)
  - [Run Benchmarks](docs/how-to/run-benchmarks.md)
  - [Use KLL Sketches](docs/how-to/use-kll-sketches.md)
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

### Released (v0.0.1)

- ✅ Core validation engine
- ✅ File format support (CSV, JSON, Parquet)
- ✅ Cloud storage integration
- ✅ OpenTelemetry support
- ✅ Data profiling and constraint suggestions

### Now (v0.0.2)

- ✅ Advanced analytics (KLL sketches, correlation, mutual information)
- ✅ Incremental computation framework for streaming data
- ✅ Metrics repository for historical tracking
- ✅ Anomaly detection with configurable strategies
- ✅ Grouped metrics computation for segment analysis
- ✅ Multi-table validation with foreign key support
- ✅ SSN and advanced format pattern detection
- ✅ Comprehensive benchmarking infrastructure
- ✅ Diátaxis-compliant documentation

### Next (v0.0.3)

- 🔜 Database connectivity (PostgreSQL, MySQL, SQLite)
- 🔜 Python bindings
- 🔜 Web UI for validation reports
- 🔜 CLI tool for standalone validation

### Future

- 🎯 Distributed execution on clusters
- 🎯 Real-time streaming validation
- 🎯 More language bindings (Java, Go)
- 🎯 Advanced ML-based anomaly detection

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
