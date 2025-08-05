# Term Documentation

Welcome to the Term documentation! Our docs are organized using the [Diátaxis](https://diataxis.fr/) framework, which provides a systematic approach to technical documentation.

## Documentation Structure

### 📚 [Tutorials](tutorials/)
**Learning-oriented** - Step-by-step guides for beginners

- [Getting Started](tutorials/01-getting-started.md) - Your first Term validation
- [Validating CSV Files](tutorials/02-validating-csv-files.md) - Work with CSV data sources
- [Understanding Analyzers](tutorials/03-understanding-analyzers.md) - Learn data quality analysis
- [Column Profiling](tutorials/04-column-profiling.md) - Automated data quality profiling
- [Connect to Your First Database](tutorials/05-database-connections.md) - Database validation basics
- [Working with Multiple Databases](tutorials/06-multi-database-validation.md) - Cross-database validation

### 🔧 [How-To Guides](how-to/)
**Task-oriented** - Practical guides for specific goals

- [Comprehensive Examples](how-to/comprehensive-examples.md) - Complete code examples for all features
- [Analyze Large Datasets](how-to/analyze-large-datasets.md) - Efficient analysis at scale
- [Migrate to Unified API](how-to/migrate-to-unified-api.md) - Upgrade from deprecated constraints
- [Optimize Performance](how-to/optimize-performance.md) - Speed up your validations
- [Use Cloud Storage](how-to/use-cloud-storage.md) - Validate data in S3, GCS, Azure
- [Configure Logging](how-to/configure-logging.md) - Set up structured logging
- [Apply Security Best Practices](how-to/security-best-practices.md) - Secure your validations
- [Write Custom Constraints](how-to/write-custom-constraints.md) - Create domain-specific validations

#### Database Connections
- [Connect to PostgreSQL](how-to/connect-postgresql.md) - PostgreSQL database validation
- [Connect to MySQL](how-to/connect-mysql.md) - MySQL database validation  
- [Connect to SQLite](how-to/connect-sqlite.md) - SQLite database validation
- [Validate Data Across Multiple Tables](how-to/validate-multiple-tables.md) - Multi-table validation
- [Secure Database Connections](how-to/secure-database-connections.md) - Database security best practices
- [Optimize Database Validation Performance](how-to/optimize-database-performance.md) - Database performance tuning

### 📖 [Reference](reference/)
**Information-oriented** - Complete technical descriptions

- [Constraints Reference](reference/constraints.md) - All available validation constraints
- [Analyzers Reference](reference/analyzers.md) - Data quality analyzers API
- [AnalysisRunner Reference](reference/analysis-runner.md) - Analyzer orchestration API
- [Database Sources Reference](reference/database-sources.md) - Database connectivity API
- [ValidationContext Reference](reference/validation-context.md) - Dynamic table name context
- [Logical Operators](reference/logical-operators.md) - AND, OR, AtLeast operators
- [CI Test Data](reference/ci-test-data.md) - Test data generation utilities
- [API Documentation](https://docs.rs/term-guard) - Complete Rust API docs

### 💡 [Explanation](explanation/)
**Understanding-oriented** - Conceptual and design discussions

- [Deequ Comparison](explanation/deequ-comparison.md) - How Term compares to AWS Deequ
- [Architecture](explanation/architecture.md) - Term's design and internals
- [Analyzer Architecture](explanation/analyzer-architecture.md) - Two-phase computation design
- [Understanding Database Connectors](explanation/database-connectors.md) - Database integration design
- [Understanding Dynamic Table Names](explanation/dynamic-table-names.md) - ValidationContext design philosophy

## Quick Links

- **New to Term?** Start with the [Getting Started Tutorial](tutorials/01-getting-started.md)
- **Need to solve a specific problem?** Check our [How-To Guides](how-to/)
- **Looking for API details?** See the [Reference](reference/) section
- **Want to understand the design?** Read our [Explanations](explanation/)

## Contributing

When contributing documentation:

1. Use the appropriate [template](templates/) for your content type
2. Place your document in the correct folder based on its purpose
3. Follow the [Diátaxis](https://diataxis.fr/) principles for technical documentation

### Which Section Should I Use?

Ask yourself what the reader wants to do:

- **Learn** → Write a Tutorial
- **Accomplish a task** → Write a How-To Guide  
- **Look up information** → Write Reference documentation
- **Understand** → Write an Explanation

## Templates

We provide templates for each documentation type:

- [Tutorial Template](templates/tutorial-template.md)
- [How-To Template](templates/how-to-template.md)
- [Reference Template](templates/reference-template.md)
- [Explanation Template](templates/explanation-template.md)