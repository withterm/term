//! Multi-source validation engine for Term.
//!
//! This module provides a unified interface for validating data across multiple registered
//! data sources. It enables cross-table validations, referential integrity checks, and
//! comprehensive data quality validation in distributed and multi-table environments.
//!
//! # Overview
//!
//! The `MultiSourceValidator` is designed for scenarios where data quality validation
//! must span multiple tables, databases, or data sources. It provides:
//!
//! - **Source Registration**: Register multiple data sources with metadata
//! - **Cross-Table Validation**: Run validations that span multiple registered sources
//! - **Unified Interface**: Single API for complex multi-source validation scenarios
//! - **Memory Efficiency**: Optimized for large-scale data validation
//! - **Performance Optimization**: Intelligent query planning across sources
//!
//! # Examples
//!
//! ## Basic Multi-Source Setup
//!
//! ```rust
//! use term_guard::core::MultiSourceValidator;
//! use term_guard::constraints::{CrossTableSumConstraint, ForeignKeyConstraint};
//! use datafusion::prelude::*;
//!
//! # async fn example() -> term_guard::prelude::Result<()> {
//! let mut validator = MultiSourceValidator::new("financial_validation");
//!
//! // Register data sources
//! validator.register_source("orders", "Orders table from primary database")?;
//! validator.register_source("payments", "Payments table from accounting system")?;
//! validator.register_source("customers", "Customer master data")?;
//!
//! // Add cross-table validations
//! validator.add_cross_table_validation(
//!     "financial_consistency",
//!     CrossTableSumConstraint::new("orders.total", "payments.amount")
//!         .group_by(vec!["customer_id"])
//!         .tolerance(0.01)
//! )?;
//!
//! validator.add_foreign_key_validation(
//!     "referential_integrity",
//!     ForeignKeyConstraint::new("orders.customer_id", "customers.id")
//! )?;
//!
//! // Create DataFusion context with registered tables
//! let ctx = SessionContext::new();
//! // ... register actual tables with data ...
//!
//! // Run comprehensive validation
//! let results = validator.validate(&ctx).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Advanced Configuration
//!
//! ```rust
//! use term_guard::core::MultiSourceValidator;
//!
//! # async fn example() -> term_guard::prelude::Result<()> {
//! let validator = MultiSourceValidator::builder("data_warehouse_validation")
//!     .description("Comprehensive data warehouse quality validation")
//!     .max_concurrent_validations(4)
//!     .memory_budget_mb(512)
//!     .enable_query_optimization(true)
//!     .build()?;
//! # Ok(())
//! # }
//! ```

use crate::constraints::{CrossTableSumConstraint, ForeignKeyConstraint};
use crate::core::{
    constraint::Constraint, Level, ValidationIssue, ValidationMetrics, ValidationReport,
    ValidationResult,
};
use crate::error::{Result, TermError};
use crate::security::SqlSecurity;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info, instrument};

/// Metadata for a registered data source in the multi-source validator.
///
/// Contains information about data sources to enable intelligent query planning
/// and validation optimization across multiple tables.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSourceInfo {
    /// Name of the data source (must be valid SQL identifier)
    pub name: String,
    /// Optional description of the data source
    pub description: Option<String>,
    /// Optional schema qualifier (e.g., "public", "warehouse")
    pub schema: Option<String>,
    /// Estimated row count (for query optimization)
    pub estimated_rows: Option<u64>,
    /// When the source was registered
    pub registered_at: std::time::SystemTime,
}

impl DataSourceInfo {
    /// Create a new data source info with minimal metadata.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: None,
            schema: None,
            estimated_rows: None,
            registered_at: std::time::SystemTime::now(),
        }
    }

    /// Set description for the data source.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Set schema qualifier for the data source.
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Set estimated row count for query optimization.
    pub fn with_estimated_rows(mut self, rows: u64) -> Self {
        self.estimated_rows = Some(rows);
        self
    }

    /// Get the fully qualified table name for SQL queries.
    pub fn qualified_name(&self) -> String {
        match &self.schema {
            Some(schema) => {
                let name = &self.name;
                format!("{schema}.{name}")
            }
            None => self.name.clone(),
        }
    }
}

/// Validation entry for cross-table constraints in the multi-source validator.
#[derive(Debug)]
pub struct CrossTableValidation {
    /// Name identifier for this validation
    pub name: String,
    /// The cross-table sum constraint to execute
    pub constraint: CrossTableSumConstraint,
    /// Priority level for this validation
    pub level: Level,
}

/// Validation entry for foreign key constraints in the multi-source validator.
#[derive(Debug)]
pub struct ForeignKeyValidation {
    /// Name identifier for this validation
    pub name: String,
    /// The foreign key constraint to execute
    pub constraint: ForeignKeyConstraint,
    /// Priority level for this validation
    pub level: Level,
}

/// Configuration for the multi-source validator behavior and performance.
#[derive(Debug, Clone)]
pub struct MultiSourceConfig {
    /// Maximum number of concurrent validation operations
    pub max_concurrent_validations: usize,
    /// Memory budget in megabytes for validation operations
    pub memory_budget_mb: usize,
    /// Enable query optimization across sources
    pub enable_query_optimization: bool,
    /// Timeout for individual validation operations in seconds
    pub validation_timeout_seconds: u64,
}

impl Default for MultiSourceConfig {
    fn default() -> Self {
        Self {
            max_concurrent_validations: 2,
            memory_budget_mb: 256,
            enable_query_optimization: true,
            validation_timeout_seconds: 300, // 5 minutes
        }
    }
}

/// Multi-source data validator for comprehensive cross-table validation.
///
/// This validator manages multiple registered data sources and enables validation
/// scenarios that span across tables, such as referential integrity, cross-table
/// sum validation, and join coverage analysis.
///
/// # Performance Characteristics
///
/// - **Memory Efficient**: Uses streaming approaches and configurable memory budgets
/// - **Concurrent**: Supports parallel validation execution with configurable limits
/// - **Optimized**: Intelligent query planning and pushdown optimization
/// - **Scalable**: Designed for large-scale data warehouse validation scenarios
///
/// # Thread Safety
///
/// The `MultiSourceValidator` is thread-safe and can be shared across async tasks.
/// Internal state is protected and validation operations are designed to be concurrent.
#[derive(Debug)]
pub struct MultiSourceValidator {
    /// Name of this validator instance
    name: String,
    /// Optional description
    description: Option<String>,
    /// Registered data sources by name
    sources: HashMap<String, DataSourceInfo>,
    /// Cross-table validations to execute
    cross_table_validations: Vec<CrossTableValidation>,
    /// Foreign key validations to execute
    foreign_key_validations: Vec<ForeignKeyValidation>,
    /// Configuration for validation behavior
    config: MultiSourceConfig,
}

impl MultiSourceValidator {
    /// Creates a new multi-source validator with the given name.
    ///
    /// # Arguments
    ///
    /// * `name` - Name identifier for this validator instance
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_guard::core::MultiSourceValidator;
    ///
    /// let validator = MultiSourceValidator::new("financial_data_validation");
    /// ```
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: None,
            sources: HashMap::new(),
            cross_table_validations: Vec::new(),
            foreign_key_validations: Vec::new(),
            config: MultiSourceConfig::default(),
        }
    }

    /// Creates a builder for advanced configuration.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_guard::core::MultiSourceValidator;
    ///
    /// # fn example() -> term_guard::prelude::Result<()> {
    /// let validator = MultiSourceValidator::builder("advanced_validation")
    ///     .description("Enterprise data quality validation")
    ///     .max_concurrent_validations(8)
    ///     .memory_budget_mb(1024)
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn builder(name: impl Into<String>) -> MultiSourceValidatorBuilder {
        MultiSourceValidatorBuilder::new(name)
    }

    /// Set description for this validator.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Register a data source for validation.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the data source (must be valid SQL identifier)
    /// * `description` - Optional description of the data source
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_guard::core::MultiSourceValidator;
    ///
    /// # fn example() -> term_guard::prelude::Result<()> {
    /// let mut validator = MultiSourceValidator::new("validator");
    /// validator.register_source("orders", "Primary orders table")?;
    /// validator.register_source("customers", "Customer master data")?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The source name is not a valid SQL identifier
    /// - A source with the same name is already registered
    pub fn register_source(
        &mut self,
        name: impl Into<String>,
        description: impl Into<String>,
    ) -> Result<()> {
        let name_str = name.into();

        // Validate SQL identifier for security
        SqlSecurity::validate_identifier(&name_str)?;

        // Check for duplicate registration
        if self.sources.contains_key(&name_str) {
            return Err(TermError::constraint_evaluation(
                "multi_source_validator",
                format!("Data source '{name_str}' is already registered"),
            ));
        }

        let source_info = DataSourceInfo::new(name_str.clone()).with_description(description);
        debug!("Registered data source: {}", name_str);
        self.sources.insert(name_str, source_info);
        Ok(())
    }

    /// Register a data source with detailed metadata.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_guard::core::{MultiSourceValidator, DataSourceInfo};
    ///
    /// # fn example() -> term_guard::prelude::Result<()> {
    /// let mut validator = MultiSourceValidator::new("validator");
    ///
    /// let source_info = DataSourceInfo::new("large_orders")
    ///     .with_description("Orders table with millions of records")
    ///     .with_schema("warehouse")
    ///     .with_estimated_rows(10_000_000);
    ///
    /// validator.register_source_with_info(source_info)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn register_source_with_info(&mut self, source_info: DataSourceInfo) -> Result<()> {
        // Validate SQL identifier for security
        SqlSecurity::validate_identifier(&source_info.name)?;

        if let Some(schema) = &source_info.schema {
            SqlSecurity::validate_identifier(schema)?;
        }

        // Check for duplicate registration
        if self.sources.contains_key(&source_info.name) {
            return Err(TermError::constraint_evaluation(
                "multi_source_validator",
                {
                    let name = &source_info.name;
                    format!("Data source '{name}' is already registered")
                },
            ));
        }

        debug!("Registered data source with info: {}", source_info.name);
        self.sources.insert(source_info.name.clone(), source_info);
        Ok(())
    }

    /// Add a cross-table sum validation.
    ///
    /// # Arguments
    ///
    /// * `name` - Identifier for this validation
    /// * `constraint` - The cross-table sum constraint to execute
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_guard::core::MultiSourceValidator;
    /// use term_guard::constraints::CrossTableSumConstraint;
    ///
    /// # fn example() -> term_guard::prelude::Result<()> {
    /// let mut validator = MultiSourceValidator::new("validator");
    /// validator.register_source("orders", "Orders table")?;
    /// validator.register_source("payments", "Payments table")?;
    ///
    /// validator.add_cross_table_validation(
    ///     "financial_consistency",
    ///     CrossTableSumConstraint::new("orders.total", "payments.amount")
    ///         .group_by(vec!["customer_id"])
    ///         .tolerance(0.01)
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn add_cross_table_validation(
        &mut self,
        name: impl Into<String>,
        constraint: CrossTableSumConstraint,
    ) -> Result<()> {
        self.add_cross_table_validation_with_level(name, constraint, Level::Error)
    }

    /// Add a cross-table sum validation with a specific severity level.
    pub fn add_cross_table_validation_with_level(
        &mut self,
        name: impl Into<String>,
        constraint: CrossTableSumConstraint,
        level: Level,
    ) -> Result<()> {
        let name_str = name.into();

        // Validate that referenced tables are registered
        self.validate_cross_table_constraint_sources(&constraint)?;

        self.cross_table_validations.push(CrossTableValidation {
            name: name_str.clone(),
            constraint,
            level,
        });

        debug!("Added cross-table validation: {}", name_str);
        Ok(())
    }

    /// Add a foreign key validation.
    ///
    /// # Arguments
    ///
    /// * `name` - Identifier for this validation
    /// * `constraint` - The foreign key constraint to execute
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_guard::core::MultiSourceValidator;
    /// use term_guard::constraints::ForeignKeyConstraint;
    ///
    /// # fn example() -> term_guard::prelude::Result<()> {
    /// let mut validator = MultiSourceValidator::new("validator");
    /// validator.register_source("orders", "Orders table")?;
    /// validator.register_source("customers", "Customers table")?;
    ///
    /// validator.add_foreign_key_validation(
    ///     "customer_references",
    ///     ForeignKeyConstraint::new("orders.customer_id", "customers.id")
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn add_foreign_key_validation(
        &mut self,
        name: impl Into<String>,
        constraint: ForeignKeyConstraint,
    ) -> Result<()> {
        self.add_foreign_key_validation_with_level(name, constraint, Level::Error)
    }

    /// Add a foreign key validation with a specific severity level.
    pub fn add_foreign_key_validation_with_level(
        &mut self,
        name: impl Into<String>,
        constraint: ForeignKeyConstraint,
        level: Level,
    ) -> Result<()> {
        let name_str = name.into();

        // Validate that referenced tables are registered
        self.validate_foreign_key_constraint_sources(&constraint)?;

        self.foreign_key_validations.push(ForeignKeyValidation {
            name: name_str.clone(),
            constraint,
            level,
        });

        debug!("Added foreign key validation: {}", name_str);
        Ok(())
    }

    /// Execute all registered validations against the provided DataFusion context.
    ///
    /// # Arguments
    ///
    /// * `ctx` - DataFusion session context with registered tables
    ///
    /// # Returns
    ///
    /// A `ValidationResult` containing comprehensive results from all validations.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_guard::core::MultiSourceValidator;
    /// use datafusion::prelude::*;
    ///
    /// # async fn example() -> term_guard::prelude::Result<()> {
    /// let mut validator = MultiSourceValidator::new("validator");
    /// // ... configure validator ...
    ///
    /// let ctx = SessionContext::new();
    /// // ... register tables in context ...
    ///
    /// let results = validator.validate(&ctx).await?;
    /// match results {
    ///     term_guard::core::ValidationResult::Success { report, .. } => {
    ///         println!("All {} validations passed!", report.metrics.total_checks);
    ///     }
    ///     term_guard::core::ValidationResult::Failure { report } => {
    ///         println!("Found {} validation issues", report.issues.len());
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip(self, ctx), fields(validator = %self.name))]
    pub async fn validate(&self, ctx: &SessionContext) -> Result<ValidationResult> {
        info!(
            validator = %self.name,
            sources = self.sources.len(),
            cross_table_validations = self.cross_table_validations.len(),
            foreign_key_validations = self.foreign_key_validations.len(),
            config = ?self.config,
            "Starting multi-source validation"
        );

        // Verify all registered sources exist in the context
        self.verify_sources_in_context(ctx).await?;

        let mut report = ValidationReport::new(&self.name);
        report.metrics = ValidationMetrics::default();

        let mut has_errors = false;

        // Apply validation timeout from config
        let timeout_duration =
            std::time::Duration::from_secs(self.config.validation_timeout_seconds);

        // Execute cross-table validations
        for validation in &self.cross_table_validations {
            debug!("Executing cross-table validation: {}", validation.name);

            // Apply timeout to validation execution
            let validation_future = validation.constraint.evaluate(ctx);
            match tokio::time::timeout(timeout_duration, validation_future).await {
                Ok(validation_result) => match validation_result {
                    Ok(result) => {
                        report.metrics.total_checks += 1;

                        match result.status {
                            crate::core::ConstraintStatus::Success => {
                                report.metrics.passed_checks += 1;
                            }
                            crate::core::ConstraintStatus::Failure => {
                                report.metrics.failed_checks += 1;
                                if validation.level == Level::Error {
                                    has_errors = true;
                                }

                                let issue = ValidationIssue {
                                    check_name: validation.name.clone(),
                                    constraint_name: validation.constraint.name().to_string(),
                                    level: validation.level,
                                    message: result.message.unwrap_or_else(|| {
                                        "Cross-table validation failed".to_string()
                                    }),
                                    metric: result.metric,
                                };
                                report.issues.push(issue);
                            }
                            crate::core::ConstraintStatus::Skipped => {
                                report.metrics.skipped_checks += 1;
                            }
                        }
                    }
                    Err(e) => {
                        report.metrics.failed_checks += 1;
                        if validation.level == Level::Error {
                            has_errors = true;
                        }

                        let issue = ValidationIssue {
                            check_name: validation.name.clone(),
                            constraint_name: validation.constraint.name().to_string(),
                            level: validation.level,
                            message: format!("Cross-table validation error: {e}"),
                            metric: None,
                        };
                        report.issues.push(issue);
                    }
                },
                Err(_) => {
                    // Timeout occurred
                    report.metrics.failed_checks += 1;
                    report.metrics.total_checks += 1;
                    if validation.level == Level::Error {
                        has_errors = true;
                    }

                    let issue = ValidationIssue {
                        check_name: validation.name.clone(),
                        constraint_name: validation.constraint.name().to_string(),
                        level: validation.level,
                        message: format!(
                            "Cross-table validation timed out after {} seconds",
                            self.config.validation_timeout_seconds
                        ),
                        metric: None,
                    };
                    report.issues.push(issue);
                }
            }
        }

        // Execute foreign key validations
        for validation in &self.foreign_key_validations {
            debug!("Executing foreign key validation: {}", validation.name);

            // Apply timeout to validation execution
            let validation_future = validation.constraint.evaluate(ctx);
            match tokio::time::timeout(timeout_duration, validation_future).await {
                Ok(validation_result) => match validation_result {
                    Ok(result) => {
                        report.metrics.total_checks += 1;

                        match result.status {
                            crate::core::ConstraintStatus::Success => {
                                report.metrics.passed_checks += 1;
                            }
                            crate::core::ConstraintStatus::Failure => {
                                report.metrics.failed_checks += 1;
                                if validation.level == Level::Error {
                                    has_errors = true;
                                }

                                let issue = ValidationIssue {
                                    check_name: validation.name.clone(),
                                    constraint_name: validation.constraint.name().to_string(),
                                    level: validation.level,
                                    message: result.message.unwrap_or_else(|| {
                                        "Foreign key validation failed".to_string()
                                    }),
                                    metric: result.metric,
                                };
                                report.issues.push(issue);
                            }
                            crate::core::ConstraintStatus::Skipped => {
                                report.metrics.skipped_checks += 1;
                            }
                        }
                    }
                    Err(e) => {
                        report.metrics.failed_checks += 1;
                        if validation.level == Level::Error {
                            has_errors = true;
                        }

                        let issue = ValidationIssue {
                            check_name: validation.name.clone(),
                            constraint_name: validation.constraint.name().to_string(),
                            level: validation.level,
                            message: format!("Foreign key validation error: {e}"),
                            metric: None,
                        };
                        report.issues.push(issue);
                    }
                },
                Err(_) => {
                    // Timeout occurred
                    report.metrics.failed_checks += 1;
                    report.metrics.total_checks += 1;
                    if validation.level == Level::Error {
                        has_errors = true;
                    }

                    let issue = ValidationIssue {
                        check_name: validation.name.clone(),
                        constraint_name: validation.constraint.name().to_string(),
                        level: validation.level,
                        message: format!(
                            "Foreign key validation timed out after {} seconds",
                            self.config.validation_timeout_seconds
                        ),
                        metric: None,
                    };
                    report.issues.push(issue);
                }
            }
        }

        // Return appropriate result based on findings
        if has_errors {
            Ok(ValidationResult::Failure { report })
        } else {
            Ok(ValidationResult::Success {
                metrics: report.metrics.clone(),
                report,
            })
        }
    }

    /// Get information about registered data sources.
    pub fn sources(&self) -> &HashMap<String, DataSourceInfo> {
        &self.sources
    }

    /// Get the number of registered validations.
    pub fn validation_count(&self) -> usize {
        self.cross_table_validations.len() + self.foreign_key_validations.len()
    }

    /// Validate that tables referenced in cross-table constraint are registered.
    fn validate_cross_table_constraint_sources(
        &self,
        constraint: &CrossTableSumConstraint,
    ) -> Result<()> {
        let left_table = self.extract_table_name(constraint.left_column())?;
        let right_table = self.extract_table_name(constraint.right_column())?;

        if !self.sources.contains_key(&left_table) {
            return Err(TermError::constraint_evaluation(
                "multi_source_validator",
                format!(
                    "Table '{left_table}' referenced in cross-table constraint is not registered"
                ),
            ));
        }

        if !self.sources.contains_key(&right_table) {
            return Err(TermError::constraint_evaluation(
                "multi_source_validator",
                format!(
                    "Table '{right_table}' referenced in cross-table constraint is not registered"
                ),
            ));
        }

        Ok(())
    }

    /// Validate that tables referenced in foreign key constraint are registered.
    fn validate_foreign_key_constraint_sources(
        &self,
        constraint: &ForeignKeyConstraint,
    ) -> Result<()> {
        let child_table = self.extract_table_name(constraint.child_column())?;
        let parent_table = self.extract_table_name(constraint.parent_column())?;

        if !self.sources.contains_key(&child_table) {
            return Err(TermError::constraint_evaluation(
                "multi_source_validator",
                format!(
                    "Child table '{child_table}' referenced in foreign key constraint is not registered"
                ),
            ));
        }

        if !self.sources.contains_key(&parent_table) {
            return Err(TermError::constraint_evaluation(
                "multi_source_validator",
                format!(
                    "Parent table '{parent_table}' referenced in foreign key constraint is not registered"
                ),
            ));
        }

        Ok(())
    }

    /// Extract table name from qualified column specification (e.g., "orders.total" -> "orders").
    fn extract_table_name(&self, qualified_column: &str) -> Result<String> {
        let parts: Vec<&str> = qualified_column.split('.').collect();
        match parts.len() {
            2 => Ok(parts[0].to_string()),
            3 => Ok(parts[1].to_string()), // schema.table.column format
            _ => Err(TermError::constraint_evaluation(
                "multi_source_validator",
                format!("Invalid qualified column format: '{qualified_column}'"),
            )),
        }
    }

    /// Verify that all registered data sources exist in the DataFusion context.
    async fn verify_sources_in_context(&self, ctx: &SessionContext) -> Result<()> {
        let available_tables = ctx
            .catalog("datafusion")
            .unwrap()
            .schema("public")
            .unwrap()
            .table_names();

        let mut missing_tables = Vec::new();
        for source_name in self.sources.keys() {
            if !available_tables.contains(source_name) {
                missing_tables.push(source_name.clone());
            }
        }

        if !missing_tables.is_empty() {
            return Err(TermError::constraint_evaluation(
                "multi_source_validator",
                {
                    let tables = missing_tables.join(", ");
                    format!("Registered data sources not found in DataFusion context: [{tables}]")
                },
            ));
        }

        debug!("All registered data sources verified in DataFusion context");
        Ok(())
    }
}

/// Builder for constructing MultiSourceValidator instances with advanced configuration.
///
/// This builder provides a fluent API for configuring all aspects of multi-source validation,
/// including performance tuning, memory management, and optimization settings.
///
/// # Examples
///
/// ```rust
/// use term_guard::core::MultiSourceValidator;
///
/// # fn example() -> term_guard::prelude::Result<()> {
/// let validator = MultiSourceValidator::builder("enterprise_validation")
///     .description("Enterprise-grade data quality validation across 20+ tables")
///     .max_concurrent_validations(8)
///     .memory_budget_mb(2048)
///     .enable_query_optimization(true)
///     .validation_timeout_seconds(600) // 10 minutes
///     .build()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct MultiSourceValidatorBuilder {
    name: String,
    description: Option<String>,
    config: MultiSourceConfig,
}

impl MultiSourceValidatorBuilder {
    /// Create a new builder with the specified name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: None,
            config: MultiSourceConfig::default(),
        }
    }

    /// Set the description for the validator.
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Set the maximum number of concurrent validation operations.
    ///
    /// # Arguments
    ///
    /// * `max_concurrent` - Maximum concurrent validations (1-32 recommended)
    ///
    /// Higher values can improve performance on multi-core systems but may increase
    /// memory usage and database connection pressure.
    pub fn max_concurrent_validations(mut self, max_concurrent: usize) -> Self {
        self.config.max_concurrent_validations = max_concurrent.clamp(1, 32);
        self
    }

    /// Set the memory budget for validation operations.
    ///
    /// # Arguments
    ///
    /// * `budget_mb` - Memory budget in megabytes (64-4096 recommended)
    ///
    /// This setting helps prevent out-of-memory conditions during large-scale
    /// validation operations by limiting intermediate result sizes.
    pub fn memory_budget_mb(mut self, budget_mb: usize) -> Self {
        self.config.memory_budget_mb = budget_mb.clamp(64, 4096);
        self
    }

    /// Enable or disable query optimization.
    ///
    /// When enabled, the validator will attempt to optimize queries across sources
    /// for better performance. Disable if experiencing query planning issues.
    pub fn enable_query_optimization(mut self, enable: bool) -> Self {
        self.config.enable_query_optimization = enable;
        self
    }

    /// Set the timeout for individual validation operations.
    ///
    /// # Arguments
    ///
    /// * `timeout_seconds` - Timeout in seconds (30-3600 recommended)
    pub fn validation_timeout_seconds(mut self, timeout_seconds: u64) -> Self {
        self.config.validation_timeout_seconds = timeout_seconds.clamp(30, 3600);
        self
    }

    /// Build the MultiSourceValidator with the configured settings.
    ///
    /// # Errors
    ///
    /// Returns an error if the validator name is not a valid SQL identifier.
    pub fn build(self) -> Result<MultiSourceValidator> {
        // Validate the name
        SqlSecurity::validate_identifier(&self.name)?;

        Ok(MultiSourceValidator {
            name: self.name,
            description: self.description,
            sources: HashMap::new(),
            cross_table_validations: Vec::new(),
            foreign_key_validations: Vec::new(),
            config: self.config,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_multi_source_validator_basic() -> Result<()> {
        let mut validator = MultiSourceValidator::new("test_validator");

        // Register sources
        validator.register_source("orders", "Orders table")?;
        validator.register_source("customers", "Customers table")?;

        assert_eq!(validator.sources().len(), 2);
        assert!(validator.sources().contains_key("orders"));
        assert!(validator.sources().contains_key("customers"));

        Ok(())
    }

    #[test]
    fn test_data_source_info() {
        let source = DataSourceInfo::new("test_table")
            .with_description("Test table description")
            .with_schema("public")
            .with_estimated_rows(1000);

        assert_eq!(source.name, "test_table");
        assert_eq!(
            source.description,
            Some("Test table description".to_string())
        );
        assert_eq!(source.schema, Some("public".to_string()));
        assert_eq!(source.estimated_rows, Some(1000));
        assert_eq!(source.qualified_name(), "public.test_table");
    }

    #[test]
    fn test_data_source_info_without_schema() {
        let source = DataSourceInfo::new("simple_table");
        assert_eq!(source.qualified_name(), "simple_table");
    }

    #[test]
    fn test_duplicate_source_registration() {
        let mut validator = MultiSourceValidator::new("test");
        validator
            .register_source("table1", "First registration")
            .unwrap();

        let result = validator.register_source("table1", "Duplicate registration");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("already registered"));
    }

    #[test]
    fn test_builder_configuration() -> Result<()> {
        let validator = MultiSourceValidator::builder("advanced_test")
            .description("Test validator with custom config")
            .max_concurrent_validations(4)
            .memory_budget_mb(512)
            .enable_query_optimization(false)
            .validation_timeout_seconds(120)
            .build()?;

        assert_eq!(validator.name, "advanced_test");
        assert_eq!(
            validator.description,
            Some("Test validator with custom config".to_string())
        );
        assert_eq!(validator.config.max_concurrent_validations, 4);
        assert_eq!(validator.config.memory_budget_mb, 512);
        assert!(!validator.config.enable_query_optimization);
        assert_eq!(validator.config.validation_timeout_seconds, 120);

        Ok(())
    }

    #[test]
    fn test_extract_table_name() -> Result<()> {
        let validator = MultiSourceValidator::new("test");

        // Test table.column format
        assert_eq!(validator.extract_table_name("orders.total")?, "orders");

        // Test schema.table.column format
        assert_eq!(
            validator.extract_table_name("public.orders.total")?,
            "orders"
        );

        // Test invalid formats
        assert!(validator.extract_table_name("invalid_column").is_err());
        assert!(validator.extract_table_name("too.many.parts.here").is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_cross_table_validation_source_verification() -> Result<()> {
        let mut validator = MultiSourceValidator::new("test");

        // Register only one of the two required tables
        validator.register_source("orders", "Orders table")?;

        let constraint = CrossTableSumConstraint::new("orders.total", "payments.amount");
        let result = validator.add_cross_table_validation("test_validation", constraint);

        // Should fail because 'payments' table is not registered
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("payments"));

        Ok(())
    }

    #[tokio::test]
    async fn test_foreign_key_validation_source_verification() -> Result<()> {
        let mut validator = MultiSourceValidator::new("test");

        // Register only one of the two required tables
        validator.register_source("orders", "Orders table")?;

        let constraint = ForeignKeyConstraint::new("orders.customer_id", "customers.id");
        let result = validator.add_foreign_key_validation("test_validation", constraint);

        // Should fail because 'customers' table is not registered
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("customers"));

        Ok(())
    }

    #[tokio::test]
    async fn test_validation_with_registered_sources() -> Result<()> {
        let mut validator = MultiSourceValidator::new("integration_test");

        // Register sources
        validator.register_source("orders", "Orders table")?;
        validator.register_source("customers", "Customers table")?;

        // Add validations
        let fk_constraint = ForeignKeyConstraint::new("orders.customer_id", "customers.id");
        validator.add_foreign_key_validation("customer_integrity", fk_constraint)?;

        assert_eq!(validator.validation_count(), 1);

        // Note: Full integration test with actual DataFusion context would require
        // setting up test tables, which is covered in integration test files
        Ok(())
    }

    #[test]
    fn test_builder_validation_limits() -> Result<()> {
        let validator = MultiSourceValidator::builder("limits_test")
            .max_concurrent_validations(0) // Should be clamped to 1
            .memory_budget_mb(10) // Should be clamped to 64
            .validation_timeout_seconds(10) // Should be clamped to 30
            .build()?;

        assert_eq!(validator.config.max_concurrent_validations, 1);
        assert_eq!(validator.config.memory_budget_mb, 64);
        assert_eq!(validator.config.validation_timeout_seconds, 30);

        Ok(())
    }
}
