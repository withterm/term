//! Uniqueness constraint implementation.
//!
//! This module provides a comprehensive constraint that handles all uniqueness-related
//! validations including full uniqueness, distinctness, unique value ratios, and primary keys.

use crate::constraints::Assertion;
use crate::core::{current_validation_context, Constraint, ConstraintMetadata, ConstraintResult};
use crate::prelude::*;
use crate::security::SqlSecurity;
use arrow::array::Array;
use async_trait::async_trait;
use datafusion::prelude::*;
use std::fmt;
use tracing::instrument;
/// Null handling strategy for uniqueness constraints.
///
/// Defines how NULL values should be treated when evaluating uniqueness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NullHandling {
    /// Exclude NULL values from uniqueness calculations (default behavior).
    /// NULLs are not counted in distinct counts but are included in total counts.
    #[default]
    Exclude,

    /// Include NULL values as regular values in uniqueness calculations.
    /// Multiple NULLs are treated as duplicate values.
    Include,

    /// Treat each NULL as a distinct value.
    /// Each NULL is considered unique from every other NULL.
    Distinct,
}

impl fmt::Display for NullHandling {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NullHandling::Exclude => write!(f, "exclude"),
            NullHandling::Include => write!(f, "include"),
            NullHandling::Distinct => write!(f, "distinct"),
        }
    }
}

/// Type of uniqueness validation to perform.
///
/// This enum encompasses all the different types of uniqueness checks that were
/// previously handled by separate constraint types.
#[derive(Debug, Clone, PartialEq)]
pub enum UniquenessType {
    /// Full uniqueness validation with configurable threshold.
    ///
    /// Replaces the functionality of `UniquenessConstraint`.
    /// Validates that at least `threshold` ratio of values are unique.
    FullUniqueness { threshold: f64 },

    /// Distinctness validation using flexible assertions.
    ///
    /// Replaces the functionality of `DistinctnessConstraint`.
    /// Validates the ratio of distinct values using assertion-based logic.
    Distinctness(Assertion),

    /// Unique value ratio validation using flexible assertions.
    ///
    /// Replaces the functionality of `UniqueValueRatioConstraint`.
    /// Validates the ratio of values that appear exactly once.
    UniqueValueRatio(Assertion),

    /// Primary key validation (unique + non-null).
    ///
    /// Replaces the functionality of `PrimaryKeyConstraint`.
    /// Enforces that values are both unique and contain no NULLs.
    PrimaryKey,

    /// Uniqueness validation that allows NULL values.
    ///
    /// Similar to FullUniqueness but with explicit NULL handling control.
    UniqueWithNulls {
        threshold: f64,
        null_handling: NullHandling,
    },

    /// Composite uniqueness with advanced null handling.
    ///
    /// Optimized for multi-column uniqueness checks with configurable
    /// null handling strategies.
    UniqueComposite {
        threshold: f64,
        null_handling: NullHandling,
        case_sensitive: bool,
    },
}

impl UniquenessType {
    /// Returns the name of this uniqueness type for tracing and metadata.
    pub fn name(&self) -> &str {
        match self {
            UniquenessType::FullUniqueness { .. } => "full_uniqueness",
            UniquenessType::Distinctness(_) => "distinctness",
            UniquenessType::UniqueValueRatio(_) => "unique_value_ratio",
            UniquenessType::PrimaryKey => "primary_key",
            UniquenessType::UniqueWithNulls { .. } => "unique_with_nulls",
            UniquenessType::UniqueComposite { .. } => "unique_composite",
        }
    }

    /// Returns a human-readable description of this uniqueness type.
    pub fn description(&self) -> String {
        match self {
            UniquenessType::FullUniqueness { threshold } => {
                let threshold_pct = threshold * 100.0;
                format!("validates that at least {threshold_pct:.1}% of values are unique")
            }
            UniquenessType::Distinctness(assertion) => {
                format!(
                    "validates that distinct value ratio {}",
                    assertion.description()
                )
            }
            UniquenessType::UniqueValueRatio(assertion) => {
                format!(
                    "validates that unique value ratio {}",
                    assertion.description()
                )
            }
            UniquenessType::PrimaryKey => {
                "validates that values form a valid primary key (unique + non-null)".to_string()
            }
            UniquenessType::UniqueWithNulls {
                threshold,
                null_handling,
            } => {
                let threshold_pct = threshold * 100.0;
                format!(
                    "validates that at least {threshold_pct:.1}% of values are unique (nulls: {null_handling})"
                )
            }
            UniquenessType::UniqueComposite {
                threshold,
                null_handling,
                case_sensitive,
            } => {
                let threshold_pct = threshold * 100.0;
                format!(
                    "validates composite uniqueness at {threshold_pct:.1}% threshold (nulls: {null_handling}, case-sensitive: {case_sensitive})"
                )
            }
        }
    }
}

/// Options for configuring uniqueness constraint behavior.
#[derive(Debug, Clone, PartialEq)]
pub struct UniquenessOptions {
    /// How to handle NULL values in uniqueness calculations.
    pub null_handling: NullHandling,

    /// Whether string comparisons should be case-sensitive.
    pub case_sensitive: bool,

    /// Whether to trim whitespace before comparison.
    pub trim_whitespace: bool,
}

impl Default for UniquenessOptions {
    fn default() -> Self {
        Self {
            null_handling: NullHandling::default(),
            case_sensitive: true,
            trim_whitespace: false,
        }
    }
}

impl UniquenessOptions {
    /// Creates new options with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the null handling strategy.
    pub fn with_null_handling(mut self, null_handling: NullHandling) -> Self {
        self.null_handling = null_handling;
        self
    }

    /// Sets whether string comparisons should be case-sensitive.
    pub fn case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
        self
    }

    /// Sets whether to trim whitespace before comparison.
    pub fn trim_whitespace(mut self, trim_whitespace: bool) -> Self {
        self.trim_whitespace = trim_whitespace;
        self
    }
}

/// A unified constraint that handles all types of uniqueness validation.
///
/// This constraint consolidates the functionality of multiple uniqueness-related constraints:
/// - `UniquenessConstraint` - Full uniqueness with threshold
/// - `DistinctnessConstraint` - Distinctness ratio validation
/// - `UniqueValueRatioConstraint` - Values appearing exactly once
/// - `PrimaryKeyConstraint` - Unique + non-null validation
///
/// # Examples
///
/// ## Full Uniqueness (replacing UniquenessConstraint)
///
/// ```rust
/// use term_guard::constraints::{UniquenessConstraint, UniquenessType};
///
/// // Single column uniqueness
/// let constraint = UniquenessConstraint::full_uniqueness("user_id", 1.0)?;
///
/// // Multi-column uniqueness with threshold
/// let constraint = UniquenessConstraint::full_uniqueness_multi(
///     vec!["email", "domain"],
///     0.95
/// )?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// ## Distinctness (replacing DistinctnessConstraint)
///
/// ```rust
/// use term_guard::constraints::{UniquenessConstraint, Assertion};
///
/// let constraint = UniquenessConstraint::distinctness(
///     vec!["category"],
///     Assertion::GreaterThan(0.8)
/// )?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// ## Primary Key (replacing PrimaryKeyConstraint)
///
/// ```rust
/// use term_guard::constraints::UniquenessConstraint;
///
/// let constraint = UniquenessConstraint::primary_key(
///     vec!["order_id", "line_item_id"]
/// )?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone)]
pub struct UniquenessConstraint {
    columns: Vec<String>,
    uniqueness_type: UniquenessType,
    options: UniquenessOptions,
}

impl UniquenessConstraint {
    /// Creates a new unified uniqueness constraint.
    ///
    /// # Arguments
    ///
    /// * `columns` - The columns to check for uniqueness
    /// * `uniqueness_type` - The type of uniqueness validation to perform
    /// * `options` - Configuration options for the constraint
    ///
    /// # Errors
    ///
    /// Returns error if column names are invalid or thresholds are out of range.
    pub fn new<I, S>(
        columns: I,
        uniqueness_type: UniquenessType,
        options: UniquenessOptions,
    ) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let column_vec: Vec<String> = columns.into_iter().map(Into::into).collect();

        if column_vec.is_empty() {
            return Err(TermError::validation_failed(
                "unified_uniqueness",
                "At least one column must be specified",
            ));
        }

        // Validate column names
        for column in &column_vec {
            SqlSecurity::validate_identifier(column)?;
        }

        // Validate thresholds in uniqueness types
        match &uniqueness_type {
            UniquenessType::FullUniqueness { threshold }
            | UniquenessType::UniqueWithNulls { threshold, .. }
            | UniquenessType::UniqueComposite { threshold, .. } => {
                if !((0.0..=1.0).contains(threshold)) {
                    return Err(TermError::validation_failed(
                        "unified_uniqueness",
                        "Threshold must be between 0.0 and 1.0",
                    ));
                }
            }
            _ => {} // Other types don't have threshold validation
        }

        Ok(Self {
            columns: column_vec,
            uniqueness_type,
            options,
        })
    }

    /// Creates a full uniqueness constraint for a single column.
    ///
    /// This replaces `UniquenessConstraint::single()`.
    pub fn full_uniqueness(column: impl Into<String>, threshold: f64) -> Result<Self> {
        Self::new(
            vec![column.into()],
            UniquenessType::FullUniqueness { threshold },
            UniquenessOptions::default(),
        )
    }

    /// Creates a full uniqueness constraint for multiple columns.
    ///
    /// This replaces `UniquenessConstraint::multiple()` and `UniquenessConstraint::with_threshold()`.
    pub fn full_uniqueness_multi<I, S>(columns: I, threshold: f64) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::new(
            columns,
            UniquenessType::FullUniqueness { threshold },
            UniquenessOptions::default(),
        )
    }

    /// Creates a distinctness constraint.
    ///
    /// This replaces `DistinctnessConstraint::new()`.
    pub fn distinctness<I, S>(columns: I, assertion: Assertion) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::new(
            columns,
            UniquenessType::Distinctness(assertion),
            UniquenessOptions::default(),
        )
    }

    /// Creates a unique value ratio constraint.
    ///
    /// This replaces `UniqueValueRatioConstraint::new()`.
    pub fn unique_value_ratio<I, S>(columns: I, assertion: Assertion) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::new(
            columns,
            UniquenessType::UniqueValueRatio(assertion),
            UniquenessOptions::default(),
        )
    }

    /// Creates a primary key constraint.
    ///
    /// This replaces `PrimaryKeyConstraint::new()`.
    pub fn primary_key<I, S>(columns: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::new(
            columns,
            UniquenessType::PrimaryKey,
            UniquenessOptions::default(),
        )
    }

    /// Creates a uniqueness constraint that allows NULLs.
    pub fn unique_with_nulls<I, S>(
        columns: I,
        threshold: f64,
        null_handling: NullHandling,
    ) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::new(
            columns,
            UniquenessType::UniqueWithNulls {
                threshold,
                null_handling,
            },
            UniquenessOptions::default(),
        )
    }

    /// Creates a composite uniqueness constraint with advanced options.
    pub fn unique_composite<I, S>(
        columns: I,
        threshold: f64,
        null_handling: NullHandling,
        case_sensitive: bool,
    ) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::new(
            columns,
            UniquenessType::UniqueComposite {
                threshold,
                null_handling,
                case_sensitive,
            },
            UniquenessOptions::new()
                .with_null_handling(null_handling)
                .case_sensitive(case_sensitive),
        )
    }

    /// Returns the columns being validated.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Returns the uniqueness type.
    pub fn uniqueness_type(&self) -> &UniquenessType {
        &self.uniqueness_type
    }

    /// Returns the constraint options.
    pub fn options(&self) -> &UniquenessOptions {
        &self.options
    }
}

#[async_trait]
impl Constraint for UniquenessConstraint {
    #[instrument(skip(self, ctx), fields(
        columns = ?self.columns,
        uniqueness_type = %self.uniqueness_type.name(),
        null_handling = %self.options.null_handling
    ))]
    async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult> {
        // Get the table name from the validation context
        let validation_ctx = current_validation_context();
        let table_name = validation_ctx.table_name();

        // Generate SQL based on uniqueness type
        let sql = self.generate_sql(table_name)?;

        let df = ctx.sql(&sql).await?;
        let batches = df.collect().await?;

        if batches.is_empty() {
            return Ok(ConstraintResult::skipped("No data to validate"));
        }

        let batch = &batches[0];
        if batch.num_rows() == 0 {
            return Ok(ConstraintResult::skipped("No data to validate"));
        }

        // Process results based on uniqueness type
        match &self.uniqueness_type {
            UniquenessType::FullUniqueness { threshold }
            | UniquenessType::UniqueWithNulls { threshold, .. }
            | UniquenessType::UniqueComposite { threshold, .. } => {
                self.evaluate_threshold_based(batch, *threshold).await
            }
            UniquenessType::Distinctness(assertion)
            | UniquenessType::UniqueValueRatio(assertion) => {
                self.evaluate_assertion_based(batch, assertion).await
            }
            UniquenessType::PrimaryKey => self.evaluate_primary_key(batch).await,
        }
    }

    fn name(&self) -> &str {
        self.uniqueness_type.name()
    }

    fn column(&self) -> Option<&str> {
        if self.columns.len() == 1 {
            Some(&self.columns[0])
        } else {
            None
        }
    }

    fn metadata(&self) -> ConstraintMetadata {
        let mut metadata = if self.columns.len() == 1 {
            ConstraintMetadata::for_column(&self.columns[0])
        } else {
            ConstraintMetadata::for_columns(&self.columns)
        };

        metadata = metadata
            .with_description(format!(
                "Unified uniqueness constraint that {}",
                self.uniqueness_type.description()
            ))
            .with_custom("uniqueness_type", self.uniqueness_type.name())
            .with_custom("null_handling", self.options.null_handling.to_string())
            .with_custom("case_sensitive", self.options.case_sensitive.to_string())
            .with_custom("constraint_type", "uniqueness");

        // Add type-specific metadata
        match &self.uniqueness_type {
            UniquenessType::FullUniqueness { threshold }
            | UniquenessType::UniqueWithNulls { threshold, .. }
            | UniquenessType::UniqueComposite { threshold, .. } => {
                metadata = metadata.with_custom("threshold", threshold.to_string());
            }
            UniquenessType::Distinctness(assertion)
            | UniquenessType::UniqueValueRatio(assertion) => {
                metadata = metadata.with_custom("assertion", assertion.to_string());
            }
            UniquenessType::PrimaryKey => {
                metadata = metadata.with_custom("strict", "true");
            }
        }

        metadata
    }
}

impl UniquenessConstraint {
    /// Generates SQL query based on the uniqueness type and options.
    fn generate_sql(&self, table_name: &str) -> Result<String> {
        match &self.uniqueness_type {
            UniquenessType::FullUniqueness { .. }
            | UniquenessType::UniqueWithNulls { .. }
            | UniquenessType::UniqueComposite { .. } => {
                self.generate_full_uniqueness_sql(table_name)
            }
            UniquenessType::Distinctness(_) => self.generate_distinctness_sql(table_name),
            UniquenessType::UniqueValueRatio(_) => self.generate_unique_value_ratio_sql(table_name),
            UniquenessType::PrimaryKey => self.generate_primary_key_sql(table_name),
        }
    }

    /// Generates SQL for full uniqueness validation.
    fn generate_full_uniqueness_sql(&self, table_name: &str) -> Result<String> {
        let escaped_columns: Result<Vec<String>> = self
            .columns
            .iter()
            .map(|col| SqlSecurity::escape_identifier(col))
            .collect();
        let escaped_columns = escaped_columns?;

        let columns_expr = if self.columns.len() == 1 {
            escaped_columns[0].clone()
        } else {
            let cols = escaped_columns.join(", ");
            format!("({cols})")
        };

        let sql = match &self.uniqueness_type {
            UniquenessType::UniqueWithNulls {
                null_handling: NullHandling::Include,
                ..
            } => {
                // Include NULLs in distinct count by coalescing to a special value
                if self.columns.len() == 1 {
                    let col = &escaped_columns[0];
                    format!(
                        "SELECT 
                            COUNT(*) as total_count,
                            COUNT(DISTINCT COALESCE({col}, '<NULL>')) as unique_count
                         FROM {table_name}"
                    )
                } else {
                    format!(
                        "SELECT 
                            COUNT(*) as total_count,
                            COUNT(DISTINCT {columns_expr}) as unique_count
                         FROM {table_name}"
                    )
                }
            }
            UniquenessType::UniqueWithNulls {
                null_handling: NullHandling::Distinct,
                ..
            } => {
                // Each NULL is treated as distinct
                if self.columns.len() == 1 {
                    let col = &escaped_columns[0];
                    format!(
                        "SELECT 
                            COUNT(*) as total_count,
                            COUNT(DISTINCT {col}) + CASE WHEN COUNT(*) - COUNT({col}) > 0 THEN COUNT(*) - COUNT({col}) ELSE 0 END as unique_count
                         FROM {table_name}"
                    )
                } else {
                    // For multi-column, this is more complex - treat as regular for now
                    format!(
                        "SELECT 
                            COUNT(*) as total_count,
                            COUNT(DISTINCT {columns_expr}) as unique_count
                         FROM {table_name}"
                    )
                }
            }
            _ => {
                // Standard uniqueness (excludes NULLs from distinct count)
                format!(
                    "SELECT 
                        COUNT(*) as total_count,
                        COUNT(DISTINCT {columns_expr}) as unique_count
                     FROM {table_name}"
                )
            }
        };

        Ok(sql)
    }

    /// Generates SQL for distinctness validation.
    fn generate_distinctness_sql(&self, table_name: &str) -> Result<String> {
        let escaped_columns: Result<Vec<String>> = self
            .columns
            .iter()
            .map(|col| SqlSecurity::escape_identifier(col))
            .collect();
        let escaped_columns = escaped_columns?;

        let sql = if self.columns.len() == 1 {
            let col = &escaped_columns[0];
            format!(
                "SELECT 
                    COUNT(DISTINCT {col}) as distinct_count,
                    COUNT(*) as total_count
                 FROM {table_name}"
            )
        } else {
            // Multi-column distinctness using concatenation with NULL handling
            let concat_expr = escaped_columns
                .iter()
                .map(|col| format!("COALESCE(CAST({col} AS VARCHAR), '<NULL>')"))
                .collect::<Vec<_>>()
                .join(" || '|' || ");

            format!(
                "SELECT 
                    COUNT(DISTINCT ({concat_expr})) as distinct_count,
                    COUNT(*) as total_count
                 FROM {table_name}"
            )
        };

        Ok(sql)
    }

    /// Generates SQL for unique value ratio validation.
    fn generate_unique_value_ratio_sql(&self, table_name: &str) -> Result<String> {
        let escaped_columns: Result<Vec<String>> = self
            .columns
            .iter()
            .map(|col| SqlSecurity::escape_identifier(col))
            .collect();
        let escaped_columns = escaped_columns?;

        let columns_list = escaped_columns.join(", ");

        let sql = format!(
            "WITH value_counts AS (
                SELECT {columns_list}, COUNT(*) as cnt
                FROM {table_name}
                GROUP BY {columns_list}
            )
            SELECT 
                COALESCE(SUM(CASE WHEN cnt = 1 THEN cnt ELSE 0 END), 0) as unique_count,
                COALESCE(SUM(cnt), 0) as total_count
            FROM value_counts"
        );

        Ok(sql)
    }

    /// Generates SQL for primary key validation.
    fn generate_primary_key_sql(&self, table_name: &str) -> Result<String> {
        let escaped_columns: Result<Vec<String>> = self
            .columns
            .iter()
            .map(|col| SqlSecurity::escape_identifier(col))
            .collect();
        let escaped_columns = escaped_columns?;

        let columns_expr = if self.columns.len() == 1 {
            escaped_columns[0].clone()
        } else {
            let cols = escaped_columns.join(", ");
            format!("({cols})")
        };

        // Check for NULLs in all columns
        let null_check = escaped_columns
            .iter()
            .map(|col| format!("{col} IS NOT NULL"))
            .collect::<Vec<_>>()
            .join(" AND ");

        let sql = format!(
            "SELECT 
                COUNT(*) as total_count,
                COUNT(DISTINCT {columns_expr}) as unique_count,
                COUNT(*) - COUNT(CASE WHEN {null_check} THEN 1 END) as null_count
             FROM {table_name}"
        );

        Ok(sql)
    }

    /// Evaluates threshold-based uniqueness results.
    async fn evaluate_threshold_based(
        &self,
        batch: &arrow::record_batch::RecordBatch,
        threshold: f64,
    ) -> Result<ConstraintResult> {
        let total_count = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| TermError::Internal("Failed to extract total count".to_string()))?
            .value(0) as f64;

        let unique_count = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| TermError::Internal("Failed to extract unique count".to_string()))?
            .value(0) as f64;

        if total_count == 0.0 {
            return Ok(ConstraintResult::skipped("No data to validate"));
        }

        let uniqueness_ratio = unique_count / total_count;

        if uniqueness_ratio >= threshold {
            Ok(ConstraintResult::success_with_metric(uniqueness_ratio))
        } else {
            Ok(ConstraintResult::failure_with_metric(
                uniqueness_ratio,
                format!(
                    "Uniqueness ratio {uniqueness_ratio:.3} is below threshold {threshold:.3} for columns: {}",
                    self.columns.join(", ")
                ),
            ))
        }
    }

    /// Evaluates assertion-based results (distinctness and unique value ratio).
    async fn evaluate_assertion_based(
        &self,
        batch: &arrow::record_batch::RecordBatch,
        assertion: &Assertion,
    ) -> Result<ConstraintResult> {
        let count = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| TermError::Internal("Failed to extract count".to_string()))?
            .value(0) as f64;

        let total_count = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| TermError::Internal("Failed to extract total count".to_string()))?
            .value(0) as f64;

        if total_count == 0.0 {
            return Ok(ConstraintResult::skipped("No data to validate"));
        }

        let ratio = count / total_count;

        if assertion.evaluate(ratio) {
            Ok(ConstraintResult::success_with_metric(ratio))
        } else {
            Ok(ConstraintResult::failure_with_metric(
                ratio,
                format!(
                    "{} ratio {ratio:.3} does not satisfy {} for columns: {}",
                    self.uniqueness_type.name(),
                    assertion.description(),
                    self.columns.join(", ")
                ),
            ))
        }
    }

    /// Evaluates primary key results.
    async fn evaluate_primary_key(
        &self,
        batch: &arrow::record_batch::RecordBatch,
    ) -> Result<ConstraintResult> {
        let total_count = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| TermError::Internal("Failed to extract total count".to_string()))?
            .value(0) as f64;

        let unique_count = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| TermError::Internal("Failed to extract unique count".to_string()))?
            .value(0) as f64;

        let null_count = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| TermError::Internal("Failed to extract null count".to_string()))?
            .value(0) as f64;

        if total_count == 0.0 {
            return Ok(ConstraintResult::skipped("No data to validate"));
        }

        // Primary key validation: no NULLs and all values unique
        if null_count > 0.0 {
            Ok(ConstraintResult::failure_with_metric(
                null_count / total_count,
                format!(
                    "Primary key columns contain {null_count} NULL values: {}",
                    self.columns.join(", ")
                ),
            ))
        } else if unique_count != total_count {
            let duplicate_ratio = (total_count - unique_count) / total_count;
            Ok(ConstraintResult::failure_with_metric(
                duplicate_ratio,
                format!(
                    "Primary key columns contain {} duplicate values: {}",
                    total_count - unique_count,
                    self.columns.join(", ")
                ),
            ))
        } else {
            Ok(ConstraintResult::success_with_metric(1.0))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constraints::Assertion;
    use crate::core::ConstraintStatus;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use std::sync::Arc;

    use crate::test_helpers::evaluate_constraint_with_context;
    async fn create_test_context(values: Vec<Option<&str>>) -> SessionContext {
        let ctx = SessionContext::new();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "test_col",
            DataType::Utf8,
            true,
        )]));

        let array = StringArray::from(values);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();

        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("data", Arc::new(provider)).unwrap();

        ctx
    }

    async fn create_multi_column_test_context(
        col1_values: Vec<Option<&str>>,
        col2_values: Vec<Option<&str>>,
    ) -> SessionContext {
        let ctx = SessionContext::new();

        let schema = Arc::new(Schema::new(vec![
            Field::new("col1", DataType::Utf8, true),
            Field::new("col2", DataType::Utf8, true),
        ]));

        let array1 = StringArray::from(col1_values);
        let array2 = StringArray::from(col2_values);
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(array1), Arc::new(array2)]).unwrap();

        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("data", Arc::new(provider)).unwrap();

        ctx
    }

    #[tokio::test]
    async fn test_full_uniqueness_single_column() {
        let values = vec![Some("A"), Some("B"), Some("C"), Some("A")];
        let ctx = create_test_context(values).await;

        let constraint = UniquenessConstraint::full_uniqueness("test_col", 0.7).unwrap();

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(0.75)); // 3 unique out of 4 total
    }

    #[tokio::test]
    async fn test_full_uniqueness_with_nulls() {
        let values = vec![Some("A"), Some("B"), None, Some("A")];
        let ctx = create_test_context(values).await;

        // Standard uniqueness (excludes NULLs from distinct count)
        let constraint = UniquenessConstraint::full_uniqueness("test_col", 0.4).unwrap();

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(0.5)); // 2 unique non-null out of 4 total
    }

    #[tokio::test]
    async fn test_distinctness_constraint() {
        let values = vec![Some("A"), Some("B"), Some("C"), Some("A")];
        let ctx = create_test_context(values).await;

        let constraint =
            UniquenessConstraint::distinctness(vec!["test_col"], Assertion::Equals(0.75)).unwrap();

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(0.75)); // 3 distinct out of 4 total
    }

    #[tokio::test]
    async fn test_unique_value_ratio_constraint() {
        let values = vec![Some("A"), Some("B"), Some("C"), Some("A")];
        let ctx = create_test_context(values).await;

        let constraint =
            UniquenessConstraint::unique_value_ratio(vec!["test_col"], Assertion::Equals(0.5))
                .unwrap();

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(0.5)); // 2 values appear exactly once out of 4 total
    }

    #[tokio::test]
    async fn test_primary_key_success() {
        let values = vec![Some("A"), Some("B"), Some("C")];
        let ctx = create_test_context(values).await;

        let constraint = UniquenessConstraint::primary_key(vec!["test_col"]).unwrap();

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(1.0));
    }

    #[tokio::test]
    async fn test_primary_key_with_nulls() {
        let values = vec![Some("A"), Some("B"), None];
        let ctx = create_test_context(values).await;

        let constraint = UniquenessConstraint::primary_key(vec!["test_col"]).unwrap();

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Failure);
        assert!(result.message.unwrap().contains("NULL values"));
    }

    #[tokio::test]
    async fn test_primary_key_with_duplicates() {
        let values = vec![Some("A"), Some("B"), Some("A")];
        let ctx = create_test_context(values).await;

        let constraint = UniquenessConstraint::primary_key(vec!["test_col"]).unwrap();

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Failure);
        assert!(result.message.unwrap().contains("duplicate values"));
    }

    #[tokio::test]
    async fn test_multi_column_uniqueness() {
        let col1_values = vec![Some("A"), Some("B"), Some("A")];
        let col2_values = vec![Some("1"), Some("2"), Some("2")];
        let ctx = create_multi_column_test_context(col1_values, col2_values).await;

        let constraint =
            UniquenessConstraint::full_uniqueness_multi(vec!["col1", "col2"], 0.9).unwrap();

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(1.0)); // All combinations are unique
    }

    #[tokio::test]
    async fn test_multi_column_distinctness() {
        let col1_values = vec![Some("A"), Some("B"), Some("A")];
        let col2_values = vec![Some("1"), Some("2"), Some("1")];
        let ctx = create_multi_column_test_context(col1_values, col2_values).await;

        let constraint =
            UniquenessConstraint::distinctness(vec!["col1", "col2"], Assertion::GreaterThan(0.5))
                .unwrap();

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        // Two distinct combinations: A|1 and B|2, plus A|1 repeated = 2/3 = 0.67
        assert!((result.metric.unwrap() - 2.0 / 3.0).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_unique_with_nulls_include() {
        let values = vec![Some("A"), Some("B"), None, None];
        let ctx = create_test_context(values).await;

        let constraint =
            UniquenessConstraint::unique_with_nulls(vec!["test_col"], 0.4, NullHandling::Include)
                .unwrap();

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(0.75)); // A, B, NULL (treated as one value) = 3/4
    }

    #[tokio::test]
    async fn test_empty_data() {
        let values: Vec<Option<&str>> = vec![];
        let ctx = create_test_context(values).await;

        let constraint = UniquenessConstraint::full_uniqueness("test_col", 1.0).unwrap();

        let result = evaluate_constraint_with_context(&constraint, &ctx, "data")
            .await
            .unwrap();
        assert_eq!(result.status, ConstraintStatus::Skipped);
    }

    #[tokio::test]
    async fn test_invalid_threshold() {
        let result = UniquenessConstraint::full_uniqueness("col", 1.5);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Threshold must be between 0.0 and 1.0"));
    }

    #[tokio::test]
    async fn test_empty_columns() {
        let columns: Vec<String> = vec![];
        let result = UniquenessConstraint::new(
            columns,
            UniquenessType::FullUniqueness { threshold: 1.0 },
            UniquenessOptions::default(),
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("At least one column must be specified"));
    }

    #[tokio::test]
    async fn test_constraint_metadata() {
        let constraint = UniquenessConstraint::full_uniqueness("test_col", 0.95).unwrap();
        let metadata = constraint.metadata();

        assert!(metadata
            .description
            .unwrap_or_default()
            .contains("Unified uniqueness constraint"));
        assert_eq!(constraint.name(), "full_uniqueness");
        assert_eq!(constraint.column(), Some("test_col"));
    }

    #[tokio::test]
    async fn test_multi_column_metadata() {
        let constraint =
            UniquenessConstraint::full_uniqueness_multi(vec!["col1", "col2"], 0.9).unwrap();

        assert_eq!(constraint.column(), None); // Multi-column has no single column
        assert_eq!(constraint.columns(), &["col1", "col2"]);
    }
}
