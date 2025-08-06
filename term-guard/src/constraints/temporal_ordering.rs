//! Temporal ordering constraint for time-based validation in Term.
//!
//! This module provides temporal ordering validation capabilities for ensuring that timestamps
//! follow expected chronological relationships, validating event sequences, and checking
//! business hour compliance.
//!
//! # Examples
//!
//! ## Basic Temporal Ordering Validation
//!
//! ```rust
//! use term_guard::constraints::TemporalOrderingConstraint;
//! use term_guard::core::{Check, Level};
//!
//! // Validate that created_at always comes before processed_at
//! let constraint = TemporalOrderingConstraint::new("events")
//!     .before_after("created_at", "processed_at");
//!
//! let check = Check::builder("temporal_consistency")
//!     .level(Level::Error)
//!     .with_constraint(constraint)
//!     .build();
//! ```
//!
//! ## Business Hours Validation
//!
//! ```rust
//! use term_guard::constraints::TemporalOrderingConstraint;
//!
//! // Validate that transactions occur during business hours
//! let constraint = TemporalOrderingConstraint::new("transactions")
//!     .business_hours("timestamp", "09:00", "17:00")
//!     .weekdays_only(true);
//! ```

use crate::core::{Constraint, ConstraintResult, ConstraintStatus};
use crate::error::{Result, TermError};
use crate::security::SqlSecurity;
use arrow::array::{Array, Int64Array};
use async_trait::async_trait;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument, warn};

/// Temporal ordering constraint for validating time-based relationships.
///
/// This constraint ensures that temporal data follows expected patterns, including:
/// - Chronological ordering between columns
/// - Business hour compliance
/// - Date range validation
/// - Event sequence validation
/// - Time gap analysis
///
/// The constraint supports various temporal data types and timezone considerations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemporalOrderingConstraint {
    /// Table name to validate
    table_name: String,
    /// Type of temporal validation to perform
    validation_type: TemporalValidationType,
    /// Whether to allow null timestamps
    allow_nulls: bool,
    /// Tolerance for temporal comparisons (in seconds)
    tolerance_seconds: i64,
    /// Maximum number of violation examples to report
    max_violations_reported: usize,
}

/// Type of temporal validation to perform
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TemporalValidationType {
    /// Validate that one column comes before another
    BeforeAfter {
        before_column: String,
        after_column: String,
        allow_equal: bool,
    },
    /// Validate that timestamps fall within business hours
    BusinessHours {
        timestamp_column: String,
        start_time: String, // Format: "HH:MM"
        end_time: String,   // Format: "HH:MM"
        weekdays_only: bool,
        timezone: Option<String>,
    },
    /// Validate that timestamps are within a specific date range
    DateRange {
        timestamp_column: String,
        min_date: Option<String>, // ISO format
        max_date: Option<String>, // ISO format
    },
    /// Validate maximum time gap between sequential events
    MaxTimeGap {
        timestamp_column: String,
        group_by_column: Option<String>,
        max_gap_seconds: i64,
    },
    /// Validate that events follow a specific sequence
    EventSequence {
        event_column: String,
        timestamp_column: String,
        expected_sequence: Vec<String>,
    },
}

impl TemporalOrderingConstraint {
    /// Create a new temporal ordering constraint.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Name of the table to validate
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_guard::constraints::TemporalOrderingConstraint;
    ///
    /// let constraint = TemporalOrderingConstraint::new("events");
    /// ```
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            validation_type: TemporalValidationType::BeforeAfter {
                before_column: String::new(),
                after_column: String::new(),
                allow_equal: false,
            },
            allow_nulls: false,
            tolerance_seconds: 0,
            max_violations_reported: 100,
        }
    }

    /// Validate that one timestamp column comes before another.
    ///
    /// # Arguments
    ///
    /// * `before_column` - Column that should contain earlier timestamps
    /// * `after_column` - Column that should contain later timestamps
    pub fn before_after(
        mut self,
        before_column: impl Into<String>,
        after_column: impl Into<String>,
    ) -> Self {
        self.validation_type = TemporalValidationType::BeforeAfter {
            before_column: before_column.into(),
            after_column: after_column.into(),
            allow_equal: false,
        };
        self
    }

    /// Validate that one timestamp column comes before or equals another.
    pub fn before_or_equal(
        mut self,
        before_column: impl Into<String>,
        after_column: impl Into<String>,
    ) -> Self {
        self.validation_type = TemporalValidationType::BeforeAfter {
            before_column: before_column.into(),
            after_column: after_column.into(),
            allow_equal: true,
        };
        self
    }

    /// Validate that timestamps fall within business hours.
    ///
    /// # Arguments
    ///
    /// * `timestamp_column` - Column containing timestamps to validate
    /// * `start_time` - Start of business hours (format: "HH:MM")
    /// * `end_time` - End of business hours (format: "HH:MM")
    pub fn business_hours(
        mut self,
        timestamp_column: impl Into<String>,
        start_time: impl Into<String>,
        end_time: impl Into<String>,
    ) -> Self {
        self.validation_type = TemporalValidationType::BusinessHours {
            timestamp_column: timestamp_column.into(),
            start_time: start_time.into(),
            end_time: end_time.into(),
            weekdays_only: false,
            timezone: None,
        };
        self
    }

    /// Set whether business hours validation should only apply to weekdays.
    pub fn weekdays_only(mut self, weekdays_only: bool) -> Self {
        if let TemporalValidationType::BusinessHours {
            timestamp_column,
            start_time,
            end_time,
            timezone,
            ..
        } = self.validation_type
        {
            self.validation_type = TemporalValidationType::BusinessHours {
                timestamp_column,
                start_time,
                end_time,
                weekdays_only,
                timezone,
            };
        }
        self
    }

    /// Set the timezone for business hours validation.
    pub fn with_timezone(mut self, timezone: impl Into<String>) -> Self {
        if let TemporalValidationType::BusinessHours {
            timestamp_column,
            start_time,
            end_time,
            weekdays_only,
            ..
        } = self.validation_type
        {
            self.validation_type = TemporalValidationType::BusinessHours {
                timestamp_column,
                start_time,
                end_time,
                weekdays_only,
                timezone: Some(timezone.into()),
            };
        }
        self
    }

    /// Validate that timestamps are within a specific date range.
    pub fn date_range(
        mut self,
        timestamp_column: impl Into<String>,
        min_date: Option<impl Into<String>>,
        max_date: Option<impl Into<String>>,
    ) -> Self {
        self.validation_type = TemporalValidationType::DateRange {
            timestamp_column: timestamp_column.into(),
            min_date: min_date.map(Into::into),
            max_date: max_date.map(Into::into),
        };
        self
    }

    /// Validate maximum time gap between sequential events.
    pub fn max_time_gap(
        mut self,
        timestamp_column: impl Into<String>,
        max_gap_seconds: i64,
    ) -> Self {
        self.validation_type = TemporalValidationType::MaxTimeGap {
            timestamp_column: timestamp_column.into(),
            group_by_column: None,
            max_gap_seconds,
        };
        self
    }

    /// Set grouping column for time gap validation.
    pub fn group_by(mut self, column: impl Into<String>) -> Self {
        if let TemporalValidationType::MaxTimeGap {
            timestamp_column,
            max_gap_seconds,
            ..
        } = self.validation_type
        {
            self.validation_type = TemporalValidationType::MaxTimeGap {
                timestamp_column,
                group_by_column: Some(column.into()),
                max_gap_seconds,
            };
        }
        self
    }

    /// Set whether to allow null timestamps.
    pub fn allow_nulls(mut self, allow: bool) -> Self {
        self.allow_nulls = allow;
        self
    }

    /// Set tolerance for temporal comparisons in seconds.
    pub fn tolerance_seconds(mut self, seconds: i64) -> Self {
        self.tolerance_seconds = seconds;
        self
    }

    /// Validate identifiers for SQL security.
    fn validate_identifiers(&self) -> Result<()> {
        SqlSecurity::validate_identifier(&self.table_name)?;

        match &self.validation_type {
            TemporalValidationType::BeforeAfter {
                before_column,
                after_column,
                ..
            } => {
                SqlSecurity::validate_identifier(before_column)?;
                SqlSecurity::validate_identifier(after_column)?;
            }
            TemporalValidationType::BusinessHours {
                timestamp_column, ..
            } => {
                SqlSecurity::validate_identifier(timestamp_column)?;
            }
            TemporalValidationType::DateRange {
                timestamp_column, ..
            } => {
                SqlSecurity::validate_identifier(timestamp_column)?;
            }
            TemporalValidationType::MaxTimeGap {
                timestamp_column,
                group_by_column,
                ..
            } => {
                SqlSecurity::validate_identifier(timestamp_column)?;
                if let Some(group_col) = group_by_column {
                    SqlSecurity::validate_identifier(group_col)?;
                }
            }
            TemporalValidationType::EventSequence {
                event_column,
                timestamp_column,
                ..
            } => {
                SqlSecurity::validate_identifier(event_column)?;
                SqlSecurity::validate_identifier(timestamp_column)?;
            }
        }

        Ok(())
    }

    /// Generate SQL query for temporal validation.
    fn generate_validation_query(&self) -> Result<String> {
        self.validate_identifiers()?;

        let _null_condition = if self.allow_nulls {
            String::new()
        } else {
            " AND {} IS NOT NULL AND {} IS NOT NULL".to_string()
        };

        let sql = match &self.validation_type {
            TemporalValidationType::BeforeAfter {
                before_column,
                after_column,
                allow_equal,
            } => {
                let comparison = if *allow_equal {
                    if self.tolerance_seconds > 0 {
                        format!(
                            "{after_column} > {before_column} + INTERVAL '{} seconds'",
                            self.tolerance_seconds
                        )
                    } else {
                        format!("{after_column} > {before_column}")
                    }
                } else if self.tolerance_seconds > 0 {
                    format!(
                        "{after_column} >= {before_column} + INTERVAL '{} seconds'",
                        self.tolerance_seconds
                    )
                } else {
                    format!("{after_column} >= {before_column}")
                };

                let null_clause = if self.allow_nulls {
                    String::new()
                } else {
                    format!(" AND {before_column} IS NOT NULL AND {after_column} IS NOT NULL")
                };

                format!(
                    "SELECT 
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN {comparison} THEN 0 ELSE 1 END) as violations
                     FROM {}
                     WHERE 1=1{null_clause}",
                    self.table_name
                )
            }
            TemporalValidationType::BusinessHours {
                timestamp_column,
                start_time,
                end_time,
                weekdays_only,
                ..
            } => {
                let time_check = format!(
                    "CAST({timestamp_column} AS TIME) BETWEEN TIME '{start_time}:00' AND TIME '{end_time}:00'"
                );

                let weekday_check = if *weekdays_only {
                    format!(" AND EXTRACT(DOW FROM {timestamp_column}) BETWEEN 1 AND 5")
                } else {
                    String::new()
                };

                let null_clause = if self.allow_nulls {
                    String::new()
                } else {
                    format!(" AND {timestamp_column} IS NOT NULL")
                };

                format!(
                    "SELECT 
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN {time_check} THEN 0 ELSE 1 END) as violations
                     FROM {}
                     WHERE 1=1{weekday_check}{null_clause}",
                    self.table_name
                )
            }
            TemporalValidationType::DateRange {
                timestamp_column,
                min_date,
                max_date,
            } => {
                let mut conditions = Vec::new();

                if let Some(min) = min_date {
                    conditions.push(format!("{timestamp_column} >= TIMESTAMP '{min}'"));
                }
                if let Some(max) = max_date {
                    conditions.push(format!("{timestamp_column} <= TIMESTAMP '{max}'"));
                }

                if conditions.is_empty() {
                    return Err(TermError::constraint_evaluation(
                        "temporal_ordering",
                        "DateRange validation requires at least min_date or max_date",
                    ));
                }

                let range_check = conditions.join(" AND ");
                let null_clause = if self.allow_nulls {
                    String::new()
                } else {
                    format!(" AND {timestamp_column} IS NOT NULL")
                };

                format!(
                    "SELECT 
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN {range_check} THEN 0 ELSE 1 END) as violations
                     FROM {}
                     WHERE 1=1{null_clause}",
                    self.table_name
                )
            }
            TemporalValidationType::MaxTimeGap {
                timestamp_column,
                group_by_column,
                max_gap_seconds,
            } => {
                let partition_clause = if let Some(group_col) = group_by_column {
                    format!("PARTITION BY {group_col}")
                } else {
                    String::new()
                };

                format!(
                    "WITH time_gaps AS (
                        SELECT 
                            {timestamp_column},
                            LAG({timestamp_column}) OVER ({partition_clause} ORDER BY {timestamp_column}) as prev_timestamp,
                            EXTRACT(EPOCH FROM {timestamp_column} - LAG({timestamp_column}) OVER ({partition_clause} ORDER BY {timestamp_column})) as gap_seconds
                        FROM {}
                        WHERE {timestamp_column} IS NOT NULL
                    )
                    SELECT 
                        COUNT(*) as total_gaps,
                        SUM(CASE WHEN gap_seconds > {max_gap_seconds} THEN 1 ELSE 0 END) as violations
                    FROM time_gaps
                    WHERE prev_timestamp IS NOT NULL",
                    self.table_name
                )
            }
            TemporalValidationType::EventSequence { .. } => {
                // Event sequence validation is more complex and would need a different approach
                return Err(TermError::constraint_evaluation(
                    "temporal_ordering",
                    "Event sequence validation not yet implemented",
                ));
            }
        };

        debug!("Generated temporal validation query: {}", sql);
        Ok(sql)
    }
}

#[async_trait]
impl Constraint for TemporalOrderingConstraint {
    #[instrument(skip(self, ctx), fields(constraint = "temporal_ordering"))]
    async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult> {
        debug!(
            "Evaluating temporal ordering constraint on table: {}",
            self.table_name
        );

        // Generate and execute validation query
        let sql = self.generate_validation_query()?;
        let df = ctx.sql(&sql).await.map_err(|e| {
            TermError::constraint_evaluation(
                "temporal_ordering",
                format!("Temporal validation query failed: {e}"),
            )
        })?;

        let batches = df.collect().await.map_err(|e| {
            TermError::constraint_evaluation(
                "temporal_ordering",
                format!("Failed to collect temporal validation results: {e}"),
            )
        })?;

        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Ok(ConstraintResult::success());
        }

        // Extract violation counts
        let batch = &batches[0];
        let total_rows = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                TermError::constraint_evaluation(
                    "temporal_ordering",
                    "Invalid total rows column type",
                )
            })?
            .value(0);

        let violations = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                TermError::constraint_evaluation(
                    "temporal_ordering",
                    "Invalid violations column type",
                )
            })?
            .value(0);

        if violations == 0 {
            debug!("Temporal ordering constraint passed: no violations found");
            return Ok(ConstraintResult::success_with_metric(1.0));
        }

        // Calculate compliance rate
        let compliance_rate = if total_rows > 0 {
            (total_rows - violations) as f64 / total_rows as f64
        } else {
            1.0
        };

        // Generate failure message
        let message = match &self.validation_type {
            TemporalValidationType::BeforeAfter {
                before_column,
                after_column,
                ..
            } => format!(
                "Temporal ordering violation: {violations} records where '{before_column}' is not before '{after_column}' ({:.2}% compliance)",
                compliance_rate * 100.0
            ),
            TemporalValidationType::BusinessHours {
                timestamp_column, ..
            } => format!(
                "Business hours violation: {violations} records with '{timestamp_column}' outside business hours ({:.2}% compliance)",
                compliance_rate * 100.0
            ),
            TemporalValidationType::DateRange {
                timestamp_column, ..
            } => format!(
                "Date range violation: {violations} records with '{timestamp_column}' outside valid range ({:.2}% compliance)",
                compliance_rate * 100.0
            ),
            TemporalValidationType::MaxTimeGap { .. } => format!(
                "Time gap violation: {violations} gaps exceed maximum allowed ({:.2}% compliance)",
                compliance_rate * 100.0
            ),
            _ => format!(
                "Temporal validation failed: {violations} violations ({:.2}% compliance)",
                compliance_rate * 100.0
            ),
        };

        warn!("{}", message);

        Ok(ConstraintResult {
            status: ConstraintStatus::Failure,
            metric: Some(compliance_rate),
            message: Some(message),
        })
    }

    fn name(&self) -> &str {
        "temporal_ordering"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::create_test_context;

    #[tokio::test]
    async fn test_before_after_success() -> Result<()> {
        let ctx = create_test_context().await?;

        // Create test table with proper ordering
        ctx.sql(
            "CREATE TABLE events_ordered (id BIGINT, created_at TIMESTAMP, processed_at TIMESTAMP)",
        )
        .await?
        .collect()
        .await?;
        ctx.sql(
            "INSERT INTO events_ordered VALUES 
            (1, '2024-01-01 10:00:00', '2024-01-01 10:05:00'),
            (2, '2024-01-01 11:00:00', '2024-01-01 11:10:00')",
        )
        .await?
        .collect()
        .await?;

        let constraint = TemporalOrderingConstraint::new("events_ordered")
            .before_after("created_at", "processed_at");

        let result = constraint.evaluate(&ctx).await?;
        assert_eq!(result.status, ConstraintStatus::Success);

        Ok(())
    }

    #[tokio::test]
    async fn test_before_after_violation() -> Result<()> {
        let ctx = create_test_context().await?;

        // Create test table with ordering violations
        ctx.sql("CREATE TABLE events_violated (id BIGINT, created_at TIMESTAMP, processed_at TIMESTAMP)")
            .await?
            .collect()
            .await?;
        ctx.sql(
            "INSERT INTO events_violated VALUES 
            (1, '2024-01-01 10:00:00', '2024-01-01 09:00:00'),
            (2, '2024-01-01 11:00:00', '2024-01-01 11:10:00')",
        )
        .await?
        .collect()
        .await?;

        let constraint = TemporalOrderingConstraint::new("events_violated")
            .before_after("created_at", "processed_at");

        let result = constraint.evaluate(&ctx).await?;
        assert_eq!(result.status, ConstraintStatus::Failure);
        assert!(result.message.is_some());

        Ok(())
    }

    #[test]
    fn test_constraint_configuration() {
        let constraint = TemporalOrderingConstraint::new("transactions")
            .business_hours("timestamp", "09:00", "17:00")
            .weekdays_only(true)
            .allow_nulls(true)
            .tolerance_seconds(60);

        assert_eq!(constraint.table_name, "transactions");
        assert!(constraint.allow_nulls);
        assert_eq!(constraint.tolerance_seconds, 60);

        if let TemporalValidationType::BusinessHours { weekdays_only, .. } =
            constraint.validation_type
        {
            assert!(weekdays_only);
        } else {
            panic!("Expected BusinessHours validation type");
        }
    }
}
