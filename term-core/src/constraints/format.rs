//! Unified format validation constraint for pattern matching and content validation.
//!
//! This module provides a single flexible constraint that consolidates all pattern-based
//! validation including email, URL, credit card detection, phone numbers, postal codes,
//! UUIDs, IP addresses, JSON, and custom regex patterns.
//!
//! ## Overview
//!
//! The `FormatConstraint` replaces multiple individual constraint types with a single,
//! powerful constraint that supports:
//!
//! - **Built-in formats**: Email, URL, phone, postal codes, UUIDs, IP addresses, JSON, dates
//! - **Custom regex patterns**: Full regex support with security validation
//! - **Rich configuration**: Case sensitivity, trimming, null handling
//! - **Performance optimization**: Pattern caching and compiled regex reuse
//! - **Security**: ReDoS protection and SQL injection prevention
//!
//! ## Quick Start Examples
//!
//! ### Basic Format Validation
//!
//! ```rust
//! use term_core::constraints::FormatConstraint;
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Email validation - require 95% of values to be valid emails
//! let email_check = FormatConstraint::email("email", 0.95)?;
//!
//! // URL validation with localhost support
//! let url_check = FormatConstraint::url("website", 0.90, true)?;
//!
//! // US phone number validation
//! let phone_check = FormatConstraint::phone("phone", 0.98, Some("US".to_string()))?;
//!
//! // UUID validation (any version)
//! let uuid_check = FormatConstraint::uuid("session_id", 1.0)?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Advanced Configuration with FormatOptions
//!
//! ```rust
//! use term_core::constraints::{FormatConstraint, FormatType, FormatOptions};
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Case-insensitive email validation with trimming
//! let flexible_email = FormatConstraint::new(
//!     "email",
//!     FormatType::Email,
//!     0.95,
//!     FormatOptions::lenient()  // case insensitive + trimming + allows nulls
//! )?;
//!
//! // Strict phone validation (no nulls, case sensitive)
//! let strict_phone = FormatConstraint::new(
//!     "phone",
//!     FormatType::Phone { country: Some("US".to_string()) },
//!     0.99,
//!     FormatOptions::strict()  // null_is_valid = false
//! )?;
//!
//! // Custom regex with options
//! let product_code = FormatConstraint::new(
//!     "product_code",
//!     FormatType::Regex(r"^[A-Z]{2}\d{4}$".to_string()),
//!     0.98,
//!     FormatOptions::new()
//!         .case_sensitive(false)
//!         .trim_before_check(true)
//! )?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Specialized Format Types
//!
//! ```rust
//! use term_core::constraints::FormatConstraint;
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Postal codes for different countries
//! let us_zip = FormatConstraint::postal_code("zip", 0.95, "US")?;
//! let uk_postcode = FormatConstraint::postal_code("postcode", 0.95, "UK")?;
//! let ca_postal = FormatConstraint::postal_code("postal", 0.95, "CA")?;
//!
//! // IP address validation
//! let ipv4_check = FormatConstraint::ipv4("client_ip", 0.99)?;
//! let ipv6_check = FormatConstraint::ipv6("server_ip", 0.99)?;
//!
//! // JSON format validation
//! let json_check = FormatConstraint::json("config", 0.98)?;
//!
//! // ISO 8601 datetime validation
//! let datetime_check = FormatConstraint::iso8601_datetime("order_date", 1.0)?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Migration from Individual Constraints
//!
//! ### Before (Deprecated)
//! ```rust,ignore
//! use term_core::constraints::{PatternConstraint, EmailConstraint, UrlConstraint};
//!
//! let email_old = EmailConstraint::new("email", 0.95);
//! let pattern_old = PatternConstraint::new("phone", r"^\d{3}-\d{3}-\d{4}$", 0.90)?;
//! let url_old = UrlConstraint::new("website", 0.85);
//! ```
//!
//! ### After (Unified API)
//! ```rust
//! use term_core::constraints::FormatConstraint;
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let email_new = FormatConstraint::email("email", 0.95)?;
//! let phone_new = FormatConstraint::phone("phone", 0.90, Some("US".to_string()))?;
//! let url_new = FormatConstraint::url("website", 0.85, false)?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Performance Considerations
//!
//! - **Pattern Caching**: Compiled regex patterns are cached for reuse
//! - **Built-in Patterns**: Predefined patterns are optimized and tested
//! - **Security**: All patterns are validated to prevent ReDoS attacks
//! - **Memory Efficiency**: Single constraint type reduces memory overhead
//!
//! ## Common Patterns and Use Cases
//!
//! ### Data Quality Checks
//! ```rust
//! use term_core::constraints::{FormatConstraint, FormatOptions};
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Customer data validation
//! let email_quality = FormatConstraint::new(
//!     "customer_email",
//!     term_core::constraints::FormatType::Email,
//!     0.98,  // 98% must be valid emails
//!     FormatOptions::lenient()  // Allow some flexibility
//! )?;
//!
//! // Credit card detection (for PII scanning)
//! let cc_detection = FormatConstraint::credit_card("description", 0.01, true)?; // Detect if > 1% contain CCs
//! # Ok(())
//! # }
//! ```
//!
//! ### International Data
//! ```rust
//! use term_core::constraints::FormatConstraint;
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Multi-region phone validation
//! let us_phones = FormatConstraint::phone("us_phone", 0.95, Some("US".to_string()))?;
//! let uk_phones = FormatConstraint::phone("uk_phone", 0.95, Some("UK".to_string()))?;
//! let intl_phones = FormatConstraint::phone("intl_phone", 0.90, None)?; // E.164 format
//!
//! // Multi-country postal codes
//! let postal_codes = vec![
//!     FormatConstraint::postal_code("us_zip", 0.99, "US")?,
//!     FormatConstraint::postal_code("ca_postal", 0.99, "CA")?,
//!     FormatConstraint::postal_code("uk_postcode", 0.99, "UK")?,
//! ];
//! # Ok(())
//! # }
//! ```

use crate::core::{Constraint, ConstraintMetadata, ConstraintResult};
use crate::prelude::*;
use crate::security::SqlSecurity;
use arrow::array::Array;
use async_trait::async_trait;
use datafusion::prelude::*;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::RwLock;
use tracing::instrument;

/// Lazy static pattern cache for compiled regex patterns
static PATTERN_CACHE: Lazy<RwLock<HashMap<String, String>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// Types of format validation that can be performed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FormatType {
    /// Custom regular expression pattern
    Regex(String),
    /// Email address validation
    Email,
    /// URL validation with optional localhost support
    Url { allow_localhost: bool },
    /// Credit card number detection with optional detection-only mode
    CreditCard { detect_only: bool },
    /// Phone number validation with optional country specification
    Phone { country: Option<String> },
    /// Postal code validation for a specific country
    PostalCode { country: String },
    /// UUID (v1, v4, or any) validation
    UUID,
    /// IPv4 address validation
    IPv4,
    /// IPv6 address validation
    IPv6,
    /// JSON format validation
    Json,
    /// ISO 8601 date-time format validation
    Iso8601DateTime,
}

impl FormatType {
    /// Returns the regex pattern for this format type.
    fn get_pattern(&self) -> Result<String> {
        let cache_key = format!("{:?}", self);

        // Check cache first
        {
            let cache = PATTERN_CACHE.read().unwrap();
            if let Some(pattern) = cache.get(&cache_key) {
                return Ok(pattern.clone());
            }
        }

        let pattern = match self {
            FormatType::Regex(pattern) => {
                SqlSecurity::validate_regex_pattern(pattern)?;
                pattern.clone()
            }
            FormatType::Email => {
                // More comprehensive email pattern
                r"^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$".to_string()
            }
            FormatType::Url { allow_localhost } => {
                if *allow_localhost {
                    r"^https?://(?:localhost|(?:[a-zA-Z0-9.-]+\.?[a-zA-Z]{2,}|(?:\d{1,3}\.){3}\d{1,3}))(?::\d+)?(?:/[^\s]*)?$".to_string()
                } else {
                    r"^https?://[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(?::\d+)?(?:/[^\s]*)?$".to_string()
                }
            }
            FormatType::CreditCard { .. } => {
                // Pattern for major credit card formats (Visa, MasterCard, Amex, Discover)
                r"^(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|3[0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12})$|^(?:\d{4}[-\s]?){3}\d{4}$".to_string()
            }
            FormatType::Phone { country } => {
                match country.as_deref() {
                    Some("US") | Some("CA") => r"^(\+?1[-.\s]?)?\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})$".to_string(),
                    Some("UK") => r"^(\+44\s?)?(?:\(?0\d{4}\)?\s?\d{6}|\(?0\d{3}\)?\s?\d{7}|\(?0\d{2}\)?\s?\d{8})$".to_string(),
                    Some("DE") => r"^(\+49\s?)?(?:\(?0\d{2,5}\)?\s?\d{4,12})$".to_string(),
                    Some("FR") => r"^(\+33\s?)?(?:\(?0\d{1}\)?\s?\d{8})$".to_string(),
                    _ => r"^[\+]?[1-9][\d]{0,15}$".to_string(), // E.164 international format
                }
            }
            FormatType::PostalCode { country } => {
                match country.as_str() {
                    "US" => r"^\d{5}(-\d{4})?$".to_string(),
                    "CA" => r"^[A-Za-z]\d[A-Za-z][ -]?\d[A-Za-z]\d$".to_string(),
                    "UK" => r"^[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2}$".to_string(),
                    "DE" => r"^\d{5}$".to_string(),
                    "FR" => r"^\d{5}$".to_string(),
                    "JP" => r"^\d{3}-\d{4}$".to_string(),
                    "AU" => r"^\d{4}$".to_string(),
                    _ => r"^[A-Za-z0-9\s-]{3,10}$".to_string(), // Generic postal code
                }
            }
            FormatType::UUID => {
                r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$".to_string()
            }
            FormatType::IPv4 => {
                r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$".to_string()
            }
            FormatType::IPv6 => {
                // Simplified IPv6 pattern that handles most common cases
                r"^([0-9a-fA-F]{0,4}:){1,7}([0-9a-fA-F]{0,4})?$|^::$|^::1$|^([0-9a-fA-F]{1,4}:)*::([0-9a-fA-F]{1,4}:)*[0-9a-fA-F]{1,4}$".to_string()
            }
            FormatType::Json => {
                // Simple JSON structure validation - starts with { or [
                r"^\s*[\{\[].*[\}\]]\s*$".to_string()
            }
            FormatType::Iso8601DateTime => {
                // ISO 8601 date-time format (basic validation)
                r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$".to_string()
            }
        };

        // Cache the pattern
        {
            let mut cache = PATTERN_CACHE.write().unwrap();
            cache.insert(cache_key, pattern.clone());
        }

        Ok(pattern)
    }

    /// Returns a human-readable name for this format type.
    pub fn name(&self) -> &str {
        match self {
            FormatType::Regex(_) => "regex",
            FormatType::Email => "email",
            FormatType::Url { .. } => "url",
            FormatType::CreditCard { .. } => "credit_card",
            FormatType::Phone { .. } => "phone",
            FormatType::PostalCode { .. } => "postal_code",
            FormatType::UUID => "uuid",
            FormatType::IPv4 => "ipv4",
            FormatType::IPv6 => "ipv6",
            FormatType::Json => "json",
            FormatType::Iso8601DateTime => "iso8601_datetime",
        }
    }

    /// Returns a human-readable description for this format type.
    pub fn description(&self) -> String {
        match self {
            FormatType::Regex(pattern) => format!("matches pattern '{}'", pattern),
            FormatType::Email => "are valid email addresses".to_string(),
            FormatType::Url { allow_localhost } => {
                if *allow_localhost {
                    "are valid URLs (including localhost)".to_string()
                } else {
                    "are valid URLs".to_string()
                }
            }
            FormatType::CreditCard { detect_only } => {
                if *detect_only {
                    "contain credit card number patterns".to_string()
                } else {
                    "are valid credit card numbers".to_string()
                }
            }
            FormatType::Phone { country } => match country.as_deref() {
                Some(c) => format!("are valid {} phone numbers", c),
                None => "are valid phone numbers".to_string(),
            },
            FormatType::PostalCode { country } => {
                format!("are valid {} postal codes", country)
            }
            FormatType::UUID => "are valid UUIDs".to_string(),
            FormatType::IPv4 => "are valid IPv4 addresses".to_string(),
            FormatType::IPv6 => "are valid IPv6 addresses".to_string(),
            FormatType::Json => "are valid JSON documents".to_string(),
            FormatType::Iso8601DateTime => "are valid ISO 8601 date-time strings".to_string(),
        }
    }
}

/// Options for format constraint behavior.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FormatOptions {
    /// Whether pattern matching should be case sensitive
    pub case_sensitive: bool,
    /// Whether to trim whitespace before checking format
    pub trim_before_check: bool,
    /// Whether NULL values should be considered valid
    pub null_is_valid: bool,
}

impl Default for FormatOptions {
    fn default() -> Self {
        Self {
            case_sensitive: true,
            trim_before_check: false,
            null_is_valid: true, // NULL values are typically considered valid in data quality
        }
    }
}

impl FormatOptions {
    /// Creates new format options with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets case sensitivity for pattern matching.
    pub fn case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
        self
    }

    /// Sets whether to trim whitespace before format checking.
    pub fn trim_before_check(mut self, trim: bool) -> Self {
        self.trim_before_check = trim;
        self
    }

    /// Sets whether NULL values should be considered valid.
    pub fn null_is_valid(mut self, null_valid: bool) -> Self {
        self.null_is_valid = null_valid;
        self
    }

    /// Creates format options for case-insensitive matching.
    ///
    /// This is a convenience method that sets case_sensitive to false.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::constraints::FormatOptions;
    ///
    /// let options = FormatOptions::case_insensitive();
    /// assert_eq!(options.case_sensitive, false);
    /// ```
    pub fn case_insensitive() -> Self {
        Self::new().case_sensitive(false)
    }

    /// Creates format options for strict validation (no nulls, case sensitive, no trimming).
    ///
    /// This is a convenience method for the most restrictive validation.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::constraints::FormatOptions;
    ///
    /// let options = FormatOptions::strict();
    /// assert_eq!(options.case_sensitive, true);
    /// assert_eq!(options.trim_before_check, false);
    /// assert_eq!(options.null_is_valid, false);
    /// ```
    pub fn strict() -> Self {
        Self::new().null_is_valid(false)
    }

    /// Creates format options for lenient validation (case insensitive, trimming, nulls allowed).
    ///
    /// This is a convenience method for the most permissive validation.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::constraints::FormatOptions;
    ///
    /// let options = FormatOptions::lenient();
    /// assert_eq!(options.case_sensitive, false);
    /// assert_eq!(options.trim_before_check, true);
    /// assert_eq!(options.null_is_valid, true);
    /// ```
    pub fn lenient() -> Self {
        Self::new()
            .case_sensitive(false)
            .trim_before_check(true)
            .null_is_valid(true)
    }

    /// Creates format options with trimming enabled.
    ///
    /// This is a convenience method that enables whitespace trimming before validation.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::constraints::FormatOptions;
    ///
    /// let options = FormatOptions::with_trimming();
    /// assert_eq!(options.trim_before_check, true);
    /// ```
    pub fn with_trimming() -> Self {
        Self::new().trim_before_check(true)
    }
}

/// A unified constraint that validates data formats and patterns.
///
/// This constraint replaces individual format constraints (PatternConstraint,
/// EmailConstraint, UrlConstraint, CreditCardConstraint) and adds support
/// for many additional formats.
///
/// # Examples
///
/// ```rust
/// use term_core::constraints::{FormatConstraint, FormatType, FormatOptions};
/// use term_core::core::Constraint;
///
/// // Email validation
/// let email_constraint = FormatConstraint::new(
///     "email",
///     FormatType::Email,
///     0.95,
///     FormatOptions::default()
/// ).unwrap();
///
/// // Phone number validation for US
/// let phone_constraint = FormatConstraint::new(
///     "phone",
///     FormatType::Phone { country: Some("US".to_string()) },
///     0.90,
///     FormatOptions::new().trim_before_check(true)
/// ).unwrap();
///
/// // Custom regex pattern
/// let code_constraint = FormatConstraint::new(
///     "product_code",
///     FormatType::Regex(r"^[A-Z]{2}\d{4}$".to_string()),
///     1.0,
///     FormatOptions::default()
/// ).unwrap();
/// ```
#[derive(Debug, Clone)]
pub struct FormatConstraint {
    /// The column to validate
    column: String,
    /// The format type to check
    format: FormatType,
    /// The minimum ratio of values that must match the format (0.0 to 1.0)
    threshold: f64,
    /// Options for format validation behavior
    options: FormatOptions,
}

impl FormatConstraint {
    /// Creates a new format constraint.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to check
    /// * `format` - The format type to validate
    /// * `threshold` - The minimum ratio of values that must match (0.0 to 1.0)
    /// * `options` - Format validation options
    ///
    /// # Errors
    ///
    /// Returns error if column name is invalid or threshold is out of range
    pub fn new(
        column: impl Into<String>,
        format: FormatType,
        threshold: f64,
        options: FormatOptions,
    ) -> Result<Self> {
        let column_str = column.into();

        // Validate inputs
        SqlSecurity::validate_identifier(&column_str)?;

        if !(0.0..=1.0).contains(&threshold) {
            return Err(TermError::SecurityError(
                "Threshold must be between 0.0 and 1.0".to_string(),
            ));
        }

        // Validate that the format can generate a pattern
        format.get_pattern()?;

        Ok(Self {
            column: column_str,
            format,
            threshold,
            options,
        })
    }

    /// Creates a format constraint for email validation.
    pub fn email(column: impl Into<String>, threshold: f64) -> Result<Self> {
        Self::new(
            column,
            FormatType::Email,
            threshold,
            FormatOptions::default(),
        )
    }

    /// Creates a format constraint for URL validation.
    pub fn url(column: impl Into<String>, threshold: f64, allow_localhost: bool) -> Result<Self> {
        Self::new(
            column,
            FormatType::Url { allow_localhost },
            threshold,
            FormatOptions::default(),
        )
    }

    /// Creates a format constraint for credit card detection.
    pub fn credit_card(
        column: impl Into<String>,
        threshold: f64,
        detect_only: bool,
    ) -> Result<Self> {
        Self::new(
            column,
            FormatType::CreditCard { detect_only },
            threshold,
            FormatOptions::default(),
        )
    }

    /// Creates a format constraint for phone number validation.
    pub fn phone(
        column: impl Into<String>,
        threshold: f64,
        country: Option<String>,
    ) -> Result<Self> {
        Self::new(
            column,
            FormatType::Phone { country },
            threshold,
            FormatOptions::new().trim_before_check(true),
        )
    }

    /// Creates a format constraint for postal code validation.
    pub fn postal_code(
        column: impl Into<String>,
        threshold: f64,
        country: impl Into<String>,
    ) -> Result<Self> {
        Self::new(
            column,
            FormatType::PostalCode {
                country: country.into(),
            },
            threshold,
            FormatOptions::new().trim_before_check(true),
        )
    }

    /// Creates a format constraint for UUID validation.
    pub fn uuid(column: impl Into<String>, threshold: f64) -> Result<Self> {
        Self::new(
            column,
            FormatType::UUID,
            threshold,
            FormatOptions::default(),
        )
    }

    /// Creates a format constraint for IPv4 address validation.
    pub fn ipv4(column: impl Into<String>, threshold: f64) -> Result<Self> {
        Self::new(
            column,
            FormatType::IPv4,
            threshold,
            FormatOptions::default(),
        )
    }

    /// Creates a format constraint for IPv6 address validation.
    pub fn ipv6(column: impl Into<String>, threshold: f64) -> Result<Self> {
        Self::new(
            column,
            FormatType::IPv6,
            threshold,
            FormatOptions::default(),
        )
    }

    /// Creates a format constraint for JSON validation.
    pub fn json(column: impl Into<String>, threshold: f64) -> Result<Self> {
        Self::new(
            column,
            FormatType::Json,
            threshold,
            FormatOptions::default(),
        )
    }

    /// Creates a format constraint for ISO 8601 date-time validation.
    pub fn iso8601_datetime(column: impl Into<String>, threshold: f64) -> Result<Self> {
        Self::new(
            column,
            FormatType::Iso8601DateTime,
            threshold,
            FormatOptions::default(),
        )
    }

    /// Creates a format constraint for custom regex pattern validation.
    pub fn regex(
        column: impl Into<String>,
        pattern: impl Into<String>,
        threshold: f64,
    ) -> Result<Self> {
        Self::new(
            column,
            FormatType::Regex(pattern.into()),
            threshold,
            FormatOptions::default(),
        )
    }
}

#[async_trait]
impl Constraint for FormatConstraint {
    #[instrument(skip(self, ctx), fields(
        column = %self.column,
        format = %self.format.name(),
        threshold = %self.threshold
    ))]
    async fn evaluate(&self, ctx: &SessionContext) -> Result<ConstraintResult> {
        let column_identifier = SqlSecurity::escape_identifier(&self.column)?;
        let pattern = self.format.get_pattern()?;
        let escaped_pattern = SqlSecurity::validate_regex_pattern(&pattern)?;

        // Build the SQL based on options
        let column_expr = if self.options.trim_before_check {
            format!("TRIM({})", column_identifier)
        } else {
            column_identifier.clone()
        };

        let pattern_operator = if self.options.case_sensitive {
            "~"
        } else {
            "~*"
        };

        let sql = if self.options.null_is_valid {
            format!(
                "SELECT 
                    COUNT(CASE WHEN {} {} '{}' OR {} IS NULL THEN 1 END) as matches,
                    COUNT(*) as total
                 FROM data",
                column_expr, pattern_operator, escaped_pattern, column_identifier
            )
        } else {
            format!(
                "SELECT 
                    COUNT(CASE WHEN {} {} '{}' THEN 1 END) as matches,
                    COUNT(*) as total
                 FROM data",
                column_expr, pattern_operator, escaped_pattern
            )
        };

        let df = ctx.sql(&sql).await?;
        let batches = df.collect().await?;

        if batches.is_empty() {
            return Ok(ConstraintResult::skipped("No data to validate"));
        }

        let batch = &batches[0];
        if batch.num_rows() == 0 {
            return Ok(ConstraintResult::skipped("No data to validate"));
        }

        let matches = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| TermError::Internal("Failed to extract match count".to_string()))?
            .value(0) as f64;

        let total = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| TermError::Internal("Failed to extract total count".to_string()))?
            .value(0) as f64;

        if total == 0.0 {
            return Ok(ConstraintResult::skipped("No data to validate"));
        }

        let match_ratio = matches / total;

        // Determine success based on format type and threshold
        let is_success = match &self.format {
            FormatType::CreditCard { detect_only: true } => {
                // For credit card detection, we want the ratio to be <= threshold
                match_ratio <= self.threshold
            }
            _ => {
                // For other formats, we want the ratio to be >= threshold
                match_ratio >= self.threshold
            }
        };

        if is_success {
            Ok(ConstraintResult::success_with_metric(match_ratio))
        } else {
            let message =
                match &self.format {
                    FormatType::CreditCard { detect_only: true } => {
                        format!(
                            "Credit card detection ratio {:.3} exceeds threshold {:.3}",
                            match_ratio, self.threshold
                        )
                    }
                    _ => {
                        format!(
                        "Format validation ratio {:.3} is below threshold {:.3} - values that {}",
                        match_ratio, self.threshold, self.format.description()
                    )
                    }
                };

            Ok(ConstraintResult::failure_with_metric(match_ratio, message))
        }
    }

    fn name(&self) -> &str {
        self.format.name()
    }

    fn column(&self) -> Option<&str> {
        Some(&self.column)
    }

    fn metadata(&self) -> ConstraintMetadata {
        let description = match &self.format {
            FormatType::CreditCard { detect_only: true } => {
                format!(
                    "Checks that no more than {:.1}% of values in '{}' {}",
                    self.threshold * 100.0,
                    self.column,
                    self.format.description()
                )
            }
            _ => {
                format!(
                    "Checks that at least {:.1}% of values in '{}' {}",
                    self.threshold * 100.0,
                    self.column,
                    self.format.description()
                )
            }
        };

        ConstraintMetadata::for_column(&self.column)
            .with_description(description)
            .with_custom("format_type", self.format.name())
            .with_custom("threshold", self.threshold.to_string())
            .with_custom("case_sensitive", self.options.case_sensitive.to_string())
            .with_custom(
                "trim_before_check",
                self.options.trim_before_check.to_string(),
            )
            .with_custom("null_is_valid", self.options.null_is_valid.to_string())
            .with_custom("constraint_type", "format")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ConstraintStatus;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use std::sync::Arc;

    async fn create_test_context(values: Vec<Option<&str>>) -> SessionContext {
        let ctx = SessionContext::new();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "text_col",
            DataType::Utf8,
            true,
        )]));

        let array = StringArray::from(values);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();

        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("data", Arc::new(provider)).unwrap();

        ctx
    }

    #[tokio::test]
    async fn test_email_format_constraint() {
        let values = vec![
            Some("test@example.com"),
            Some("user@domain.org"),
            Some("invalid-email"),
            Some("another@test.net"),
        ];
        let ctx = create_test_context(values).await;

        let constraint = FormatConstraint::email("text_col", 0.7).unwrap();

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(0.75)); // 3 out of 4 are emails
        assert_eq!(constraint.name(), "email");
    }

    #[tokio::test]
    async fn test_url_format_constraint() {
        let values = vec![
            Some("https://example.com"),
            Some("http://test.org"),
            Some("not-a-url"),
            Some("https://another.site.net/path"),
        ];
        let ctx = create_test_context(values).await;

        let constraint = FormatConstraint::url("text_col", 0.7, false).unwrap();

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(0.75)); // 3 out of 4 are URLs
        assert_eq!(constraint.name(), "url");
    }

    #[tokio::test]
    async fn test_url_with_localhost() {
        let values = vec![
            Some("https://localhost:3000"),
            Some("http://localhost"),
            Some("https://example.com"),
            Some("not-a-url"),
        ];
        let ctx = create_test_context(values).await;

        let constraint = FormatConstraint::url("text_col", 0.7, true).unwrap();

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(0.75)); // 3 out of 4 are URLs (including localhost)
    }

    #[tokio::test]
    async fn test_credit_card_detection() {
        let values = vec![
            Some("4111-1111-1111-1111"),
            Some("5555 5555 5555 4444"),
            Some("normal text"),
            Some("4111111111111111"), // Visa format
        ];
        let ctx = create_test_context(values).await;

        // Expect no more than 80% to be credit card numbers
        let constraint = FormatConstraint::credit_card("text_col", 0.8, true).unwrap();

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(constraint.name(), "credit_card");
    }

    #[tokio::test]
    async fn test_phone_number_us() {
        let values = vec![
            Some("(555) 123-4567"),
            Some("555-123-4567"),
            Some("5551234567"),
            Some("invalid-phone"),
        ];
        let ctx = create_test_context(values).await;

        let constraint = FormatConstraint::phone("text_col", 0.7, Some("US".to_string())).unwrap();

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(constraint.name(), "phone");
    }

    #[tokio::test]
    async fn test_postal_code_us() {
        let values = vec![
            Some("12345"),
            Some("12345-6789"),
            Some("invalid"),
            Some("98765"),
        ];
        let ctx = create_test_context(values).await;

        let constraint = FormatConstraint::postal_code("text_col", 0.7, "US").unwrap();

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(0.75)); // 3 out of 4 are valid US postal codes
        assert_eq!(constraint.name(), "postal_code");
    }

    #[tokio::test]
    async fn test_uuid_format() {
        let values = vec![
            Some("550e8400-e29b-41d4-a716-446655440000"),
            Some("6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
            Some("invalid-uuid"),
            Some("6ba7b811-9dad-11d1-80b4-00c04fd430c8"),
        ];
        let ctx = create_test_context(values).await;

        let constraint = FormatConstraint::uuid("text_col", 0.7).unwrap();

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(0.75)); // 3 out of 4 are UUIDs
        assert_eq!(constraint.name(), "uuid");
    }

    #[tokio::test]
    async fn test_ipv4_format() {
        let values = vec![
            Some("192.168.1.1"),
            Some("10.0.0.1"),
            Some("256.256.256.256"), // Invalid - out of range
            Some("172.16.0.1"),
        ];
        let ctx = create_test_context(values).await;

        let constraint = FormatConstraint::ipv4("text_col", 0.7).unwrap();

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(0.75)); // 3 out of 4 are valid IPv4
        assert_eq!(constraint.name(), "ipv4");
    }

    #[tokio::test]
    async fn test_ipv6_format() {
        let values = vec![
            Some("2001:0db8:85a3:0000:0000:8a2e:0370:7334"),
            Some("2001:db8:85a3::8a2e:370:7334"),
            Some("invalid-ipv6"),
            Some("::1"),
        ];
        let ctx = create_test_context(values).await;

        let constraint = FormatConstraint::ipv6("text_col", 0.7).unwrap();

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(0.75)); // 3 out of 4 are valid IPv6
        assert_eq!(constraint.name(), "ipv6");
    }

    #[tokio::test]
    async fn test_json_format() {
        let values = vec![
            Some(r#"{"key": "value"}"#),
            Some(r#"[1, 2, 3]"#),
            Some("not json"),
            Some(r#"{"nested": {"key": "value"}}"#),
        ];
        let ctx = create_test_context(values).await;

        let constraint = FormatConstraint::json("text_col", 0.7).unwrap();

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(0.75)); // 3 out of 4 look like JSON
        assert_eq!(constraint.name(), "json");
    }

    #[tokio::test]
    async fn test_iso8601_datetime_format() {
        let values = vec![
            Some("2023-12-25T10:30:00Z"),
            Some("2023-12-25T10:30:00.123Z"),
            Some("invalid-datetime"),
            Some("2023-12-25T10:30:00+05:30"),
        ];
        let ctx = create_test_context(values).await;

        let constraint = FormatConstraint::iso8601_datetime("text_col", 0.7).unwrap();

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(0.75)); // 3 out of 4 are ISO 8601
        assert_eq!(constraint.name(), "iso8601_datetime");
    }

    #[tokio::test]
    async fn test_custom_regex_format() {
        let values = vec![
            Some("ABC123"),
            Some("DEF456"),
            Some("invalid"),
            Some("GHI789"),
        ];
        let ctx = create_test_context(values).await;

        // Pattern to match 3 letters followed by 3 digits
        let constraint = FormatConstraint::regex("text_col", r"^[A-Z]{3}\d{3}$", 0.7).unwrap();

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(0.75)); // 3 out of 4 match
        assert_eq!(constraint.name(), "regex");
    }

    #[tokio::test]
    async fn test_format_options_case_insensitive() {
        let values = vec![
            Some("abc123"),
            Some("DEF456"),
            Some("invalid"),
            Some("ghi789"),
        ];
        let ctx = create_test_context(values).await;

        // Pattern should match both upper and lower case when case_insensitive is true
        let constraint = FormatConstraint::new(
            "text_col",
            FormatType::Regex(r"^[A-Z]{3}\d{3}$".to_string()),
            0.7,
            FormatOptions::new().case_sensitive(false),
        )
        .unwrap();

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(0.75)); // 3 out of 4 match (case insensitive)
    }

    #[tokio::test]
    async fn test_format_options_trim_whitespace() {
        let values = vec![
            Some("  test@example.com  "),
            Some("user@domain.org"),
            Some("  invalid-email  "),
            Some(" another@test.net "),
        ];
        let ctx = create_test_context(values).await;

        let constraint = FormatConstraint::new(
            "text_col",
            FormatType::Email,
            0.7,
            FormatOptions::new().trim_before_check(true),
        )
        .unwrap();

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Success);
        assert_eq!(result.metric, Some(0.75)); // 3 out of 4 are emails after trimming
    }

    #[tokio::test]
    async fn test_format_options_null_handling() {
        let values = vec![Some("test@example.com"), None, Some("invalid-email"), None];
        let ctx = create_test_context(values).await;

        // With null_is_valid = true (default)
        let constraint1 = FormatConstraint::new(
            "text_col",
            FormatType::Email,
            0.6,
            FormatOptions::new().null_is_valid(true),
        )
        .unwrap();

        let result1 = constraint1.evaluate(&ctx).await.unwrap();
        assert_eq!(result1.status, ConstraintStatus::Success);
        assert_eq!(result1.metric, Some(0.75)); // 1 email + 2 nulls out of 4

        // With null_is_valid = false
        let constraint2 = FormatConstraint::new(
            "text_col",
            FormatType::Email,
            0.2,
            FormatOptions::new().null_is_valid(false),
        )
        .unwrap();

        let result2 = constraint2.evaluate(&ctx).await.unwrap();
        assert_eq!(result2.status, ConstraintStatus::Success);
        assert_eq!(result2.metric, Some(0.25)); // Only 1 email out of 4
    }

    #[tokio::test]
    async fn test_constraint_failure() {
        let values = vec![
            Some("invalid"),
            Some("also_invalid"),
            Some("nope"),
            Some("still_invalid"),
        ];
        let ctx = create_test_context(values).await;

        let constraint = FormatConstraint::email("text_col", 0.5).unwrap();

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Failure);
        assert_eq!(result.metric, Some(0.0)); // No values are emails
        assert!(result.message.is_some());
    }

    #[tokio::test]
    async fn test_empty_data() {
        let ctx = create_test_context(vec![]).await;
        let constraint = FormatConstraint::email("text_col", 0.9).unwrap();

        let result = constraint.evaluate(&ctx).await.unwrap();
        assert_eq!(result.status, ConstraintStatus::Skipped);
    }

    #[test]
    fn test_invalid_threshold() {
        let result = FormatConstraint::email("col", 1.5);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Threshold must be between 0.0 and 1.0"));
    }

    #[test]
    fn test_pattern_caching() {
        // Test that patterns are cached for performance
        let format1 = FormatType::Email;
        let format2 = FormatType::Email;

        let pattern1 = format1.get_pattern().unwrap();
        let pattern2 = format2.get_pattern().unwrap();

        assert_eq!(pattern1, pattern2);

        // Accessing cache multiple times should be fast
        for _ in 0..100 {
            let _ = format1.get_pattern().unwrap();
        }
    }

    #[test]
    fn test_format_type_descriptions() {
        assert_eq!(FormatType::Email.description(), "are valid email addresses");
        assert_eq!(
            FormatType::Url {
                allow_localhost: true
            }
            .description(),
            "are valid URLs (including localhost)"
        );
        assert_eq!(
            FormatType::Phone {
                country: Some("US".to_string())
            }
            .description(),
            "are valid US phone numbers"
        );
        assert_eq!(
            FormatType::PostalCode {
                country: "CA".to_string()
            }
            .description(),
            "are valid CA postal codes"
        );
    }

    #[test]
    fn test_all_format_types_have_patterns() {
        // Ensure all format types can generate valid patterns
        let formats = vec![
            FormatType::Email,
            FormatType::Url {
                allow_localhost: false,
            },
            FormatType::Url {
                allow_localhost: true,
            },
            FormatType::CreditCard { detect_only: false },
            FormatType::Phone { country: None },
            FormatType::Phone {
                country: Some("US".to_string()),
            },
            FormatType::PostalCode {
                country: "US".to_string(),
            },
            FormatType::UUID,
            FormatType::IPv4,
            FormatType::IPv6,
            FormatType::Json,
            FormatType::Iso8601DateTime,
            FormatType::Regex(r"^\d+$".to_string()),
        ];

        for format in formats {
            assert!(
                format.get_pattern().is_ok(),
                "Format {:?} should have a valid pattern",
                format
            );
        }
    }

    #[test]
    fn test_format_options_convenience_methods() {
        // Test case_insensitive()
        let options = FormatOptions::case_insensitive();
        assert!(!options.case_sensitive);
        assert!(!options.trim_before_check);
        assert!(options.null_is_valid);

        // Test strict()
        let options = FormatOptions::strict();
        assert!(options.case_sensitive);
        assert!(!options.trim_before_check);
        assert!(!options.null_is_valid);

        // Test lenient()
        let options = FormatOptions::lenient();
        assert!(!options.case_sensitive);
        assert!(options.trim_before_check);
        assert!(options.null_is_valid);

        // Test with_trimming()
        let options = FormatOptions::with_trimming();
        assert!(options.case_sensitive);
        assert!(options.trim_before_check);
        assert!(options.null_is_valid);
    }
}
