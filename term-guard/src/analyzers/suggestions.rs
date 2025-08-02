//! Constraint suggestion system that analyzes column profiles to recommend data quality checks.
//!
//! This module implements a rule-based system that examines column profiles and suggests
//! appropriate data quality constraints with confidence scores. The system supports
//! various types of constraints including completeness, uniqueness, patterns, and ranges.
//!
//! ## Architecture
//!
//! The suggestion system consists of:
//! - `ConstraintSuggestionRule` trait for implementing specific rule logic
//! - `SuggestedConstraint` struct representing a recommendation
//! - Individual rule implementations for different constraint types
//! - `SuggestionEngine` for orchestrating rule evaluation
//!
//! ## Example Usage
//!
//! ```rust,no_run
//! use term_guard::analyzers::{SuggestionEngine, CompletenessRule, ColumnProfile, BasicStatistics, DetectedDataType};
//! use term_guard::test_fixtures::create_minimal_tpc_h_context;
//!
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! let ctx = create_minimal_tpc_h_context().await.unwrap();
//! // In a real scenario, profile would come from ColumnProfiler
//! let profile = ColumnProfile {
//!     column_name: "l_orderkey".to_string(),
//!     data_type: DetectedDataType::Integer,
//!     basic_stats: BasicStatistics {
//!         row_count: 1000,
//!         null_count: 10,
//!         null_percentage: 0.01,
//!         approximate_cardinality: 980,
//!         min_value: Some("1".to_string()),
//!         max_value: Some("1000".to_string()),
//!         sample_values: vec!["1".to_string(), "500".to_string(), "1000".to_string()],
//!     },
//!     categorical_histogram: None,
//!     numeric_distribution: None,
//!     passes_executed: vec![1, 2, 3],
//!     profiling_time_ms: 50,
//! };
//!
//! let engine = SuggestionEngine::new()
//!     .add_rule(Box::new(CompletenessRule::new()));
//!
//! let suggestions = engine.suggest_constraints(&profile);
//! for suggestion in suggestions {
//!     println!("Suggested: {} (confidence: {:.2})",
//!              suggestion.check_type, suggestion.confidence);
//! }
//! # })
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, instrument};

use crate::analyzers::profiler::{ColumnProfile, DetectedDataType};

/// A suggested constraint with confidence and rationale
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SuggestedConstraint {
    /// Type of check to apply (e.g., "is_complete", "has_uniqueness")
    pub check_type: String,
    /// Column name this constraint applies to
    pub column: String,
    /// Parameters for the constraint (e.g., threshold values)
    pub parameters: HashMap<String, ConstraintParameter>,
    /// Confidence score from 0.0 to 1.0
    pub confidence: f64,
    /// Human-readable explanation for the suggestion
    pub rationale: String,
    /// Priority level for implementation
    pub priority: SuggestionPriority,
}

/// Parameter value for constraint configuration
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConstraintParameter {
    Float(f64),
    Integer(i64),
    String(String),
    Boolean(bool),
}

/// Priority levels for constraint suggestions
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SuggestionPriority {
    Critical, // Data quality issues likely to cause failures
    High,     // Important constraints for data integrity
    Medium,   // Useful constraints for monitoring
    Low,      // Optional constraints for completeness
}

/// Trait for implementing constraint suggestion rules
pub trait ConstraintSuggestionRule: Send + Sync {
    /// Apply this rule to a column profile and return suggested constraints
    fn apply(&self, profile: &ColumnProfile) -> Vec<SuggestedConstraint>;

    /// Get a human-readable name for this rule
    fn name(&self) -> &str;

    /// Get a description of what this rule analyzes
    fn description(&self) -> &str;
}

/// Engine that orchestrates multiple suggestion rules
pub struct SuggestionEngine {
    rules: Vec<Box<dyn ConstraintSuggestionRule>>,
    confidence_threshold: f64,
    max_suggestions_per_column: usize,
}

impl SuggestionEngine {
    /// Create a new suggestion engine with default configuration
    pub fn new() -> Self {
        Self {
            rules: Vec::new(),
            confidence_threshold: 0.5,
            max_suggestions_per_column: 10,
        }
    }

    /// Add a suggestion rule to the engine
    pub fn add_rule(mut self, rule: Box<dyn ConstraintSuggestionRule>) -> Self {
        self.rules.push(rule);
        self
    }

    /// Set the minimum confidence threshold for suggestions
    pub fn confidence_threshold(mut self, threshold: f64) -> Self {
        self.confidence_threshold = threshold.clamp(0.0, 1.0);
        self
    }

    /// Set the maximum number of suggestions per column
    pub fn max_suggestions_per_column(mut self, max: usize) -> Self {
        self.max_suggestions_per_column = max;
        self
    }

    /// Generate constraint suggestions for a column profile
    #[instrument(skip(self, profile))]
    pub fn suggest_constraints(&self, profile: &ColumnProfile) -> Vec<SuggestedConstraint> {
        debug!(
            column = profile.column_name,
            rules_count = self.rules.len(),
            "Generating constraint suggestions"
        );

        let mut all_suggestions = Vec::new();

        // Apply each rule to the profile
        for rule in &self.rules {
            let rule_suggestions = rule.apply(profile);
            debug!(
                rule = rule.name(),
                suggestions_count = rule_suggestions.len(),
                "Applied suggestion rule"
            );
            all_suggestions.extend(rule_suggestions);
        }

        // Filter by confidence threshold
        all_suggestions.retain(|s| s.confidence >= self.confidence_threshold);

        // Sort by confidence (descending) and priority
        all_suggestions.sort_by(|a, b| {
            b.confidence
                .partial_cmp(&a.confidence)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| priority_order(&b.priority).cmp(&priority_order(&a.priority)))
        });

        // Limit the number of suggestions
        all_suggestions.truncate(self.max_suggestions_per_column);

        debug!(
            column = profile.column_name,
            suggestions_count = all_suggestions.len(),
            "Generated constraint suggestions"
        );

        all_suggestions
    }

    /// Generate suggestions for multiple column profiles
    pub fn suggest_constraints_batch(
        &self,
        profiles: &[ColumnProfile],
    ) -> HashMap<String, Vec<SuggestedConstraint>> {
        profiles
            .iter()
            .map(|profile| {
                (
                    profile.column_name.clone(),
                    self.suggest_constraints(profile),
                )
            })
            .collect()
    }
}

impl Default for SuggestionEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper function to get priority order for sorting
fn priority_order(priority: &SuggestionPriority) -> u8 {
    match priority {
        SuggestionPriority::Critical => 0,
        SuggestionPriority::High => 1,
        SuggestionPriority::Medium => 2,
        SuggestionPriority::Low => 3,
    }
}

/// Completeness rule that suggests constraints based on null percentage
pub struct CompletenessRule {
    high_completeness_threshold: f64,
    medium_completeness_threshold: f64,
}

impl CompletenessRule {
    /// Create a new completeness rule with default thresholds
    pub fn new() -> Self {
        Self {
            high_completeness_threshold: 0.98,
            medium_completeness_threshold: 0.90,
        }
    }

    /// Create a completeness rule with custom thresholds
    pub fn with_thresholds(high: f64, medium: f64) -> Self {
        Self {
            high_completeness_threshold: high.clamp(0.0, 1.0),
            medium_completeness_threshold: medium.clamp(0.0, 1.0),
        }
    }
}

impl Default for CompletenessRule {
    fn default() -> Self {
        Self::new()
    }
}

impl ConstraintSuggestionRule for CompletenessRule {
    fn apply(&self, profile: &ColumnProfile) -> Vec<SuggestedConstraint> {
        let completeness = 1.0 - profile.basic_stats.null_percentage;
        let mut suggestions = Vec::new();

        if completeness >= self.high_completeness_threshold {
            suggestions.push(SuggestedConstraint {
                check_type: "is_complete".to_string(),
                column: profile.column_name.clone(),
                parameters: HashMap::new(),
                confidence: 0.9,
                rationale: format!(
                    "Column is {:.1}%+ complete, suggesting completeness constraint",
                    completeness * 100.0
                ),
                priority: SuggestionPriority::High,
            });
        } else if completeness >= self.medium_completeness_threshold {
            let mut params = HashMap::new();
            params.insert(
                "threshold".to_string(),
                ConstraintParameter::Float(completeness - 0.02),
            );

            suggestions.push(SuggestedConstraint {
                check_type: "has_completeness".to_string(),
                column: profile.column_name.clone(),
                parameters: params,
                confidence: 0.8,
                rationale: format!(
                    "Column has {:.1}% completeness, suggesting threshold constraint",
                    completeness * 100.0
                ),
                priority: SuggestionPriority::Medium,
            });
        } else if completeness < 0.5 {
            // Very low completeness - might indicate data quality issues
            suggestions.push(SuggestedConstraint {
                check_type: "monitor_completeness".to_string(),
                column: profile.column_name.clone(),
                parameters: HashMap::new(),
                confidence: 0.7,
                rationale: format!(
                    "Column has only {:.1}% completeness, suggesting monitoring",
                    completeness * 100.0
                ),
                priority: SuggestionPriority::Critical,
            });
        }

        suggestions
    }

    fn name(&self) -> &str {
        "CompletenessRule"
    }

    fn description(&self) -> &str {
        "Analyzes null percentage to suggest completeness constraints"
    }
}

/// Uniqueness rule that suggests constraints for potential key columns
pub struct UniquenessRule {
    high_uniqueness_threshold: f64,
    medium_uniqueness_threshold: f64,
}

impl UniquenessRule {
    /// Create a new uniqueness rule with default thresholds
    pub fn new() -> Self {
        Self {
            high_uniqueness_threshold: 0.95,
            medium_uniqueness_threshold: 0.80,
        }
    }

    /// Create a uniqueness rule with custom thresholds
    pub fn with_thresholds(high: f64, medium: f64) -> Self {
        Self {
            high_uniqueness_threshold: high.clamp(0.0, 1.0),
            medium_uniqueness_threshold: medium.clamp(0.0, 1.0),
        }
    }
}

impl Default for UniquenessRule {
    fn default() -> Self {
        Self::new()
    }
}

impl ConstraintSuggestionRule for UniquenessRule {
    fn apply(&self, profile: &ColumnProfile) -> Vec<SuggestedConstraint> {
        let total_rows = profile.basic_stats.row_count as f64;
        let unique_ratio = if total_rows > 0.0 {
            profile.basic_stats.approximate_cardinality as f64 / total_rows
        } else {
            0.0
        };

        let mut suggestions = Vec::new();

        if unique_ratio >= self.high_uniqueness_threshold {
            suggestions.push(SuggestedConstraint {
                check_type: "is_unique".to_string(),
                column: profile.column_name.clone(),
                parameters: HashMap::new(),
                confidence: 0.9,
                rationale: format!(
                    "Column has {:.1}% unique values, suggesting uniqueness constraint",
                    unique_ratio * 100.0
                ),
                priority: SuggestionPriority::High,
            });
        } else if unique_ratio >= self.medium_uniqueness_threshold {
            let mut params = HashMap::new();
            params.insert(
                "threshold".to_string(),
                ConstraintParameter::Float(unique_ratio - 0.05),
            );

            suggestions.push(SuggestedConstraint {
                check_type: "has_uniqueness".to_string(),
                column: profile.column_name.clone(),
                parameters: params,
                confidence: 0.7,
                rationale: format!(
                    "Column has {:.1}% unique values, suggesting uniqueness monitoring",
                    unique_ratio * 100.0
                ),
                priority: SuggestionPriority::Medium,
            });
        }

        // Special case: if the column looks like an ID based on naming patterns
        let column_lower = profile.column_name.to_lowercase();
        if (column_lower.contains("id") || column_lower.contains("key")) && unique_ratio > 0.7 {
            suggestions.push(SuggestedConstraint {
                check_type: "primary_key_candidate".to_string(),
                column: profile.column_name.clone(),
                parameters: HashMap::new(),
                confidence: 0.8,
                rationale: "Column name suggests identifier and has high uniqueness".to_string(),
                priority: SuggestionPriority::High,
            });
        }

        suggestions
    }

    fn name(&self) -> &str {
        "UniquenessRule"
    }

    fn description(&self) -> &str {
        "Analyzes cardinality to suggest uniqueness constraints for potential keys"
    }
}

/// Pattern rule that identifies common data formats
pub struct PatternRule;

impl PatternRule {
    /// Create a new pattern rule
    pub fn new() -> Self {
        Self
    }

    /// Check if values match email pattern
    fn is_email_pattern(&self, samples: &[String]) -> bool {
        samples
            .iter()
            .take(10)
            .all(|s| s.contains('@') && s.contains('.'))
    }

    /// Check if values match date pattern
    fn is_date_pattern(&self, samples: &[String]) -> bool {
        samples
            .iter()
            .take(10)
            .all(|s| s.contains('-') || s.contains('/') || s.len() == 8)
    }

    /// Check if values match phone pattern
    fn is_phone_pattern(&self, samples: &[String]) -> bool {
        samples
            .iter()
            .take(10)
            .all(|s| s.chars().filter(|c| c.is_numeric()).count() >= 10)
    }
}

impl Default for PatternRule {
    fn default() -> Self {
        Self::new()
    }
}

impl ConstraintSuggestionRule for PatternRule {
    fn apply(&self, profile: &ColumnProfile) -> Vec<SuggestedConstraint> {
        let mut suggestions = Vec::new();

        if profile.data_type == DetectedDataType::String
            && !profile.basic_stats.sample_values.is_empty()
        {
            let samples = &profile.basic_stats.sample_values;

            if self.is_email_pattern(samples) {
                suggestions.push(SuggestedConstraint {
                    check_type: "matches_email_pattern".to_string(),
                    column: profile.column_name.clone(),
                    parameters: HashMap::new(),
                    confidence: 0.85,
                    rationale: "Sample values suggest email format".to_string(),
                    priority: SuggestionPriority::Medium,
                });
            }

            if self.is_date_pattern(samples) {
                suggestions.push(SuggestedConstraint {
                    check_type: "matches_date_pattern".to_string(),
                    column: profile.column_name.clone(),
                    parameters: HashMap::new(),
                    confidence: 0.75,
                    rationale: "Sample values suggest date format".to_string(),
                    priority: SuggestionPriority::Medium,
                });
            }

            if self.is_phone_pattern(samples) {
                suggestions.push(SuggestedConstraint {
                    check_type: "matches_phone_pattern".to_string(),
                    column: profile.column_name.clone(),
                    parameters: HashMap::new(),
                    confidence: 0.70,
                    rationale: "Sample values suggest phone number format".to_string(),
                    priority: SuggestionPriority::Low,
                });
            }
        }

        suggestions
    }

    fn name(&self) -> &str {
        "PatternRule"
    }

    fn description(&self) -> &str {
        "Identifies common data patterns like emails, dates, and phone numbers"
    }
}

/// Range rule that suggests min/max constraints for numeric columns
pub struct RangeRule;

impl RangeRule {
    /// Create a new range rule
    pub fn new() -> Self {
        Self
    }
}

impl Default for RangeRule {
    fn default() -> Self {
        Self::new()
    }
}

impl ConstraintSuggestionRule for RangeRule {
    fn apply(&self, profile: &ColumnProfile) -> Vec<SuggestedConstraint> {
        let mut suggestions = Vec::new();

        match profile.data_type {
            DetectedDataType::Integer | DetectedDataType::Double => {
                // Try to get min/max from basic_stats
                if let (Some(ref min_str), Some(ref max_str)) = (
                    &profile.basic_stats.min_value,
                    &profile.basic_stats.max_value,
                ) {
                    if let (Ok(min_val), Ok(max_val)) =
                        (min_str.parse::<f64>(), max_str.parse::<f64>())
                    {
                        let range = max_val - min_val;

                        // Suggest range constraints if the range is reasonable
                        if range > 0.0 && min_val >= 0.0 {
                            let mut min_params = HashMap::new();
                            min_params.insert(
                                "threshold".to_string(),
                                ConstraintParameter::Float(min_val),
                            );

                            suggestions.push(SuggestedConstraint {
                                check_type: "has_min".to_string(),
                                column: profile.column_name.clone(),
                                parameters: min_params,
                                confidence: 0.8,
                                rationale: format!("Minimum value observed: {min_val}"),
                                priority: SuggestionPriority::Medium,
                            });

                            let mut max_params = HashMap::new();
                            max_params.insert(
                                "threshold".to_string(),
                                ConstraintParameter::Float(max_val),
                            );

                            suggestions.push(SuggestedConstraint {
                                check_type: "has_max".to_string(),
                                column: profile.column_name.clone(),
                                parameters: max_params,
                                confidence: 0.8,
                                rationale: format!("Maximum value observed: {max_val}"),
                                priority: SuggestionPriority::Medium,
                            });

                            // Suggest positive values constraint if applicable
                            if min_val >= 0.0 {
                                suggestions.push(SuggestedConstraint {
                                    check_type: "is_positive".to_string(),
                                    column: profile.column_name.clone(),
                                    parameters: HashMap::new(),
                                    confidence: 0.9,
                                    rationale: "All observed values are non-negative".to_string(),
                                    priority: SuggestionPriority::High,
                                });
                            }
                        }
                    }
                }

                // Check for potential quantile-based constraints from numeric distribution
                if let Some(distribution) = &profile.numeric_distribution {
                    if let Some(quantiles) = distribution.quantiles.get("P99") {
                        let mut outlier_params = HashMap::new();
                        outlier_params.insert(
                            "threshold".to_string(),
                            ConstraintParameter::Float(*quantiles),
                        );

                        suggestions.push(SuggestedConstraint {
                            check_type: "has_no_outliers".to_string(),
                            column: profile.column_name.clone(),
                            parameters: outlier_params,
                            confidence: 0.7,
                            rationale: "Suggests outlier detection based on P99".to_string(),
                            priority: SuggestionPriority::Low,
                        });
                    }
                }
            }
            _ => {}
        }

        suggestions
    }

    fn name(&self) -> &str {
        "RangeRule"
    }

    fn description(&self) -> &str {
        "Suggests min/max constraints and outlier detection for numeric columns"
    }
}

/// Data type rule that enforces consistent types
pub struct DataTypeRule;

impl DataTypeRule {
    /// Create a new data type rule
    pub fn new() -> Self {
        Self
    }
}

impl Default for DataTypeRule {
    fn default() -> Self {
        Self::new()
    }
}

impl ConstraintSuggestionRule for DataTypeRule {
    fn apply(&self, profile: &ColumnProfile) -> Vec<SuggestedConstraint> {
        let mut suggestions = Vec::new();

        match &profile.data_type {
            DetectedDataType::Mixed => {
                suggestions.push(SuggestedConstraint {
                    check_type: "has_consistent_type".to_string(),
                    column: profile.column_name.clone(),
                    parameters: HashMap::new(),
                    confidence: 0.9,
                    rationale: "Column has mixed data types, suggesting type consistency check"
                        .to_string(),
                    priority: SuggestionPriority::Critical,
                });
            }
            DetectedDataType::Unknown => {
                suggestions.push(SuggestedConstraint {
                    check_type: "validate_data_type".to_string(),
                    column: profile.column_name.clone(),
                    parameters: HashMap::new(),
                    confidence: 0.8,
                    rationale: "Unable to determine data type, suggesting validation".to_string(),
                    priority: SuggestionPriority::High,
                });
            }
            detected_type => {
                let mut params = HashMap::new();
                params.insert(
                    "expected_type".to_string(),
                    ConstraintParameter::String(format!("{detected_type:?}")),
                );

                suggestions.push(SuggestedConstraint {
                    check_type: "has_data_type".to_string(),
                    column: profile.column_name.clone(),
                    parameters: params,
                    confidence: 0.85,
                    rationale: format!("Column consistently contains {detected_type:?} values"),
                    priority: SuggestionPriority::Medium,
                });
            }
        }

        suggestions
    }

    fn name(&self) -> &str {
        "DataTypeRule"
    }

    fn description(&self) -> &str {
        "Suggests data type validation constraints based on detected types"
    }
}

/// Cardinality rule that detects categorical columns
pub struct CardinalityRule {
    categorical_threshold: u64,
    low_cardinality_threshold: u64,
}

impl CardinalityRule {
    /// Create a new cardinality rule with default thresholds
    pub fn new() -> Self {
        Self {
            categorical_threshold: 50,
            low_cardinality_threshold: 10,
        }
    }

    /// Create a cardinality rule with custom thresholds
    pub fn with_thresholds(categorical: u64, low_cardinality: u64) -> Self {
        Self {
            categorical_threshold: categorical,
            low_cardinality_threshold: low_cardinality,
        }
    }
}

impl Default for CardinalityRule {
    fn default() -> Self {
        Self::new()
    }
}

impl ConstraintSuggestionRule for CardinalityRule {
    fn apply(&self, profile: &ColumnProfile) -> Vec<SuggestedConstraint> {
        let mut suggestions = Vec::new();
        let cardinality = profile.basic_stats.approximate_cardinality;
        let total_rows = profile.basic_stats.row_count;

        if cardinality <= self.low_cardinality_threshold {
            suggestions.push(SuggestedConstraint {
                check_type: "is_categorical".to_string(),
                column: profile.column_name.clone(),
                parameters: HashMap::new(),
                confidence: 0.9,
                rationale: format!(
                    "Column has only {cardinality} distinct values, suggesting categorical constraint"
                ),
                priority: SuggestionPriority::High,
            });

            // Suggest specific value constraints if we have histogram data
            if let Some(histogram) = &profile.categorical_histogram {
                let valid_values: Vec<String> =
                    histogram.buckets.iter().map(|b| b.value.clone()).collect();

                let mut params = HashMap::new();
                params.insert(
                    "valid_values".to_string(),
                    ConstraintParameter::String(valid_values.join(",")),
                );

                suggestions.push(SuggestedConstraint {
                    check_type: "is_in_set".to_string(),
                    column: profile.column_name.clone(),
                    parameters: params,
                    confidence: 0.85,
                    rationale: "Column has well-defined categorical values".to_string(),
                    priority: SuggestionPriority::Medium,
                });
            }
        } else if cardinality <= self.categorical_threshold {
            let mut params = HashMap::new();
            params.insert(
                "threshold".to_string(),
                ConstraintParameter::Integer(cardinality as i64),
            );

            suggestions.push(SuggestedConstraint {
                check_type: "has_max_cardinality".to_string(),
                column: profile.column_name.clone(),
                parameters: params,
                confidence: 0.7,
                rationale: format!(
                    "Column has {cardinality} distinct values, suggesting cardinality monitoring"
                ),
                priority: SuggestionPriority::Medium,
            });
        }

        // Check for very high cardinality that might indicate data quality issues
        if total_rows > 0 && cardinality as f64 / total_rows as f64 > 0.8 {
            suggestions.push(SuggestedConstraint {
                check_type: "monitor_cardinality".to_string(),
                column: profile.column_name.clone(),
                parameters: HashMap::new(),
                confidence: 0.6,
                rationale: "High cardinality might indicate data quality issues".to_string(),
                priority: SuggestionPriority::Low,
            });
        }

        suggestions
    }

    fn name(&self) -> &str {
        "CardinalityRule"
    }

    fn description(&self) -> &str {
        "Detects categorical columns and suggests cardinality constraints"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyzers::profiler::{
        BasicStatistics, CategoricalBucket, CategoricalHistogram, DetectedDataType,
        NumericDistribution,
    };
    use std::collections::HashMap;

    fn create_test_profile(column_name: &str, null_percentage: f64) -> ColumnProfile {
        ColumnProfile {
            column_name: column_name.to_string(),
            data_type: DetectedDataType::String,
            basic_stats: BasicStatistics {
                row_count: 1000,
                null_count: (1000.0 * null_percentage) as u64,
                null_percentage,
                approximate_cardinality: 500,
                min_value: None,
                max_value: None,
                sample_values: vec!["A".to_string(), "B".to_string()],
            },
            categorical_histogram: None,
            numeric_distribution: None,
            profiling_time_ms: 100,
            passes_executed: vec![1],
        }
    }

    fn create_numeric_profile(column_name: &str, min_val: f64, max_val: f64) -> ColumnProfile {
        let mut quantiles = HashMap::new();
        quantiles.insert("P99".to_string(), max_val * 0.99);

        ColumnProfile {
            column_name: column_name.to_string(),
            data_type: DetectedDataType::Double,
            basic_stats: BasicStatistics {
                row_count: 1000,
                null_count: 0,
                null_percentage: 0.0,
                approximate_cardinality: 800,
                min_value: Some(min_val.to_string()),
                max_value: Some(max_val.to_string()),
                sample_values: vec![min_val.to_string(), max_val.to_string()],
            },
            categorical_histogram: None,
            numeric_distribution: Some(NumericDistribution {
                mean: Some((min_val + max_val) / 2.0),
                std_dev: Some(10.0),
                variance: Some(100.0),
                quantiles,
                outlier_count: 0,
                skewness: None,
                kurtosis: None,
            }),
            profiling_time_ms: 100,
            passes_executed: vec![1, 3],
        }
    }

    fn create_categorical_profile(column_name: &str, cardinality: u64) -> ColumnProfile {
        let buckets = vec![
            CategoricalBucket {
                value: "A".to_string(),
                count: 400,
            },
            CategoricalBucket {
                value: "B".to_string(),
                count: 300,
            },
            CategoricalBucket {
                value: "C".to_string(),
                count: 200,
            },
            CategoricalBucket {
                value: "D".to_string(),
                count: 100,
            },
        ];

        ColumnProfile {
            column_name: column_name.to_string(),
            data_type: DetectedDataType::String,
            basic_stats: BasicStatistics {
                row_count: 1000,
                null_count: 0,
                null_percentage: 0.0,
                approximate_cardinality: cardinality,
                min_value: None,
                max_value: None,
                sample_values: vec!["A".to_string(), "B".to_string()],
            },
            categorical_histogram: Some(CategoricalHistogram {
                buckets,
                total_count: 1000,
                entropy: 1.5,
                top_values: vec![("A".to_string(), 400), ("B".to_string(), 300)],
            }),
            numeric_distribution: None,
            profiling_time_ms: 100,
            passes_executed: vec![1, 2],
        }
    }

    #[test]
    fn test_completeness_rule_high_completeness() {
        let rule = CompletenessRule::new();
        let profile = create_test_profile("test_col", 0.01); // 99% complete

        let suggestions = rule.apply(&profile);
        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0].check_type, "is_complete");
        assert_eq!(suggestions[0].confidence, 0.9);
        assert_eq!(suggestions[0].priority, SuggestionPriority::High);
    }

    #[test]
    fn test_completeness_rule_medium_completeness() {
        let rule = CompletenessRule::new();
        let profile = create_test_profile("test_col", 0.05); // 95% complete

        let suggestions = rule.apply(&profile);
        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0].check_type, "has_completeness");
        assert_eq!(suggestions[0].confidence, 0.8);
        assert_eq!(suggestions[0].priority, SuggestionPriority::Medium);

        // Check threshold parameter
        if let Some(ConstraintParameter::Float(threshold)) =
            suggestions[0].parameters.get("threshold")
        {
            assert!(*threshold < 0.95);
            assert!(*threshold > 0.90);
        } else {
            panic!("Expected threshold parameter");
        }
    }

    #[test]
    fn test_completeness_rule_low_completeness() {
        let rule = CompletenessRule::new();
        let profile = create_test_profile("test_col", 0.6); // 40% complete

        let suggestions = rule.apply(&profile);
        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0].check_type, "monitor_completeness");
        assert_eq!(suggestions[0].confidence, 0.7);
        assert_eq!(suggestions[0].priority, SuggestionPriority::Critical);
    }

    #[test]
    fn test_suggestion_engine_confidence_filtering() {
        let engine = SuggestionEngine::new()
            .confidence_threshold(0.85)
            .add_rule(Box::new(CompletenessRule::new()));

        let profile = create_test_profile("test_col", 0.05); // 95% complete, confidence 0.8
        let suggestions = engine.suggest_constraints(&profile);

        // Should be filtered out due to confidence threshold
        assert_eq!(suggestions.len(), 0);
    }

    #[test]
    fn test_suggestion_engine_max_suggestions() {
        let engine = SuggestionEngine::new()
            .max_suggestions_per_column(1)
            .add_rule(Box::new(CompletenessRule::new()));

        let profile = create_test_profile("test_col", 0.01); // High completeness
        let suggestions = engine.suggest_constraints(&profile);

        assert!(suggestions.len() <= 1);
    }

    #[test]
    fn test_uniqueness_rule_high_uniqueness() {
        let rule = UniquenessRule::new();
        let mut profile = create_test_profile("test_col", 0.0);
        profile.basic_stats.approximate_cardinality = 980; // 98% unique

        let suggestions = rule.apply(&profile);
        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0].check_type, "is_unique");
        assert_eq!(suggestions[0].confidence, 0.9);
    }

    #[test]
    fn test_uniqueness_rule_id_column() {
        let rule = UniquenessRule::new();
        let mut profile = create_test_profile("user_id", 0.0);
        profile.basic_stats.approximate_cardinality = 800; // 80% unique

        let suggestions = rule.apply(&profile);
        assert!(suggestions
            .iter()
            .any(|s| s.check_type == "primary_key_candidate"));
    }

    #[test]
    fn test_pattern_rule_email() {
        let rule = PatternRule::new();
        let mut profile = create_test_profile("email", 0.0);
        profile.basic_stats.sample_values = vec![
            "user@example.com".to_string(),
            "test@domain.org".to_string(),
        ];

        let suggestions = rule.apply(&profile);
        assert!(suggestions
            .iter()
            .any(|s| s.check_type == "matches_email_pattern"));
    }

    #[test]
    fn test_range_rule_numeric() {
        let rule = RangeRule::new();
        let profile = create_numeric_profile("age", 0.0, 100.0);

        let suggestions = rule.apply(&profile);
        assert!(suggestions.iter().any(|s| s.check_type == "has_min"));
        assert!(suggestions.iter().any(|s| s.check_type == "has_max"));
        assert!(suggestions.iter().any(|s| s.check_type == "is_positive"));
    }

    #[test]
    fn test_data_type_rule_mixed() {
        let rule = DataTypeRule::new();
        let mut profile = create_test_profile("mixed_col", 0.0);
        profile.data_type = DetectedDataType::Mixed;

        let suggestions = rule.apply(&profile);
        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0].check_type, "has_consistent_type");
        assert_eq!(suggestions[0].priority, SuggestionPriority::Critical);
    }

    #[test]
    fn test_cardinality_rule_categorical() {
        let rule = CardinalityRule::new();
        let profile = create_categorical_profile("status", 4);

        let suggestions = rule.apply(&profile);
        assert!(suggestions.iter().any(|s| s.check_type == "is_categorical"));
        assert!(suggestions.iter().any(|s| s.check_type == "is_in_set"));
    }

    #[test]
    fn test_suggestion_engine_with_all_rules() {
        let engine = SuggestionEngine::new()
            .add_rule(Box::new(CompletenessRule::new()))
            .add_rule(Box::new(UniquenessRule::new()))
            .add_rule(Box::new(PatternRule::new()))
            .add_rule(Box::new(RangeRule::new()))
            .add_rule(Box::new(DataTypeRule::new()))
            .add_rule(Box::new(CardinalityRule::new()));

        let profile = create_numeric_profile("price", 0.0, 999.99);
        let suggestions = engine.suggest_constraints(&profile);

        // Should get suggestions from multiple rules
        assert!(!suggestions.is_empty());

        // Verify they're sorted by confidence
        for i in 1..suggestions.len() {
            assert!(suggestions[i - 1].confidence >= suggestions[i].confidence);
        }
    }

    #[test]
    fn test_suggestion_batch_processing() {
        let engine = SuggestionEngine::new().add_rule(Box::new(CompletenessRule::new()));

        let profiles = vec![
            create_test_profile("col1", 0.01),
            create_test_profile("col2", 0.05),
        ];

        let batch_results = engine.suggest_constraints_batch(&profiles);
        assert_eq!(batch_results.len(), 2);
        assert!(batch_results.contains_key("col1"));
        assert!(batch_results.contains_key("col2"));
    }
}
