//! Logical operators and column specifications for unified constraint APIs.
//!
//! This module provides the core infrastructure for combining constraint results
//! across multiple columns using logical operators like AND, OR, and threshold-based
//! operators.

use std::fmt;

/// Logical operators for combining multiple boolean results.
///
/// These operators define how to evaluate constraint results across multiple columns
/// or multiple constraint evaluations.
///
/// # Examples
///
/// ```rust
/// use term_core::core::LogicalOperator;
///
/// // All columns must satisfy the constraint
/// let all = LogicalOperator::All;
///
/// // At least one column must satisfy
/// let any = LogicalOperator::Any;
///
/// // Exactly 2 columns must satisfy
/// let exactly_two = LogicalOperator::Exactly(2);
///
/// // At least 3 columns must satisfy
/// let at_least_three = LogicalOperator::AtLeast(3);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalOperator {
    /// All values must satisfy the condition (AND logic)
    All,
    /// At least one value must satisfy the condition (OR logic)
    Any,
    /// Exactly N values must satisfy the condition
    Exactly(usize),
    /// At least N values must satisfy the condition
    AtLeast(usize),
    /// At most N values must satisfy the condition
    AtMost(usize),
}

impl LogicalOperator {
    /// Evaluates a slice of boolean results according to this operator.
    ///
    /// # Arguments
    ///
    /// * `results` - A slice of boolean values to evaluate
    ///
    /// # Returns
    ///
    /// `true` if the results satisfy the operator's condition, `false` otherwise
    ///
    /// # Examples
    ///
    /// ```rust
    /// use term_core::core::LogicalOperator;
    ///
    /// let results = vec![true, false, true];
    ///
    /// assert!(LogicalOperator::Any.evaluate(&results));
    /// assert!(!LogicalOperator::All.evaluate(&results));
    /// assert!(LogicalOperator::Exactly(2).evaluate(&results));
    /// assert!(LogicalOperator::AtLeast(1).evaluate(&results));
    /// assert!(LogicalOperator::AtMost(2).evaluate(&results));
    /// ```
    pub fn evaluate(&self, results: &[bool]) -> bool {
        if results.is_empty() {
            return match self {
                LogicalOperator::All => true,  // Vacuous truth
                LogicalOperator::Any => false, // No elements satisfy
                LogicalOperator::Exactly(n) => *n == 0,
                LogicalOperator::AtLeast(n) => *n == 0,
                LogicalOperator::AtMost(_) => true, // 0 <= any n
            };
        }

        let true_count = results.iter().filter(|&&x| x).count();

        match self {
            LogicalOperator::All => true_count == results.len(),
            LogicalOperator::Any => true_count > 0,
            LogicalOperator::Exactly(n) => true_count == *n,
            LogicalOperator::AtLeast(n) => true_count >= *n,
            LogicalOperator::AtMost(n) => true_count <= *n,
        }
    }

    /// Returns a human-readable description of the operator.
    pub fn description(&self) -> String {
        match self {
            LogicalOperator::All => "all".to_string(),
            LogicalOperator::Any => "any".to_string(),
            LogicalOperator::Exactly(n) => format!("exactly {}", n),
            LogicalOperator::AtLeast(n) => format!("at least {}", n),
            LogicalOperator::AtMost(n) => format!("at most {}", n),
        }
    }
}

impl fmt::Display for LogicalOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.description())
    }
}

/// Specification for columns in a constraint.
///
/// This enum allows constraints to work with either a single column or multiple columns,
/// providing a unified interface for both cases.
///
/// # Examples
///
/// ```rust
/// use term_core::core::ColumnSpec;
///
/// // Single column
/// let single = ColumnSpec::Single("user_id".to_string());
///
/// // Multiple columns
/// let multiple = ColumnSpec::Multiple(vec!["email".to_string(), "phone".to_string()]);
///
/// // Convert from various types
/// let from_str = ColumnSpec::from("user_id");
/// let from_vec = ColumnSpec::from(vec!["col1", "col2"]);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnSpec {
    /// A single column
    Single(String),
    /// Multiple columns
    Multiple(Vec<String>),
}

impl ColumnSpec {
    /// Returns the columns as a vector, regardless of whether this is a single or multiple column spec.
    pub fn as_vec(&self) -> Vec<&str> {
        match self {
            ColumnSpec::Single(col) => vec![col.as_str()],
            ColumnSpec::Multiple(cols) => cols.iter().map(|s| s.as_str()).collect(),
        }
    }

    /// Returns the number of columns in this specification.
    pub fn len(&self) -> usize {
        match self {
            ColumnSpec::Single(_) => 1,
            ColumnSpec::Multiple(cols) => cols.len(),
        }
    }

    /// Returns true if this specification contains no columns.
    pub fn is_empty(&self) -> bool {
        match self {
            ColumnSpec::Single(_) => false,
            ColumnSpec::Multiple(cols) => cols.is_empty(),
        }
    }

    /// Returns true if this is a single column specification.
    pub fn is_single(&self) -> bool {
        matches!(self, ColumnSpec::Single(_))
    }

    /// Returns true if this is a multiple column specification.
    pub fn is_multiple(&self) -> bool {
        matches!(self, ColumnSpec::Multiple(_))
    }

    /// Converts to a multiple column specification, even if currently single.
    pub fn to_multiple(self) -> Vec<String> {
        match self {
            ColumnSpec::Single(col) => vec![col],
            ColumnSpec::Multiple(cols) => cols,
        }
    }
}

impl From<String> for ColumnSpec {
    fn from(s: String) -> Self {
        ColumnSpec::Single(s)
    }
}

impl From<&str> for ColumnSpec {
    fn from(s: &str) -> Self {
        ColumnSpec::Single(s.to_string())
    }
}

impl From<Vec<String>> for ColumnSpec {
    fn from(v: Vec<String>) -> Self {
        match v.len() {
            1 => ColumnSpec::Single(v.into_iter().next().unwrap()),
            _ => ColumnSpec::Multiple(v),
        }
    }
}

impl From<Vec<&str>> for ColumnSpec {
    fn from(v: Vec<&str>) -> Self {
        let strings: Vec<String> = v.into_iter().map(|s| s.to_string()).collect();
        strings.into()
    }
}

impl<const N: usize> From<[&str; N]> for ColumnSpec {
    fn from(arr: [&str; N]) -> Self {
        let vec: Vec<String> = arr.iter().map(|s| s.to_string()).collect();
        vec.into()
    }
}

/// Builder pattern for constraint options.
///
/// This trait provides a fluent interface for configuring constraints
/// with various options.
pub trait ConstraintOptionsBuilder: Sized {
    /// Sets the logical operator for combining results.
    fn with_operator(self, operator: LogicalOperator) -> Self;

    /// Sets the threshold value.
    fn with_threshold(self, threshold: f64) -> Self;

    /// Enables or disables a boolean option.
    fn with_option(self, name: &str, value: bool) -> Self;
}

/// Result of evaluating a logical expression.
///
/// This struct contains both the overall boolean result and detailed information
/// about individual evaluations.
#[derive(Debug, Clone)]
pub struct LogicalResult {
    /// The overall result of the logical evaluation
    pub result: bool,
    /// Individual results that were combined
    pub individual_results: Vec<(String, bool)>,
    /// The operator used for combination
    pub operator: LogicalOperator,
    /// Optional detailed message
    pub message: Option<String>,
}

impl LogicalResult {
    /// Creates a new logical result.
    pub fn new(
        result: bool,
        individual_results: Vec<(String, bool)>,
        operator: LogicalOperator,
    ) -> Self {
        Self {
            result,
            individual_results,
            operator,
            message: None,
        }
    }

    /// Creates a logical result with a message.
    pub fn with_message(mut self, message: String) -> Self {
        self.message = Some(message);
        self
    }

    /// Returns the columns that passed the evaluation.
    pub fn passed_columns(&self) -> Vec<&str> {
        self.individual_results
            .iter()
            .filter(|(_, passed)| *passed)
            .map(|(col, _)| col.as_str())
            .collect()
    }

    /// Returns the columns that failed the evaluation.
    pub fn failed_columns(&self) -> Vec<&str> {
        self.individual_results
            .iter()
            .filter(|(_, passed)| !*passed)
            .map(|(col, _)| col.as_str())
            .collect()
    }

    /// Returns the pass rate (ratio of passed to total).
    pub fn pass_rate(&self) -> f64 {
        if self.individual_results.is_empty() {
            return 0.0;
        }
        let passed = self.individual_results.iter().filter(|(_, p)| *p).count();
        passed as f64 / self.individual_results.len() as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_logical_operator_all() {
        let op = LogicalOperator::All;

        assert!(op.evaluate(&[])); // Vacuous truth
        assert!(op.evaluate(&[true]));
        assert!(!op.evaluate(&[false]));
        assert!(op.evaluate(&[true, true, true]));
        assert!(!op.evaluate(&[true, false, true]));
    }

    #[test]
    fn test_logical_operator_any() {
        let op = LogicalOperator::Any;

        assert!(!op.evaluate(&[])); // No elements satisfy
        assert!(op.evaluate(&[true]));
        assert!(!op.evaluate(&[false]));
        assert!(op.evaluate(&[true, false, false]));
        assert!(!op.evaluate(&[false, false, false]));
    }

    #[test]
    fn test_logical_operator_exactly() {
        let op = LogicalOperator::Exactly(2);

        assert!(!op.evaluate(&[]));
        assert!(!op.evaluate(&[true]));
        assert!(!op.evaluate(&[true, true, true]));
        assert!(op.evaluate(&[true, true, false]));
        assert!(op.evaluate(&[true, false, true]));

        // Edge case: Exactly(0)
        let op_zero = LogicalOperator::Exactly(0);
        assert!(op_zero.evaluate(&[]));
        assert!(!op_zero.evaluate(&[true]));
        assert!(op_zero.evaluate(&[false, false]));
    }

    #[test]
    fn test_logical_operator_at_least() {
        let op = LogicalOperator::AtLeast(2);

        assert!(!op.evaluate(&[]));
        assert!(!op.evaluate(&[true]));
        assert!(op.evaluate(&[true, true]));
        assert!(op.evaluate(&[true, true, true]));
        assert!(op.evaluate(&[true, true, false]));
        assert!(!op.evaluate(&[true, false, false]));
    }

    #[test]
    fn test_logical_operator_at_most() {
        let op = LogicalOperator::AtMost(2);

        assert!(op.evaluate(&[]));
        assert!(op.evaluate(&[true]));
        assert!(op.evaluate(&[true, true]));
        assert!(!op.evaluate(&[true, true, true]));
        assert!(op.evaluate(&[true, false, false]));
        assert!(op.evaluate(&[false, false, false]));
    }

    #[test]
    fn test_column_spec_single() {
        let spec = ColumnSpec::Single("user_id".to_string());

        assert_eq!(spec.len(), 1);
        assert!(!spec.is_empty());
        assert!(spec.is_single());
        assert!(!spec.is_multiple());
        assert_eq!(spec.as_vec(), vec!["user_id"]);
    }

    #[test]
    fn test_column_spec_multiple() {
        let spec = ColumnSpec::Multiple(vec!["email".to_string(), "phone".to_string()]);

        assert_eq!(spec.len(), 2);
        assert!(!spec.is_empty());
        assert!(!spec.is_single());
        assert!(spec.is_multiple());
        assert_eq!(spec.as_vec(), vec!["email", "phone"]);
    }

    #[test]
    fn test_column_spec_conversions() {
        // From &str
        let spec = ColumnSpec::from("test");
        assert!(matches!(spec, ColumnSpec::Single(s) if s == "test"));

        // From String
        let spec = ColumnSpec::from("test".to_string());
        assert!(matches!(spec, ColumnSpec::Single(s) if s == "test"));

        // From Vec with single element
        let spec = ColumnSpec::from(vec!["test"]);
        assert!(matches!(spec, ColumnSpec::Single(s) if s == "test"));

        // From Vec with multiple elements
        let spec = ColumnSpec::from(vec!["test1", "test2"]);
        assert!(matches!(spec, ColumnSpec::Multiple(v) if v.len() == 2));

        // From array
        let spec = ColumnSpec::from(["a", "b", "c"]);
        assert!(matches!(spec, ColumnSpec::Multiple(v) if v.len() == 3));
    }

    #[test]
    fn test_logical_result() {
        let individual = vec![
            ("col1".to_string(), true),
            ("col2".to_string(), false),
            ("col3".to_string(), true),
        ];

        let result = LogicalResult::new(true, individual, LogicalOperator::AtLeast(2));

        assert_eq!(result.passed_columns(), vec!["col1", "col3"]);
        assert_eq!(result.failed_columns(), vec!["col2"]);
        assert_eq!(result.pass_rate(), 2.0 / 3.0);
    }
}
