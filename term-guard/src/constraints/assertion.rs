//! Assertion types for statistical constraints.

use serde::{Deserialize, Serialize};
use std::fmt;
/// An assertion that can be evaluated against a numeric value.
///
/// Used by statistical constraints to define success criteria.
///
/// # Examples
///
/// ```rust
/// use term_guard::constraints::Assertion;
///
/// // Value must equal 100
/// let assertion = Assertion::Equals(100.0);
/// assert!(assertion.evaluate(100.0));
///
/// // Value must be greater than 50
/// let assertion = Assertion::GreaterThan(50.0);
/// assert!(assertion.evaluate(75.0));
///
/// // Value must be between 10 and 20
/// let assertion = Assertion::Between(10.0, 20.0);
/// assert!(assertion.evaluate(15.0));
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Assertion {
    /// Value must equal the specified value (with epsilon tolerance)
    Equals(f64),
    /// Value must not equal the specified value
    NotEquals(f64),
    /// Value must be greater than the specified value
    GreaterThan(f64),
    /// Value must be greater than or equal to the specified value
    GreaterThanOrEqual(f64),
    /// Value must be less than the specified value
    LessThan(f64),
    /// Value must be less than or equal to the specified value
    LessThanOrEqual(f64),
    /// Value must be between the specified range (inclusive)
    Between(f64, f64),
    /// Value must not be between the specified range
    NotBetween(f64, f64),
}

impl Assertion {
    /// Evaluates the assertion against a value.
    pub fn evaluate(&self, value: f64) -> bool {
        const EPSILON: f64 = 1e-10;

        match self {
            Assertion::Equals(expected) => (value - expected).abs() < EPSILON,
            Assertion::NotEquals(expected) => (value - expected).abs() >= EPSILON,
            Assertion::GreaterThan(threshold) => value > *threshold,
            Assertion::GreaterThanOrEqual(threshold) => value >= *threshold,
            Assertion::LessThan(threshold) => value < *threshold,
            Assertion::LessThanOrEqual(threshold) => value <= *threshold,
            Assertion::Between(min, max) => value >= *min && value <= *max,
            Assertion::NotBetween(min, max) => value < *min || value > *max,
        }
    }

    /// Returns a human-readable description of the assertion.
    pub fn description(&self) -> String {
        match self {
            Assertion::Equals(v) => format!("equals {v}"),
            Assertion::NotEquals(v) => format!("not equals {v}"),
            Assertion::GreaterThan(v) => format!("greater than {v}"),
            Assertion::GreaterThanOrEqual(v) => format!("greater than or equal to {v}"),
            Assertion::LessThan(v) => format!("less than {v}"),
            Assertion::LessThanOrEqual(v) => format!("less than or equal to {v}"),
            Assertion::Between(min, max) => format!("between {min} and {max}"),
            Assertion::NotBetween(min, max) => format!("not between {min} and {max}"),
        }
    }
}

impl fmt::Display for Assertion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.description())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_helpers::evaluate_constraint_with_context;
    #[test]
    fn test_equals() {
        let assertion = Assertion::Equals(10.0);
        assert!(assertion.evaluate(10.0));
        assert!(!assertion.evaluate(10.1));
    }

    #[test]
    fn test_not_equals() {
        let assertion = Assertion::NotEquals(10.0);
        assert!(!assertion.evaluate(10.0));
        assert!(assertion.evaluate(10.1));
    }

    #[test]
    fn test_greater_than() {
        let assertion = Assertion::GreaterThan(10.0);
        assert!(assertion.evaluate(10.1));
        assert!(!assertion.evaluate(10.0));
        assert!(!assertion.evaluate(9.9));
    }

    #[test]
    fn test_between() {
        let assertion = Assertion::Between(10.0, 20.0);
        assert!(assertion.evaluate(15.0));
        assert!(assertion.evaluate(10.0));
        assert!(assertion.evaluate(20.0));
        assert!(!assertion.evaluate(9.9));
        assert!(!assertion.evaluate(20.1));
    }

    #[test]
    fn test_description() {
        assert_eq!(Assertion::Equals(10.0).description(), "equals 10");
        assert_eq!(Assertion::GreaterThan(5.0).description(), "greater than 5");
        assert_eq!(
            Assertion::Between(1.0, 10.0).description(),
            "between 1 and 10"
        );
    }
}
