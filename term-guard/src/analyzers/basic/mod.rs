//! Basic statistical analyzers for common metrics.
//!
//! This module provides implementations of fundamental analyzers that compute
//! basic statistics like size, completeness, distinctness, and aggregations.
//! These analyzers serve as both useful functionality and reference implementations
//! for building custom analyzers.

mod completeness;
mod distinctness;
mod mean;
mod min_max;
mod size;
mod sum;

pub use completeness::{CompletenessAnalyzer, CompletenessState};
pub use distinctness::{DistinctnessAnalyzer, DistinctnessState};
pub use mean::{MeanAnalyzer, MeanState};
pub use min_max::{MaxAnalyzer, MinAnalyzer, MinMaxState};
pub use size::{SizeAnalyzer, SizeState};
pub use sum::{SumAnalyzer, SumState};

#[cfg(test)]
mod tests;
