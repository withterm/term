//! Advanced analyzers for sophisticated data quality metrics.
//!
//! This module provides implementations of advanced analyzers that compute
//! more complex metrics including approximate algorithms, statistical measures,
//! and information theory metrics. These build on the foundation of basic analyzers.

mod approx_count_distinct;
mod compliance;
mod data_type;
mod entropy;
mod histogram;
pub mod kll_sketch;
mod standard_deviation;

pub use approx_count_distinct::{ApproxCountDistinctAnalyzer, ApproxCountDistinctState};
pub use compliance::{ComplianceAnalyzer, ComplianceState};
pub use data_type::{DataTypeAnalyzer, DataTypeState};
pub use entropy::{EntropyAnalyzer, EntropyState};
pub use histogram::{HistogramAnalyzer, HistogramState};
pub use kll_sketch::KllSketch;
pub use standard_deviation::{StandardDeviationAnalyzer, StandardDeviationState};

#[cfg(test)]
mod tests;
