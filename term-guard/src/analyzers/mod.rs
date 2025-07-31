//! Core analyzer framework for computing metrics from data.
//!
//! This module provides the foundational traits and types for building analyzers
//! that compute metrics independently of validation checks. Analyzers support
//! incremental computation through state management and can be efficiently
//! combined by the AnalysisRunner.

pub mod advanced;
pub mod basic;
pub mod context;
pub mod errors;
pub mod runner;
pub mod traits;
pub mod types;

pub use context::AnalyzerContext;
pub use errors::{AnalyzerError, AnalyzerResult};
pub use runner::AnalysisRunner;
pub use traits::{Analyzer, AnalyzerState};
pub use types::{MetricDistribution, MetricValue};

#[cfg(test)]
mod tests;
