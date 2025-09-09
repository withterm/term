//! Incremental computation framework for efficient processing of growing datasets.
//!
//! This module provides infrastructure for stateful metrics computation that can
//! efficiently handle append-only data, partitioned datasets, and incremental updates
//! without reprocessing all historical data.
//!
//! ## Architecture
//!
//! The incremental computation system consists of:
//! - `StateStore`: Abstraction for persisting analyzer states
//! - `IncrementalAnalysisRunner`: Orchestrates incremental analysis across partitions
//! - Enhanced `AnalyzerState` implementations with merge capabilities
//!
//! ## Example
//!
//! ```rust,ignore
//! use term_guard::analyzers::incremental::{IncrementalAnalysisRunner, FileSystemStateStore};
//! use term_guard::analyzers::basic::SizeAnalyzer;
//! use datafusion::prelude::*;
//!
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! // Create state store
//! let state_store = FileSystemStateStore::new("/tmp/term_states")?;
//!
//! // Create incremental runner
//! let mut runner = IncrementalAnalysisRunner::new(Box::new(state_store))
//!     .add_analyzer(Box::new(SizeAnalyzer::new()));
//!
//! // Process new partition
//! let ctx = SessionContext::new();
//! // Register data for "2024-01-15" partition...
//!
//! let results = runner.analyze_partition(&ctx, "2024-01-15").await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! ```

mod runner;
mod state_store;

#[cfg(test)]
mod tests;

pub use runner::{IncrementalAnalysisRunner, IncrementalConfig};
pub use state_store::{FileSystemStateStore, StateMap, StateStore};
