//! Core analyzer framework for computing metrics from data.
//!
//! This module provides the foundational traits and types for building analyzers
//! that compute metrics independently of validation checks. Analyzers support
//! incremental computation through state management and can be efficiently
//! combined by the AnalysisRunner.
//!
//! ## Available Analyzers
//!
//! - **Basic Analyzers** (`basic`): Fundamental metrics like count, mean, min/max
//! - **Advanced Analyzers** (`advanced`): Complex metrics like entropy, correlation  
//! - **Column Profiler** (`profiler`): Three-pass algorithm for comprehensive column analysis
//! - **Type Inference Engine** (`inference`): Robust data type detection from string data
//! - **Constraint Suggestions** (`suggestions`): Intelligent recommendations for data quality checks
//!
//! ## Key Features
//!
//! ### Type Inference Engine
//! Automatically detects column data types with confidence scores:
//! - Numeric types (Integer, Float, Decimal with precision/scale)
//! - Temporal types (Date, DateTime, Time with format detection)
//! - Boolean values (various representations: true/false, yes/no, 1/0, etc.)
//! - Categorical vs. free text distinction
//! - Mixed type columns with graceful handling
//!
//! ### Column Profiler
//! Efficient three-pass profiling algorithm:
//! - Pass 1: Basic statistics and type sampling
//! - Pass 2: Histogram computation for low-cardinality columns
//! - Pass 3: Distribution analysis for numeric columns
//!
//! ### Constraint Suggestion Engine
//! Rule-based system that analyzes column profiles to recommend data quality constraints:
//! - **Completeness**: Suggests null checks based on current completeness levels
//! - **Uniqueness**: Identifies potential primary keys and unique constraints
//! - **Patterns**: Detects common formats (email, date, phone)
//! - **Ranges**: Recommends min/max bounds for numeric data
//! - **Data Types**: Ensures type consistency across columns
//! - **Cardinality**: Identifies categorical columns and monitors distinct values
//!
//! ## Example Usage
//!
//! ```rust
//! use term_guard::analyzers::{TypeInferenceEngine, ColumnProfiler, SuggestionEngine};
//! use term_guard::analyzers::{CompletenessRule, UniquenessRule, CardinalityRule};
//! use term_guard::test_fixtures::create_minimal_tpc_h_context;
//!
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! let ctx = create_minimal_tpc_h_context().await.unwrap();
//!
//! // Type inference
//! let engine = TypeInferenceEngine::builder()
//!     .confidence_threshold(0.8)
//!     .sample_size(1000)
//!     .build();
//!
//! let inference = engine.infer_column_type(&ctx, "lineitem", "l_quantity").await.unwrap();
//! println!("Inferred type: {:?} (confidence: {:.2})",
//!          inference.inferred_type, inference.confidence);
//!
//! // Column profiling
//! let profiler = ColumnProfiler::builder()
//!     .cardinality_threshold(100)
//!     .build();
//!
//! let profile = profiler.profile_column(&ctx, "lineitem", "l_returnflag").await.unwrap();
//! println!("Profile: {} passes executed in {}ms",
//!          profile.passes_executed.len(), profile.profiling_time_ms);
//!
//! // Constraint suggestions
//! let suggestion_engine = SuggestionEngine::new()
//!     .add_rule(Box::new(CompletenessRule::new()))
//!     .add_rule(Box::new(UniquenessRule::new()))
//!     .add_rule(Box::new(CardinalityRule::new()))
//!     .confidence_threshold(0.7);
//!
//! let suggestions = suggestion_engine.suggest_constraints(&profile);
//! for suggestion in suggestions {
//!     println!("Suggested: {} (confidence: {:.2})",
//!              suggestion.check_type, suggestion.confidence);
//!     println!("  Rationale: {}", suggestion.rationale);
//! }
//! # })
//! ```

pub mod advanced;
pub mod basic;
pub mod context;
pub mod errors;
pub mod runner;
pub mod suggestions;
pub mod traits;
pub mod types;

pub use context::AnalyzerContext;
pub use errors::{AnalyzerError, AnalyzerResult};
pub use runner::AnalysisRunner;
pub use suggestions::{
    CardinalityRule, CompletenessRule, ConstraintParameter, ConstraintSuggestionRule, DataTypeRule,
    PatternRule, RangeRule, SuggestedConstraint, SuggestionEngine, SuggestionPriority,
    UniquenessRule,
};
pub use traits::{Analyzer, AnalyzerState};
pub use types::{MetricDistribution, MetricValue};

#[cfg(test)]
mod tests;
