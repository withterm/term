//! Anomaly detection framework for data quality monitoring.
//!
//! This module provides infrastructure for detecting anomalies in metrics over time
//! using various statistical methods. It's designed to work with the analyzer framework
//! to monitor data quality metrics and alert when significant deviations occur.
//!
//! ## Architecture
//!
//! The anomaly detection system consists of:
//! - `AnomalyDetector`: Core trait for anomaly detection implementations
//! - `AnomalyDetectionStrategy`: Alternative strategy pattern for detection
//! - `MetricsRepository`: Storage abstraction for historical metrics
//! - Detection strategies: RelativeRateOfChange, AbsoluteChange, Z-score
//! - `AnomalyDetectionRunner`: Orchestrates detection across metrics
//!
//! ## Example
//!
//! ```rust,ignore
//! use term_guard::analyzers::anomaly::{
//!     AnomalyDetectionRunner, InMemoryMetricsRepository,
//!     RelativeRateOfChangeDetector, ZScoreDetector
//! };
//! use term_guard::analyzers::AnalysisRunner;
//! use datafusion::prelude::*;
//!
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! // Create metrics repository
//! let repository = InMemoryMetricsRepository::new();
//!
//! // Create detection runner with strategies
//! let detector = AnomalyDetectionRunner::builder()
//!     .repository(Box::new(repository))
//!     .add_detector("completeness.*", Box::new(RelativeRateOfChangeDetector::new(0.1)))
//!     .add_detector("size", Box::new(ZScoreDetector::new(3.0)))
//!     .build();
//!
//! // Run analysis
//! let ctx = SessionContext::new();
//! let runner = AnalysisRunner::new();
//! let metrics = runner.run(&ctx).await?;
//!
//! // Detect anomalies
//! let anomalies = detector.detect_anomalies(&metrics).await?;
//! for anomaly in anomalies {
//!     println!("Anomaly detected in {}: {} (confidence: {:.2})",
//!              anomaly.metric_name, anomaly.description, anomaly.confidence);
//! }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! ```

mod detector;
pub mod strategy;

// Re-export detector types
pub use detector::{
    AbsoluteChangeDetector, Anomaly, AnomalyDetectionConfig, AnomalyDetectionRunner,
    AnomalyDetectionRunnerBuilder, AnomalyDetector, InMemoryMetricsConfig,
    InMemoryMetricsRepository, MemoryStats, MetricDataPoint, MetricsRepository,
    RelativeRateOfChangeDetector, ZScoreDetector,
};

// Re-export strategy types
pub use strategy::{
    AnomalyDetectionStrategy, AnomalyResult, MetricPoint, RelativeRateOfChangeStrategy,
};
