//! Term Cloud SDK for metrics persistence and observability.
//!
//! This module provides integration with the Term Cloud platform,
//! enabling centralized metrics storage, alerting, and historical analysis.

mod buffer;
mod cache;
mod client;
mod error;
mod types;
mod worker;

pub use buffer::{BufferEntry, MetricsBuffer};
pub use cache::{CacheEntry, OfflineCache};
pub use client::{
    HealthResponse, IngestResponse, MetricsQuery, MetricsResponse, Pagination, TermCloudClient,
};
pub use error::{CloudError, CloudResult};
pub use types::{
    CloudConfig, CloudHistogram, CloudHistogramBucket, CloudMetadata, CloudMetric,
    CloudMetricValue, CloudResultKey, CloudValidationIssue, CloudValidationResult,
};
pub use worker::{UploadWorker, WorkerStats};
