//! Term Nexus SDK for metrics persistence and observability.
//!
//! This module provides integration with the Term Nexus platform,
//! enabling centralized metrics storage and historical analysis.

mod buffer;
mod cache;
mod client;
mod error;
mod repository;
mod types;
mod worker;

pub use buffer::{BufferEntry, MetricsBuffer};
pub use cache::{CacheEntry, OfflineCache};
pub use client::{
    HealthResponse, IngestResponse, MetricsQuery, MetricsResponse, NexusClient, Pagination,
};
pub use error::{NexusError, NexusResult};
pub use repository::NexusRepository;
pub use types::{
    NexusConfig, NexusHistogram, NexusHistogramBucket, NexusMetadata, NexusMetric,
    NexusMetricValue, NexusResultKey, NexusValidationIssue, NexusValidationResult,
};
pub use worker::{UploadWorker, WorkerStats};
