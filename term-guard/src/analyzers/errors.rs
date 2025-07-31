//! Error types for the analyzer framework.

use thiserror::Error;

/// Result type for analyzer operations.
pub type AnalyzerResult<T> = Result<T, AnalyzerError>;

/// Errors that can occur during analyzer operations.
#[derive(Error, Debug)]
pub enum AnalyzerError {
    /// Error occurred while computing state from data.
    #[error("Failed to compute state: {0}")]
    StateComputation(String),

    /// Error occurred while computing metric from state.
    #[error("Failed to compute metric: {0}")]
    MetricComputation(String),

    /// Error occurred while merging states.
    #[error("Failed to merge states: {0}")]
    StateMerge(String),

    /// DataFusion query execution error.
    #[error("Query execution failed: {0}")]
    QueryExecution(#[from] datafusion::error::DataFusionError),

    /// Arrow computation error.
    #[error("Arrow computation failed: {0}")]
    ArrowComputation(#[from] arrow::error::ArrowError),

    /// Invalid configuration or parameters.
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),

    /// Data type mismatch or invalid data.
    #[error("Invalid data: {0}")]
    InvalidData(String),

    /// No data available for analysis.
    #[error("No data available for analysis")]
    NoData,

    /// Serialization/deserialization error.
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Generic analyzer error with custom message.
    #[error("{0}")]
    Custom(String),
}

impl AnalyzerError {
    /// Creates a state computation error with the given message.
    pub fn state_computation(msg: impl Into<String>) -> Self {
        Self::StateComputation(msg.into())
    }

    /// Creates a metric computation error with the given message.
    pub fn metric_computation(msg: impl Into<String>) -> Self {
        Self::MetricComputation(msg.into())
    }

    /// Creates a state merge error with the given message.
    pub fn state_merge(msg: impl Into<String>) -> Self {
        Self::StateMerge(msg.into())
    }

    /// Creates an invalid configuration error with the given message.
    pub fn invalid_config(msg: impl Into<String>) -> Self {
        Self::InvalidConfiguration(msg.into())
    }

    /// Creates an invalid data error with the given message.
    pub fn invalid_data(msg: impl Into<String>) -> Self {
        Self::InvalidData(msg.into())
    }

    /// Creates a custom error with the given message.
    pub fn custom(msg: impl Into<String>) -> Self {
        Self::Custom(msg.into())
    }

    /// Creates an execution error with the given message.
    pub fn execution(msg: impl Into<String>) -> Self {
        Self::Custom(format!("Execution error: {}", msg.into()))
    }

    /// Creates an internal error with the given message.
    pub fn internal(msg: impl Into<String>) -> Self {
        Self::Custom(format!("Internal error: {}", msg.into()))
    }
}

/// Converts serde_json errors to AnalyzerError.
impl From<serde_json::Error> for AnalyzerError {
    fn from(err: serde_json::Error) -> Self {
        Self::Serialization(err.to_string())
    }
}
