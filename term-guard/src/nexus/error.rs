use thiserror::Error;

/// Errors that can occur when interacting with Term Nexus.
#[derive(Debug, Error)]
pub enum NexusError {
    /// Authentication failed (invalid or expired API key).
    #[error("Authentication failed: {message}")]
    Authentication { message: String },

    /// Network error (connection failed, timeout, etc.).
    #[error("Network error: {message}")]
    Network { message: String },

    /// Rate limited by the server.
    #[error("Rate limited. Retry after {retry_after_secs:?} seconds")]
    RateLimited { retry_after_secs: Option<u64> },

    /// Server returned an error.
    #[error("Server error ({status}): {message}")]
    ServerError { status: u16, message: String },

    /// Request validation failed.
    #[error("Invalid request: {message}")]
    InvalidRequest { message: String },

    /// Serialization/deserialization error.
    #[error("Serialization error: {message}")]
    Serialization { message: String },

    /// Buffer overflow (too many pending metrics).
    #[error("Buffer overflow: {pending_count} metrics pending, max is {max_size}")]
    BufferOverflow {
        pending_count: usize,
        max_size: usize,
    },

    /// Offline cache error.
    #[error("Cache error: {message}")]
    CacheError { message: String },

    /// Configuration error.
    #[error("Configuration error: {message}")]
    Configuration { message: String },
}

impl NexusError {
    /// Returns true if this error is transient and the operation should be retried.
    pub fn is_retryable(&self) -> bool {
        match self {
            NexusError::Network { .. } => true,
            NexusError::RateLimited { .. } => true,
            NexusError::ServerError { status, .. } => *status >= 500,
            _ => false,
        }
    }

    /// Returns the suggested retry delay in seconds, if available.
    pub fn retry_after(&self) -> Option<u64> {
        match self {
            NexusError::RateLimited { retry_after_secs } => *retry_after_secs,
            _ => None,
        }
    }
}

/// Result type for nexus operations.
pub type NexusResult<T> = std::result::Result<T, NexusError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nexus_error_display() {
        let err = NexusError::Authentication {
            message: "Invalid API key".to_string(),
        };
        assert!(err.to_string().contains("Invalid API key"));
    }

    #[test]
    fn test_nexus_error_is_retryable() {
        assert!(!NexusError::Authentication {
            message: "test".to_string()
        }
        .is_retryable());
        assert!(NexusError::Network {
            message: "timeout".to_string()
        }
        .is_retryable());
        assert!(NexusError::RateLimited {
            retry_after_secs: Some(60)
        }
        .is_retryable());
        assert!(NexusError::ServerError {
            status: 500,
            message: "internal".to_string()
        }
        .is_retryable());
    }
}
