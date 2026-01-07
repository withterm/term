//! Logging utilities and configuration for Term.
//!
//! This module provides utilities for performance-sensitive logging configuration
//! and best practices for structured logging with OpenTelemetry integration.

use tracing::Level;

/// Logging configuration for Term.
///
/// This configuration allows fine-grained control over logging behavior
/// to ensure minimal performance impact in production environments.
#[derive(Debug, Clone)]
pub struct LogConfig {
    /// Base log level for Term components
    pub base_level: Level,
    /// Whether to log constraint evaluation details
    pub log_constraint_details: bool,
    /// Whether to log data source operations
    pub log_data_operations: bool,
    /// Whether to include metrics in log output
    pub log_metrics: bool,
    /// Maximum length for logged field values (to prevent huge logs)
    pub max_field_length: usize,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            base_level: Level::INFO,
            log_constraint_details: false,
            log_data_operations: true,
            log_metrics: true,
            max_field_length: 256,
        }
    }
}

impl LogConfig {
    /// Creates a verbose configuration suitable for debugging.
    pub fn verbose() -> Self {
        Self {
            base_level: Level::DEBUG,
            log_constraint_details: true,
            log_data_operations: true,
            log_metrics: true,
            max_field_length: 1024,
        }
    }

    /// Creates a minimal configuration for production with lowest overhead.
    pub fn production() -> Self {
        Self {
            base_level: Level::WARN,
            log_constraint_details: false,
            log_data_operations: false,
            log_metrics: false,
            max_field_length: 128,
        }
    }

    /// Creates a balanced configuration suitable for most use cases.
    pub fn balanced() -> Self {
        Self::default()
    }
}

/// Macro for performance-sensitive debug logging.
///
/// This macro only evaluates its arguments if debug logging is enabled,
/// avoiding the overhead of formatting when logs won't be emitted.
#[macro_export]
macro_rules! perf_debug {
    ($config:expr, $($arg:tt)*) => {
        if $config.base_level <= tracing::Level::DEBUG {
            tracing::debug!($($arg)*);
        }
    };
}

/// Macro for conditional constraint logging.
#[macro_export]
macro_rules! log_constraint {
    ($config:expr, $($arg:tt)*) => {
        if $config.log_constraint_details {
            tracing::debug!($($arg)*);
        }
    };
}

/// Macro for conditional data operation logging.
#[macro_export]
macro_rules! log_data_op {
    ($config:expr, $($arg:tt)*) => {
        if $config.log_data_operations {
            tracing::info!($($arg)*);
        }
    };
}

/// Truncates a string to the maximum field length if needed.
pub fn truncate_field(value: &str, max_length: usize) -> String {
    if value.len() <= max_length {
        value.to_string()
    } else {
        let truncated = &value[..max_length];
        format!("{truncated}...(truncated)")
    }
}

/// Utilities for setting up structured logging with OpenTelemetry integration.
pub mod setup {
    use tracing::Level;

    /// Configuration for Term's logging setup.
    #[derive(Debug, Clone)]
    pub struct LoggingConfig {
        /// Log level for the application
        pub level: Level,
        /// Log level for Term components specifically
        pub term_level: Level,
        /// Whether to use JSON output format
        pub json_format: bool,
        /// Whether to include trace correlation (requires telemetry feature)
        pub trace_correlation: bool,
        /// Environment filter override
        pub env_filter: Option<String>,
    }

    impl Default for LoggingConfig {
        fn default() -> Self {
            Self {
                level: Level::INFO,
                term_level: Level::DEBUG,
                json_format: false,
                trace_correlation: false,
                env_filter: None,
            }
        }
    }

    impl LoggingConfig {
        /// Creates a configuration for production use.
        pub fn production() -> Self {
            Self {
                level: Level::WARN,
                term_level: Level::INFO,
                json_format: true,
                trace_correlation: true,
                env_filter: None,
            }
        }

        /// Creates a configuration for development use.
        pub fn development() -> Self {
            Self {
                level: Level::DEBUG,
                term_level: Level::DEBUG,
                json_format: false,
                trace_correlation: false,
                env_filter: None,
            }
        }

        /// Creates a configuration for structured logging with trace correlation.
        pub fn structured() -> Self {
            Self {
                level: Level::INFO,
                term_level: Level::DEBUG,
                json_format: true,
                trace_correlation: true,
                env_filter: None,
            }
        }

        /// Sets the log level for the application.
        pub fn with_level(mut self, level: Level) -> Self {
            self.level = level;
            self
        }

        /// Sets the log level for Term components.
        pub fn with_term_level(mut self, level: Level) -> Self {
            self.term_level = level;
            self
        }

        /// Sets whether to use JSON output format.
        pub fn with_json_format(mut self, enabled: bool) -> Self {
            self.json_format = enabled;
            self
        }

        /// Sets whether to include trace correlation.
        pub fn with_trace_correlation(mut self, enabled: bool) -> Self {
            self.trace_correlation = enabled;
            self
        }

        /// Sets a custom environment filter.
        pub fn with_env_filter(mut self, filter: impl Into<String>) -> Self {
            self.env_filter = Some(filter.into());
            self
        }

        /// Builds the environment filter string.
        pub fn env_filter(&self) -> String {
            if let Some(ref filter) = self.env_filter {
                filter.clone()
            } else {
                format!(
                    "{}={},term_guard={}",
                    self.level.as_str().to_lowercase(),
                    self.level.as_str().to_lowercase(),
                    self.term_level.as_str().to_lowercase()
                )
            }
        }
    }

    /// Initializes basic logging without OpenTelemetry.
    ///
    /// This is suitable for applications that don't need trace correlation.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use term_guard::logging::setup::{LoggingConfig, init_logging};
    ///
    /// // Initialize with default configuration
    /// init_logging(LoggingConfig::default()).unwrap();
    ///
    /// // Initialize with custom configuration
    /// let config = LoggingConfig::development()
    ///     .with_json_format(true);
    /// init_logging(config).unwrap();
    /// ```
    pub fn init_logging(config: LoggingConfig) -> Result<(), Box<dyn std::error::Error>> {
        use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

        let env_filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new(config.env_filter()));

        let fmt_layer = if config.json_format {
            tracing_subscriber::fmt::layer().json().boxed()
        } else {
            tracing_subscriber::fmt::layer().boxed()
        };

        let subscriber = tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer);

        subscriber.init();

        Ok(())
    }

    /// Initializes logging with OpenTelemetry integration for trace correlation.
    ///
    /// This requires the `telemetry` feature to be enabled. When telemetry is disabled,
    /// this function falls back to basic logging.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use term_guard::logging::setup::{LoggingConfig, init_logging_with_telemetry};
    /// use opentelemetry_sdk::trace::{TracerProvider, Tracer};
    ///
    /// // Note: This function is temporarily disabled due to dependency conflicts
    /// // Create a concrete tracer (not BoxedTracer)
    /// let provider = TracerProvider::default();
    /// let tracer = provider.tracer("my-service");
    ///
    /// // Initialize structured logging with trace correlation
    /// let config = LoggingConfig::structured();
    /// init_logging_with_telemetry(config, tracer).unwrap();
    /// ```
    #[cfg(feature = "telemetry")]
    #[allow(dead_code)] // TODO: Fix compatibility with newer tracing-opentelemetry version
    pub fn init_logging_with_telemetry<T>(
        _config: LoggingConfig,
        _tracer: T,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        T: opentelemetry::trace::Tracer + 'static,
    {
        // TODO: This function needs to be updated for compatibility with tracing-opentelemetry 0.26
        // The layer composition is causing type inference issues. For now, users should
        // initialize telemetry separately from logging.
        Err("init_logging_with_telemetry is temporarily disabled due to dependency version conflicts. Please initialize telemetry and logging separately.".into())
    }

    /// Fallback when telemetry feature is not enabled.
    #[cfg(not(feature = "telemetry"))]
    pub fn init_logging_with_telemetry(
        config: LoggingConfig,
        _tracer: (),
    ) -> Result<(), Box<dyn std::error::Error>> {
        tracing::warn!("Telemetry feature not enabled, falling back to basic logging");
        init_logging(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_config_defaults() {
        let config = LogConfig::default();
        assert_eq!(config.base_level, Level::INFO);
        assert!(!config.log_constraint_details);
        assert!(config.log_data_operations);
        assert!(config.log_metrics);
        assert_eq!(config.max_field_length, 256);
    }

    #[test]
    fn test_log_config_verbose() {
        let config = LogConfig::verbose();
        assert_eq!(config.base_level, Level::DEBUG);
        assert!(config.log_constraint_details);
        assert!(config.log_data_operations);
        assert!(config.log_metrics);
        assert_eq!(config.max_field_length, 1024);
    }

    #[test]
    fn test_log_config_production() {
        let config = LogConfig::production();
        assert_eq!(config.base_level, Level::WARN);
        assert!(!config.log_constraint_details);
        assert!(!config.log_data_operations);
        assert!(!config.log_metrics);
        assert_eq!(config.max_field_length, 128);
    }

    #[test]
    fn test_truncate_field() {
        let short_text = "hello";
        assert_eq!(truncate_field(short_text, 10), "hello");

        let long_text = "this is a very long text that should be truncated";
        assert_eq!(truncate_field(long_text, 10), "this is a ...(truncated)");
    }
}
