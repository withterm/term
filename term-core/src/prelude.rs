//! Prelude for commonly used types and traits in term-core.

pub use crate::core::{TermContext, TermContextConfig};
pub use crate::error::{ErrorContext, Result, TermError};
pub use crate::formatters::{FormatterConfig, ResultFormatter};
pub use crate::logging::LogConfig;
pub use crate::telemetry::{TermSpan, TermTelemetry};
