use napi_derive::napi;
use term_guard::core::{ConstraintStatus as CoreStatus, Level as CoreLevel};

#[napi]
pub enum Level {
    Error,
    Warning,
    Info,
}

impl From<CoreLevel> for Level {
    fn from(level: CoreLevel) -> Self {
        match level {
            CoreLevel::Error => Level::Error,
            CoreLevel::Warning => Level::Warning,
            CoreLevel::Info => Level::Info,
        }
    }
}

impl From<Level> for CoreLevel {
    fn from(level: Level) -> Self {
        match level {
            Level::Error => CoreLevel::Error,
            Level::Warning => CoreLevel::Warning,
            Level::Info => CoreLevel::Info,
        }
    }
}

#[napi(object)]
pub struct ValidationIssue {
    pub check_name: String,
    pub level: String,
    pub message: String,
}

#[napi(object)]
pub struct ValidationReport {
    pub suite_name: String,
    pub total_checks: u32,
    pub passed_checks: u32,
    pub failed_checks: u32,
    pub issues: Vec<ValidationIssue>,
}

#[napi(object)]
pub struct PerformanceMetrics {
    pub total_duration_ms: f64,
    pub checks_per_second: f64,
}

#[napi(object)]
pub struct ValidationResult {
    pub status: String,
    pub report: ValidationReport,
    pub metrics: Option<PerformanceMetrics>,
}

#[napi]
pub enum ConstraintStatus {
    Success,
    Failure,
    Skipped,
}

impl From<CoreStatus> for ConstraintStatus {
    fn from(status: CoreStatus) -> Self {
        match status {
            CoreStatus::Success => ConstraintStatus::Success,
            CoreStatus::Failure => ConstraintStatus::Failure,
            CoreStatus::Skipped => ConstraintStatus::Skipped,
        }
    }
}
