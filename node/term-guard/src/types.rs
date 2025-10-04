use napi::bindgen_prelude::*;
use napi_derive::napi;

#[napi]
pub enum Level {
    Error,
    Warning,
    Info,
}

impl From<Level> for term_guard::core::Level {
    fn from(level: Level) -> Self {
        match level {
            Level::Error => term_guard::core::Level::Error,
            Level::Warning => term_guard::core::Level::Warning,
            Level::Info => term_guard::core::Level::Info,
        }
    }
}

impl From<term_guard::core::Level> for Level {
    fn from(level: term_guard::core::Level) -> Self {
        match level {
            term_guard::core::Level::Error => Level::Error,
            term_guard::core::Level::Warning => Level::Warning,
            term_guard::core::Level::Info => Level::Info,
        }
    }
}

#[napi(object)]
pub struct ValidationResult {
    pub success: bool,
    pub score: f64,
    pub results: Vec<ValidationReport>,
}

impl From<term_guard::core::ValidationResult> for ValidationResult {
    fn from(result: term_guard::core::ValidationResult) -> Self {
        match result {
            term_guard::core::ValidationResult::Success { metrics, report } => Self {
                success: true,
                score: metrics.success_rate(),
                results: report
                    .issues
                    .iter()
                    .map(|issue| ValidationReport {
                        check_name: issue.check_name.clone(),
                        check_level: format!("{:?}", issue.level),
                        check_description: Some(issue.constraint_name.clone()),
                        constraint_message: Some(issue.message.clone()),
                        status: "Failure".to_string(),
                        metric: issue.metric,
                    })
                    .collect(),
            },
            term_guard::core::ValidationResult::Failure { report } => Self {
                success: false,
                score: 0.0,
                results: report
                    .issues
                    .iter()
                    .map(|issue| ValidationReport {
                        check_name: issue.check_name.clone(),
                        check_level: format!("{:?}", issue.level),
                        check_description: Some(issue.constraint_name.clone()),
                        constraint_message: Some(issue.message.clone()),
                        status: "Failure".to_string(),
                        metric: issue.metric,
                    })
                    .collect(),
            },
        }
    }
}

#[napi(object)]
pub struct ValidationReport {
    pub check_name: String,
    pub check_level: String,
    pub check_description: Option<String>,
    pub constraint_message: Option<String>,
    pub status: String,
    pub metric: Option<f64>,
}
