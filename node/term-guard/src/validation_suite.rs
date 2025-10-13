use crate::check::Check;
use crate::data_source::DataSource;
use crate::types::{PerformanceMetrics, ValidationIssue, ValidationReport, ValidationResult};
use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::sync::Arc;
use std::time::Instant;
use term_guard::core::ValidationSuite as CoreValidationSuite;

#[napi]
pub struct ValidationSuite {
    inner: Arc<CoreValidationSuite>,
}

#[napi]
impl ValidationSuite {
    #[napi(factory)]
    pub fn builder(name: String) -> ValidationSuiteBuilder {
        ValidationSuiteBuilder::new(name)
    }

    #[napi(getter)]
    pub fn name(&self) -> String {
        self.inner.name().to_string()
    }

    #[napi(getter)]
    pub fn description(&self) -> Option<String> {
        self.inner.description().map(|s| s.to_string())
    }

    #[napi]
    pub async fn run(&self, data: &DataSource) -> Result<ValidationResult> {
        let start = Instant::now();

        // Get the SessionContext from the DataSource
        let ctx = data.get_context().await?;

        // Run the validation suite
        let result = self
            .inner
            .run(&ctx)
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;

        let duration = start.elapsed();
        let duration_ms = duration.as_secs_f64() * 1000.0;

        // Convert the core result to our NAPI types
        let validation_report = convert_result(&result);

        let metrics = Some(PerformanceMetrics {
            total_duration_ms: duration_ms,
            checks_per_second: if duration_ms > 0.0 {
                (validation_report.total_checks as f64) / (duration_ms / 1000.0)
            } else {
                0.0
            },
        });

        Ok(ValidationResult {
            status: if validation_report.failed_checks == 0 {
                "success".to_string()
            } else {
                "failure".to_string()
            },
            report: validation_report,
            metrics,
        })
    }

    #[napi(getter)]
    pub fn check_count(&self) -> u32 {
        self.inner.checks().len() as u32
    }
}

#[napi]
pub struct ValidationSuiteBuilder {
    name: String,
    description: Option<String>,
    checks: Vec<Arc<term_guard::core::Check>>,
}

#[napi]
impl ValidationSuiteBuilder {
    #[napi(constructor)]
    pub fn new(name: String) -> Self {
        ValidationSuiteBuilder {
            name,
            description: None,
            checks: Vec::new(),
        }
    }

    #[napi]
    pub fn description(&mut self, desc: String) -> &Self {
        self.description = Some(desc);
        self
    }

    #[napi]
    pub fn add_check(&mut self, check: &Check) -> &Self {
        self.checks.push(check.get_inner());
        self
    }

    #[napi]
    pub fn add_checks(&mut self, checks: Vec<&Check>) -> &Self {
        for check in checks {
            self.checks.push(check.get_inner());
        }
        self
    }

    #[napi]
    pub fn build(&self) -> Result<ValidationSuite> {
        let mut builder = CoreValidationSuite::builder(&self.name);

        if let Some(desc) = &self.description {
            builder = builder.description(desc);
        }

        for check in &self.checks {
            // Convert Arc<Check> to Check by dereferencing
            builder = builder.check(check.as_ref().clone());
        }

        let suite = builder.build();

        Ok(ValidationSuite {
            inner: Arc::new(suite),
        })
    }
}

fn convert_result(result: &term_guard::core::ValidationResult) -> ValidationReport {
    use term_guard::core::ValidationResult;
    
    let report = match result {
        ValidationResult::Success { report, .. } => report,
        ValidationResult::Failure { report } => report,
    };
    
    // Convert issues from the report
    let issues: Vec<ValidationIssue> = report
        .issues
        .iter()
        .map(|issue| ValidationIssue {
            check_name: issue.check_name.clone(),
            level: format!("{:?}", issue.level),
            message: issue.message.clone(),
        })
        .collect();

    let total = report.metrics.total_checks as u32;
    let passed = report.metrics.passed_checks as u32;
    let failed = report.metrics.failed_checks as u32;

    ValidationReport {
        suite_name: report.suite_name.clone(),
        total_checks: total,
        passed_checks: passed,
        failed_checks: failed,
        issues,
    }
}
