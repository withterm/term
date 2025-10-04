use crate::check::Check;
use crate::data_source::DataSource;
use crate::types::{PerformanceMetrics, ValidationIssue, ValidationReport, ValidationResult};
use datafusion::prelude::SessionContext;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::sync::Arc;
use std::time::Instant;
use term_guard::core::{ValidationReport as CoreReport, ValidationSuite as CoreValidationSuite};

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
        self.inner.name.clone()
    }

    #[napi(getter)]
    pub fn description(&self) -> Option<String> {
        self.inner.description.clone()
    }

    #[napi]
    pub async fn run(&self, data: &DataSource) -> Result<ValidationResult> {
        let start = Instant::now();

        // Get the SessionContext from the DataSource
        let ctx = data.get_context().await?;

        // Run the validation suite
        let report = self
            .inner
            .run(&ctx)
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;

        let duration = start.elapsed();
        let duration_ms = duration.as_secs_f64() * 1000.0;

        // Convert the core report to our NAPI types
        let validation_report = convert_report(&report);

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
        self.inner.checks.len() as u32
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
            builder = builder.add_check_arc(check.clone());
        }

        let suite = builder
            .build()
            .map_err(|e| Error::from_reason(e.to_string()))?;

        Ok(ValidationSuite {
            inner: Arc::new(suite),
        })
    }
}

fn convert_report(report: &CoreReport) -> ValidationReport {
    let issues: Vec<ValidationIssue> = report
        .check_results
        .iter()
        .filter_map(|result| {
            if result.status != term_guard::core::ConstraintStatus::Success {
                Some(ValidationIssue {
                    check_name: result.check_name.clone(),
                    level: format!("{:?}", result.level),
                    message: result
                        .message
                        .clone()
                        .unwrap_or_else(|| format!("Check {} failed", result.check_name)),
                })
            } else {
                None
            }
        })
        .collect();

    let total = report.check_results.len() as u32;
    let passed = report
        .check_results
        .iter()
        .filter(|r| r.status == term_guard::core::ConstraintStatus::Success)
        .count() as u32;
    let failed = total - passed;

    ValidationReport {
        suite_name: report.suite_name.clone(),
        total_checks: total,
        passed_checks: passed,
        failed_checks: failed,
        issues,
    }
}
