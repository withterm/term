use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::sync::Arc;

use crate::check::Check;
use crate::data_source::DataSource;
use crate::types::{Level, ValidationResult};

#[napi]
pub struct ValidationSuite {
    inner: Arc<term_guard::core::ValidationSuite>,
}

#[napi]
impl ValidationSuite {
    #[napi]
    pub async fn run(&self, data: &DataSource) -> Result<ValidationResult> {
        let result = self
            .inner
            .run(data.context().as_ref())
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;

        Ok(ValidationResult::from(result))
    }

    #[napi(getter)]
    pub fn name(&self) -> String {
        self.inner.name().to_string()
    }

    #[napi(getter)]
    pub fn description(&self) -> Option<String> {
        self.inner.description().map(|s| s.to_string())
    }

    #[napi(getter)]
    pub fn checks(&self) -> Vec<Check> {
        self.inner
            .checks()
            .iter()
            .map(|c| Check::from_inner(c.clone()))
            .collect()
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
    #[napi(factory)]
    pub fn new(name: String) -> Self {
        Self {
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
        self.checks.push(check.inner().clone());
        self
    }

    #[napi]
    pub fn add_checks(&mut self, checks: Vec<&Check>) -> &Self {
        for check in checks {
            self.checks.push(check.inner().clone());
        }
        self
    }

    #[napi]
    pub fn build(&self) -> Result<ValidationSuite> {
        let mut builder = term_guard::core::ValidationSuite::builder(&self.name);

        if let Some(desc) = &self.description {
            builder = builder.description(desc);
        }

        for check in &self.checks {
            builder = builder.check(check.as_ref().clone());
        }

        let suite = builder.build();

        Ok(ValidationSuite {
            inner: Arc::new(suite),
        })
    }
}
