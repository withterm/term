use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::sync::Arc;
use term_guard::constraints::Assertion;

use crate::types::Level;

#[napi]
pub struct Check {
    inner: Arc<term_guard::core::Check>,
}

impl Check {
    pub(crate) fn from_inner(inner: Arc<term_guard::core::Check>) -> Self {
        Self { inner }
    }

    pub(crate) fn inner(&self) -> &Arc<term_guard::core::Check> {
        &self.inner
    }
}

#[napi]
impl Check {
    #[napi(getter)]
    pub fn name(&self) -> String {
        self.inner.name().to_string()
    }

    #[napi(getter)]
    pub fn description(&self) -> Option<String> {
        self.inner.description().map(|s| s.to_string())
    }
}

#[napi]
pub struct CheckBuilder {
    name: String,
    level: Level,
    description: Option<String>,
}

#[napi]
impl CheckBuilder {
    #[napi(factory)]
    pub fn new(name: String) -> Self {
        Self {
            name,
            level: Level::Error,
            description: None,
        }
    }

    #[napi]
    pub fn level(&mut self, level: Level) -> &Self {
        self.level = level;
        self
    }

    #[napi]
    pub fn description(&mut self, desc: String) -> &Self {
        self.description = Some(desc);
        self
    }

    fn create_builder(&self) -> term_guard::core::CheckBuilder {
        let level_clone = match self.level {
            Level::Error => Level::Error,
            Level::Warning => Level::Warning,
            Level::Info => Level::Info,
        };

        let mut builder = term_guard::core::Check::builder(&self.name).level(level_clone.into());

        if let Some(ref desc) = self.description {
            builder = builder.description(desc);
        }

        builder
    }

    #[napi]
    pub fn completeness(&mut self, column: String, threshold: f64) -> Result<Check> {
        let options = term_guard::core::ConstraintOptions::new().with_threshold(threshold);
        let check = self.create_builder().completeness(column, options).build();

        Ok(Check::from_inner(Arc::new(check)))
    }

    #[napi]
    pub fn uniqueness(&mut self, column: String, threshold: Option<f64>) -> Result<Check> {
        let threshold = threshold.unwrap_or(1.0);
        let check = self
            .create_builder()
            .validates_uniqueness(vec![column], threshold)
            .build();

        Ok(Check::from_inner(Arc::new(check)))
    }

    #[napi]
    pub fn has_min(&mut self, column: String, min_value: f64) -> Result<Check> {
        let assertion = Assertion::Equals(min_value);
        let check = self.create_builder().has_min(column, assertion).build();

        Ok(Check::from_inner(Arc::new(check)))
    }

    #[napi]
    pub fn has_max(&mut self, column: String, max_value: f64) -> Result<Check> {
        let assertion = Assertion::Equals(max_value);
        let check = self.create_builder().has_max(column, assertion).build();

        Ok(Check::from_inner(Arc::new(check)))
    }

    #[napi]
    pub fn has_mean(
        &mut self,
        column: String,
        expected: f64,
        tolerance: Option<f64>,
    ) -> Result<Check> {
        let assertion = if let Some(tol) = tolerance {
            Assertion::Between(expected - tol, expected + tol)
        } else {
            Assertion::Equals(expected)
        };
        let check = self.create_builder().has_mean(column, assertion).build();

        Ok(Check::from_inner(Arc::new(check)))
    }

    #[napi]
    pub fn has_sum(
        &mut self,
        column: String,
        expected: f64,
        tolerance: Option<f64>,
    ) -> Result<Check> {
        let assertion = if let Some(tol) = tolerance {
            Assertion::Between(expected - tol, expected + tol)
        } else {
            Assertion::Equals(expected)
        };
        let check = self.create_builder().has_sum(column, assertion).build();

        Ok(Check::from_inner(Arc::new(check)))
    }

    #[napi]
    pub fn has_standard_deviation(
        &mut self,
        column: String,
        expected: f64,
        tolerance: Option<f64>,
    ) -> Result<Check> {
        let assertion = if let Some(tol) = tolerance {
            Assertion::Between(expected - tol, expected + tol)
        } else {
            Assertion::Equals(expected)
        };
        let check = self
            .create_builder()
            .has_standard_deviation(column, assertion)
            .build();

        Ok(Check::from_inner(Arc::new(check)))
    }

    #[napi]
    pub fn has_quantile(
        &mut self,
        column: String,
        quantile: f64,
        expected: f64,
        tolerance: Option<f64>,
    ) -> Result<Check> {
        let assertion = if let Some(tol) = tolerance {
            Assertion::Between(expected - tol, expected + tol)
        } else {
            Assertion::Equals(expected)
        };
        let check = self
            .create_builder()
            .has_approx_quantile(column, quantile, assertion)
            .build();

        Ok(Check::from_inner(Arc::new(check)))
    }

    #[napi]
    pub fn validates_email(&mut self, column: String, threshold: Option<f64>) -> Result<Check> {
        let threshold = threshold.unwrap_or(1.0);
        let check = self
            .create_builder()
            .validates_email(column, threshold)
            .build();

        Ok(Check::from_inner(Arc::new(check)))
    }

    #[napi]
    pub fn validates_uuid(&mut self, column: String, threshold: Option<f64>) -> Result<Check> {
        let threshold = threshold.unwrap_or(1.0);
        let check = self
            .create_builder()
            .validates_uuid(column, threshold)
            .build();

        Ok(Check::from_inner(Arc::new(check)))
    }

    #[napi]
    pub fn satisfies(&mut self, expression: String) -> Result<Check> {
        let check = self
            .create_builder()
            .satisfies(&expression, None::<String>)
            .build();

        Ok(Check::from_inner(Arc::new(check)))
    }

    #[napi]
    pub fn has_size(&mut self, expected: i64) -> Result<Check> {
        let assertion = Assertion::Equals(expected as f64);
        let check = self.create_builder().has_size(assertion).build();

        Ok(Check::from_inner(Arc::new(check)))
    }

    #[napi]
    pub fn build(&self) -> Result<Check> {
        let check = self.create_builder().build();
        Ok(Check::from_inner(Arc::new(check)))
    }
}
