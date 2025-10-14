use crate::types::Level;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::sync::Arc;
use term_guard::constraints::{Assertion, UniquenessOptions, UniquenessType};
use term_guard::core::{Check as CoreCheck, ConstraintOptions, Level as CoreLevel};

#[napi]
pub struct Check {
    inner: Arc<CoreCheck>,
}

#[napi]
impl Check {
    #[napi(getter)]
    pub fn name(&self) -> String {
        self.inner.name().to_string()
    }

    #[napi(getter)]
    pub fn level(&self) -> Level {
        self.inner.level().into()
    }

    #[napi(getter)]
    pub fn description(&self) -> Option<String> {
        self.inner.description().map(|s| s.to_string())
    }
}

#[napi]
pub struct CheckBuilder {
    name: String,
    level: CoreLevel,
    description: Option<String>,
}

#[napi]
impl CheckBuilder {
    #[napi(constructor)]
    pub fn new(name: String) -> Self {
        CheckBuilder {
            name,
            level: CoreLevel::Error,
            description: None,
        }
    }

    #[napi]
    pub fn level(&mut self, level: Level) -> &Self {
        self.level = level.into();
        self
    }

    #[napi]
    pub fn description(&mut self, desc: String) -> &Self {
        self.description = Some(desc);
        self
    }

    #[napi]
    pub fn is_complete(&mut self, column: String, ratio: Option<f64>) -> Result<Check> {
        let mut builder = CoreCheck::builder(&self.name).level(self.level);

        if let Some(desc) = &self.description {
            builder = builder.description(desc);
        }

        let threshold = ratio.unwrap_or(1.0);
        let check = builder
            .completeness(
                column.as_str(),
                ConstraintOptions::default().with_threshold(threshold),
            )
            .build();

        Ok(Check {
            inner: Arc::new(check),
        })
    }

    #[napi]
    pub fn has_min(&mut self, column: String, min_value: f64) -> Result<Check> {
        let mut builder = CoreCheck::builder(&self.name).level(self.level);

        if let Some(desc) = &self.description {
            builder = builder.description(desc);
        }

        let check = builder
            .has_min(column.as_str(), Assertion::GreaterThanOrEqual(min_value))
            .build();

        Ok(Check {
            inner: Arc::new(check),
        })
    }

    #[napi]
    pub fn has_max(&mut self, column: String, max_value: f64) -> Result<Check> {
        let mut builder = CoreCheck::builder(&self.name).level(self.level);

        if let Some(desc) = &self.description {
            builder = builder.description(desc);
        }

        let check = builder
            .has_max(column.as_str(), Assertion::LessThanOrEqual(max_value))
            .build();

        Ok(Check {
            inner: Arc::new(check),
        })
    }

    #[napi]
    pub fn is_unique(&mut self, column: String) -> Result<Check> {
        let mut builder = CoreCheck::builder(&self.name).level(self.level);

        if let Some(desc) = &self.description {
            builder = builder.description(desc);
        }

        let check = builder
            .uniqueness(
                vec![column.as_str()],
                UniquenessType::FullUniqueness { threshold: 1.0 },
                UniquenessOptions::default(),
            )
            .build();

        Ok(Check {
            inner: Arc::new(check),
        })
    }

    #[napi]
    pub fn has_mean(
        &mut self,
        column: String,
        expected: f64,
        tolerance: Option<f64>,
    ) -> Result<Check> {
        let mut builder = CoreCheck::builder(&self.name).level(self.level);

        if let Some(desc) = &self.description {
            builder = builder.description(desc);
        }

        let assertion = if let Some(tol) = tolerance {
            Assertion::Between(expected - tol, expected + tol)
        } else {
            Assertion::Equals(expected)
        };
        let check = builder.has_mean(column.as_str(), assertion).build();

        Ok(Check {
            inner: Arc::new(check),
        })
    }

    #[napi]
    pub fn build(&mut self) -> Result<Check> {
        // Generic build for simple checks
        let mut builder = CoreCheck::builder(&self.name).level(self.level);

        if let Some(desc) = &self.description {
            builder = builder.description(desc);
        }

        // Default to a simple row count check
        let check = builder.has_size(Assertion::GreaterThan(0.0)).build();

        Ok(Check {
            inner: Arc::new(check),
        })
    }
}

impl Check {
    pub(crate) fn get_inner(&self) -> Arc<CoreCheck> {
        self.inner.clone()
    }
}
