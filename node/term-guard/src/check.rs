use crate::types::Level;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::sync::Arc;
use term_guard::core::{Check as CoreCheck, Level as CoreLevel};

#[napi]
pub struct Check {
    inner: Arc<CoreCheck>,
}

#[napi]
impl Check {
    #[napi(getter)]
    pub fn name(&self) -> String {
        self.inner.name.clone()
    }

    #[napi(getter)]
    pub fn level(&self) -> Level {
        self.inner.level.clone().into()
    }

    #[napi(getter)]
    pub fn description(&self) -> Option<String> {
        self.inner.description.clone()
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
        let mut builder = CoreCheck::builder(&self.name).level(self.level.clone());

        if let Some(desc) = &self.description {
            builder = builder.description(desc);
        }

        let threshold = ratio.unwrap_or(1.0);
        let check = builder
            .is_complete(&column, threshold)
            .build()
            .map_err(|e| Error::from_reason(e.to_string()))?;

        Ok(Check {
            inner: Arc::new(check),
        })
    }

    #[napi]
    pub fn has_min(&mut self, column: String, min_value: f64) -> Result<Check> {
        let mut builder = CoreCheck::builder(&self.name).level(self.level.clone());

        if let Some(desc) = &self.description {
            builder = builder.description(desc);
        }

        let check = builder
            .has_min(&column, min_value)
            .build()
            .map_err(|e| Error::from_reason(e.to_string()))?;

        Ok(Check {
            inner: Arc::new(check),
        })
    }

    #[napi]
    pub fn has_max(&mut self, column: String, max_value: f64) -> Result<Check> {
        let mut builder = CoreCheck::builder(&self.name).level(self.level.clone());

        if let Some(desc) = &self.description {
            builder = builder.description(desc);
        }

        let check = builder
            .has_max(&column, max_value)
            .build()
            .map_err(|e| Error::from_reason(e.to_string()))?;

        Ok(Check {
            inner: Arc::new(check),
        })
    }

    #[napi]
    pub fn is_unique(&mut self, column: String) -> Result<Check> {
        let mut builder = CoreCheck::builder(&self.name).level(self.level.clone());

        if let Some(desc) = &self.description {
            builder = builder.description(desc);
        }

        let check = builder
            .is_unique(&column)
            .build()
            .map_err(|e| Error::from_reason(e.to_string()))?;

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
        let mut builder = CoreCheck::builder(&self.name).level(self.level.clone());

        if let Some(desc) = &self.description {
            builder = builder.description(desc);
        }

        let tol = tolerance.unwrap_or(0.01);
        let check = builder
            .has_mean(&column, expected, tol)
            .build()
            .map_err(|e| Error::from_reason(e.to_string()))?;

        Ok(Check {
            inner: Arc::new(check),
        })
    }

    #[napi]
    pub fn build(&mut self) -> Result<Check> {
        // Generic build for simple checks
        let mut builder = CoreCheck::builder(&self.name).level(self.level.clone());

        if let Some(desc) = &self.description {
            builder = builder.description(desc);
        }

        // Default to a simple row count check
        let check = builder
            .has_size(|size| size > 0)
            .build()
            .map_err(|e| Error::from_reason(e.to_string()))?;

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
