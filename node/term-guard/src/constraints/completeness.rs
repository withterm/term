use napi::bindgen_prelude::*;
use napi_derive::napi;

#[napi(object)]
pub struct CompletenessOptions {
    pub threshold: f64,
    pub include_nulls: bool,
}

#[napi]
impl CompletenessOptions {
    #[napi(factory)]
    pub fn full() -> CompletenessOptions {
        CompletenessOptions {
            threshold: 1.0,
            include_nulls: false,
        }
    }

    #[napi(factory)]
    pub fn threshold(value: f64) -> CompletenessOptions {
        CompletenessOptions {
            threshold: value,
            include_nulls: false,
        }
    }

    #[napi(factory)]
    pub fn with_nulls(threshold: f64) -> CompletenessOptions {
        CompletenessOptions {
            threshold,
            include_nulls: true,
        }
    }
}

/// Extension methods for CheckBuilder to add completeness constraints
impl crate::check::CheckBuilder {
    /// Add a completeness constraint for a column
    pub fn completeness_with_options(
        &mut self,
        column: String,
        options: CompletenessOptions,
    ) -> Result<crate::check::Check> {
        let mut builder = term_guard::core::Check::builder(&self.name).level(self.level.clone());

        if let Some(desc) = &self.description {
            builder = builder.description(desc);
        }

        let check = builder
            .is_complete(&column, options.threshold)
            .build()
            .map_err(|e| Error::from_reason(e.to_string()))?;

        Ok(crate::check::Check::from_inner(std::sync::Arc::new(check)))
    }

    /// Add a multi-column completeness constraint
    pub fn multi_column_completeness(
        &mut self,
        columns: Vec<String>,
        threshold: f64,
    ) -> Result<crate::check::Check> {
        let mut builder = term_guard::core::Check::builder(&self.name).level(self.level.clone());

        if let Some(desc) = &self.description {
            builder = builder.description(desc);
        }

        // For multi-column, we check if ANY column is complete
        // This is a simplified implementation - in production you might want different logic
        let column_expr = columns.join(", ");
        let check = builder
            .is_complete(&column_expr, threshold)
            .build()
            .map_err(|e| Error::from_reason(e.to_string()))?;

        Ok(crate::check::Check::from_inner(std::sync::Arc::new(check)))
    }
}
