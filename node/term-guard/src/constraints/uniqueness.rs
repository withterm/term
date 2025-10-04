use napi::bindgen_prelude::*;
use napi_derive::napi;

#[napi(object)]
pub struct UniquenessOptions {
    pub approximate: bool,
    pub threshold: Option<f64>,
}

#[napi]
impl UniquenessOptions {
    #[napi(factory)]
    pub fn exact() -> UniquenessOptions {
        UniquenessOptions {
            approximate: false,
            threshold: None,
        }
    }

    #[napi(factory)]
    pub fn approximate(threshold: f64) -> UniquenessOptions {
        UniquenessOptions {
            approximate: true,
            threshold: Some(threshold),
        }
    }
}

/// Extension methods for CheckBuilder to add uniqueness constraints
impl crate::check::CheckBuilder {
    /// Add a uniqueness constraint for a single column
    pub fn uniqueness(
        &mut self,
        column: String,
        options: Option<UniquenessOptions>,
    ) -> Result<crate::check::Check> {
        let mut builder = term_guard::core::Check::builder(&self.name).level(self.level.clone());

        if let Some(desc) = &self.description {
            builder = builder.description(desc);
        }

        let check = if let Some(opts) = options {
            if opts.approximate && opts.threshold.is_some() {
                // For approximate uniqueness, we check that the ratio of distinct values is above threshold
                builder
                    .has_uniqueness(&column, opts.threshold.unwrap())
                    .build()
                    .map_err(|e| Error::from_reason(e.to_string()))?
            } else {
                // Exact uniqueness
                builder
                    .is_unique(&column)
                    .build()
                    .map_err(|e| Error::from_reason(e.to_string()))?
            }
        } else {
            // Default to exact uniqueness
            builder
                .is_unique(&column)
                .build()
                .map_err(|e| Error::from_reason(e.to_string()))?
        };

        Ok(crate::check::Check::from_inner(std::sync::Arc::new(check)))
    }

    /// Add a multi-column uniqueness constraint (composite key)
    pub fn multi_column_uniqueness(&mut self, columns: Vec<String>) -> Result<crate::check::Check> {
        let mut builder = term_guard::core::Check::builder(&self.name).level(self.level.clone());

        if let Some(desc) = &self.description {
            builder = builder.description(desc);
        }

        // For multi-column uniqueness, concatenate the columns
        let composite_expr = columns
            .iter()
            .map(|c| format!("CAST({} AS VARCHAR)", c))
            .collect::<Vec<_>>()
            .join(" || '_' || ");

        let check = builder
            .is_unique(&composite_expr)
            .build()
            .map_err(|e| Error::from_reason(e.to_string()))?;

        Ok(crate::check::Check::from_inner(std::sync::Arc::new(check)))
    }
}
