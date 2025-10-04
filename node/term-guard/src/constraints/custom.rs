use napi::bindgen_prelude::*;
use napi_derive::napi;

#[napi(object)]
pub struct CustomConstraintOptions {
    pub threshold: Option<f64>,
    pub message: Option<String>,
}

#[napi]
impl CustomConstraintOptions {
    #[napi(factory)]
    pub fn default() -> CustomConstraintOptions {
        CustomConstraintOptions {
            threshold: None,
            message: None,
        }
    }

    #[napi(factory)]
    pub fn with_message(message: String) -> CustomConstraintOptions {
        CustomConstraintOptions {
            threshold: None,
            message: Some(message),
        }
    }

    #[napi(factory)]
    pub fn with_threshold_and_message(threshold: f64, message: String) -> CustomConstraintOptions {
        CustomConstraintOptions {
            threshold: Some(threshold),
            message: Some(message),
        }
    }
}

/// Extension methods for CheckBuilder to add custom constraints
impl crate::check::CheckBuilder {
    /// Add a custom SQL expression constraint
    pub fn sql_expression(
        &mut self,
        expression: String,
        options: Option<CustomConstraintOptions>,
    ) -> Result<crate::check::Check> {
        let mut builder = term_guard::core::Check::builder(&self.name).level(self.level.clone());

        if let Some(desc) = &self.description {
            builder = builder.description(desc);
        }

        let threshold = options.as_ref().and_then(|o| o.threshold).unwrap_or(1.0);

        let check = builder
            .satisfies(&expression, threshold)
            .build()
            .map_err(|e| Error::from_reason(e.to_string()))?;

        Ok(crate::check::Check::from_inner(std::sync::Arc::new(check)))
    }

    /// Add a custom assertion constraint with a predicate
    pub fn custom_assertion<F>(
        &mut self,
        name: String,
        predicate: F,
        options: Option<CustomConstraintOptions>,
    ) -> Result<crate::check::Check>
    where
        F: Fn(usize) -> bool + Send + Sync + 'static,
    {
        let mut builder = term_guard::core::Check::builder(&self.name).level(self.level.clone());

        if let Some(desc) = &self.description {
            builder = builder.description(desc);
        } else if let Some(opts) = &options {
            if let Some(msg) = &opts.message {
                builder = builder.description(msg);
            }
        }

        let check = builder
            .has_size(predicate)
            .build()
            .map_err(|e| Error::from_reason(e.to_string()))?;

        Ok(crate::check::Check::from_inner(std::sync::Arc::new(check)))
    }

    /// Add a between values constraint
    pub fn between(
        &mut self,
        column: String,
        lower: f64,
        upper: f64,
        inclusive: bool,
    ) -> Result<crate::check::Check> {
        let expression = if inclusive {
            format!("{} >= {} AND {} <= {}", column, lower, column, upper)
        } else {
            format!("{} > {} AND {} < {}", column, lower, column, upper)
        };

        self.sql_expression(expression, None)
    }

    /// Add a constraint to check if values are in a set
    pub fn is_in(&mut self, column: String, values: Vec<String>) -> Result<crate::check::Check> {
        let values_str = values
            .iter()
            .map(|v| format!("'{}'", v.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(", ");

        let expression = format!("{} IN ({})", column, values_str);
        self.sql_expression(expression, None)
    }

    /// Add a constraint to check if values are NOT in a set
    pub fn is_not_in(
        &mut self,
        column: String,
        values: Vec<String>,
    ) -> Result<crate::check::Check> {
        let values_str = values
            .iter()
            .map(|v| format!("'{}'", v.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(", ");

        let expression = format!("{} NOT IN ({})", column, values_str);
        self.sql_expression(expression, None)
    }

    /// Add a constraint to check column correlation
    pub fn correlation(
        &mut self,
        column1: String,
        column2: String,
        min_correlation: f64,
    ) -> Result<crate::check::Check> {
        let mut builder = term_guard::core::Check::builder(&self.name).level(self.level.clone());

        if let Some(desc) = &self.description {
            builder = builder.description(desc);
        }

        let check = builder
            .has_correlation(&column1, &column2, min_correlation)
            .build()
            .map_err(|e| Error::from_reason(e.to_string()))?;

        Ok(crate::check::Check::from_inner(std::sync::Arc::new(check)))
    }
}
