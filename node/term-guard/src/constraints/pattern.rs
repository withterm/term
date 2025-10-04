use napi::bindgen_prelude::*;
use napi_derive::napi;

#[napi]
pub enum FormatType {
    Email,
    SSN,
    Phone,
    UUID,
    URL,
    IPv4,
    IPv6,
    CreditCard,
    ZipCode,
}

impl FormatType {
    fn to_pattern(&self) -> &str {
        match self {
            FormatType::Email => r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
            FormatType::SSN => r"^\d{3}-\d{2}-\d{4}$",
            FormatType::Phone => r"^\+?[1-9]\d{1,14}$",
            FormatType::UUID => r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            FormatType::URL => r"^https?://[^\s/$.?#].[^\s]*$",
            FormatType::IPv4 => r"^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$",
            FormatType::IPv6 => r"^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$",
            FormatType::CreditCard => r"^\d{13,19}$",
            FormatType::ZipCode => r"^\d{5}(-\d{4})?$",
        }
    }
}

#[napi(object)]
pub struct PatternOptions {
    pub threshold: f64,
    pub case_sensitive: bool,
}

#[napi]
impl PatternOptions {
    #[napi(factory)]
    pub fn default() -> PatternOptions {
        PatternOptions {
            threshold: 1.0,
            case_sensitive: true,
        }
    }

    #[napi(factory)]
    pub fn with_threshold(threshold: f64) -> PatternOptions {
        PatternOptions {
            threshold,
            case_sensitive: true,
        }
    }

    #[napi(factory)]
    pub fn case_insensitive(threshold: f64) -> PatternOptions {
        PatternOptions {
            threshold,
            case_sensitive: false,
        }
    }
}

/// Extension methods for CheckBuilder to add pattern constraints
impl crate::check::CheckBuilder {
    /// Add a regex pattern constraint
    pub fn pattern(
        &mut self,
        column: String,
        pattern: String,
        options: Option<PatternOptions>,
    ) -> Result<crate::check::Check> {
        let mut builder = term_guard::core::Check::builder(&self.name).level(self.level.clone());

        if let Some(desc) = &self.description {
            builder = builder.description(desc);
        }

        let threshold = options.as_ref().map(|o| o.threshold).unwrap_or(1.0);
        let check = builder
            .matches_pattern(&column, &pattern, threshold)
            .build()
            .map_err(|e| Error::from_reason(e.to_string()))?;

        Ok(crate::check::Check::from_inner(std::sync::Arc::new(check)))
    }

    /// Add a format validation constraint
    pub fn format(
        &mut self,
        column: String,
        format_type: FormatType,
        threshold: Option<f64>,
    ) -> Result<crate::check::Check> {
        let pattern = format_type.to_pattern().to_string();
        let options = threshold.map(|t| PatternOptions::with_threshold(t));
        self.pattern(column, pattern, options)
    }

    /// Add a contains string constraint
    pub fn contains(
        &mut self,
        column: String,
        substring: String,
        threshold: Option<f64>,
    ) -> Result<crate::check::Check> {
        let mut builder = term_guard::core::Check::builder(&self.name).level(self.level.clone());

        if let Some(desc) = &self.description {
            builder = builder.description(desc);
        }

        let threshold = threshold.unwrap_or(1.0);
        let check = builder
            .contains_string(&column, &substring, threshold)
            .build()
            .map_err(|e| Error::from_reason(e.to_string()))?;

        Ok(crate::check::Check::from_inner(std::sync::Arc::new(check)))
    }

    /// Add a starts with constraint
    pub fn starts_with(
        &mut self,
        column: String,
        prefix: String,
        threshold: Option<f64>,
    ) -> Result<crate::check::Check> {
        let pattern = format!("^{}", regex::escape(&prefix));
        let options = threshold.map(|t| PatternOptions::with_threshold(t));
        self.pattern(column, pattern, options)
    }

    /// Add an ends with constraint
    pub fn ends_with(
        &mut self,
        column: String,
        suffix: String,
        threshold: Option<f64>,
    ) -> Result<crate::check::Check> {
        let pattern = format!("{}$", regex::escape(&suffix));
        let options = threshold.map(|t| PatternOptions::with_threshold(t));
        self.pattern(column, pattern, options)
    }

    /// Add a string length constraint
    pub fn string_length(
        &mut self,
        column: String,
        min: Option<i32>,
        max: Option<i32>,
    ) -> Result<crate::check::Check> {
        let mut builder = term_guard::core::Check::builder(&self.name).level(self.level.clone());

        if let Some(desc) = &self.description {
            builder = builder.description(desc);
        }

        // Create a pattern based on min/max length
        let pattern = match (min, max) {
            (Some(min), Some(max)) => format!("^.{{{},{}}}$", min, max),
            (Some(min), None) => format!("^.{{{}，}}$", min),
            (None, Some(max)) => format!("^.{{0,{}}}$", max),
            (None, None) => return Err(Error::from_reason("Must specify min or max length")),
        };

        let check = builder
            .matches_pattern(&column, &pattern, 1.0)
            .build()
            .map_err(|e| Error::from_reason(e.to_string()))?;

        Ok(crate::check::Check::from_inner(std::sync::Arc::new(check)))
    }
}
