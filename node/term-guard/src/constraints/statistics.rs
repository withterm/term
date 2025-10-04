use napi::bindgen_prelude::*;
use napi_derive::napi;

#[napi(object)]
pub struct StatisticalOptions {
    pub tolerance: f64,
}

#[napi]
impl StatisticalOptions {
    #[napi(factory)]
    pub fn exact() -> StatisticalOptions {
        StatisticalOptions { tolerance: 0.0 }
    }

    #[napi(factory)]
    pub fn with_tolerance(tolerance: f64) -> StatisticalOptions {
        StatisticalOptions { tolerance }
    }
}

#[napi(object)]
pub struct HistogramBin {
    pub lower: f64,
    pub upper: f64,
    pub count: i64,
}

/// Extension methods for CheckBuilder to add statistical constraints
impl crate::check::CheckBuilder {
    /// Add a minimum value constraint
    pub fn min_value(&mut self, column: String, min: f64) -> Result<crate::check::Check> {
        let mut builder = term_guard::core::Check::builder(&self.name).level(self.level.clone());

        if let Some(desc) = &self.description {
            builder = builder.description(desc);
        }

        let check = builder
            .has_min(&column, min)
            .build()
            .map_err(|e| Error::from_reason(e.to_string()))?;

        Ok(crate::check::Check::from_inner(std::sync::Arc::new(check)))
    }

    /// Add a maximum value constraint
    pub fn max_value(&mut self, column: String, max: f64) -> Result<crate::check::Check> {
        let mut builder = term_guard::core::Check::builder(&self.name).level(self.level.clone());

        if let Some(desc) = &self.description {
            builder = builder.description(desc);
        }

        let check = builder
            .has_max(&column, max)
            .build()
            .map_err(|e| Error::from_reason(e.to_string()))?;

        Ok(crate::check::Check::from_inner(std::sync::Arc::new(check)))
    }

    /// Add a mean value constraint
    pub fn mean_value(
        &mut self,
        column: String,
        expected: f64,
        options: Option<StatisticalOptions>,
    ) -> Result<crate::check::Check> {
        let mut builder = term_guard::core::Check::builder(&self.name).level(self.level.clone());

        if let Some(desc) = &self.description {
            builder = builder.description(desc);
        }

        let tolerance = options.map(|o| o.tolerance).unwrap_or(0.01);
        let check = builder
            .has_mean(&column, expected, tolerance)
            .build()
            .map_err(|e| Error::from_reason(e.to_string()))?;

        Ok(crate::check::Check::from_inner(std::sync::Arc::new(check)))
    }

    /// Add a sum value constraint
    pub fn sum_value(
        &mut self,
        column: String,
        expected: f64,
        options: Option<StatisticalOptions>,
    ) -> Result<crate::check::Check> {
        let mut builder = term_guard::core::Check::builder(&self.name).level(self.level.clone());

        if let Some(desc) = &self.description {
            builder = builder.description(desc);
        }

        let tolerance = options.map(|o| o.tolerance).unwrap_or(0.01);
        let check = builder
            .has_sum(&column, expected, tolerance)
            .build()
            .map_err(|e| Error::from_reason(e.to_string()))?;

        Ok(crate::check::Check::from_inner(std::sync::Arc::new(check)))
    }

    /// Add a standard deviation constraint
    pub fn standard_deviation(
        &mut self,
        column: String,
        expected: f64,
        options: Option<StatisticalOptions>,
    ) -> Result<crate::check::Check> {
        let mut builder = term_guard::core::Check::builder(&self.name).level(self.level.clone());

        if let Some(desc) = &self.description {
            builder = builder.description(desc);
        }

        let tolerance = options.map(|o| o.tolerance).unwrap_or(0.01);
        let check = builder
            .has_std_dev(&column, expected, tolerance)
            .build()
            .map_err(|e| Error::from_reason(e.to_string()))?;

        Ok(crate::check::Check::from_inner(std::sync::Arc::new(check)))
    }

    /// Add a quantile/percentile constraint
    pub fn quantile(
        &mut self,
        column: String,
        quantile: f64,
        expected: f64,
        options: Option<StatisticalOptions>,
    ) -> Result<crate::check::Check> {
        let mut builder = term_guard::core::Check::builder(&self.name).level(self.level.clone());

        if let Some(desc) = &self.description {
            builder = builder.description(desc);
        }

        let tolerance = options.map(|o| o.tolerance).unwrap_or(0.01);
        let check = builder
            .has_quantile(&column, quantile, expected, tolerance)
            .build()
            .map_err(|e| Error::from_reason(e.to_string()))?;

        Ok(crate::check::Check::from_inner(std::sync::Arc::new(check)))
    }

    /// Add a median (50th percentile) constraint
    pub fn median(
        &mut self,
        column: String,
        expected: f64,
        options: Option<StatisticalOptions>,
    ) -> Result<crate::check::Check> {
        self.quantile(column, 0.5, expected, options)
    }
}
