use datafusion::prelude::*;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::path::Path;
use std::sync::Arc;

#[napi]
pub struct DataSource {
    context: Arc<SessionContext>,
}

impl DataSource {
    pub(crate) fn context(&self) -> &Arc<SessionContext> {
        &self.context
    }
}

#[napi]
impl DataSource {
    #[napi(factory)]
    pub async fn from_csv(path: String) -> Result<DataSource> {
        let ctx = SessionContext::new();
        ctx.register_csv("data", &path, CsvReadOptions::default())
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;

        Ok(DataSource {
            context: Arc::new(ctx),
        })
    }

    #[napi(factory)]
    pub async fn from_parquet(path: String) -> Result<DataSource> {
        let ctx = SessionContext::new();
        ctx.register_parquet("data", &path, Default::default())
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;

        Ok(DataSource {
            context: Arc::new(ctx),
        })
    }

    #[napi(factory)]
    pub async fn from_json(path: String) -> Result<DataSource> {
        let ctx = SessionContext::new();
        ctx.register_json("data", &path, Default::default())
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;

        Ok(DataSource {
            context: Arc::new(ctx),
        })
    }

    #[napi]
    pub async fn sql(&self, query: String) -> Result<Vec<String>> {
        let df = self
            .context
            .sql(&query)
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;

        let records = df
            .collect()
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;

        let mut results = Vec::new();
        for batch in records {
            // Convert batch to JSON string representation
            // For now, just return the number of rows as a simple response
            let row_count = batch.num_rows();
            results.push(format!("{{\"rows\": {}}}", row_count));
        }

        Ok(results)
    }
}

#[napi]
pub struct DataSourceBuilder {
    path: Option<String>,
    format: Option<String>,
}

#[napi]
impl DataSourceBuilder {
    #[napi(factory)]
    pub fn new() -> Self {
        Self {
            path: None,
            format: None,
        }
    }

    #[napi]
    pub fn path(&mut self, path: String) -> &Self {
        self.path = Some(path);
        self
    }

    #[napi]
    pub fn format(&mut self, format: String) -> &Self {
        self.format = Some(format);
        self
    }

    #[napi]
    pub async fn build(&self) -> Result<DataSource> {
        let path = self
            .path
            .clone()
            .ok_or_else(|| Error::from_reason("Path is required"))?;

        let format = self.format.as_deref().unwrap_or_else(|| {
            Path::new(&path)
                .extension()
                .and_then(|ext| ext.to_str())
                .unwrap_or("csv")
        });

        match format {
            "csv" => DataSource::from_csv(path).await,
            "parquet" => DataSource::from_parquet(path).await,
            "json" => DataSource::from_json(path).await,
            _ => Err(Error::from_reason(format!(
                "Unsupported format: {}",
                format
            ))),
        }
    }
}
