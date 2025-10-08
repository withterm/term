pub mod datafusion;
pub mod file;
pub mod memory;

#[cfg(feature = "cloud-storage")]
pub mod cloud;

use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::sync::Arc;

#[napi]
pub enum DataSourceType {
    Csv,
    Json,
    Parquet,
    Memory,
    #[cfg(feature = "cloud-storage")]
    S3,
    #[cfg(feature = "cloud-storage")]
    Azure,
    #[cfg(feature = "cloud-storage")]
    Gcs,
}

#[napi]
pub struct UnifiedDataSource {
    context: Arc<datafusion::prelude::SessionContext>,
    table_name: String,
    source_type: String,
}

impl UnifiedDataSource {
    pub(crate) fn context(&self) -> &Arc<datafusion::prelude::SessionContext> {
        &self.context
    }

    pub(crate) fn table_name(&self) -> &str {
        &self.table_name
    }
}

#[napi]
impl UnifiedDataSource {
    #[napi(factory)]
    pub async fn from_file(path: String, format: Option<String>) -> Result<UnifiedDataSource> {
        let file_format =
            format.unwrap_or_else(|| path.split('.').last().unwrap_or("csv").to_lowercase());

        let data_source = match file_format.as_str() {
            "csv" => file::FileDataSource::from_csv(path, None).await?,
            "json" => file::FileDataSource::from_json(path, None).await?,
            "parquet" => file::FileDataSource::from_parquet(path, None).await?,
            _ => {
                return Err(Error::from_reason(format!(
                    "Unsupported file format: {}",
                    file_format
                )));
            }
        };

        Ok(UnifiedDataSource {
            context: data_source.context().clone(),
            table_name: data_source.table_name().to_string(),
            source_type: file_format,
        })
    }

    #[napi(factory)]
    pub async fn from_memory(json_data: String) -> Result<UnifiedDataSource> {
        let data_source = memory::MemoryDataSource::from_json_array(json_data).await?;

        Ok(UnifiedDataSource {
            context: data_source.context().clone(),
            table_name: data_source.table_name().to_string(),
            source_type: "memory".to_string(),
        })
    }

    #[napi]
    pub async fn sql(&self, query: String) -> Result<String> {
        let df = self
            .context
            .sql(&query)
            .await
            .map_err(|e| Error::from_reason(format!("SQL execution failed: {}", e)))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| Error::from_reason(format!("Failed to collect results: {}", e)))?;

        let mut results = Vec::new();
        for batch in batches {
            let json = arrow_json::writer::record_batches_to_json_rows(&[&batch])
                .map_err(|e| Error::from_reason(format!("Failed to convert to JSON: {}", e)))?;
            results.extend(json);
        }

        serde_json::to_string(&results)
            .map_err(|e| Error::from_reason(format!("Failed to serialize results: {}", e)))
    }

    #[napi]
    pub async fn get_schema(&self) -> Result<Vec<String>> {
        let df = self
            .context
            .table(&self.table_name)
            .await
            .map_err(|e| Error::from_reason(format!("Failed to get table: {}", e)))?;

        let schema = df.schema();
        let mut schema_info = Vec::new();

        for field in schema.fields() {
            schema_info.push(format!("{}: {}", field.name(), field.data_type()));
        }

        Ok(schema_info)
    }

    #[napi]
    pub async fn count_rows(&self) -> Result<i64> {
        let df = self
            .context
            .sql(&format!("SELECT COUNT(*) FROM {}", self.table_name))
            .await
            .map_err(|e| Error::from_reason(format!("Failed to execute count query: {}", e)))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| Error::from_reason(format!("Failed to collect results: {}", e)))?;

        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Ok(0);
        }

        let count_array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| Error::from_reason("Failed to extract count"))?;

        Ok(count_array.value(0))
    }

    #[napi]
    pub fn get_source_type(&self) -> String {
        self.source_type.clone()
    }
}
