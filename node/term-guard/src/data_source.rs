use datafusion::prelude::*;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::sync::Arc;
use tokio::sync::Mutex;

#[napi]
pub struct DataSource {
    ctx: Arc<Mutex<SessionContext>>,
    table_name: String,
}

#[napi]
impl DataSource {
    #[napi(factory)]
    pub async fn from_parquet(path: String) -> Result<DataSource> {
        let ctx = SessionContext::new();

        // Register the parquet file as a table
        ctx.register_parquet("data", &path, ParquetReadOptions::default())
            .await
            .map_err(|e| Error::from_reason(format!("Failed to read parquet file: {}", e)))?;

        Ok(DataSource {
            ctx: Arc::new(Mutex::new(ctx)),
            table_name: "data".to_string(),
        })
    }

    #[napi(factory)]
    pub async fn from_csv(path: String) -> Result<DataSource> {
        let ctx = SessionContext::new();

        // Register the CSV file as a table
        ctx.register_csv("data", &path, CsvReadOptions::default())
            .await
            .map_err(|e| Error::from_reason(format!("Failed to read CSV file: {}", e)))?;

        Ok(DataSource {
            ctx: Arc::new(Mutex::new(ctx)),
            table_name: "data".to_string(),
        })
    }

    #[napi(factory)]
    pub async fn from_json(path: String) -> Result<DataSource> {
        let ctx = SessionContext::new();

        // Register the JSON file as a table
        ctx.register_json("data", &path, NdJsonReadOptions::default())
            .await
            .map_err(|e| Error::from_reason(format!("Failed to read JSON file: {}", e)))?;

        Ok(DataSource {
            ctx: Arc::new(Mutex::new(ctx)),
            table_name: "data".to_string(),
        })
    }

    #[napi]
    pub async fn get_row_count(&self) -> Result<i64> {
        let ctx = self.ctx.lock().await;
        let df = ctx
            .sql("SELECT COUNT(*) as count FROM data")
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;

        if let Some(batch) = batches.first() {
            if let Some(col) = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
            {
                if let Some(count) = col.value(0).try_into().ok() {
                    return Ok(count);
                }
            }
        }

        Ok(0)
    }

    #[napi]
    pub async fn get_column_names(&self) -> Result<Vec<String>> {
        let ctx = self.ctx.lock().await;
        let df = ctx
            .table("data")
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;

        let schema = df.schema();
        let fields = schema.fields();

        Ok(fields.iter().map(|f| f.name().clone()).collect())
    }

    #[napi(getter)]
    pub fn table_name(&self) -> String {
        self.table_name.clone()
    }

    pub(crate) async fn get_context(&self) -> Result<SessionContext> {
        Ok(self.ctx.lock().await.clone())
    }
}

#[napi]
pub struct DataSourceBuilder {
    ctx: SessionContext,
}

#[napi]
impl DataSourceBuilder {
    #[napi(constructor)]
    pub fn new() -> Self {
        DataSourceBuilder {
            ctx: SessionContext::new(),
        }
    }

    #[napi]
    pub async unsafe fn register_parquet(&mut self, name: String, path: String) -> Result<()> {
        self.ctx
            .register_parquet(&name, &path, ParquetReadOptions::default())
            .await
            .map_err(|e| Error::from_reason(format!("Failed to register parquet: {}", e)))?;
        Ok(())
    }

    #[napi]
    pub async unsafe fn register_csv(&mut self, name: String, path: String) -> Result<()> {
        self.ctx
            .register_csv(&name, &path, CsvReadOptions::default())
            .await
            .map_err(|e| Error::from_reason(format!("Failed to register CSV: {}", e)))?;
        Ok(())
    }

    #[napi]
    pub fn build(&self) -> Result<DataSource> {
        Ok(DataSource {
            ctx: Arc::new(Mutex::new(self.ctx.clone())),
            table_name: "data".to_string(),
        })
    }
}
