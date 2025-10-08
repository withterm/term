use datafusion::prelude::*;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::sync::Arc;

#[napi(object)]
pub struct CsvOptions {
    pub delimiter: Option<String>,
    pub has_headers: Option<bool>,
    pub skip_rows: Option<i32>,
    pub quote_char: Option<String>,
    pub escape_char: Option<String>,
    pub encoding: Option<String>,
    pub max_records: Option<i32>,
    pub null_value: Option<String>,
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            delimiter: Some(",".to_string()),
            has_headers: Some(true),
            skip_rows: Some(0),
            quote_char: Some("\"".to_string()),
            escape_char: None,
            encoding: Some("utf8".to_string()),
            max_records: None,
            null_value: None,
        }
    }
}

#[napi(object)]
pub struct JsonOptions {
    pub lines: Option<bool>,
    pub max_records: Option<i32>,
    pub schema_inference_sample_size: Option<i32>,
}

impl Default for JsonOptions {
    fn default() -> Self {
        Self {
            lines: Some(true),
            max_records: None,
            schema_inference_sample_size: Some(1000),
        }
    }
}

#[napi(object)]
pub struct ParquetOptions {
    pub enable_statistics: Option<bool>,
    pub enable_page_index: Option<bool>,
    pub batch_size: Option<i32>,
    pub row_group_size: Option<i32>,
}

impl Default for ParquetOptions {
    fn default() -> Self {
        Self {
            enable_statistics: Some(true),
            enable_page_index: Some(true),
            batch_size: Some(1024),
            row_group_size: None,
        }
    }
}

#[napi]
pub struct FileDataSource {
    context: Arc<SessionContext>,
    table_name: String,
}

impl FileDataSource {
    pub(crate) fn context(&self) -> &Arc<SessionContext> {
        &self.context
    }

    pub(crate) fn table_name(&self) -> &str {
        &self.table_name
    }
}

#[napi]
impl FileDataSource {
    #[napi(factory)]
    pub async fn from_csv(path: String, options: Option<CsvOptions>) -> Result<FileDataSource> {
        let opts = options.unwrap_or_default();
        let ctx = SessionContext::new();

        let mut csv_opts = CsvReadOptions::default().has_header(opts.has_headers.unwrap_or(true));

        if let Some(delimiter) = opts.delimiter {
            if let Some(first_char) = delimiter.chars().next() {
                csv_opts = csv_opts.delimiter(first_char as u8);
            }
        }

        if let Some(skip) = opts.skip_rows {
            csv_opts = csv_opts.skip_rows(skip as usize);
        }

        if let Some(quote) = opts.quote_char {
            if let Some(first_char) = quote.chars().next() {
                csv_opts = csv_opts.quote(first_char as u8);
            }
        }

        if let Some(escape) = opts.escape_char {
            if let Some(first_char) = escape.chars().next() {
                csv_opts = csv_opts.escape(Some(first_char as u8));
            }
        }

        ctx.register_csv("data", &path, csv_opts)
            .await
            .map_err(|e| Error::from_reason(format!("Failed to register CSV: {}", e)))?;

        Ok(FileDataSource {
            context: Arc::new(ctx),
            table_name: "data".to_string(),
        })
    }

    #[napi(factory)]
    pub async fn from_json(path: String, options: Option<JsonOptions>) -> Result<FileDataSource> {
        let opts = options.unwrap_or_default();
        let ctx = SessionContext::new();

        let mut json_opts = NdJsonReadOptions::default()
            .schema_infer_max_records(opts.schema_inference_sample_size.unwrap_or(1000) as usize);

        if let Some(max_records) = opts.max_records {
            json_opts = json_opts.file_batch_size(max_records as usize);
        }

        ctx.register_json("data", &path, json_opts)
            .await
            .map_err(|e| Error::from_reason(format!("Failed to register JSON: {}", e)))?;

        Ok(FileDataSource {
            context: Arc::new(ctx),
            table_name: "data".to_string(),
        })
    }

    #[napi(factory)]
    pub async fn from_parquet(
        path: String,
        options: Option<ParquetOptions>,
    ) -> Result<FileDataSource> {
        let opts = options.unwrap_or_default();
        let ctx = SessionContext::new();

        let mut parquet_opts = ParquetReadOptions::default();

        if let Some(enable_stats) = opts.enable_statistics {
            parquet_opts = parquet_opts.parquet_pruning(enable_stats);
        }

        if let Some(enable_page_index) = opts.enable_page_index {
            parquet_opts = parquet_opts.parquet_page_filter(enable_page_index);
        }

        ctx.register_parquet("data", &path, parquet_opts)
            .await
            .map_err(|e| Error::from_reason(format!("Failed to register Parquet: {}", e)))?;

        Ok(FileDataSource {
            context: Arc::new(ctx),
            table_name: "data".to_string(),
        })
    }

    #[napi]
    pub async fn get_schema(&self) -> Result<Vec<String>> {
        let df = self
            .context
            .table("data")
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
            .sql("SELECT COUNT(*) FROM data")
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
}
