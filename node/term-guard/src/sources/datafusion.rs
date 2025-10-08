use arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::prelude::*;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::sync::Arc;

#[napi]
pub struct DataFusionContext {
    context: Arc<SessionContext>,
    table_names: Vec<String>,
}

impl DataFusionContext {
    pub(crate) fn context(&self) -> &Arc<SessionContext> {
        &self.context
    }
}

#[napi]
impl DataFusionContext {
    #[napi(constructor)]
    pub fn new() -> Self {
        Self {
            context: Arc::new(SessionContext::new()),
            table_names: Vec::new(),
        }
    }

    #[napi]
    pub async fn register_csv(
        &mut self,
        table_name: String,
        path: String,
        has_header: Option<bool>,
    ) -> Result<()> {
        let csv_options = CsvReadOptions::default().has_header(has_header.unwrap_or(true));

        self.context
            .register_csv(&table_name, &path, csv_options)
            .await
            .map_err(|e| Error::from_reason(format!("Failed to register CSV: {}", e)))?;

        if !self.table_names.contains(&table_name) {
            self.table_names.push(table_name);
        }

        Ok(())
    }

    #[napi]
    pub async fn register_parquet(&mut self, table_name: String, path: String) -> Result<()> {
        self.context
            .register_parquet(&table_name, &path, Default::default())
            .await
            .map_err(|e| Error::from_reason(format!("Failed to register Parquet: {}", e)))?;

        if !self.table_names.contains(&table_name) {
            self.table_names.push(table_name);
        }

        Ok(())
    }

    #[napi]
    pub async fn register_json(&mut self, table_name: String, path: String) -> Result<()> {
        self.context
            .register_json(&table_name, &path, Default::default())
            .await
            .map_err(|e| Error::from_reason(format!("Failed to register JSON: {}", e)))?;

        if !self.table_names.contains(&table_name) {
            self.table_names.push(table_name);
        }

        Ok(())
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
    pub async fn execute(&self, query: String) -> Result<Vec<String>> {
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
            results.push(format!(
                "Batch: {} rows, {} columns",
                batch.num_rows(),
                batch.num_columns()
            ));
        }

        Ok(results)
    }

    #[napi]
    pub fn list_tables(&self) -> Vec<String> {
        self.table_names.clone()
    }

    #[napi]
    pub async fn get_table_schema(&self, table_name: String) -> Result<Vec<String>> {
        let df =
            self.context.table(&table_name).await.map_err(|e| {
                Error::from_reason(format!("Table '{}' not found: {}", table_name, e))
            })?;

        let schema = df.schema();
        let mut schema_info = Vec::new();

        for field in schema.fields() {
            schema_info.push(format!("{}: {}", field.name(), field.data_type()));
        }

        Ok(schema_info)
    }

    #[napi]
    pub async fn create_view(&mut self, view_name: String, sql: String) -> Result<()> {
        let df =
            self.context.sql(&sql).await.map_err(|e| {
                Error::from_reason(format!("Failed to execute SQL for view: {}", e))
            })?;

        self.context
            .register_table(&view_name, df.into_view())
            .map_err(|e| Error::from_reason(format!("Failed to create view: {}", e)))?;

        if !self.table_names.contains(&view_name) {
            self.table_names.push(view_name);
        }

        Ok(())
    }

    #[napi]
    pub async fn drop_table(&mut self, table_name: String) -> Result<()> {
        self.context
            .deregister_table(&table_name)
            .map_err(|e| Error::from_reason(format!("Failed to drop table: {}", e)))?;

        self.table_names.retain(|name| name != &table_name);
        Ok(())
    }

    #[napi]
    pub async fn union_tables(
        &mut self,
        result_table: String,
        table1: String,
        table2: String,
    ) -> Result<()> {
        let sql = format!(
            "CREATE VIEW {} AS SELECT * FROM {} UNION ALL SELECT * FROM {}",
            result_table, table1, table2
        );

        let df = self
            .context
            .sql(&sql)
            .await
            .map_err(|e| Error::from_reason(format!("Failed to union tables: {}", e)))?;

        self.context
            .register_table(&result_table, df.into_view())
            .map_err(|e| Error::from_reason(format!("Failed to register union result: {}", e)))?;

        if !self.table_names.contains(&result_table) {
            self.table_names.push(result_table);
        }

        Ok(())
    }

    #[napi]
    pub async fn join_tables(
        &mut self,
        result_table: String,
        left_table: String,
        right_table: String,
        left_key: String,
        right_key: String,
        join_type: Option<String>,
    ) -> Result<()> {
        let join = join_type.as_deref().unwrap_or("INNER");

        let sql = format!(
            "CREATE VIEW {} AS SELECT * FROM {} {} JOIN {} ON {}.{} = {}.{}",
            result_table,
            left_table,
            join,
            right_table,
            left_table,
            left_key,
            right_table,
            right_key
        );

        let df = self
            .context
            .sql(&sql)
            .await
            .map_err(|e| Error::from_reason(format!("Failed to join tables: {}", e)))?;

        self.context
            .register_table(&result_table, df.into_view())
            .map_err(|e| Error::from_reason(format!("Failed to register join result: {}", e)))?;

        if !self.table_names.contains(&result_table) {
            self.table_names.push(result_table);
        }

        Ok(())
    }
}
