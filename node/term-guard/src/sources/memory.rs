use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

#[napi]
pub struct MemoryDataSource {
    context: Arc<SessionContext>,
    table_name: String,
}

impl MemoryDataSource {
    pub(crate) fn context(&self) -> &Arc<SessionContext> {
        &self.context
    }

    pub(crate) fn table_name(&self) -> &str {
        &self.table_name
    }
}

fn infer_arrow_type(value: &Value) -> DataType {
    match value {
        Value::Bool(_) => DataType::Boolean,
        Value::Number(n) => {
            if n.is_f64() {
                DataType::Float64
            } else if n.is_i64() {
                DataType::Int64
            } else {
                DataType::Float64
            }
        }
        Value::String(_) => DataType::Utf8,
        Value::Null => DataType::Null,
        _ => DataType::Utf8,
    }
}

fn json_value_to_array(values: Vec<Option<Value>>, data_type: &DataType) -> Result<Arc<dyn Array>> {
    match data_type {
        DataType::Boolean => {
            let array: BooleanArray = values
                .iter()
                .map(|v| match v {
                    Some(Value::Bool(b)) => Some(*b),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(array))
        }
        DataType::Int64 => {
            let array: Int64Array = values
                .iter()
                .map(|v| match v {
                    Some(Value::Number(n)) => n.as_i64(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(array))
        }
        DataType::Float64 => {
            let array: Float64Array = values
                .iter()
                .map(|v| match v {
                    Some(Value::Number(n)) => n.as_f64(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(array))
        }
        DataType::Utf8 => {
            let array: StringArray = values
                .iter()
                .map(|v| match v {
                    Some(Value::String(s)) => Some(s.as_str()),
                    Some(v) => Some(v.to_string().as_str()),
                    None => None,
                })
                .collect();
            Ok(Arc::new(array))
        }
        _ => {
            let array: StringArray = values
                .iter()
                .map(|v| match v {
                    Some(v) => Some(v.to_string().as_str()),
                    None => None,
                })
                .collect();
            Ok(Arc::new(array))
        }
    }
}

#[napi]
impl MemoryDataSource {
    #[napi(factory)]
    pub async fn from_json_array(data: String) -> Result<MemoryDataSource> {
        let json_data: Vec<Value> = serde_json::from_str(&data)
            .map_err(|e| Error::from_reason(format!("Invalid JSON data: {}", e)))?;

        if json_data.is_empty() {
            return Err(Error::from_reason("Empty data array"));
        }

        let first_obj = json_data[0]
            .as_object()
            .ok_or_else(|| Error::from_reason("First element is not an object"))?;

        let mut schema_fields = Vec::new();
        let mut column_types = HashMap::new();

        for (key, value) in first_obj.iter() {
            let data_type = infer_arrow_type(value);
            schema_fields.push(Field::new(key, data_type.clone(), true));
            column_types.insert(key.clone(), data_type);
        }

        let mut column_data: HashMap<String, Vec<Option<Value>>> = HashMap::new();
        for key in first_obj.keys() {
            column_data.insert(key.clone(), Vec::new());
        }

        for obj in json_data.iter() {
            if let Some(object) = obj.as_object() {
                for key in first_obj.keys() {
                    let value = object.get(key).cloned();
                    column_data.get_mut(key).unwrap().push(value);
                }
            }
        }

        let schema = Arc::new(Schema::new(schema_fields));
        let mut arrays = Vec::new();

        for field in schema.fields() {
            let column_values = column_data.get(field.name()).unwrap();
            let array = json_value_to_array(column_values.clone(), field.data_type())?;
            arrays.push(array);
        }

        let batch = RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| Error::from_reason(format!("Failed to create RecordBatch: {}", e)))?;

        let ctx = SessionContext::new();
        let df = ctx.read_batch(batch).map_err(|e| {
            Error::from_reason(format!("Failed to create DataFrame from batch: {}", e))
        })?;

        ctx.register_table("data", df.into_view())
            .map_err(|e| Error::from_reason(format!("Failed to register table: {}", e)))?;

        Ok(MemoryDataSource {
            context: Arc::new(ctx),
            table_name: "data".to_string(),
        })
    }

    #[napi(factory)]
    pub async fn from_records(records: Vec<String>) -> Result<MemoryDataSource> {
        if records.is_empty() {
            return Err(Error::from_reason("Empty records array"));
        }

        let mut json_objects = Vec::new();
        for record in records {
            let obj: Value = serde_json::from_str(&record)
                .map_err(|e| Error::from_reason(format!("Invalid JSON record: {}", e)))?;
            json_objects.push(obj);
        }

        let json_array = serde_json::to_string(&json_objects)
            .map_err(|e| Error::from_reason(format!("Failed to serialize records: {}", e)))?;

        Self::from_json_array(json_array).await
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

    #[napi]
    pub async fn query(&self, sql: String) -> Result<String> {
        let df = self
            .context
            .sql(&sql)
            .await
            .map_err(|e| Error::from_reason(format!("Failed to execute query: {}", e)))?;

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
}
