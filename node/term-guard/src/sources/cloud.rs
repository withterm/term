#[cfg(feature = "cloud-storage")]
use datafusion::prelude::*;
#[cfg(feature = "cloud-storage")]
use napi::bindgen_prelude::*;
#[cfg(feature = "cloud-storage")]
use napi_derive::napi;
#[cfg(feature = "cloud-storage")]
use std::sync::Arc;

#[cfg(feature = "cloud-storage")]
#[napi(object)]
pub struct S3Options {
    pub region: String,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub endpoint: Option<String>,
    pub allow_http: Option<bool>,
}

#[cfg(feature = "cloud-storage")]
#[napi(object)]
pub struct AzureOptions {
    pub account_name: String,
    pub access_key: Option<String>,
    pub sas_token: Option<String>,
    pub container_name: String,
}

#[cfg(feature = "cloud-storage")]
#[napi(object)]
pub struct GcsOptions {
    pub project_id: String,
    pub credentials_path: Option<String>,
    pub bucket_name: String,
}

#[cfg(feature = "cloud-storage")]
#[napi]
pub struct CloudDataSource {
    context: Arc<SessionContext>,
    table_name: String,
    source_type: String,
}

#[cfg(feature = "cloud-storage")]
impl CloudDataSource {
    pub(crate) fn context(&self) -> &Arc<SessionContext> {
        &self.context
    }

    pub(crate) fn table_name(&self) -> &str {
        &self.table_name
    }
}

#[cfg(feature = "cloud-storage")]
#[napi]
impl CloudDataSource {
    #[napi(factory)]
    pub async fn from_s3(path: String, options: S3Options) -> Result<CloudDataSource> {
        use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};

        let mut builder = AmazonS3Builder::new().with_region(&options.region);

        if let Some(access_key_id) = options.access_key_id {
            builder = builder.with_access_key_id(access_key_id);
        }

        if let Some(secret_access_key) = options.secret_access_key {
            builder = builder.with_secret_access_key(secret_access_key);
        }

        if let Some(session_token) = options.session_token {
            builder = builder.with_token(session_token);
        }

        if let Some(endpoint) = options.endpoint {
            builder = builder.with_endpoint(endpoint);
        }

        if let Some(allow_http) = options.allow_http {
            if allow_http {
                builder = builder.with_allow_http(true);
            }
        }

        let store = builder
            .build()
            .map_err(|e| Error::from_reason(format!("Failed to create S3 client: {}", e)))?;

        let ctx = SessionContext::new();
        let runtime = ctx.runtime_env();
        runtime
            .register_object_store("s3", Arc::new(store))
            .map_err(|e| Error::from_reason(format!("Failed to register S3 store: {}", e)))?;

        let file_extension = path.split('.').last().unwrap_or("parquet").to_lowercase();

        match file_extension.as_str() {
            "parquet" => {
                ctx.register_parquet(&format!("s3://{}", path), "data", Default::default())
                    .await
                    .map_err(|e| {
                        Error::from_reason(format!("Failed to register Parquet from S3: {}", e))
                    })?;
            }
            "csv" => {
                ctx.register_csv(&format!("s3://{}", path), "data", Default::default())
                    .await
                    .map_err(|e| {
                        Error::from_reason(format!("Failed to register CSV from S3: {}", e))
                    })?;
            }
            "json" => {
                ctx.register_json(&format!("s3://{}", path), "data", Default::default())
                    .await
                    .map_err(|e| {
                        Error::from_reason(format!("Failed to register JSON from S3: {}", e))
                    })?;
            }
            _ => {
                return Err(Error::from_reason(format!(
                    "Unsupported file format: {}",
                    file_extension
                )));
            }
        }

        Ok(CloudDataSource {
            context: Arc::new(ctx),
            table_name: "data".to_string(),
            source_type: "s3".to_string(),
        })
    }

    #[napi(factory)]
    pub async fn from_azure(path: String, options: AzureOptions) -> Result<CloudDataSource> {
        use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};

        let mut builder = MicrosoftAzureBuilder::new()
            .with_account(options.account_name)
            .with_container_name(options.container_name);

        if let Some(access_key) = options.access_key {
            builder = builder.with_access_key(access_key);
        }

        if let Some(sas_token) = options.sas_token {
            builder = builder.with_sas_token(sas_token);
        }

        let store = builder
            .build()
            .map_err(|e| Error::from_reason(format!("Failed to create Azure client: {}", e)))?;

        let ctx = SessionContext::new();
        let runtime = ctx.runtime_env();
        runtime
            .register_object_store("azure", Arc::new(store))
            .map_err(|e| Error::from_reason(format!("Failed to register Azure store: {}", e)))?;

        let file_extension = path.split('.').last().unwrap_or("parquet").to_lowercase();

        match file_extension.as_str() {
            "parquet" => {
                ctx.register_parquet(&format!("azure://{}", path), "data", Default::default())
                    .await
                    .map_err(|e| {
                        Error::from_reason(format!("Failed to register Parquet from Azure: {}", e))
                    })?;
            }
            "csv" => {
                ctx.register_csv(&format!("azure://{}", path), "data", Default::default())
                    .await
                    .map_err(|e| {
                        Error::from_reason(format!("Failed to register CSV from Azure: {}", e))
                    })?;
            }
            "json" => {
                ctx.register_json(&format!("azure://{}", path), "data", Default::default())
                    .await
                    .map_err(|e| {
                        Error::from_reason(format!("Failed to register JSON from Azure: {}", e))
                    })?;
            }
            _ => {
                return Err(Error::from_reason(format!(
                    "Unsupported file format: {}",
                    file_extension
                )));
            }
        }

        Ok(CloudDataSource {
            context: Arc::new(ctx),
            table_name: "data".to_string(),
            source_type: "azure".to_string(),
        })
    }

    #[napi(factory)]
    pub async fn from_gcs(path: String, options: GcsOptions) -> Result<CloudDataSource> {
        use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};

        let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(options.bucket_name);

        if let Some(credentials_path) = options.credentials_path {
            builder = builder.with_service_account_path(credentials_path);
        }

        let store = builder
            .build()
            .map_err(|e| Error::from_reason(format!("Failed to create GCS client: {}", e)))?;

        let ctx = SessionContext::new();
        let runtime = ctx.runtime_env();
        runtime
            .register_object_store("gcs", Arc::new(store))
            .map_err(|e| Error::from_reason(format!("Failed to register GCS store: {}", e)))?;

        let file_extension = path.split('.').last().unwrap_or("parquet").to_lowercase();

        match file_extension.as_str() {
            "parquet" => {
                ctx.register_parquet(&format!("gcs://{}", path), "data", Default::default())
                    .await
                    .map_err(|e| {
                        Error::from_reason(format!("Failed to register Parquet from GCS: {}", e))
                    })?;
            }
            "csv" => {
                ctx.register_csv(&format!("gcs://{}", path), "data", Default::default())
                    .await
                    .map_err(|e| {
                        Error::from_reason(format!("Failed to register CSV from GCS: {}", e))
                    })?;
            }
            "json" => {
                ctx.register_json(&format!("gcs://{}", path), "data", Default::default())
                    .await
                    .map_err(|e| {
                        Error::from_reason(format!("Failed to register JSON from GCS: {}", e))
                    })?;
            }
            _ => {
                return Err(Error::from_reason(format!(
                    "Unsupported file format: {}",
                    file_extension
                )));
            }
        }

        Ok(CloudDataSource {
            context: Arc::new(ctx),
            table_name: "data".to_string(),
            source_type: "gcs".to_string(),
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
    pub fn get_source_type(&self) -> String {
        self.source_type.clone()
    }
}
