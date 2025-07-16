//! Cloud storage connectors for S3, GCS, and Azure Blob Storage.
//!
//! This module provides data source implementations for cloud object storage services,
//! with support for authentication, retry logic, and streaming large files.

use crate::prelude::*;
use crate::security::SecureString;
use crate::sources::DataSource;
use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::prelude::SessionContext;
use std::fmt::Debug;
use std::sync::Arc;
use tracing::instrument;

#[cfg(feature = "s3")]
use object_store::aws::AmazonS3Builder;

#[cfg(feature = "gcs")]
use object_store::gcp::GoogleCloudStorageBuilder;

#[cfg(feature = "azure")]
use object_store::azure::MicrosoftAzureBuilder;

use object_store::{ObjectStore, RetryConfig};
use url::Url;

/// S3 authentication configuration.
#[derive(Debug, Clone)]
pub enum S3Auth {
    /// Use IAM instance credentials
    InstanceCredentials,
    /// Use access key and secret
    AccessKey {
        access_key_id: String,
        secret_access_key: SecureString,
        session_token: Option<SecureString>,
    },
    /// Use AWS profile from credentials file
    Profile(String),
}

/// Configuration for S3 data source.
#[derive(Debug, Clone)]
pub struct S3Config {
    /// S3 bucket name
    pub bucket: String,
    /// Object key or prefix
    pub key: String,
    /// AWS region (optional, will be auto-detected if not specified)
    pub region: Option<String>,
    /// Authentication method
    pub auth: S3Auth,
    /// Custom endpoint (for S3-compatible services)
    pub endpoint: Option<String>,
}

/// S3 data source implementation.
#[cfg(feature = "s3")]
#[derive(Debug)]
pub struct S3Source {
    config: S3Config,
    schema: Option<Arc<Schema>>,
    object_store: Arc<dyn ObjectStore>,
}

#[cfg(feature = "s3")]
impl S3Source {
    /// Creates a new S3 data source.
    pub async fn new(config: S3Config) -> Result<Self> {
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&config.bucket)
            .with_retry(RetryConfig {
                max_retries: 3,
                retry_timeout: std::time::Duration::from_secs(30),
                ..Default::default()
            });

        if let Some(region) = &config.region {
            builder = builder.with_region(region);
        }

        if let Some(endpoint) = &config.endpoint {
            builder = builder.with_endpoint(endpoint);
        }

        match &config.auth {
            S3Auth::InstanceCredentials => {
                // IAM instance credentials will be auto-detected
            }
            S3Auth::AccessKey {
                access_key_id,
                secret_access_key,
                session_token,
            } => {
                builder = builder
                    .with_access_key_id(access_key_id)
                    .with_secret_access_key(secret_access_key.expose());

                if let Some(token) = session_token {
                    builder = builder.with_token(token.expose());
                }
            }
            S3Auth::Profile(_profile) => {
                // Profile support requires AWS SDK, not directly supported by object_store
                // Users should set AWS_PROFILE environment variable instead
                return Err(TermError::Configuration(
                    "Profile authentication requires AWS_PROFILE environment variable".to_string(),
                ));
            }
        }

        let object_store = Arc::new(builder.build().map_err(|e| TermError::DataSource {
            source_type: "S3".to_string(),
            message: format!("Failed to create S3 client: {}", e),
            source: Some(Box::new(e)),
        })?);

        Ok(Self {
            config,
            schema: None,
            object_store,
        })
    }

    /// Creates an S3 source with IAM instance credentials.
    pub async fn from_iam(bucket: String, key: String, region: Option<String>) -> Result<Self> {
        Self::new(S3Config {
            bucket,
            key,
            region,
            auth: S3Auth::InstanceCredentials,
            endpoint: None,
        })
        .await
    }

    /// Creates an S3 source with access key authentication.
    pub async fn from_access_key(
        bucket: String,
        key: String,
        access_key_id: String,
        secret_access_key: impl Into<String>,
        region: Option<String>,
    ) -> Result<Self> {
        Self::new(S3Config {
            bucket,
            key,
            region,
            auth: S3Auth::AccessKey {
                access_key_id,
                secret_access_key: SecureString::new(secret_access_key.into()),
                session_token: None,
            },
            endpoint: None,
        })
        .await
    }
}

#[cfg(feature = "s3")]
#[async_trait]
impl DataSource for S3Source {
    #[instrument(skip(self, ctx, telemetry), fields(table_name = %table_name, source_type = "s3", bucket = %self.config.bucket))]
    async fn register_with_telemetry(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        telemetry: Option<&Arc<TermTelemetry>>,
    ) -> Result<()> {
        // Create telemetry span for data source loading
        let mut _datasource_span = if let Some(tel) = telemetry {
            tel.start_datasource_span("s3", table_name)
        } else {
            TermSpan::noop()
        };

        // Start timing for metrics
        #[cfg(feature = "telemetry")]
        let load_start = std::time::Instant::now();

        let s3_url = format!("s3://{}/{}", self.config.bucket, self.config.key);
        let url = Url::parse(&s3_url).map_err(|e| TermError::DataSource {
            source_type: "S3".to_string(),
            message: format!("Invalid S3 URL: {}", e),
            source: Some(Box::new(e)),
        })?;

        // Register the object store with DataFusion
        ctx.runtime_env()
            .object_store_registry
            .register_store(&url, self.object_store.clone());

        // Determine file format and register appropriately
        let path = self.config.key.to_lowercase();
        if path.ends_with(".parquet") {
            ctx.register_parquet(table_name, &s3_url, Default::default())
                .await?;
        } else if path.ends_with(".csv") || path.ends_with(".csv.gz") {
            ctx.register_csv(table_name, &s3_url, Default::default())
                .await?;
        } else if path.ends_with(".json") || path.ends_with(".jsonl") {
            ctx.register_json(table_name, &s3_url, Default::default())
                .await?;
        } else {
            return Err(TermError::DataSource {
                source_type: "S3".to_string(),
                message: format!("Unsupported file format for key: {}", self.config.key),
                source: None,
            });
        }

        // Record data load duration in metrics
        #[cfg(feature = "telemetry")]
        if let Some(tel) = telemetry {
            if let Some(metrics) = tel.metrics() {
                let load_duration = load_start.elapsed().as_secs_f64();
                let attrs = vec![
                    opentelemetry::KeyValue::new("data_source.type", "s3"),
                    opentelemetry::KeyValue::new("data_source.bucket", self.config.bucket.clone()),
                    opentelemetry::KeyValue::new("data_source.key", self.config.key.clone()),
                    opentelemetry::KeyValue::new("data_source.table", table_name.to_string()),
                ];
                metrics.record_data_load_duration(load_duration, &attrs);
            }
        }

        Ok(())
    }

    fn schema(&self) -> Option<&Arc<Schema>> {
        self.schema.as_ref()
    }

    fn description(&self) -> String {
        format!("S3 source: s3://{}/{}", self.config.bucket, self.config.key)
    }
}

/// Google Cloud Storage authentication configuration.
#[derive(Debug, Clone)]
pub enum GcsAuth {
    /// Use Application Default Credentials
    ApplicationDefault,
    /// Use service account key file
    ServiceAccountKey(String),
    /// Use service account JSON string
    ServiceAccountJson(String),
}

/// Configuration for Google Cloud Storage data source.
#[derive(Debug, Clone)]
pub struct GcsConfig {
    /// GCS bucket name
    pub bucket: String,
    /// Object name or prefix
    pub object: String,
    /// Authentication method
    pub auth: GcsAuth,
}

/// Google Cloud Storage data source implementation.
#[cfg(feature = "gcs")]
#[derive(Debug)]
pub struct GcsSource {
    config: GcsConfig,
    schema: Option<Arc<Schema>>,
    object_store: Arc<dyn ObjectStore>,
}

#[cfg(feature = "gcs")]
impl GcsSource {
    /// Creates a new GCS data source.
    pub async fn new(config: GcsConfig) -> Result<Self> {
        let mut builder = GoogleCloudStorageBuilder::new()
            .with_bucket_name(&config.bucket)
            .with_retry(RetryConfig {
                max_retries: 3,
                retry_timeout: std::time::Duration::from_secs(30),
                ..Default::default()
            });

        match &config.auth {
            GcsAuth::ApplicationDefault => {
                // ADC will be auto-detected
            }
            GcsAuth::ServiceAccountKey(path) => {
                builder = builder.with_service_account_path(path);
            }
            GcsAuth::ServiceAccountJson(json) => {
                builder = builder.with_service_account_key(json);
            }
        }

        let object_store = Arc::new(builder.build().map_err(|e| TermError::DataSource {
            source_type: "GCS".to_string(),
            message: format!("Failed to create GCS client: {}", e),
            source: Some(Box::new(e)),
        })?);

        Ok(Self {
            config,
            schema: None,
            object_store,
        })
    }

    /// Creates a GCS source with Application Default Credentials.
    pub async fn from_adc(bucket: String, object: String) -> Result<Self> {
        Self::new(GcsConfig {
            bucket,
            object,
            auth: GcsAuth::ApplicationDefault,
        })
        .await
    }

    /// Creates a GCS source with service account key file.
    pub async fn from_service_account_file(
        bucket: String,
        object: String,
        key_path: String,
    ) -> Result<Self> {
        Self::new(GcsConfig {
            bucket,
            object,
            auth: GcsAuth::ServiceAccountKey(key_path),
        })
        .await
    }
}

#[cfg(feature = "gcs")]
#[async_trait]
impl DataSource for GcsSource {
    #[instrument(skip(self, ctx, telemetry), fields(table_name = %table_name, source_type = "gcs", bucket = %self.config.bucket))]
    async fn register_with_telemetry(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        telemetry: Option<&Arc<TermTelemetry>>,
    ) -> Result<()> {
        // Create telemetry span for data source loading
        let mut _datasource_span = if let Some(tel) = telemetry {
            tel.start_datasource_span("gcs", table_name)
        } else {
            TermSpan::noop()
        };

        // Start timing for metrics
        #[cfg(feature = "telemetry")]
        let load_start = std::time::Instant::now();

        let gcs_url = format!("gs://{}/{}", self.config.bucket, self.config.object);
        let url = Url::parse(&gcs_url).map_err(|e| TermError::DataSource {
            source_type: "GCS".to_string(),
            message: format!("Invalid GCS URL: {}", e),
            source: Some(Box::new(e)),
        })?;

        // Register the object store with DataFusion
        ctx.runtime_env()
            .object_store_registry
            .register_store(&url, self.object_store.clone());

        // Determine file format and register appropriately
        let path = self.config.object.to_lowercase();
        if path.ends_with(".parquet") {
            ctx.register_parquet(table_name, &gcs_url, Default::default())
                .await?;
        } else if path.ends_with(".csv") || path.ends_with(".csv.gz") {
            ctx.register_csv(table_name, &gcs_url, Default::default())
                .await?;
        } else if path.ends_with(".json") || path.ends_with(".jsonl") {
            ctx.register_json(table_name, &gcs_url, Default::default())
                .await?;
        } else {
            return Err(TermError::DataSource {
                source_type: "GCS".to_string(),
                message: format!("Unsupported file format for object: {}", self.config.object),
                source: None,
            });
        }

        // Record data load duration in metrics
        #[cfg(feature = "telemetry")]
        if let Some(tel) = telemetry {
            if let Some(metrics) = tel.metrics() {
                let load_duration = load_start.elapsed().as_secs_f64();
                let attrs = vec![
                    opentelemetry::KeyValue::new("data_source.type", "gcs"),
                    opentelemetry::KeyValue::new("data_source.bucket", self.config.bucket.clone()),
                    opentelemetry::KeyValue::new("data_source.object", self.config.object.clone()),
                    opentelemetry::KeyValue::new("data_source.table", table_name.to_string()),
                ];
                metrics.record_data_load_duration(load_duration, &attrs);
            }
        }

        Ok(())
    }

    fn schema(&self) -> Option<&Arc<Schema>> {
        self.schema.as_ref()
    }

    fn description(&self) -> String {
        format!(
            "GCS source: gs://{}/{}",
            self.config.bucket, self.config.object
        )
    }
}

/// Azure Blob Storage authentication configuration.
#[derive(Debug, Clone)]
pub enum AzureAuth {
    /// Use Azure CLI credentials
    AzureCli,
    /// Use access key
    AccessKey(SecureString),
    /// Use SAS token
    SasToken(SecureString),
    /// Use client secret
    ClientSecret {
        client_id: String,
        client_secret: SecureString,
        tenant_id: String,
    },
}

/// Configuration for Azure Blob Storage data source.
#[derive(Debug, Clone)]
pub struct AzureConfig {
    /// Storage account name
    pub account: String,
    /// Container name
    pub container: String,
    /// Blob name or prefix
    pub blob: String,
    /// Authentication method
    pub auth: AzureAuth,
}

/// Azure Blob Storage data source implementation.
#[cfg(feature = "azure")]
#[derive(Debug)]
pub struct AzureBlobSource {
    config: AzureConfig,
    schema: Option<Arc<Schema>>,
    object_store: Arc<dyn ObjectStore>,
}

#[cfg(feature = "azure")]
impl AzureBlobSource {
    /// Creates a new Azure Blob Storage data source.
    pub async fn new(config: AzureConfig) -> Result<Self> {
        let mut builder = MicrosoftAzureBuilder::new()
            .with_account(&config.account)
            .with_container_name(&config.container)
            .with_retry(RetryConfig {
                max_retries: 3,
                retry_timeout: std::time::Duration::from_secs(30),
                ..Default::default()
            });

        match &config.auth {
            AzureAuth::AzureCli => {
                builder = builder.with_use_azure_cli(true);
            }
            AzureAuth::AccessKey(key) => {
                builder = builder.with_access_key(key.expose());
            }
            AzureAuth::SasToken(_token) => {
                // SAS token support requires URL parameter, not a separate method
                return Err(TermError::Configuration(
                    "SAS token authentication should be provided as part of the connection string"
                        .to_string(),
                ));
            }
            AzureAuth::ClientSecret {
                client_id,
                client_secret,
                tenant_id,
            } => {
                builder = builder
                    .with_client_id(client_id)
                    .with_client_secret(client_secret.expose())
                    .with_tenant_id(tenant_id);
            }
        }

        let object_store = Arc::new(builder.build().map_err(|e| TermError::DataSource {
            source_type: "Azure".to_string(),
            message: format!("Failed to create Azure client: {}", e),
            source: Some(Box::new(e)),
        })?);

        Ok(Self {
            config,
            schema: None,
            object_store,
        })
    }

    /// Creates an Azure source with access key authentication.
    pub async fn from_access_key(
        account: String,
        container: String,
        blob: String,
        access_key: impl Into<String>,
    ) -> Result<Self> {
        Self::new(AzureConfig {
            account,
            container,
            blob,
            auth: AzureAuth::AccessKey(SecureString::new(access_key.into())),
        })
        .await
    }

    /// Creates an Azure source with Azure CLI credentials.
    pub async fn from_azure_cli(account: String, container: String, blob: String) -> Result<Self> {
        Self::new(AzureConfig {
            account,
            container,
            blob,
            auth: AzureAuth::AzureCli,
        })
        .await
    }
}

#[cfg(feature = "azure")]
#[async_trait]
impl DataSource for AzureBlobSource {
    #[instrument(skip(self, ctx, telemetry), fields(table_name = %table_name, source_type = "azure", account = %self.config.account))]
    async fn register_with_telemetry(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        telemetry: Option<&Arc<TermTelemetry>>,
    ) -> Result<()> {
        // Create telemetry span for data source loading
        let mut _datasource_span = if let Some(tel) = telemetry {
            tel.start_datasource_span("azure", table_name)
        } else {
            TermSpan::noop()
        };

        // Start timing for metrics
        #[cfg(feature = "telemetry")]
        let load_start = std::time::Instant::now();

        let azure_url = format!(
            "az://{}/{}/{}",
            self.config.account, self.config.container, self.config.blob
        );

        let base_url = format!("az://{}", self.config.container);
        let url = Url::parse(&base_url).map_err(|e| TermError::DataSource {
            source_type: "Azure".to_string(),
            message: format!("Invalid Azure URL: {}", e),
            source: Some(Box::new(e)),
        })?;

        // Register the object store with DataFusion
        ctx.runtime_env()
            .object_store_registry
            .register_store(&url, self.object_store.clone());

        // Determine file format and register appropriately
        let path = self.config.blob.to_lowercase();
        if path.ends_with(".parquet") {
            ctx.register_parquet(table_name, &azure_url, Default::default())
                .await?;
        } else if path.ends_with(".csv") || path.ends_with(".csv.gz") {
            ctx.register_csv(table_name, &azure_url, Default::default())
                .await?;
        } else if path.ends_with(".json") || path.ends_with(".jsonl") {
            ctx.register_json(table_name, &azure_url, Default::default())
                .await?;
        } else {
            return Err(TermError::DataSource {
                source_type: "Azure".to_string(),
                message: format!("Unsupported file format for blob: {}", self.config.blob),
                source: None,
            });
        }

        // Record data load duration in metrics
        #[cfg(feature = "telemetry")]
        if let Some(tel) = telemetry {
            if let Some(metrics) = tel.metrics() {
                let load_duration = load_start.elapsed().as_secs_f64();
                let attrs = vec![
                    opentelemetry::KeyValue::new("data_source.type", "azure"),
                    opentelemetry::KeyValue::new(
                        "data_source.account",
                        self.config.account.clone(),
                    ),
                    opentelemetry::KeyValue::new(
                        "data_source.container",
                        self.config.container.clone(),
                    ),
                    opentelemetry::KeyValue::new("data_source.blob", self.config.blob.clone()),
                    opentelemetry::KeyValue::new("data_source.table", table_name.to_string()),
                ];
                metrics.record_data_load_duration(load_duration, &attrs);
            }
        }

        Ok(())
    }

    fn schema(&self) -> Option<&Arc<Schema>> {
        self.schema.as_ref()
    }

    fn description(&self) -> String {
        format!(
            "Azure Blob source: {}/{}/{}",
            self.config.account, self.config.container, self.config.blob
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "s3")]
    #[tokio::test]
    async fn test_s3_config() {
        let config = S3Config {
            bucket: "test-bucket".to_string(),
            key: "test-key.parquet".to_string(),
            region: Some("us-east-1".to_string()),
            auth: S3Auth::InstanceCredentials,
            endpoint: None,
        };

        // Note: Actual S3 source creation would require valid credentials
        // This just tests the configuration structure
        assert_eq!(config.bucket, "test-bucket");
        assert_eq!(config.key, "test-key.parquet");
    }

    #[cfg(feature = "gcs")]
    #[test]
    fn test_gcs_config() {
        let config = GcsConfig {
            bucket: "test-bucket".to_string(),
            object: "test-object.csv".to_string(),
            auth: GcsAuth::ApplicationDefault,
        };

        assert_eq!(config.bucket, "test-bucket");
        assert_eq!(config.object, "test-object.csv");
    }

    #[cfg(feature = "azure")]
    #[test]
    fn test_azure_config() {
        let config = AzureConfig {
            account: "testaccount".to_string(),
            container: "testcontainer".to_string(),
            blob: "test-blob.json".to_string(),
            auth: AzureAuth::AzureCli,
        };

        assert_eq!(config.account, "testaccount");
        assert_eq!(config.container, "testcontainer");
        assert_eq!(config.blob, "test-blob.json");
    }
}
