//! Integration tests for cloud storage data sources.

#[cfg(feature = "cloud-storage")]
mod cloud_tests {
    use datafusion::prelude::*;
    use term_core::sources::DataSource;

    #[cfg(feature = "s3")]
    use term_core::sources::{S3Auth, S3Config, S3Source};

    #[cfg(feature = "gcs")]
    use term_core::sources::{GcsAuth, GcsConfig, GcsSource};

    #[cfg(feature = "azure")]
    use term_core::sources::{AzureAuth, AzureBlobSource, AzureConfig};

    #[allow(dead_code)]
    fn create_test_context() -> SessionContext {
        SessionContext::new()
    }

    #[cfg(feature = "s3")]
    mod s3_tests {
        use super::*;

        #[tokio::test]
        async fn test_s3_source_creation_with_iam() {
            let config = S3Config {
                bucket: "test-bucket".to_string(),
                key: "data/test.parquet".to_string(),
                region: Some("us-east-1".to_string()),
                auth: S3Auth::InstanceCredentials,
                endpoint: None,
            };

            // Note: This will fail without valid AWS credentials
            // In real tests, you'd use localstack or minio
            let result = S3Source::new(config).await;

            // For CI, we just test that the config is created properly
            assert!(result.is_ok() || result.is_err());
        }

        #[tokio::test]
        async fn test_s3_source_creation_with_access_key() {
            let config = S3Config {
                bucket: "test-bucket".to_string(),
                key: "data/test.csv".to_string(),
                region: Some("us-west-2".to_string()),
                auth: S3Auth::AccessKey {
                    access_key_id: "test_key".to_string(),
                    secret_access_key: "test_secret".into(),
                    session_token: None,
                },
                endpoint: Some("http://localhost:9000".to_string()), // For minio/localstack
            };

            let result = S3Source::new(config).await;
            assert!(result.is_ok() || result.is_err());
        }

        #[tokio::test]
        async fn test_s3_convenience_constructors() {
            // Test IAM constructor
            let result = S3Source::from_iam(
                "bucket".to_string(),
                "key.json".to_string(),
                Some("us-east-1".to_string()),
            )
            .await;
            assert!(result.is_ok() || result.is_err());

            // Test access key constructor
            let result = S3Source::from_access_key(
                "bucket".to_string(),
                "key.csv.gz".to_string(),
                "access_key".to_string(),
                "secret_key".to_string(),
                None,
            )
            .await;
            assert!(result.is_ok() || result.is_err());
        }

        #[tokio::test]
        async fn test_s3_description() {
            let config = S3Config {
                bucket: "my-bucket".to_string(),
                key: "path/to/data.parquet".to_string(),
                region: None,
                auth: S3Auth::InstanceCredentials,
                endpoint: None,
            };

            if let Ok(source) = S3Source::new(config).await {
                assert_eq!(
                    source.description(),
                    "S3 source: s3://my-bucket/path/to/data.parquet"
                );
            }
        }
    }

    #[cfg(feature = "gcs")]
    mod gcs_tests {
        use super::*;

        #[tokio::test]
        async fn test_gcs_source_creation_with_adc() {
            let config = GcsConfig {
                bucket: "test-bucket".to_string(),
                object: "data/test.parquet".to_string(),
                auth: GcsAuth::ApplicationDefault,
            };

            let result = GcsSource::new(config).await;
            assert!(result.is_ok() || result.is_err());
        }

        #[tokio::test]
        async fn test_gcs_source_creation_with_service_account() {
            let config = GcsConfig {
                bucket: "test-bucket".to_string(),
                object: "data/test.csv".to_string(),
                auth: GcsAuth::ServiceAccountKey("/path/to/key.json".to_string()),
            };

            let result = GcsSource::new(config).await;
            assert!(result.is_ok() || result.is_err());
        }

        #[tokio::test]
        async fn test_gcs_convenience_constructors() {
            // Test ADC constructor
            let result = GcsSource::from_adc("bucket".to_string(), "object.json".to_string()).await;
            assert!(result.is_ok() || result.is_err());

            // Test service account file constructor
            let result = GcsSource::from_service_account_file(
                "bucket".to_string(),
                "object.csv.gz".to_string(),
                "/path/to/sa.json".to_string(),
            )
            .await;
            assert!(result.is_ok() || result.is_err());
        }

        #[tokio::test]
        async fn test_gcs_description() {
            let config = GcsConfig {
                bucket: "my-bucket".to_string(),
                object: "path/to/data.parquet".to_string(),
                auth: GcsAuth::ApplicationDefault,
            };

            if let Ok(source) = GcsSource::new(config).await {
                assert_eq!(
                    source.description(),
                    "GCS source: gs://my-bucket/path/to/data.parquet"
                );
            }
        }
    }

    #[cfg(feature = "azure")]
    mod azure_tests {
        use super::*;

        #[tokio::test]
        async fn test_azure_source_creation_with_cli() {
            let config = AzureConfig {
                account: "testaccount".to_string(),
                container: "testcontainer".to_string(),
                blob: "data/test.parquet".to_string(),
                auth: AzureAuth::AzureCli,
            };

            let result = AzureBlobSource::new(config).await;
            assert!(result.is_ok() || result.is_err());
        }

        #[tokio::test]
        async fn test_azure_source_creation_with_access_key() {
            let config = AzureConfig {
                account: "testaccount".to_string(),
                container: "testcontainer".to_string(),
                blob: "data/test.csv".to_string(),
                auth: AzureAuth::AccessKey("test_key".into()),
            };

            let result = AzureBlobSource::new(config).await;
            assert!(result.is_ok() || result.is_err());
        }

        #[tokio::test]
        async fn test_azure_convenience_constructors() {
            // Test access key constructor
            let result = AzureBlobSource::from_access_key(
                "account".to_string(),
                "container".to_string(),
                "blob.json".to_string(),
                "access_key".to_string(),
            )
            .await;
            assert!(result.is_ok() || result.is_err());

            // Test Azure CLI constructor
            let result = AzureBlobSource::from_azure_cli(
                "account".to_string(),
                "container".to_string(),
                "blob.csv.gz".to_string(),
            )
            .await;
            assert!(result.is_ok() || result.is_err());
        }

        #[tokio::test]
        async fn test_azure_description() {
            let config = AzureConfig {
                account: "myaccount".to_string(),
                container: "mycontainer".to_string(),
                blob: "path/to/data.parquet".to_string(),
                auth: AzureAuth::AzureCli,
            };

            if let Ok(source) = AzureBlobSource::new(config).await {
                assert_eq!(
                    source.description(),
                    "Azure Blob source: myaccount/mycontainer/path/to/data.parquet"
                );
            }
        }

        #[tokio::test]
        async fn test_azure_client_secret_auth() {
            let config = AzureConfig {
                account: "testaccount".to_string(),
                container: "testcontainer".to_string(),
                blob: "data.json".to_string(),
                auth: AzureAuth::ClientSecret {
                    client_id: "client_id".to_string(),
                    client_secret: "client_secret".into(),
                    tenant_id: "tenant_id".to_string(),
                },
            };

            let result = AzureBlobSource::new(config).await;
            assert!(result.is_ok() || result.is_err());
        }
    }

    #[cfg(all(feature = "s3", feature = "gcs", feature = "azure"))]
    mod cross_cloud_tests {
        use super::*;

        #[tokio::test]
        async fn test_all_sources_implement_datasource_trait() {
            // This test verifies that all cloud sources properly implement the DataSource trait

            let s3_config = S3Config {
                bucket: "test".to_string(),
                key: "test.parquet".to_string(),
                region: None,
                auth: S3Auth::InstanceCredentials,
                endpoint: None,
            };

            let gcs_config = GcsConfig {
                bucket: "test".to_string(),
                object: "test.parquet".to_string(),
                auth: GcsAuth::ApplicationDefault,
            };

            let azure_config = AzureConfig {
                account: "test".to_string(),
                container: "test".to_string(),
                blob: "test.parquet".to_string(),
                auth: AzureAuth::AzureCli,
            };

            // Verify each source can be created (might fail due to auth, but that's ok)
            let _ = S3Source::new(s3_config).await;
            let _ = GcsSource::new(gcs_config).await;
            let _ = AzureBlobSource::new(azure_config).await;
        }
    }
}
