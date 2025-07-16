# Cloud Storage Support

Term provides native support for validating data stored in cloud object storage services including Amazon S3, Google Cloud Storage, and Azure Blob Storage.

## Features

- **Multiple Authentication Methods**: Support for IAM roles, access keys, service accounts, and more
- **Automatic Retry Logic**: Built-in exponential backoff for transient failures
- **Streaming Support**: Efficient handling of large files without loading entire dataset into memory
- **Format Detection**: Automatic detection of file formats (Parquet, CSV, JSON)
- **Compression Support**: Transparent handling of compressed files

## Installation

Add the desired cloud storage features to your `Cargo.toml`:

```toml
[dependencies]
term-guard = { version = "0.1", features = ["s3"] }
# or
term-guard = { version = "0.1", features = ["gcs"] }
# or
term-guard = { version = "0.1", features = ["azure"] }
# or all cloud providers
term-guard = { version = "0.1", features = ["all-cloud"] }
```

## Amazon S3

### Authentication Options

#### IAM Instance Credentials

```rust
use term_guard::sources::S3Source;

let source = S3Source::from_iam(
    "my-bucket".to_string(),
    "data/file.parquet".to_string(),
    Some("us-east-1".to_string()),
)
.await?;
```

#### Access Keys

```rust
let source = S3Source::from_access_key(
    "my-bucket".to_string(),
    "data/file.csv".to_string(),
    "AKIAIOSFODNN7EXAMPLE".to_string(),
    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
    None, // Region (optional)
)
.await?;
```

#### AWS Profile

```rust
use term_guard::sources::{S3Auth, S3Config, S3Source};

let config = S3Config {
    bucket: "my-bucket".to_string(),
    key: "data/file.json".to_string(),
    region: None,
    auth: S3Auth::Profile("production".to_string()),
    endpoint: None,
};

let source = S3Source::new(config).await?;
```

### S3-Compatible Services

For MinIO, Wasabi, or other S3-compatible services:

```rust
let config = S3Config {
    bucket: "my-bucket".to_string(),
    key: "data/file.parquet".to_string(),
    region: None,
    auth: S3Auth::AccessKey {
        access_key_id: "minioadmin".to_string(),
        secret_access_key: "minioadmin".to_string(),
        session_token: None,
    },
    endpoint: Some("http://localhost:9000".to_string()),
};

let source = S3Source::new(config).await?;
```

## Google Cloud Storage

### Authentication Options

#### Application Default Credentials

```rust
use term_guard::sources::GcsSource;

let source = GcsSource::from_adc(
    "my-bucket".to_string(),
    "data/file.parquet".to_string(),
)
.await?;
```

#### Service Account Key File

```rust
let source = GcsSource::from_service_account_file(
    "my-bucket".to_string(),
    "data/file.csv".to_string(),
    "/path/to/service-account-key.json".to_string(),
)
.await?;
```

#### Service Account JSON

```rust
use term_guard::sources::{GcsAuth, GcsConfig, GcsSource};

let config = GcsConfig {
    bucket: "my-bucket".to_string(),
    object: "data/file.json".to_string(),
    auth: GcsAuth::ServiceAccountJson(service_account_json_string),
};

let source = GcsSource::new(config).await?;
```

## Azure Blob Storage

### Authentication Options

#### Azure CLI

```rust
use term_guard::sources::AzureBlobSource;

let source = AzureBlobSource::from_azure_cli(
    "mystorageaccount".to_string(),
    "mycontainer".to_string(),
    "data/file.parquet".to_string(),
)
.await?;
```

#### Access Key

```rust
let source = AzureBlobSource::from_access_key(
    "mystorageaccount".to_string(),
    "mycontainer".to_string(),
    "data/file.csv".to_string(),
    "your-storage-account-key".to_string(),
)
.await?;
```

#### SAS Token

```rust
use term_guard::sources::{AzureAuth, AzureConfig, AzureBlobSource};

let config = AzureConfig {
    account: "mystorageaccount".to_string(),
    container: "mycontainer".to_string(),
    blob: "data/file.json".to_string(),
    auth: AzureAuth::SasToken("your-sas-token".to_string()),
};

let source = AzureBlobSource::new(config).await?;
```

#### Client Secret (Service Principal)

```rust
let config = AzureConfig {
    account: "mystorageaccount".to_string(),
    container: "mycontainer".to_string(),
    blob: "data/file.parquet".to_string(),
    auth: AzureAuth::ClientSecret {
        client_id: "your-client-id".to_string(),
        client_secret: "your-client-secret".to_string(),
        tenant_id: "your-tenant-id".to_string(),
    },
};

let source = AzureBlobSource::new(config).await?;
```

## Usage Example

Once you have created a cloud storage source, you can use it with Term's validation framework:

```rust
use datafusion::prelude::*;
use term_guard::prelude::*;
use term_guard::sources::S3Source;

#[tokio::main]
async fn main() -> Result<()> {
    // Create a DataFusion context
    let ctx = SessionContext::new();

    // Create and register the data source
    let source = S3Source::from_iam(
        "my-data-bucket".to_string(),
        "customers/2024/data.parquet".to_string(),
        Some("us-east-1".to_string()),
    )
    .await?;
    
    source.register(&ctx, "customers").await?;

    // Create a validation suite
    let suite = ValidationSuite::builder("Customer Data Quality")
        .add_check(
            Check::new("completeness")
                .with_constraint(constraints::completeness("customer_id", None))
                .with_constraint(constraints::completeness("email", None))
        )
        .add_check(
            Check::new("validity")
                .with_constraint(constraints::is_non_negative("age"))
                .with_constraint(constraints::pattern_match("email", r"^[^@]+@[^@]+\.[^@]+$"))
        )
        .build();

    // Run validations
    let results = suite.run(&ctx).await?;
    println!("{}", results.summary());

    Ok(())
}
```

## Performance Considerations

### Retry Logic

All cloud storage sources include automatic retry logic with exponential backoff:

- Maximum retries: 3
- Retry timeout: 30 seconds
- Exponential backoff between retries

### Streaming Large Files

Term leverages DataFusion's native object store integration for efficient streaming:

- Files are not fully loaded into memory
- Data is processed in chunks
- Supports parallel processing for better performance

### File Format Support

The following file formats are supported:

- **Parquet**: `.parquet`
- **CSV**: `.csv`, `.csv.gz`
- **JSON**: `.json`, `.jsonl`

Compression is automatically detected and handled for supported formats.

## Best Practices

1. **Use IAM/Managed Identity when possible**: Avoid hardcoding credentials
2. **Set appropriate retry timeouts**: Adjust based on your network conditions
3. **Use Parquet format**: For best performance with large datasets
4. **Enable compression**: To reduce data transfer costs
5. **Leverage prefixes**: Use object prefixes to validate multiple files

## Troubleshooting

### Authentication Errors

- **S3**: Ensure IAM roles have `s3:GetObject` and `s3:ListBucket` permissions
- **GCS**: Verify service account has `storage.objects.get` permission
- **Azure**: Check that the storage account allows the authentication method

### Network Issues

- Increase retry timeout for slow networks
- Use regional endpoints to reduce latency
- Consider using private endpoints for security

### Format Detection

If automatic format detection fails:

1. Ensure file extensions are correct
2. Check that files are not corrupted
3. Verify compression format is supported

## Environment Variables

The cloud storage sources respect standard environment variables:

### S3
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_SESSION_TOKEN`
- `AWS_PROFILE`
- `AWS_REGION`

### GCS
- `GOOGLE_APPLICATION_CREDENTIALS`
- `GOOGLE_CLOUD_PROJECT`

### Azure
- `AZURE_STORAGE_ACCOUNT`
- `AZURE_STORAGE_ACCESS_KEY`
- `AZURE_CLIENT_ID`
- `AZURE_CLIENT_SECRET`
- `AZURE_TENANT_ID`