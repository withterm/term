# How to Use the Term Cloud SDK

> **Type**: How-To Guide (Task-oriented)
> **Audience**: Practitioners using Term
> **Goal**: Persist validation metrics to Term Cloud for centralized monitoring and alerting

## Goal

Send validation metrics from Term to Term Cloud for centralized storage, historical analysis, and webhook-based alerting.

## Prerequisites

Before you begin, ensure you have:
- [ ] Term v0.0.2 or later installed
- [ ] A Term Cloud API key (obtain from [Term Cloud Dashboard](https://cloud.term.dev))
- [ ] The `cloud` feature enabled in your `Cargo.toml`

## Enable the Cloud Feature

Add the `cloud` feature to your `Cargo.toml`:

```toml
[dependencies]
term-guard = { version = "0.0.2", features = ["cloud"] }
```

## Quick Start

```rust,ignore
use std::time::Duration;
use term_guard::cloud::{CloudConfig, TermCloudRepository};
use term_guard::repository::{MetricsRepository, ResultKey};
use term_guard::analyzers::AnalyzerContext;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure the cloud connection
    let config = CloudConfig::new("your-api-key")
        .with_buffer_size(1000)
        .with_flush_interval(Duration::from_secs(5));

    // Create the repository
    let repository = TermCloudRepository::new(config)?;

    // Create metrics with tags
    let key = ResultKey::now()
        .with_tag("environment", "production")
        .with_tag("dataset", "orders");

    let mut context = AnalyzerContext::with_dataset("orders_table");
    context.store_metric("completeness.user_id", term_guard::analyzers::MetricValue::Double(0.98));

    // Save metrics (buffered, uploaded asynchronously)
    repository.save(key, context).await?;

    // Always shutdown gracefully
    repository.shutdown().await?;

    Ok(())
}
```

## Configuration Options

The `CloudConfig` builder provides the following options:

| Option | Default | Description |
|--------|---------|-------------|
| `api_key` | Required | Your Term Cloud API key |
| `endpoint` | `https://api.term.dev` | Custom API endpoint |
| `timeout` | 30 seconds | HTTP request timeout |
| `max_retries` | 3 | Maximum retry attempts for failed uploads |
| `buffer_size` | 1000 | Maximum metrics to buffer in memory |
| `batch_size` | 100 | Number of metrics per upload batch |
| `flush_interval` | 5 seconds | How often to flush buffered metrics |
| `offline_cache_path` | Platform default | Path for offline cache database |

### Full Configuration Example

```rust,ignore
use std::time::Duration;
use term_guard::cloud::CloudConfig;

let config = CloudConfig::new("your-api-key")
    .with_endpoint("https://custom.api.endpoint.com")
    .with_timeout(Duration::from_secs(60))
    .with_max_retries(5)
    .with_buffer_size(5000)
    .with_batch_size(200)
    .with_flush_interval(Duration::from_secs(10))
    .with_offline_cache_path("/var/cache/myapp/term_metrics.db");
```

## Tagging Metrics

Use `ResultKey` to add tags that help organize and filter metrics in Term Cloud:

```rust,ignore
use term_guard::repository::ResultKey;

// Create a key with the current timestamp
let key = ResultKey::now()
    .with_tag("environment", "production")
    .with_tag("dataset", "users_table")
    .with_tag("pipeline", "daily-etl")
    .with_tag("version", "1.2.3");

// Or create a key with a specific timestamp
let key = ResultKey::new(1704931200000)  // Unix millis
    .with_tag("env", "staging");

// Add multiple tags at once
use std::collections::HashMap;
let mut tags = HashMap::new();
tags.insert("env".to_string(), "prod".to_string());
tags.insert("region".to_string(), "us-east-1".to_string());

let key = ResultKey::now().with_tags(tags);
```

### Tag Validation Rules

Tags are validated before upload:
- Tag keys cannot be empty
- Tag keys must be 256 characters or fewer
- Tag values must be 1024 characters or fewer
- Maximum 100 tags per key
- Control characters and null bytes are not allowed

## Webhook Alerts

Configure webhook notifications for validation failures:

```rust,ignore
use std::time::Duration;
use term_guard::cloud::{
    AlertPayload, AlertSeverity, CloudValidationResult, WebhookClient, WebhookConfig,
};

// Configure the webhook
let webhook_config = WebhookConfig::new("https://your-webhook.example.com/alerts")
    .with_header("Authorization", "Bearer your-token")
    .with_min_severity(AlertSeverity::Warning)  // Only alert on Warning or Critical
    .with_details(true)  // Include validation details
    .with_secret("your-hmac-secret")  // HMAC-SHA256 signing
    .with_timeout(Duration::from_secs(10));

// Create the webhook client
let webhook_client = WebhookClient::new(webhook_config)?;

// Create an alert from validation results
let validation_result = CloudValidationResult {
    status: "error".to_string(),
    total_checks: 10,
    passed_checks: 7,
    failed_checks: 3,
    issues: vec![],  // Add specific issues here
};

let payload = AlertPayload::from_validation_result(
    &validation_result,
    "orders_table",
    "production"
).with_dashboard_url("https://cloud.term.dev/run/123");

// Send the alert
webhook_client.send(&payload).await?;
```

### Alert Severity Levels

| Severity | Triggered When |
|----------|----------------|
| `Info` | All checks passed |
| `Warning` | Some checks failed (less than 50%) |
| `Critical` | Many checks failed (50%+) or status is "error" |

### Webhook Payload Structure

The webhook receives a JSON payload:

```json
{
  "title": "Validation Critical: 3 of 10 checks failed",
  "severity": "critical",
  "dataset": "orders_table",
  "environment": "production",
  "summary": {
    "total_checks": 10,
    "passed": 7,
    "failed": 3,
    "status": "error"
  },
  "details": [
    {
      "check": "DataQuality",
      "constraint": "Completeness",
      "level": "error",
      "message": "Column 'user_id' has 15% null values",
      "metric": 0.85
    }
  ],
  "timestamp": "2024-01-10T12:00:00Z",
  "dashboard_url": "https://cloud.term.dev/run/123"
}
```

### Webhook Signature Verification

When `with_secret()` is configured, requests include an `X-Signature-256` header with an HMAC-SHA256 signature. Verify it server-side:

```rust,ignore
use term_guard::cloud::WebhookClient;

// Verify the signature on your server
fn verify_signature(body: &str, signature: &str, secret: &str) -> bool {
    let expected = WebhookClient::sign_payload(body, secret);
    format!("sha256={}", expected) == signature
}
```

## Offline Support

The Term Cloud SDK includes an offline cache for resilience against network failures.

### Enable Offline Cache

```rust,ignore
use term_guard::cloud::{CloudConfig, TermCloudRepository};

let config = CloudConfig::new("your-api-key")
    .with_offline_cache_path("/var/cache/myapp/term_metrics.db");

let mut repository = TermCloudRepository::new(config)?;

// Initialize the cache (creates the SQLite database)
repository.setup_cache(None)?;  // Uses path from config

// Or specify a custom path at setup time
repository.setup_cache(Some(std::path::Path::new("/custom/path/cache.db")))?;
```

### Sync Cached Metrics

After recovering from a network outage, sync cached metrics:

```rust,ignore
// Check connectivity first
match repository.health_check().await {
    Ok(response) => {
        println!("Connected to Term Cloud v{}", response.version);

        // Sync any cached metrics
        let synced = repository.sync_offline_cache().await?;
        println!("Synced {} cached metrics", synced);
    }
    Err(e) => {
        eprintln!("Not connected: {}", e);
    }
}
```

### How Offline Mode Works

1. Metrics are first buffered in memory
2. A background worker uploads buffered metrics in batches
3. If upload fails, metrics are saved to the SQLite cache
4. On next successful connection, cached metrics are uploaded
5. During shutdown, any remaining buffered metrics go to cache

## Graceful Shutdown

Always call `shutdown()` to ensure all metrics are uploaded or cached:

```rust,ignore
use term_guard::cloud::{CloudConfig, TermCloudRepository};

let repository = TermCloudRepository::new(config)?;

// ... save metrics ...

// Graceful shutdown: waits for uploads, caches remaining metrics
let stats = repository.shutdown().await?;

if let Some(s) = stats {
    println!("Uploaded {} metrics, {} failed", s.metrics_uploaded, s.metrics_failed);
}
```

### Monitoring Buffer Status

Check the current buffer state before shutdown:

```rust,ignore
let pending = repository.pending_count().await;
println!("Pending metrics: {}", pending);

// Force immediate flush if needed
repository.flush().await?;
```

## Troubleshooting

### Problem: "Offline cache not configured" error
**Solution:** Call `setup_cache()` after creating the repository:
```rust,ignore
let mut repository = TermCloudRepository::new(config)?;
repository.setup_cache(None)?;
```

### Problem: Metrics not appearing in Term Cloud
**Solution:**
1. Check that you're calling `shutdown()` or `flush()` before the program exits
2. Verify your API key is correct
3. Check network connectivity with `health_check()`

### Problem: BufferOverflow error
**Solution:** Increase the buffer size or reduce the flush interval:
```rust,ignore
let config = CloudConfig::new("key")
    .with_buffer_size(10000)
    .with_flush_interval(Duration::from_secs(1));
```

### Problem: Tag validation errors
**Solution:** Ensure tags meet the validation rules:
- Keys cannot be empty
- Keys must be <= 256 characters
- Values must be <= 1024 characters
- No control characters or null bytes

### Problem: Webhook not receiving alerts
**Solution:**
1. Verify the webhook URL is accessible
2. Check that the alert severity meets `min_severity` threshold
3. Ensure HTTPS certificate is valid (or use HTTP for testing)

## Security Considerations

- Store API keys securely (environment variables, secrets manager)
- API keys are never logged; they use `SecureString` internally
- Use HTTPS endpoints in production
- Configure webhook secrets for payload verification
- The offline cache stores metrics locally; secure the cache file appropriately

## Related Guides

- [How to Configure Logging](configure-logging.md) - Enable tracing for debugging
- [How to Optimize Performance](optimize-performance.md) - Performance tuning
- [Reference: Metrics Repository](../reference/metrics-repository.md) - Full API reference

---

<!-- How-To Guide Checklist for Authors:
- [x] Focuses on accomplishing a specific task
- [x] Assumes user already knows Term basics
- [x] Provides complete, working code examples
- [x] Includes common variations
- [x] Has troubleshooting section
- [x] Links to related guides
- [x] Can be completed in under 10 minutes
- [x] Avoids teaching or explaining concepts
-->
