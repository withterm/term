# How to Use the Term Nexus SDK

> **Type**: How-To Guide (Task-oriented)
> **Audience**: Practitioners using Term
> **Goal**: Persist validation metrics to Term Nexus for centralized monitoring

## Goal

Send validation metrics from Term to Term Nexus for centralized storage and historical analysis.

## Prerequisites

Before you begin, ensure you have:
- [ ] Term v0.0.2 or later installed
- [ ] A Term Nexus API key (obtain from [Term Dashboard](https://app.withterm.com))
- [ ] The `nexus` feature enabled in your `Cargo.toml`

## Enable the Nexus Feature

Add the `nexus` feature to your `Cargo.toml`:

```toml
[dependencies]
term-guard = { version = "0.0.2", features = ["nexus"] }
```

## Quick Start

```rust,ignore
use std::time::Duration;
use term_guard::nexus::{NexusConfig, NexusRepository};
use term_guard::repository::{MetricsRepository, ResultKey};
use term_guard::analyzers::AnalyzerContext;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure the nexus connection
    let config = NexusConfig::new("your-api-key")
        .with_buffer_size(1000)
        .with_flush_interval(Duration::from_secs(5));

    // Create the repository
    let repository = NexusRepository::new(config)?;

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

The `NexusConfig` builder provides the following options:

| Option | Default | Description |
|--------|---------|-------------|
| `api_key` | Required | Your Term Nexus API key |
| `endpoint` | `https://api.withterm.com` | Custom API endpoint |
| `timeout` | 30 seconds | HTTP request timeout |
| `max_retries` | 3 | Maximum retry attempts for failed uploads |
| `buffer_size` | 1000 | Maximum metrics to buffer in memory |
| `batch_size` | 100 | Number of metrics per upload batch |
| `flush_interval` | 5 seconds | How often to flush buffered metrics |
| `offline_cache_path` | Platform default | Path for offline cache database |

### Full Configuration Example

```rust,ignore
use std::time::Duration;
use term_guard::nexus::NexusConfig;

let config = NexusConfig::new("your-api-key")
    .with_endpoint("https://api.withterm.com")
    .with_timeout(Duration::from_secs(60))
    .with_max_retries(5)
    .with_buffer_size(5000)
    .with_batch_size(200)
    .with_flush_interval(Duration::from_secs(10))
    .with_offline_cache_path("/var/cache/myapp/term_metrics.db");
```

## Tagging Metrics

Use `ResultKey` to add tags that help organize and filter metrics in Term Nexus:

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

## Offline Support

The Term Nexus SDK includes an offline cache for resilience against network failures.

### Enable Offline Cache

```rust,ignore
use term_guard::nexus::{NexusConfig, NexusRepository};

let config = NexusConfig::new("your-api-key")
    .with_offline_cache_path("/var/cache/myapp/term_metrics.db");

let mut repository = NexusRepository::new(config)?;

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
        println!("Connected to Term Nexus v{}", response.version);

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
use term_guard::nexus::{NexusConfig, NexusRepository};

let repository = NexusRepository::new(config)?;

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
let mut repository = NexusRepository::new(config)?;
repository.setup_cache(None)?;
```

### Problem: Metrics not appearing in Term Nexus
**Solution:**
1. Check that you're calling `shutdown()` or `flush()` before the program exits
2. Verify your API key is correct
3. Check network connectivity with `health_check()`

### Problem: BufferOverflow error
**Solution:** Increase the buffer size or reduce the flush interval:
```rust,ignore
let config = NexusConfig::new("key")
    .with_buffer_size(10000)
    .with_flush_interval(Duration::from_secs(1));
```

### Problem: Tag validation errors
**Solution:** Ensure tags meet the validation rules:
- Keys cannot be empty
- Keys must be <= 256 characters
- Values must be <= 1024 characters
- No control characters or null bytes

## Security Considerations

- Store API keys securely (environment variables, secrets manager)
- API keys are never logged; they use `SecureString` internally
- Use HTTPS endpoints in production
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
