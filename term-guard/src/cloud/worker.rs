use std::time::{Duration, Instant};

use rand::Rng;
use tokio::sync::watch;
use tokio::time::interval;
use tracing::{debug, error, info, instrument, warn};

use crate::cloud::{
    BufferEntry, CloudConfig, CloudError, CloudMetric, CloudResult, MetricsBuffer, TermCloudClient,
};

/// Background worker for uploading metrics to Term Cloud.
pub struct UploadWorker {
    client: TermCloudClient,
    buffer: MetricsBuffer,
    shutdown: watch::Receiver<bool>,
    batch_size: usize,
    flush_interval: Duration,
    max_retries: u32,
    stats: WorkerStats,
}

/// Statistics from the upload worker.
#[derive(Debug, Clone, Default)]
pub struct WorkerStats {
    pub metrics_uploaded: u64,
    pub metrics_failed: u64,
    pub batches_sent: u64,
    pub retries: u64,
}

impl UploadWorker {
    /// Create a new upload worker.
    ///
    /// # Errors
    ///
    /// Returns an error if the cloud client cannot be created.
    pub fn new(
        config: CloudConfig,
        buffer: MetricsBuffer,
        shutdown: watch::Receiver<bool>,
    ) -> CloudResult<Self> {
        let client = TermCloudClient::new(config.clone())?;

        Ok(Self {
            batch_size: config.batch_size(),
            flush_interval: config.flush_interval(),
            max_retries: config.max_retries(),
            client,
            buffer,
            shutdown,
            stats: WorkerStats::default(),
        })
    }

    /// Run the upload worker until shutdown.
    ///
    /// Returns the accumulated statistics from the worker's operation.
    #[instrument(skip(self))]
    pub async fn run(mut self) -> WorkerStats {
        info!("Upload worker started");
        let mut interval = interval(self.flush_interval);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.flush().await;
                }
                Ok(()) = self.shutdown.changed() => {
                    if *self.shutdown.borrow() {
                        info!("Shutdown signal received, flushing remaining metrics");
                        self.flush_all().await;
                        break;
                    }
                }
            }
        }

        info!(
            "Upload worker stopped: {} metrics uploaded, {} failed, {} batches, {} retries",
            self.stats.metrics_uploaded,
            self.stats.metrics_failed,
            self.stats.batches_sent,
            self.stats.retries
        );
        self.stats
    }

    /// Flush a batch of metrics.
    async fn flush(&mut self) {
        let entries = self.buffer.drain(self.batch_size).await;
        if entries.is_empty() {
            return;
        }

        debug!("Flushing {} metrics", entries.len());
        self.upload_batch(entries).await;
    }

    /// Flush all remaining metrics (used during shutdown).
    async fn flush_all(&mut self) {
        loop {
            let entries = self.buffer.drain(self.batch_size).await;
            if entries.is_empty() {
                break;
            }
            self.upload_batch(entries).await;
        }
    }

    /// Upload a batch of metrics, handling retries.
    async fn upload_batch(&mut self, entries: Vec<BufferEntry>) {
        let metrics: Vec<CloudMetric> = entries.iter().map(|e| e.metric.clone()).collect();
        let batch_size = entries.len() as u64;

        match self.client.ingest(&metrics).await {
            Ok(response) => {
                debug!(
                    "Batch uploaded: {} accepted, {} rejected",
                    response.accepted, response.rejected
                );
                self.stats.metrics_uploaded += response.accepted as u64;
                self.stats.metrics_failed += response.rejected as u64;
                self.stats.batches_sent += 1;
                if !response.errors.is_empty() {
                    warn!("Upload errors: {:?}", response.errors);
                }
            }
            Err(e) if e.is_retryable() => {
                warn!("Retryable error uploading batch: {}", e);
                self.handle_retry(entries, &e).await;
            }
            Err(e) => {
                error!("Non-retryable error uploading batch: {}", e);
                self.stats.metrics_failed += batch_size;
            }
        }
    }

    /// Handle retrying failed entries with exponential backoff.
    ///
    /// Re-queues entries with a `ready_at` timestamp calculated via exponential
    /// backoff. The buffer's `drain()` method respects this timestamp, ensuring
    /// entries are not retried until their backoff period has elapsed.
    async fn handle_retry(&mut self, entries: Vec<BufferEntry>, error: &CloudError) {
        let retry_after = error.retry_after();

        for entry in entries {
            if entry.retry_count < self.max_retries {
                let backoff = self.calculate_backoff(entry.retry_count, retry_after);
                let ready_at = Instant::now() + backoff;

                self.stats.retries += 1;
                if let Err(e) = self.buffer.push_retry(entry, ready_at).await {
                    warn!("Failed to requeue metric for retry: {}", e);
                    self.stats.metrics_failed += 1;
                }
            } else {
                warn!("Dropping metric after {} retries", entry.retry_count);
                self.stats.metrics_failed += 1;
            }
        }
    }

    /// Calculate exponential backoff delay with jitter.
    ///
    /// Uses the formula: base_delay * 2^retry_count + jitter
    /// where retry_count is capped at 5 (max 32x multiplier).
    fn calculate_backoff(&self, retry_count: u32, retry_after: Option<u64>) -> Duration {
        let base_delay = retry_after.unwrap_or(1);
        let capped_retry = retry_count.min(5);
        let backoff_secs = base_delay * (1 << capped_retry);

        let jitter_ms = rand::rng().random_range(0..1000);
        Duration::from_secs(backoff_secs) + Duration::from_millis(jitter_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_worker_shutdown() {
        let config = CloudConfig::new("test-key")
            .with_endpoint("http://localhost:1")
            .with_flush_interval(Duration::from_millis(100));

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let buffer = MetricsBuffer::new(100);

        let worker = UploadWorker::new(config, buffer.clone(), shutdown_rx).unwrap();

        let handle = tokio::spawn(async move { worker.run().await });

        shutdown_tx.send(true).unwrap();

        let result = tokio::time::timeout(Duration::from_secs(1), handle).await;
        assert!(result.is_ok());
        let stats = result.unwrap().unwrap();
        assert_eq!(stats.metrics_uploaded, 0);
    }

    #[tokio::test]
    async fn test_worker_returns_stats() {
        let config = CloudConfig::new("test-key")
            .with_endpoint("http://localhost:1")
            .with_flush_interval(Duration::from_millis(50));

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let buffer = MetricsBuffer::new(100);

        let worker = UploadWorker::new(config, buffer.clone(), shutdown_rx).unwrap();

        let handle = tokio::spawn(async move { worker.run().await });

        tokio::time::sleep(Duration::from_millis(100)).await;
        shutdown_tx.send(true).unwrap();

        let result = tokio::time::timeout(Duration::from_secs(1), handle).await;
        assert!(result.is_ok());
        let stats = result.unwrap().unwrap();
        assert_eq!(stats.batches_sent, 0);
        assert_eq!(stats.retries, 0);
    }

    #[test]
    fn test_calculate_backoff() {
        let config = CloudConfig::new("test-key").with_endpoint("http://localhost:1");

        let (_, shutdown_rx) = watch::channel(false);
        let buffer = MetricsBuffer::new(100);
        let worker = UploadWorker::new(config, buffer, shutdown_rx).unwrap();

        let delay0 = worker.calculate_backoff(0, Some(1));
        assert!(delay0 >= Duration::from_secs(1));
        assert!(delay0 < Duration::from_secs(2));

        let delay1 = worker.calculate_backoff(1, Some(1));
        assert!(delay1 >= Duration::from_secs(2));
        assert!(delay1 < Duration::from_secs(3));

        let delay5 = worker.calculate_backoff(5, Some(1));
        assert!(delay5 >= Duration::from_secs(32));
        assert!(delay5 < Duration::from_secs(33));

        let delay_capped = worker.calculate_backoff(10, Some(1));
        assert!(delay_capped >= Duration::from_secs(32));
        assert!(delay_capped < Duration::from_secs(33));
    }

    #[test]
    fn test_calculate_backoff_uses_retry_after() {
        let config = CloudConfig::new("test-key").with_endpoint("http://localhost:1");

        let (_, shutdown_rx) = watch::channel(false);
        let buffer = MetricsBuffer::new(100);
        let worker = UploadWorker::new(config, buffer, shutdown_rx).unwrap();

        let delay = worker.calculate_backoff(0, Some(5));
        assert!(delay >= Duration::from_secs(5));
        assert!(delay < Duration::from_secs(6));

        let delay_with_backoff = worker.calculate_backoff(2, Some(5));
        assert!(delay_with_backoff >= Duration::from_secs(20));
        assert!(delay_with_backoff < Duration::from_secs(21));
    }
}
