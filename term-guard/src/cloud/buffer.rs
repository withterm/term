use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::cloud::{CloudError, CloudMetric, CloudResult};

/// Entry in the metrics buffer with retry metadata.
#[derive(Debug, Clone)]
pub struct BufferEntry {
    pub metric: CloudMetric,
    pub retry_count: u32,
    pub queued_at: std::time::Instant,
}

/// In-memory buffer for pending metrics uploads.
pub struct MetricsBuffer {
    entries: Arc<Mutex<VecDeque<BufferEntry>>>,
    max_size: usize,
}

impl MetricsBuffer {
    /// Create a new buffer with the given maximum size.
    pub fn new(max_size: usize) -> Self {
        Self {
            entries: Arc::new(Mutex::new(VecDeque::with_capacity(max_size))),
            max_size,
        }
    }

    /// Push a metric to the buffer.
    pub async fn push(&self, metric: CloudMetric) -> CloudResult<()> {
        let mut entries = self.entries.lock().await;

        if entries.len() >= self.max_size {
            return Err(CloudError::BufferOverflow {
                pending_count: entries.len(),
                max_size: self.max_size,
            });
        }

        entries.push_back(BufferEntry {
            metric,
            retry_count: 0,
            queued_at: std::time::Instant::now(),
        });

        Ok(())
    }

    /// Push a metric for retry (increments retry count).
    pub async fn push_retry(&self, mut entry: BufferEntry) -> CloudResult<()> {
        let mut entries = self.entries.lock().await;

        if entries.len() >= self.max_size {
            return Err(CloudError::BufferOverflow {
                pending_count: entries.len(),
                max_size: self.max_size,
            });
        }

        entry.retry_count += 1;
        entries.push_back(entry);

        Ok(())
    }

    /// Drain up to `count` entries from the buffer.
    pub async fn drain(&self, count: usize) -> Vec<BufferEntry> {
        let mut entries = self.entries.lock().await;
        let drain_count = std::cmp::min(count, entries.len());
        entries.drain(..drain_count).collect()
    }

    /// Get the current number of entries in the buffer.
    pub async fn len(&self) -> usize {
        self.entries.lock().await.len()
    }

    /// Check if the buffer is empty.
    pub async fn is_empty(&self) -> bool {
        self.entries.lock().await.is_empty()
    }

    /// Get all entries without removing them (for persistence).
    pub async fn peek_all(&self) -> Vec<BufferEntry> {
        self.entries.lock().await.iter().cloned().collect()
    }

    /// Clear the buffer and return all entries.
    pub async fn clear(&self) -> Vec<BufferEntry> {
        let mut entries = self.entries.lock().await;
        std::mem::take(&mut *entries).into_iter().collect()
    }
}

impl Clone for MetricsBuffer {
    fn clone(&self) -> Self {
        Self {
            entries: Arc::clone(&self.entries),
            max_size: self.max_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cloud::{CloudMetadata, CloudResultKey};
    use std::collections::HashMap;

    fn make_test_metric() -> CloudMetric {
        CloudMetric {
            result_key: CloudResultKey {
                dataset_date: 1704931200000,
                tags: HashMap::new(),
            },
            metrics: HashMap::new(),
            metadata: CloudMetadata {
                dataset_name: Some("test".to_string()),
                start_time: None,
                end_time: None,
                term_version: "0.0.2".to_string(),
                custom: HashMap::new(),
            },
            validation_result: None,
        }
    }

    #[tokio::test]
    async fn test_buffer_push_and_drain() {
        let buffer = MetricsBuffer::new(100);

        buffer.push(make_test_metric()).await.unwrap();
        buffer.push(make_test_metric()).await.unwrap();

        assert_eq!(buffer.len().await, 2);

        let drained = buffer.drain(10).await;
        assert_eq!(drained.len(), 2);
        assert_eq!(buffer.len().await, 0);
    }

    #[tokio::test]
    async fn test_buffer_overflow() {
        let buffer = MetricsBuffer::new(2);

        buffer.push(make_test_metric()).await.unwrap();
        buffer.push(make_test_metric()).await.unwrap();

        let result = buffer.push(make_test_metric()).await;
        assert!(matches!(result, Err(CloudError::BufferOverflow { .. })));
    }

    #[tokio::test]
    async fn test_buffer_push_retry() {
        let buffer = MetricsBuffer::new(10);

        buffer.push(make_test_metric()).await.unwrap();
        let mut drained = buffer.drain(1).await;
        let entry = drained.pop().unwrap();
        assert_eq!(entry.retry_count, 0);

        buffer.push_retry(entry).await.unwrap();
        let mut drained = buffer.drain(1).await;
        let entry = drained.pop().unwrap();
        assert_eq!(entry.retry_count, 1);
    }

    #[tokio::test]
    async fn test_buffer_peek_all() {
        let buffer = MetricsBuffer::new(100);

        buffer.push(make_test_metric()).await.unwrap();
        buffer.push(make_test_metric()).await.unwrap();

        let peeked = buffer.peek_all().await;
        assert_eq!(peeked.len(), 2);
        assert_eq!(buffer.len().await, 2);
    }

    #[tokio::test]
    async fn test_buffer_clear() {
        let buffer = MetricsBuffer::new(100);

        buffer.push(make_test_metric()).await.unwrap();
        buffer.push(make_test_metric()).await.unwrap();

        let cleared = buffer.clear().await;
        assert_eq!(cleared.len(), 2);
        assert!(buffer.is_empty().await);
    }

    #[tokio::test]
    async fn test_buffer_clone_shares_state() {
        let buffer1 = MetricsBuffer::new(100);
        let buffer2 = buffer1.clone();

        buffer1.push(make_test_metric()).await.unwrap();
        assert_eq!(buffer2.len().await, 1);

        buffer2.push(make_test_metric()).await.unwrap();
        assert_eq!(buffer1.len().await, 2);
    }

    #[tokio::test]
    async fn test_buffer_is_empty() {
        let buffer = MetricsBuffer::new(100);
        assert!(buffer.is_empty().await);

        buffer.push(make_test_metric()).await.unwrap();
        assert!(!buffer.is_empty().await);
    }
}
