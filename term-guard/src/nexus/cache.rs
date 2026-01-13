//! Offline cache for metrics persistence when network is unavailable.

use std::path::Path;
use std::sync::Mutex;
use std::time::Instant;

use rusqlite::Connection;
use tracing::warn;

use crate::nexus::{BufferEntry, NexusError, NexusMetric, NexusResult};

/// Entry loaded from the cache, with its database ID for selective deletion.
#[derive(Debug)]
pub struct CacheEntry {
    /// Database ID for this entry, used with `delete_ids()`.
    pub id: i64,
    /// The buffered metric entry.
    pub entry: BufferEntry,
}

/// SQLite-backed offline cache for metrics persistence.
pub struct OfflineCache {
    conn: Mutex<Connection>,
}

impl OfflineCache {
    /// Create or open a cache at the given file path.
    pub fn new(path: &Path) -> NexusResult<Self> {
        let conn = Connection::open(path).map_err(|e| NexusError::CacheError {
            message: format!("Failed to open cache database: {e}"),
        })?;

        let cache = Self {
            conn: Mutex::new(conn),
        };
        cache.init_schema()?;
        Ok(cache)
    }

    /// Create an in-memory cache for testing.
    pub fn in_memory() -> NexusResult<Self> {
        let conn = Connection::open_in_memory().map_err(|e| NexusError::CacheError {
            message: format!("Failed to create in-memory cache: {e}"),
        })?;

        let cache = Self {
            conn: Mutex::new(conn),
        };
        cache.init_schema()?;
        Ok(cache)
    }

    fn init_schema(&self) -> NexusResult<()> {
        let conn = self.conn.lock().map_err(|e| NexusError::CacheError {
            message: format!("Failed to acquire lock: {e}"),
        })?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS pending_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                metric_json TEXT NOT NULL,
                retry_count INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
            )",
            [],
        )
        .map_err(|e| NexusError::CacheError {
            message: format!("Failed to create schema: {e}"),
        })?;

        Ok(())
    }

    /// Save a metric to the cache.
    pub fn save(&self, metric: &NexusMetric, retry_count: u32) -> NexusResult<()> {
        let metric_json = serde_json::to_string(metric).map_err(|e| NexusError::CacheError {
            message: format!("Failed to serialize metric: {e}"),
        })?;

        let conn = self.conn.lock().map_err(|e| NexusError::CacheError {
            message: format!("Failed to acquire lock: {e}"),
        })?;

        conn.execute(
            "INSERT INTO pending_metrics (metric_json, retry_count) VALUES (?1, ?2)",
            rusqlite::params![metric_json, retry_count],
        )
        .map_err(|e| NexusError::CacheError {
            message: format!("Failed to save metric: {e}"),
        })?;

        Ok(())
    }

    /// Load all pending metrics from the cache.
    ///
    /// Returns entries with their database IDs for selective deletion after successful upload.
    pub fn load_all(&self) -> NexusResult<Vec<CacheEntry>> {
        let conn = self.conn.lock().map_err(|e| NexusError::CacheError {
            message: format!("Failed to acquire lock: {e}"),
        })?;

        let mut stmt = conn
            .prepare("SELECT id, metric_json, retry_count FROM pending_metrics ORDER BY id")
            .map_err(|e| NexusError::CacheError {
                message: format!("Failed to prepare query: {e}"),
            })?;

        let now = Instant::now();
        let entries = stmt
            .query_map([], |row| {
                let id: i64 = row.get(0)?;
                let metric_json: String = row.get(1)?;
                let retry_count: u32 = row.get(2)?;
                Ok((id, metric_json, retry_count))
            })
            .map_err(|e| NexusError::CacheError {
                message: format!("Failed to query metrics: {e}"),
            })?
            .filter_map(|result| match result {
                Ok((id, json, retry_count)) => match serde_json::from_str::<NexusMetric>(&json) {
                    Ok(metric) => Some(CacheEntry {
                        id,
                        entry: BufferEntry {
                            metric,
                            retry_count,
                            queued_at: now,
                            ready_at: now,
                        },
                    }),
                    Err(e) => {
                        warn!("Failed to deserialize cached metric (id={}): {}", id, e);
                        None
                    }
                },
                Err(e) => {
                    warn!("Failed to read cache row: {}", e);
                    None
                }
            })
            .collect();

        Ok(entries)
    }

    /// Delete specific entries by their database IDs.
    ///
    /// Returns the number of entries deleted.
    pub fn delete_ids(&self, ids: &[i64]) -> NexusResult<usize> {
        if ids.is_empty() {
            return Ok(0);
        }

        let conn = self.conn.lock().map_err(|e| NexusError::CacheError {
            message: format!("Failed to acquire lock: {e}"),
        })?;

        let placeholders: Vec<_> = ids.iter().map(|_| "?").collect();
        let sql = format!(
            "DELETE FROM pending_metrics WHERE id IN ({})",
            placeholders.join(", ")
        );

        let mut stmt = conn.prepare(&sql).map_err(|e| NexusError::CacheError {
            message: format!("Failed to prepare delete query: {e}"),
        })?;

        let deleted = stmt
            .execute(rusqlite::params_from_iter(ids.iter()))
            .map_err(|e| NexusError::CacheError {
                message: format!("Failed to delete metrics: {e}"),
            })?;

        Ok(deleted)
    }

    /// Remove all cached entries.
    pub fn clear(&self) -> NexusResult<()> {
        let conn = self.conn.lock().map_err(|e| NexusError::CacheError {
            message: format!("Failed to acquire lock: {e}"),
        })?;

        conn.execute("DELETE FROM pending_metrics", [])
            .map_err(|e| NexusError::CacheError {
                message: format!("Failed to clear cache: {e}"),
            })?;

        Ok(())
    }

    /// Get count of pending metrics.
    pub fn count(&self) -> NexusResult<usize> {
        let conn = self.conn.lock().map_err(|e| NexusError::CacheError {
            message: format!("Failed to acquire lock: {e}"),
        })?;

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM pending_metrics", [], |row| row.get(0))
            .map_err(|e| NexusError::CacheError {
                message: format!("Failed to count metrics: {e}"),
            })?;

        Ok(count as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nexus::{NexusMetadata, NexusResultKey};
    use std::collections::HashMap;

    fn make_test_metric() -> NexusMetric {
        NexusMetric {
            result_key: NexusResultKey {
                dataset_date: 1704931200000,
                tags: HashMap::new(),
            },
            metrics: HashMap::new(),
            metadata: NexusMetadata {
                dataset_name: Some("test".to_string()),
                start_time: None,
                end_time: None,
                term_version: "0.0.2".to_string(),
                custom: HashMap::new(),
            },
            validation_result: None,
        }
    }

    #[test]
    fn test_cache_save_and_load() {
        let cache = OfflineCache::in_memory().unwrap();

        let metric1 = make_test_metric();
        let metric2 = make_test_metric();

        cache.save(&metric1, 0).unwrap();
        cache.save(&metric2, 2).unwrap();

        assert_eq!(cache.count().unwrap(), 2);

        let entries = cache.load_all().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].entry.retry_count, 0);
        assert_eq!(entries[1].entry.retry_count, 2);
        assert!(entries[0].id < entries[1].id);
    }

    #[test]
    fn test_cache_clear() {
        let cache = OfflineCache::in_memory().unwrap();

        cache.save(&make_test_metric(), 0).unwrap();
        cache.save(&make_test_metric(), 0).unwrap();

        assert_eq!(cache.count().unwrap(), 2);

        cache.clear().unwrap();

        assert_eq!(cache.count().unwrap(), 0);
        assert!(cache.load_all().unwrap().is_empty());
    }

    #[test]
    fn test_cache_file_persistence() {
        let temp_dir = tempfile::tempdir().unwrap();
        let cache_path = temp_dir.path().join("metrics.db");

        {
            let cache = OfflineCache::new(&cache_path).unwrap();
            cache.save(&make_test_metric(), 1).unwrap();
            assert_eq!(cache.count().unwrap(), 1);
        }

        {
            let cache = OfflineCache::new(&cache_path).unwrap();
            assert_eq!(cache.count().unwrap(), 1);
            let entries = cache.load_all().unwrap();
            assert_eq!(entries[0].entry.retry_count, 1);
        }
    }

    #[test]
    fn test_cache_delete_ids() {
        let cache = OfflineCache::in_memory().unwrap();

        cache.save(&make_test_metric(), 0).unwrap();
        cache.save(&make_test_metric(), 1).unwrap();
        cache.save(&make_test_metric(), 2).unwrap();

        assert_eq!(cache.count().unwrap(), 3);

        let entries = cache.load_all().unwrap();
        let ids_to_delete: Vec<i64> = vec![entries[0].id, entries[2].id];

        let deleted = cache.delete_ids(&ids_to_delete).unwrap();
        assert_eq!(deleted, 2);
        assert_eq!(cache.count().unwrap(), 1);

        let remaining = cache.load_all().unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].entry.retry_count, 1);
    }

    #[test]
    fn test_cache_delete_ids_empty() {
        let cache = OfflineCache::in_memory().unwrap();
        cache.save(&make_test_metric(), 0).unwrap();

        let deleted = cache.delete_ids(&[]).unwrap();
        assert_eq!(deleted, 0);
        assert_eq!(cache.count().unwrap(), 1);
    }

    #[test]
    fn test_cache_empty_load() {
        let cache = OfflineCache::in_memory().unwrap();
        let entries = cache.load_all().unwrap();
        assert!(entries.is_empty());
    }
}
