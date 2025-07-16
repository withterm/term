//! Statistics caching for query optimization.

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Cache entry with timestamp.
#[derive(Debug, Clone)]
struct CacheEntry {
    value: f64,
    timestamp: Instant,
}

/// Caches statistics to avoid redundant computations.
#[derive(Debug)]
pub struct StatsCache {
    /// The cache storage
    cache: HashMap<String, CacheEntry>,
    /// Time-to-live for cache entries
    ttl: Duration,
    /// Maximum number of entries
    max_entries: usize,
}

impl StatsCache {
    /// Creates a new statistics cache.
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
            ttl: Duration::from_secs(300), // 5 minutes default
            max_entries: 1000,
        }
    }

    /// Creates a cache with custom configuration.
    pub fn with_config(ttl: Duration, max_entries: usize) -> Self {
        Self {
            cache: HashMap::new(),
            ttl,
            max_entries,
        }
    }

    /// Gets a value from the cache.
    pub fn get(&self, key: &str) -> Option<f64> {
        self.cache.get(key).and_then(|entry| {
            if entry.timestamp.elapsed() < self.ttl {
                Some(entry.value)
            } else {
                None
            }
        })
    }

    /// Sets a value in the cache.
    pub fn set(&mut self, key: String, value: f64) {
        // Evict oldest entries if at capacity
        if self.cache.len() >= self.max_entries {
            self.evict_oldest();
        }

        self.cache.insert(
            key,
            CacheEntry {
                value,
                timestamp: Instant::now(),
            },
        );
    }

    /// Clears the entire cache.
    pub fn clear(&mut self) {
        self.cache.clear();
    }

    /// Removes expired entries.
    pub fn remove_expired(&mut self) {
        let now = Instant::now();
        self.cache
            .retain(|_, entry| now.duration_since(entry.timestamp) < self.ttl);
    }

    /// Gets the current size of the cache.
    pub fn size(&self) -> usize {
        self.cache.len()
    }

    /// Evicts the oldest entry.
    fn evict_oldest(&mut self) {
        if let Some((oldest_key, _)) = self
            .cache
            .iter()
            .min_by_key(|(_, entry)| entry.timestamp)
            .map(|(k, v)| (k.clone(), v.clone()))
        {
            self.cache.remove(&oldest_key);
        }
    }

    /// Gets cache statistics.
    pub fn stats(&self) -> CacheStats {
        let total_entries = self.cache.len();
        let expired_entries = self
            .cache
            .values()
            .filter(|entry| entry.timestamp.elapsed() >= self.ttl)
            .count();

        CacheStats {
            total_entries,
            expired_entries,
            active_entries: total_entries - expired_entries,
        }
    }
}

/// Statistics about the cache.
#[derive(Debug)]
pub struct CacheStats {
    /// Total number of entries
    pub total_entries: usize,
    /// Number of expired entries
    pub expired_entries: usize,
    /// Number of active (non-expired) entries
    pub active_entries: usize,
}

impl Default for StatsCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_basic_operations() {
        let mut cache = StatsCache::new();

        // Test set and get
        cache.set("test_key".to_string(), 42.0);
        assert_eq!(cache.get("test_key"), Some(42.0));

        // Test non-existent key
        assert_eq!(cache.get("missing_key"), None);
    }

    #[test]
    fn test_cache_expiration() {
        let mut cache = StatsCache::with_config(Duration::from_millis(100), 10);

        cache.set("test_key".to_string(), 42.0);
        assert_eq!(cache.get("test_key"), Some(42.0));

        // Wait for expiration
        std::thread::sleep(Duration::from_millis(150));
        assert_eq!(cache.get("test_key"), None);
    }

    #[test]
    fn test_cache_eviction() {
        let mut cache = StatsCache::with_config(Duration::from_secs(60), 2);

        cache.set("key1".to_string(), 1.0);
        cache.set("key2".to_string(), 2.0);

        // This should evict the oldest entry
        cache.set("key3".to_string(), 3.0);

        assert_eq!(cache.size(), 2);
    }

    #[test]
    fn test_cache_stats() {
        let mut cache = StatsCache::new();

        cache.set("key1".to_string(), 1.0);
        cache.set("key2".to_string(), 2.0);

        let stats = cache.stats();
        assert_eq!(stats.total_entries, 2);
        assert_eq!(stats.active_entries, 2);
        assert_eq!(stats.expired_entries, 0);
    }
}
