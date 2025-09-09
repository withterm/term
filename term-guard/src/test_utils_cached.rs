//! Cached test utilities for faster test execution.
//!
//! This module provides cached versions of expensive test setup operations,
//! particularly SessionContext creation with TPC-H data.

use datafusion::prelude::SessionContext;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::prelude::*;
use crate::test_utils::{create_tpc_h_context as create_tpc_h_context_uncached, ScaleFactor};

/// Type alias for the context cache to avoid clippy complexity warning
type ContextCacheMap = HashMap<String, Arc<SessionContext>>;
type ContextCache = Arc<RwLock<ContextCacheMap>>;

/// Thread-safe cache for SessionContexts
static CONTEXT_CACHE: Lazy<ContextCache> = Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

/// Creates or retrieves a cached TPC-H context at the specified scale factor.
///
/// This function significantly reduces test execution time by caching
/// SessionContext instances that are expensive to create. The cache is
/// shared across all tests in a single test run.
///
/// # Arguments
///
/// * `scale` - The scale factor for data generation
///
/// # Returns
///
/// An Arc-wrapped SessionContext that can be shared across tests
///
/// # Example
///
/// ```rust,no_run
/// use term_guard::test_utils_cached::{get_or_create_tpc_h_context, ScaleFactor};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let ctx = get_or_create_tpc_h_context(ScaleFactor::SF01).await?;
/// // Use ctx for testing - it may be a cached instance
/// # Ok(())
/// # }
/// ```
pub async fn get_or_create_tpc_h_context(scale: ScaleFactor) -> Result<Arc<SessionContext>> {
    let cache_key = format!("tpc_h_{scale:?}");

    // Try to get from cache with read lock
    {
        let cache = CONTEXT_CACHE.read().await;
        if let Some(ctx) = cache.get(&cache_key) {
            tracing::debug!("Using cached TPC-H context for scale {:?}", scale);
            return Ok(Arc::clone(ctx));
        }
    }

    // Need to create - acquire write lock
    let mut cache = CONTEXT_CACHE.write().await;

    // Double-check after acquiring write lock (another thread might have created it)
    if let Some(ctx) = cache.get(&cache_key) {
        tracing::debug!(
            "Using cached TPC-H context for scale {:?} (created by another thread)",
            scale
        );
        return Ok(Arc::clone(ctx));
    }

    // Create new context
    tracing::info!("Creating new TPC-H context for scale {:?}", scale);
    let ctx = Arc::new(create_tpc_h_context_uncached(scale).await?);
    cache.insert(cache_key, Arc::clone(&ctx));

    Ok(ctx)
}

/// Clears the context cache.
///
/// This can be useful in tests that need to ensure a fresh context
/// or for memory management in long-running test suites.
#[allow(dead_code)]
pub async fn clear_context_cache() {
    let mut cache = CONTEXT_CACHE.write().await;
    cache.clear();
    tracing::info!("Cleared TPC-H context cache");
}

/// Returns the current size of the context cache.
#[allow(dead_code)]
pub async fn cache_size() -> usize {
    let cache = CONTEXT_CACHE.read().await;
    cache.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_context_caching() -> Result<()> {
        // Clear cache to start fresh
        clear_context_cache().await;
        assert_eq!(cache_size().await, 0);

        // First call should create and cache
        let ctx1 = get_or_create_tpc_h_context(ScaleFactor::SF01).await?;
        assert_eq!(cache_size().await, 1);

        // Second call should return cached instance
        let ctx2 = get_or_create_tpc_h_context(ScaleFactor::SF01).await?;
        assert_eq!(cache_size().await, 1);

        // Check they're the same instance
        assert!(Arc::ptr_eq(&ctx1, &ctx2));

        // Different scale factor should create new instance
        let _ctx3 = get_or_create_tpc_h_context(ScaleFactor::SF1).await?;
        assert_eq!(cache_size().await, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_cache_access() -> Result<()> {
        use tokio::task::JoinSet;

        clear_context_cache().await;

        // Spawn multiple tasks trying to get the same context
        let mut tasks = JoinSet::new();

        for i in 0..10 {
            tasks.spawn(async move {
                let ctx = get_or_create_tpc_h_context(ScaleFactor::SF01)
                    .await
                    .expect("Failed to get context");
                (i, ctx)
            });
        }

        let mut contexts = Vec::new();
        while let Some(result) = tasks.join_next().await {
            let (_id, ctx) = result.expect("Task failed");
            contexts.push(ctx);
        }

        // All contexts should be the same instance
        assert_eq!(contexts.len(), 10);
        for ctx in &contexts[1..] {
            assert!(Arc::ptr_eq(&contexts[0], ctx));
        }

        // Cache should have only one entry
        assert_eq!(cache_size().await, 1);

        Ok(())
    }
}
