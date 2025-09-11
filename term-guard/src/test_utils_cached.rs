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
        // Note: We can't reliably test cache size in parallel test execution
        // because other tests may be using the same global cache.
        // We'll focus on testing that caching works correctly.

        // First call should create and cache
        let ctx1 = get_or_create_tpc_h_context(ScaleFactor::SF01).await?;
        let initial_size = cache_size().await;
        assert!(initial_size > 0, "Cache should contain at least one entry");

        // Second call should return cached instance
        let ctx2 = get_or_create_tpc_h_context(ScaleFactor::SF01).await?;
        let size_after_second = cache_size().await;
        assert_eq!(
            size_after_second, initial_size,
            "Cache size shouldn't change for same scale"
        );

        // Check they're the same instance
        assert!(
            Arc::ptr_eq(&ctx1, &ctx2),
            "Should return the same cached instance"
        );

        // Different scale factor should create new instance
        let ctx3 = get_or_create_tpc_h_context(ScaleFactor::SF1).await?;
        let size_after_different = cache_size().await;
        assert!(
            size_after_different > initial_size,
            "Cache should grow with different scale"
        );
        assert!(
            !Arc::ptr_eq(&ctx1, &ctx3),
            "Different scales should have different instances"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_cache_access() -> Result<()> {
        use tokio::task::JoinSet;

        // Use a unique scale factor to avoid interference with other tests
        let scale = ScaleFactor::SF01;

        // Get initial reference to compare against
        let reference_ctx = get_or_create_tpc_h_context(scale).await?;

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

        // All contexts should be the same instance as the reference
        assert_eq!(contexts.len(), 10);
        for ctx in &contexts {
            assert!(
                Arc::ptr_eq(&reference_ctx, ctx),
                "All concurrent accesses should return the same cached instance"
            );
        }

        Ok(())
    }
}
