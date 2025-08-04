//! Incremental computation framework for efficient processing of growing datasets.
//!
//! This module provides infrastructure for stateful metrics computation that can
//! efficiently handle append-only data, partitioned datasets, and incremental updates
//! without reprocessing all historical data.
//!
//! ## Architecture
//!
//! The incremental computation system consists of:
//! - `StateStore`: Abstraction for persisting analyzer states
//! - `IncrementalAnalysisRunner`: Orchestrates incremental analysis across partitions
//! - Enhanced `AnalyzerState` implementations with merge capabilities
//!
//! ## Example
//!
//! ```rust,ignore
//! use term_guard::analyzers::incremental::{IncrementalAnalysisRunner, FileSystemStateStore};
//! use term_guard::analyzers::basic::SizeAnalyzer;
//! use datafusion::prelude::*;
//!
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! // Create state store
//! let state_store = FileSystemStateStore::new("/tmp/term_states")?;
//!
//! // Create incremental runner
//! let mut runner = IncrementalAnalysisRunner::new(
//!     Box::new(state_store),
//!     vec![Box::new(SizeAnalyzer::new())],
//! );
//!
//! // Process new partition
//! let ctx = SessionContext::new();
//! // Register data for "2024-01-15" partition...
//!
//! let results = runner.analyze_partition(&ctx, "2024-01-15").await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! ```

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json;
use tokio::fs;
use tracing::{debug, info, instrument, warn};

use crate::analyzers::{AnalyzerContext, AnalyzerError, AnalyzerResult};

/// Trait for storing and retrieving analyzer states.
///
/// Implementations handle persistence of intermediate computation states,
/// enabling incremental analysis across data partitions.
#[async_trait]
pub trait StateStore: Send + Sync {
    /// Loads the state for a specific partition.
    ///
    /// # Arguments
    /// * `partition` - The partition identifier (e.g., "2024-01-15")
    ///
    /// # Returns
    /// A map of analyzer names to their serialized states
    async fn load_state(&self, partition: &str) -> AnalyzerResult<StateMap>;

    /// Saves the state for a specific partition.
    ///
    /// # Arguments
    /// * `partition` - The partition identifier
    /// * `state` - Map of analyzer names to serialized states
    async fn save_state(&self, partition: &str, state: StateMap) -> AnalyzerResult<()>;

    /// Lists all known partitions.
    ///
    /// # Returns
    /// Vector of partition identifiers ordered by name
    async fn list_partitions(&self) -> AnalyzerResult<Vec<String>>;

    /// Deletes the state for a specific partition.
    ///
    /// # Arguments
    /// * `partition` - The partition identifier to delete
    async fn delete_partition(&self, partition: &str) -> AnalyzerResult<()>;

    /// Loads states for multiple partitions.
    ///
    /// # Arguments
    /// * `partitions` - List of partition identifiers
    ///
    /// # Returns
    /// Map of partition names to their state maps
    async fn load_states_batch(
        &self,
        partitions: &[String],
    ) -> AnalyzerResult<HashMap<String, StateMap>> {
        let mut results = HashMap::new();
        for partition in partitions {
            let state = self.load_state(partition).await?;
            results.insert(partition.clone(), state);
        }
        Ok(results)
    }
}

/// Type alias for state storage - maps analyzer names to serialized states
pub type StateMap = HashMap<String, Vec<u8>>;

/// File system implementation of StateStore.
///
/// Stores states as JSON files organized by partition in a directory structure:
/// ```text
/// base_path/
/// ├── 2024-01-14/
/// │   ├── size.json
/// │   ├── completeness_col1.json
/// │   └── mean_col2.json
/// └── 2024-01-15/
///     └── ...
/// ```
pub struct FileSystemStateStore {
    base_path: PathBuf,
}

impl FileSystemStateStore {
    /// Creates a new file system state store.
    ///
    /// # Arguments
    /// * `base_path` - Directory path for storing state files
    pub fn new<P: AsRef<Path>>(base_path: P) -> AnalyzerResult<Self> {
        let base_path = base_path.as_ref().to_path_buf();

        // Ensure base directory exists
        std::fs::create_dir_all(&base_path).map_err(|e| {
            AnalyzerError::Custom(format!("Failed to create state directory: {e}"))
        })?;

        Ok(Self { base_path })
    }

    /// Gets the directory path for a partition
    fn partition_path(&self, partition: &str) -> PathBuf {
        self.base_path.join(partition)
    }

    /// Gets the file path for a specific analyzer state
    fn state_file_path(&self, partition: &str, analyzer_name: &str) -> PathBuf {
        self.partition_path(partition)
            .join(format!("{analyzer_name}.json"))
    }
}

#[async_trait]
impl StateStore for FileSystemStateStore {
    #[instrument(skip(self))]
    async fn load_state(&self, partition: &str) -> AnalyzerResult<StateMap> {
        let partition_dir = self.partition_path(partition);
        let mut state_map = StateMap::new();

        if !partition_dir.exists() {
            debug!(partition = %partition, "No state directory found");
            return Ok(state_map);
        }

        let mut entries = fs::read_dir(&partition_dir).await.map_err(|e| {
            AnalyzerError::Custom(format!("Failed to read partition directory: {e}"))
        })?;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| AnalyzerError::Custom(format!("Failed to read directory entry: {e}")))?
        {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                let analyzer_name = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .ok_or_else(|| AnalyzerError::Custom("Invalid state file name".to_string()))?;

                let state_data = fs::read(&path).await.map_err(|e| {
                    AnalyzerError::Custom(format!("Failed to read state file: {e}"))
                })?;

                state_map.insert(analyzer_name.to_string(), state_data);
            }
        }

        debug!(partition = %partition, states = state_map.len(), "Loaded partition state");
        Ok(state_map)
    }

    #[instrument(skip(self, state))]
    async fn save_state(&self, partition: &str, state: StateMap) -> AnalyzerResult<()> {
        let partition_dir = self.partition_path(partition);

        // Create partition directory
        fs::create_dir_all(&partition_dir).await.map_err(|e| {
            AnalyzerError::Custom(format!("Failed to create partition directory: {e}"))
        })?;

        // Count states for logging
        let state_count = state.len();

        // Save each analyzer state
        for (analyzer_name, state_data) in state {
            let file_path = self.state_file_path(partition, &analyzer_name);

            fs::write(&file_path, state_data)
                .await
                .map_err(|e| AnalyzerError::Custom(format!("Failed to write state file: {e}")))?;
        }

        debug!(partition = %partition, states = state_count, "Saved partition state");
        Ok(())
    }

    #[instrument(skip(self))]
    async fn list_partitions(&self) -> AnalyzerResult<Vec<String>> {
        let mut partitions = Vec::new();

        if !self.base_path.exists() {
            return Ok(partitions);
        }

        let mut entries = fs::read_dir(&self.base_path)
            .await
            .map_err(|e| AnalyzerError::Custom(format!("Failed to read base directory: {e}")))?;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| AnalyzerError::Custom(format!("Failed to read directory entry: {e}")))?
        {
            if entry
                .file_type()
                .await
                .map_err(|e| AnalyzerError::Custom(format!("Failed to get file type: {e}")))?
                .is_dir()
            {
                if let Some(name) = entry.file_name().to_str() {
                    partitions.push(name.to_string());
                }
            }
        }

        partitions.sort();
        debug!(count = partitions.len(), "Listed partitions");
        Ok(partitions)
    }

    #[instrument(skip(self))]
    async fn delete_partition(&self, partition: &str) -> AnalyzerResult<()> {
        let partition_dir = self.partition_path(partition);

        if partition_dir.exists() {
            fs::remove_dir_all(&partition_dir)
                .await
                .map_err(|e| AnalyzerError::Custom(format!("Failed to delete partition: {e}")))?;
            debug!(partition = %partition, "Deleted partition");
        }

        Ok(())
    }
}

/// Configuration for incremental analysis
#[derive(Debug, Clone)]
pub struct IncrementalConfig {
    /// Whether to fail fast on first error
    pub fail_fast: bool,
    /// Whether to save empty states
    pub save_empty_states: bool,
    /// Maximum number of partitions to merge at once
    pub max_merge_batch_size: usize,
}

impl Default for IncrementalConfig {
    fn default() -> Self {
        Self {
            fail_fast: true,
            save_empty_states: false,
            max_merge_batch_size: 100,
        }
    }
}

/// Type-erased wrapper for serializing analyzer states
#[derive(Serialize, Deserialize)]
struct SerializedState {
    analyzer_type: String,
    state_data: serde_json::Value,
}

/// Orchestrates incremental analysis across partitions.
///
/// This is a placeholder implementation that demonstrates the incremental
/// analysis concepts without full state serialization support.
#[allow(dead_code)]
pub struct IncrementalAnalysisRunner {
    state_store: Box<dyn StateStore>,
    config: IncrementalConfig,
}

impl IncrementalAnalysisRunner {
    /// Creates a new incremental analysis runner (placeholder implementation)
    pub fn new(
        state_store: Box<dyn StateStore>,
        _analyzers: Vec<()>, // Placeholder parameter
    ) -> Self {
        Self {
            state_store,
            config: IncrementalConfig::default(),
        }
    }

    /// Creates a new incremental analysis runner with custom config (placeholder implementation)
    pub fn with_config(
        state_store: Box<dyn StateStore>,
        _analyzers: Vec<()>, // Placeholder parameter
        config: IncrementalConfig,
    ) -> Self {
        Self {
            state_store,
            config,
        }
    }

    /// Analyzes a single partition (placeholder implementation)
    ///
    /// # Arguments
    /// * `_ctx` - DataFusion context with registered data  
    /// * `partition` - Partition identifier
    ///
    /// # Returns
    /// Empty analysis context (placeholder)
    #[instrument(skip(self, _ctx))]
    pub async fn analyze_partition(
        &self,
        _ctx: &SessionContext,
        partition: &str,
    ) -> AnalyzerResult<AnalyzerContext> {
        info!(partition = %partition, "Placeholder partition analysis");

        // Return empty context for now
        Ok(AnalyzerContext::new())
    }

    /// Computes metrics over a range of partitions by merging their states.
    ///
    /// This is a placeholder for now - full implementation would require
    /// more complex state deserialization and type handling.
    ///
    /// # Arguments
    /// * `partitions` - List of partition identifiers to analyze
    ///
    /// # Returns
    /// The merged analysis context with aggregate metrics
    #[instrument(skip(self))]
    pub async fn analyze_partitions(
        &self,
        partitions: &[String],
    ) -> AnalyzerResult<AnalyzerContext> {
        info!(
            partitions = partitions.len(),
            "Analyzing multiple partitions"
        );

        if partitions.is_empty() {
            return Ok(AnalyzerContext::new());
        }

        // For now, return empty context - full implementation would require
        // more sophisticated state deserialization
        warn!("Multi-partition analysis not yet fully implemented");
        Ok(AnalyzerContext::new())
    }

    /// Analyzes new data and merges with existing partition state (placeholder implementation)
    ///
    /// # Arguments
    /// * `_ctx` - DataFusion context with new data
    /// * `partition` - Partition identifier
    #[instrument(skip(self, _ctx))]
    pub async fn analyze_incremental(
        &self,
        _ctx: &SessionContext,
        partition: &str,
    ) -> AnalyzerResult<AnalyzerContext> {
        info!(partition = %partition, "Placeholder incremental analysis");

        // Return empty context for now
        Ok(AnalyzerContext::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_filesystem_state_store() {
        let temp_dir = TempDir::new().unwrap();
        let store = FileSystemStateStore::new(temp_dir.path()).unwrap();

        // Test empty partition list
        let partitions = store.list_partitions().await.unwrap();
        assert!(partitions.is_empty());

        // Test save and load
        let mut state_map = StateMap::new();
        state_map.insert("test_analyzer".to_string(), vec![1, 2, 3, 4]);

        store
            .save_state("2024-01-15", state_map.clone())
            .await
            .unwrap();

        let loaded = store.load_state("2024-01-15").await.unwrap();
        assert_eq!(loaded.get("test_analyzer"), state_map.get("test_analyzer"));

        // Test partition list
        let partitions = store.list_partitions().await.unwrap();
        assert_eq!(partitions, vec!["2024-01-15"]);

        // Test delete
        store.delete_partition("2024-01-15").await.unwrap();
        let partitions = store.list_partitions().await.unwrap();
        assert!(partitions.is_empty());
    }

    #[tokio::test]
    async fn test_incremental_runner_basic() {
        // This test would require implementing proper state serialization
        // for the basic analyzers, which we'll do in the next step
    }
}
