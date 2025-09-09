//! State storage abstraction for incremental computation.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tokio::fs;
use tracing::{debug, instrument};

use crate::analyzers::{AnalyzerError, AnalyzerResult};

/// Type alias for state storage - maps analyzer names to serialized states
pub type StateMap = HashMap<String, Vec<u8>>;

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
        std::fs::create_dir_all(&base_path)
            .map_err(|e| AnalyzerError::Custom(format!("Failed to create state directory: {e}")))?;

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
