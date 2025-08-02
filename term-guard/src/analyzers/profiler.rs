//! Column profiling with three-pass algorithm for efficient data analysis.
//!
//! The ColumnProfiler implements a three-pass algorithm to efficiently analyze columns:
//!
//! **Pass 1: Basic Statistics and Type Sampling**
//! - Sample data to determine column data types
//! - Count nulls and estimate cardinality  
//! - Compute basic statistics (min/max/mean)
//!
//! **Pass 2: Histogram Computation (Low-Cardinality Columns)**
//! - For columns with cardinality below threshold
//! - Compute exact value distributions
//! - Build categorical histograms
//!
//! **Pass 3: Distribution Analysis (Numeric Columns)**
//! - For numeric columns with high cardinality
//! - Compute quantiles using efficient algorithms
//! - Calculate advanced statistical measures
//!
//! # Example
//!
//! ```rust
//! use term_guard::analyzers::profiler::ColumnProfiler;
//! use term_guard::test_fixtures::create_minimal_tpc_h_context;
//!
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! let profiler = ColumnProfiler::builder()
//!     .cardinality_threshold(100)
//!     .sample_size(10000)
//!     .build();
//!
//! let ctx = create_minimal_tpc_h_context().await.unwrap();
//! let profile = profiler.profile_column(&ctx, "lineitem", "l_returnflag").await.unwrap();
//!
//! println!("Column type: {:?}", profile.data_type);
//! println!("Null percentage: {:.2}%", profile.basic_stats.null_percentage * 100.0);
//! # })
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::{info, instrument};

use crate::analyzers::errors::AnalyzerError;

/// Result type for profiler operations
pub type ProfilerResult<T> = Result<T, AnalyzerError>;

/// Configuration for the three-pass profiling algorithm
#[derive(Debug, Clone)]
pub struct ProfilerConfig {
    /// Cardinality threshold to decide between Pass 2 and Pass 3
    pub cardinality_threshold: u64,
    /// Sample size for Pass 1 type detection
    pub sample_size: u64,
    /// Maximum memory usage in bytes
    pub max_memory_bytes: u64,
    /// Enable parallel processing where possible
    pub enable_parallel: bool,
    /// DataFusion memory pool configuration
    pub memory_pool: MemoryPoolConfig,
}

/// Memory pool configuration for DataFusion
#[derive(Debug, Clone)]
pub struct MemoryPoolConfig {
    /// Memory pool size in bytes (None uses DataFusion defaults)
    pub size: Option<u64>,
    /// Memory pool type
    pub pool_type: MemoryPoolType,
    /// Maximum number of partitions for parallel processing
    pub max_partitions: Option<usize>,
    /// Enable memory manager optimizations
    pub enable_memory_manager: bool,
}

/// Types of memory pools available
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryPoolType {
    /// Greedy memory pool (default) - fast allocation, may use more memory
    Greedy,
    /// Fair memory pool - slower but more memory-efficient
    Fair,
    /// Unbounded memory pool - no memory limits (use with caution)
    Unbounded,
}

impl Default for MemoryPoolConfig {
    fn default() -> Self {
        Self {
            size: None, // Use DataFusion defaults
            pool_type: MemoryPoolType::Greedy,
            max_partitions: None,
            enable_memory_manager: true,
        }
    }
}

impl Default for ProfilerConfig {
    fn default() -> Self {
        Self {
            cardinality_threshold: 100,
            sample_size: 10000,
            max_memory_bytes: 512 * 1024 * 1024, // 512MB
            enable_parallel: true,
            memory_pool: MemoryPoolConfig::default(),
        }
    }
}

/// Progress callback for profiling operations
pub type ProgressCallback = Arc<dyn Fn(ProfilerProgress) + Send + Sync>;

/// Progress information during profiling
#[derive(Debug, Clone)]
pub struct ProfilerProgress {
    pub current_pass: u8,
    pub total_passes: u8,
    pub column_name: String,
    pub message: String,
}

/// Detected data type for a column
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DetectedDataType {
    /// Boolean values (true/false)
    Boolean,
    /// Integer numbers
    Integer,
    /// Floating point numbers
    Double,
    /// Date values
    Date,
    /// Timestamp values
    Timestamp,
    /// String/text values
    String,
    /// Mixed types detected
    Mixed,
    /// Unknown or unable to determine
    Unknown,
}

/// Basic statistics computed in Pass 1
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BasicStatistics {
    pub row_count: u64,
    pub null_count: u64,
    pub null_percentage: f64,
    pub approximate_cardinality: u64,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
    pub sample_values: Vec<String>,
}

/// A bucket for categorical data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoricalBucket {
    pub value: String,
    pub count: u64,
}

/// Histogram data for categorical columns (Pass 2)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoricalHistogram {
    pub buckets: Vec<CategoricalBucket>,
    pub total_count: u64,
    pub entropy: f64,
    pub top_values: Vec<(String, u64)>,
}

/// Distribution analysis for numeric columns (Pass 3)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NumericDistribution {
    pub mean: Option<f64>,
    pub std_dev: Option<f64>,
    pub variance: Option<f64>,
    pub quantiles: HashMap<String, f64>, // P50, P95, P99, etc.
    pub outlier_count: u64,
    pub skewness: Option<f64>,
    pub kurtosis: Option<f64>,
}

/// Complete column profile result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnProfile {
    pub column_name: String,
    pub data_type: DetectedDataType,
    pub basic_stats: BasicStatistics,
    pub categorical_histogram: Option<CategoricalHistogram>,
    pub numeric_distribution: Option<NumericDistribution>,
    pub profiling_time_ms: u64,
    pub passes_executed: Vec<u8>,
}

/// Builder for ColumnProfiler
pub struct ColumnProfilerBuilder {
    config: ProfilerConfig,
    progress_callback: Option<ProgressCallback>,
}

impl ColumnProfilerBuilder {
    /// Set the cardinality threshold for deciding between Pass 2 and Pass 3
    pub fn cardinality_threshold(mut self, threshold: u64) -> Self {
        self.config.cardinality_threshold = threshold;
        self
    }

    /// Set the sample size for Pass 1 type detection
    pub fn sample_size(mut self, size: u64) -> Self {
        self.config.sample_size = size;
        self
    }

    /// Set maximum memory usage in bytes
    pub fn max_memory_bytes(mut self, bytes: u64) -> Self {
        self.config.max_memory_bytes = bytes;
        self
    }

    /// Enable or disable parallel processing
    pub fn enable_parallel(mut self, enable: bool) -> Self {
        self.config.enable_parallel = enable;
        self
    }

    /// Set DataFusion memory pool size in bytes
    pub fn datafusion_memory_pool_size(mut self, size: u64) -> Self {
        self.config.memory_pool.size = Some(size);
        self
    }

    /// Configure the memory pool type
    pub fn memory_pool_type(mut self, pool_type: MemoryPoolType) -> Self {
        self.config.memory_pool.pool_type = pool_type;
        self
    }

    /// Set maximum number of partitions for parallel processing
    pub fn max_partitions(mut self, max_partitions: usize) -> Self {
        self.config.memory_pool.max_partitions = Some(max_partitions);
        self
    }

    /// Enable or disable memory manager optimizations
    pub fn enable_memory_manager(mut self, enable: bool) -> Self {
        self.config.memory_pool.enable_memory_manager = enable;
        self
    }

    /// Configure memory pool with custom configuration
    pub fn memory_pool_config(mut self, config: MemoryPoolConfig) -> Self {
        self.config.memory_pool = config;
        self
    }

    /// Set progress callback
    pub fn progress_callback<F>(mut self, callback: F) -> Self
    where
        F: Fn(ProfilerProgress) + Send + Sync + 'static,
    {
        self.progress_callback = Some(Arc::new(callback));
        self
    }

    /// Build the ColumnProfiler
    pub fn build(self) -> ColumnProfiler {
        ColumnProfiler {
            config: self.config,
            progress_callback: self.progress_callback,
        }
    }
}

/// Main ColumnProfiler that orchestrates the three-pass algorithm
pub struct ColumnProfiler {
    config: ProfilerConfig,
    progress_callback: Option<ProgressCallback>,
}

impl ColumnProfiler {
    /// Create a new builder for ColumnProfiler
    pub fn builder() -> ColumnProfilerBuilder {
        ColumnProfilerBuilder {
            config: ProfilerConfig::default(),
            progress_callback: None,
        }
    }

    /// Apply memory pool configuration to a SessionContext
    pub fn configure_session_context(&self, _ctx: &SessionContext) -> ProfilerResult<()> {
        // Note: DataFusion 48.0 has limited runtime configuration options
        // This method provides a placeholder for future memory pool configuration
        // when DataFusion exposes more runtime configuration APIs

        if let Some(size) = self.config.memory_pool.size {
            // Log the intended memory pool size for debugging
            tracing::info!(
                memory_pool_size = size,
                pool_type = ?self.config.memory_pool.pool_type,
                "Memory pool configuration requested (runtime configuration pending DataFusion API)"
            );
        }

        Ok(())
    }

    /// Create a new SessionContext with optimized memory configuration
    pub fn create_optimized_session_context(&self) -> ProfilerResult<SessionContext> {
        let mut config = datafusion::execution::config::SessionConfig::default();

        // Configure batch size based on memory constraints
        if let Some(memory_size) = self.config.memory_pool.size {
            // Estimate optimal batch size based on available memory
            let estimated_batch_size = (memory_size / (1024 * 1024)).clamp(1024, 8192) as usize;
            config = config.with_batch_size(estimated_batch_size);
        }

        // Configure partitions for parallel processing
        if let Some(max_partitions) = self.config.memory_pool.max_partitions {
            config = config.with_target_partitions(max_partitions);
        }

        // Enable memory manager optimizations
        if self.config.memory_pool.enable_memory_manager {
            // Future: Configure memory manager when API becomes available
            tracing::debug!("Memory manager optimizations enabled");
        }

        let ctx = SessionContext::new_with_config(config);

        Ok(ctx)
    }

    /// Create a ColumnProfiler with default configuration
    pub fn new() -> Self {
        Self::builder().build()
    }

    /// Profile a single column using the three-pass algorithm
    #[instrument(skip(self, ctx))]
    pub async fn profile_column(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        column_name: &str,
    ) -> ProfilerResult<ColumnProfile> {
        let start_time = std::time::Instant::now();
        let mut passes_executed = Vec::new();

        info!(
            table = table_name,
            column = column_name,
            "Starting three-pass column profiling"
        );

        // Pass 1: Basic statistics and type sampling
        self.report_progress(
            1,
            3,
            column_name,
            "Computing basic statistics and type detection",
        );
        let basic_stats = self.execute_pass1(ctx, table_name, column_name).await?;
        let data_type = self.detect_data_type(&basic_stats).await?;
        passes_executed.push(1);

        let mut categorical_histogram = None;
        let mut numeric_distribution = None;

        // Decide between Pass 2 and Pass 3 based on cardinality and data type
        if basic_stats.approximate_cardinality <= self.config.cardinality_threshold {
            // Pass 2: Histogram computation for low-cardinality columns
            self.report_progress(2, 3, column_name, "Computing categorical histogram");
            categorical_histogram = Some(
                self.execute_pass2(ctx, table_name, column_name, &basic_stats)
                    .await?,
            );
            passes_executed.push(2);
        } else if matches!(
            data_type,
            DetectedDataType::Integer | DetectedDataType::Double
        ) {
            // Pass 3: Distribution analysis for numeric columns
            self.report_progress(3, 3, column_name, "Analyzing numeric distribution");
            numeric_distribution = Some(
                self.execute_pass3(ctx, table_name, column_name, &basic_stats)
                    .await?,
            );
            passes_executed.push(3);
        }

        let profiling_time_ms = start_time.elapsed().as_millis() as u64;

        info!(
            table = table_name,
            column = column_name,
            time_ms = profiling_time_ms,
            passes = ?passes_executed,
            "Completed column profiling"
        );

        Ok(ColumnProfile {
            column_name: column_name.to_string(),
            data_type,
            basic_stats,
            categorical_histogram,
            numeric_distribution,
            profiling_time_ms,
            passes_executed,
        })
    }

    /// Profile multiple columns in parallel
    #[instrument(skip(self, ctx))]
    pub async fn profile_columns(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        column_names: &[String],
    ) -> ProfilerResult<Vec<ColumnProfile>> {
        if self.config.enable_parallel && column_names.len() > 1 {
            // Parallel execution
            let mut handles = Vec::new();

            for column_name in column_names {
                let ctx = ctx.clone();
                let table_name = table_name.to_string();
                let column_name = column_name.clone();
                let profiler = self.clone_for_parallel();

                let handle = tokio::spawn(async move {
                    profiler
                        .profile_column(&ctx, &table_name, &column_name)
                        .await
                });
                handles.push(handle);
            }

            let mut results = Vec::new();
            for handle in handles {
                match handle.await {
                    Ok(Ok(profile)) => results.push(profile),
                    Ok(Err(e)) => return Err(e),
                    Err(e) => {
                        return Err(AnalyzerError::execution(format!("Task join error: {e}")))
                    }
                }
            }

            Ok(results)
        } else {
            // Sequential execution
            let mut results = Vec::new();
            for column_name in column_names {
                let profile = self.profile_column(ctx, table_name, column_name).await?;
                results.push(profile);
            }
            Ok(results)
        }
    }

    /// Helper to clone profiler for parallel execution
    fn clone_for_parallel(&self) -> Self {
        Self {
            config: self.config.clone(),
            progress_callback: self.progress_callback.clone(),
        }
    }

    /// Report progress to callback if configured
    fn report_progress(
        &self,
        current_pass: u8,
        total_passes: u8,
        column_name: &str,
        message: &str,
    ) {
        if let Some(callback) = &self.progress_callback {
            callback(ProfilerProgress {
                current_pass,
                total_passes,
                column_name: column_name.to_string(),
                message: message.to_string(),
            });
        }
    }

    /// Execute Pass 1: Basic statistics and type sampling
    #[instrument(skip(self, ctx))]
    async fn execute_pass1(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        column_name: &str,
    ) -> ProfilerResult<BasicStatistics> {
        // Sample data for type detection and basic statistics
        let sample_sql = format!(
            "SELECT {column_name} FROM {table_name} WHERE {column_name} IS NOT NULL LIMIT {}",
            self.config.sample_size
        );

        let sample_df = ctx
            .sql(&sample_sql)
            .await
            .map_err(|e| AnalyzerError::execution(e.to_string()))?;
        let sample_batches = sample_df
            .collect()
            .await
            .map_err(|e| AnalyzerError::execution(e.to_string()))?;

        // Count total rows and nulls
        let stats_sql = format!(
            "SELECT 
                COUNT(*) as total_count,
                COUNT({column_name}) as non_null_count,
                COUNT(DISTINCT {column_name}) as distinct_count
             FROM {table_name}"
        );

        let stats_df = ctx
            .sql(&stats_sql)
            .await
            .map_err(|e| AnalyzerError::execution(e.to_string()))?;
        let stats_batches = stats_df
            .collect()
            .await
            .map_err(|e| AnalyzerError::execution(e.to_string()))?;

        if stats_batches.is_empty() || stats_batches[0].num_rows() == 0 {
            return Err(AnalyzerError::invalid_data(
                "No data found for statistics computation".to_string(),
            ));
        }

        let stats_batch = &stats_batches[0];
        let total_count = self.extract_u64(stats_batch, 0, "total_count")?;
        let non_null_count = self.extract_u64(stats_batch, 1, "non_null_count")?;
        let distinct_count = self.extract_u64(stats_batch, 2, "distinct_count")?;

        let null_count = total_count - non_null_count;
        let null_percentage = if total_count > 0 {
            null_count as f64 / total_count as f64
        } else {
            0.0
        };

        // Extract sample values
        let mut sample_values = Vec::new();
        for batch in &sample_batches {
            if batch.num_rows() > 0 {
                let column_data = batch.column(0);
                for i in 0..batch.num_rows().min(10) {
                    // Limit samples
                    if !column_data.is_null(i) {
                        let value = self.extract_string_value(column_data, i)?;
                        sample_values.push(value);
                    }
                }
            }
        }

        // Get min/max values if numeric-like
        let (min_value, max_value) = self
            .get_min_max_values(ctx, table_name, column_name)
            .await?;

        Ok(BasicStatistics {
            row_count: total_count,
            null_count,
            null_percentage,
            approximate_cardinality: distinct_count,
            min_value,
            max_value,
            sample_values,
        })
    }

    /// Execute Pass 2: Histogram computation for low-cardinality columns
    #[instrument(skip(self, ctx))]
    async fn execute_pass2(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        column_name: &str,
        _basic_stats: &BasicStatistics,
    ) -> ProfilerResult<CategoricalHistogram> {
        // Get exact value distribution
        let histogram_sql = format!(
            "SELECT 
                {column_name} as value,
                COUNT(*) as count
             FROM {table_name}
             WHERE {column_name} IS NOT NULL
             GROUP BY {column_name}
             ORDER BY count DESC"
        );

        let df = ctx
            .sql(&histogram_sql)
            .await
            .map_err(|e| AnalyzerError::execution(e.to_string()))?;
        let batches = df
            .collect()
            .await
            .map_err(|e| AnalyzerError::execution(e.to_string()))?;

        let mut buckets = Vec::new();
        let mut top_values = Vec::new();
        let mut total_count = 0u64;

        for batch in &batches {
            for i in 0..batch.num_rows() {
                let value = self.extract_string_value(batch.column(0), i)?;
                let count = self.extract_u64(batch, 1, "count")?;

                buckets.push(CategoricalBucket {
                    value: value.clone(),
                    count,
                });

                if top_values.len() < 10 {
                    top_values.push((value, count));
                }

                total_count += count;
            }
        }

        // Calculate entropy
        let entropy = self.calculate_entropy(&buckets, total_count);

        Ok(CategoricalHistogram {
            buckets,
            total_count,
            entropy,
            top_values,
        })
    }

    /// Execute Pass 3: Distribution analysis for numeric columns
    #[instrument(skip(self, ctx))]
    async fn execute_pass3(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        column_name: &str,
        _basic_stats: &BasicStatistics,
    ) -> ProfilerResult<NumericDistribution> {
        // Compute advanced statistics for numeric columns
        let stats_sql = format!(
            "SELECT 
                AVG(CAST({column_name} AS DOUBLE)) as mean,
                STDDEV(CAST({column_name} AS DOUBLE)) as std_dev,
                VAR_SAMP(CAST({column_name} AS DOUBLE)) as variance
             FROM {table_name}
             WHERE {column_name} IS NOT NULL"
        );

        let stats_df = ctx
            .sql(&stats_sql)
            .await
            .map_err(|e| AnalyzerError::execution(e.to_string()))?;
        let stats_batches = stats_df
            .collect()
            .await
            .map_err(|e| AnalyzerError::execution(e.to_string()))?;

        let mut mean = None;
        let mut std_dev = None;
        let mut variance = None;

        if !stats_batches.is_empty() && stats_batches[0].num_rows() > 0 {
            let batch = &stats_batches[0];
            mean = self.extract_optional_f64(batch, 0)?;
            std_dev = self.extract_optional_f64(batch, 1)?;
            variance = self.extract_optional_f64(batch, 2)?;
        }

        // Calculate quantiles using APPROX_PERCENTILE if available
        let mut quantiles = HashMap::new();
        let percentiles = vec![("P50", 0.5), ("P90", 0.9), ("P95", 0.95), ("P99", 0.99)];

        for (name, percentile) in percentiles {
            if let Ok(value) = self
                .calculate_percentile(ctx, table_name, column_name, percentile)
                .await
            {
                quantiles.insert(name.to_string(), value);
            }
        }

        // Placeholder for additional statistics
        let outlier_count = 0; // TODO: Implement outlier detection
        let skewness = None; // TODO: Implement skewness calculation
        let kurtosis = None; // TODO: Implement kurtosis calculation

        Ok(NumericDistribution {
            mean,
            std_dev,
            variance,
            quantiles,
            outlier_count,
            skewness,
            kurtosis,
        })
    }

    /// Detect data type from sample values and basic statistics
    async fn detect_data_type(
        &self,
        basic_stats: &BasicStatistics,
    ) -> ProfilerResult<DetectedDataType> {
        if basic_stats.sample_values.is_empty() {
            return Ok(DetectedDataType::Unknown);
        }

        let mut type_counts = HashMap::new();

        for value in &basic_stats.sample_values {
            let detected_type = self.classify_value(value);
            *type_counts.entry(detected_type).or_insert(0) += 1;
        }

        // Find the most common type
        let dominant_type = type_counts
            .into_iter()
            .max_by_key(|(_, count)| *count)
            .map(|(data_type, _)| data_type)
            .unwrap_or(DetectedDataType::Unknown);

        Ok(dominant_type)
    }

    /// Classify a single value to determine its data type
    fn classify_value(&self, value: &str) -> DetectedDataType {
        let trimmed = value.trim();

        // Boolean check
        if trimmed.eq_ignore_ascii_case("true") || trimmed.eq_ignore_ascii_case("false") {
            return DetectedDataType::Boolean;
        }

        // Integer check
        if trimmed.parse::<i64>().is_ok() {
            return DetectedDataType::Integer;
        }

        // Double check
        if trimmed.parse::<f64>().is_ok() {
            return DetectedDataType::Double;
        }

        // Date patterns (simplified)
        if trimmed.len() == 10 && trimmed.matches('-').count() == 2 {
            return DetectedDataType::Date;
        }

        // Timestamp patterns (simplified)
        if trimmed.contains('T') || trimmed.contains(' ') && trimmed.len() > 15 {
            return DetectedDataType::Timestamp;
        }

        DetectedDataType::String
    }

    /// Helper methods for data extraction
    fn extract_u64(
        &self,
        batch: &arrow::record_batch::RecordBatch,
        col_idx: usize,
        col_name: &str,
    ) -> ProfilerResult<u64> {
        use arrow::array::Array;

        let column = batch.column(col_idx);
        if column.is_null(0) {
            return Err(AnalyzerError::invalid_data(format!(
                "Null value in {col_name} column"
            )));
        }

        if let Some(arr) = column.as_any().downcast_ref::<arrow::array::UInt64Array>() {
            Ok(arr.value(0))
        } else if let Some(arr) = column.as_any().downcast_ref::<arrow::array::Int64Array>() {
            Ok(arr.value(0) as u64)
        } else {
            Err(AnalyzerError::invalid_data(format!(
                "Expected integer for {col_name}"
            )))
        }
    }

    fn extract_optional_f64(
        &self,
        batch: &arrow::record_batch::RecordBatch,
        col_idx: usize,
    ) -> ProfilerResult<Option<f64>> {
        use arrow::array::Array;

        let column = batch.column(col_idx);
        if column.is_null(0) {
            return Ok(None);
        }

        if let Some(arr) = column.as_any().downcast_ref::<arrow::array::Float64Array>() {
            Ok(Some(arr.value(0)))
        } else {
            Ok(None)
        }
    }

    fn extract_string_value(
        &self,
        column: &dyn arrow::array::Array,
        row_idx: usize,
    ) -> ProfilerResult<String> {
        if column.is_null(row_idx) {
            return Ok("NULL".to_string());
        }

        if let Some(arr) = column.as_any().downcast_ref::<arrow::array::StringArray>() {
            Ok(arr.value(row_idx).to_string())
        } else if let Some(arr) = column
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
        {
            Ok(arr.value(row_idx).to_string())
        } else if let Some(arr) = column.as_any().downcast_ref::<arrow::array::Int64Array>() {
            Ok(arr.value(row_idx).to_string())
        } else if let Some(arr) = column.as_any().downcast_ref::<arrow::array::Float64Array>() {
            Ok(arr.value(row_idx).to_string())
        } else if let Some(arr) = column.as_any().downcast_ref::<arrow::array::BooleanArray>() {
            Ok(arr.value(row_idx).to_string())
        } else {
            // Generic fallback - use display representation
            Ok("UNKNOWN".to_string())
        }
    }

    async fn get_min_max_values(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        column_name: &str,
    ) -> ProfilerResult<(Option<String>, Option<String>)> {
        let sql = format!(
            "SELECT MIN({column_name}) as min_val, MAX({column_name}) as max_val FROM {table_name} WHERE {column_name} IS NOT NULL"
        );

        let df = ctx
            .sql(&sql)
            .await
            .map_err(|e| AnalyzerError::execution(e.to_string()))?;
        let batches = df
            .collect()
            .await
            .map_err(|e| AnalyzerError::execution(e.to_string()))?;

        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Ok((None, None));
        }

        let batch = &batches[0];
        let min_val = if batch.column(0).is_null(0) {
            None
        } else {
            Some(self.extract_string_value(batch.column(0), 0)?)
        };

        let max_val = if batch.column(1).is_null(0) {
            None
        } else {
            Some(self.extract_string_value(batch.column(1), 0)?)
        };

        Ok((min_val, max_val))
    }

    async fn calculate_percentile(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        column_name: &str,
        percentile: f64,
    ) -> ProfilerResult<f64> {
        // Try to use DataFusion's approx_percentile function first
        let approx_sql = format!(
            "SELECT approx_percentile(CAST({column_name} AS DOUBLE), {percentile}) as percentile_val
             FROM {table_name} 
             WHERE {column_name} IS NOT NULL"
        );

        match ctx.sql(&approx_sql).await {
            Ok(df) => {
                let batches = df
                    .collect()
                    .await
                    .map_err(|e| AnalyzerError::execution(e.to_string()))?;

                if !batches.is_empty() && batches[0].num_rows() > 0 {
                    let batch = &batches[0];
                    if let Some(value) = self.extract_optional_f64(batch, 0)? {
                        return Ok(value);
                    }
                }
            }
            Err(_) => {
                // approx_percentile function not available, use fallback method
                return self
                    .calculate_percentile_fallback(ctx, table_name, column_name, percentile)
                    .await;
            }
        }

        // If approx_percentile returned no result, try fallback
        self.calculate_percentile_fallback(ctx, table_name, column_name, percentile)
            .await
    }

    /// Enhanced fallback percentile calculation with linear interpolation
    async fn calculate_percentile_fallback(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        column_name: &str,
        percentile: f64,
    ) -> ProfilerResult<f64> {
        // First, get the count of non-null values
        let count_sql = format!(
            "SELECT COUNT(*) as total_count FROM {table_name} WHERE {column_name} IS NOT NULL"
        );

        let count_df = ctx
            .sql(&count_sql)
            .await
            .map_err(|e| AnalyzerError::execution(e.to_string()))?;
        let count_batches = count_df
            .collect()
            .await
            .map_err(|e| AnalyzerError::execution(e.to_string()))?;

        if count_batches.is_empty() || count_batches[0].num_rows() == 0 {
            return Err(AnalyzerError::invalid_data(
                "No data found for percentile calculation".to_string(),
            ));
        }

        let total_count = self.extract_u64(&count_batches[0], 0, "total_count")?;
        if total_count == 0 {
            return Err(AnalyzerError::invalid_data(
                "No non-null values for percentile calculation".to_string(),
            ));
        }

        // For small datasets, use simple position calculation
        if total_count <= 100 {
            return self
                .calculate_percentile_simple(ctx, table_name, column_name, percentile, total_count)
                .await;
        }

        // For larger datasets, use linear interpolation method for more accuracy
        let position = percentile * (total_count - 1) as f64;
        let lower_index = position.floor() as u64;
        let upper_index = position.ceil() as u64;
        let fraction = position - position.floor();

        if lower_index == upper_index {
            // Exact position, no interpolation needed
            let percentile_sql = format!(
                "SELECT CAST({column_name} AS DOUBLE) as value 
                 FROM {table_name} 
                 WHERE {column_name} IS NOT NULL 
                 ORDER BY {column_name} 
                 LIMIT 1 OFFSET {lower_index}"
            );

            let percentile_df = ctx
                .sql(&percentile_sql)
                .await
                .map_err(|e| AnalyzerError::execution(e.to_string()))?;
            let percentile_batches = percentile_df
                .collect()
                .await
                .map_err(|e| AnalyzerError::execution(e.to_string()))?;

            if !percentile_batches.is_empty() && percentile_batches[0].num_rows() > 0 {
                let batch = &percentile_batches[0];
                if let Some(value) = self.extract_optional_f64(batch, 0)? {
                    return Ok(value);
                }
            }
        } else {
            // Linear interpolation between two values
            let range_sql = format!(
                "SELECT CAST({column_name} AS DOUBLE) as value 
                 FROM {table_name} 
                 WHERE {column_name} IS NOT NULL 
                 ORDER BY {column_name} 
                 LIMIT 2 OFFSET {lower_index}"
            );

            let range_df = ctx
                .sql(&range_sql)
                .await
                .map_err(|e| AnalyzerError::execution(e.to_string()))?;
            let range_batches = range_df
                .collect()
                .await
                .map_err(|e| AnalyzerError::execution(e.to_string()))?;

            if !range_batches.is_empty() && range_batches[0].num_rows() >= 2 {
                let batch = &range_batches[0];
                if let (Some(lower_value), Some(upper_value)) = (
                    self.extract_optional_f64(batch, 0)?,
                    // Extract from row 1
                    if batch.num_rows() > 1 {
                        let column = batch.column(0);
                        if !column.is_null(1) {
                            column
                                .as_any()
                                .downcast_ref::<arrow::array::Float64Array>()
                                .map(|arr| arr.value(1))
                        } else {
                            None
                        }
                    } else {
                        None
                    },
                ) {
                    // Linear interpolation: lower + fraction * (upper - lower)
                    let interpolated_value = lower_value + fraction * (upper_value - lower_value);
                    return Ok(interpolated_value);
                } else if let Some(lower_value) = self.extract_optional_f64(batch, 0)? {
                    // Fallback to single value if upper value not available
                    return Ok(lower_value);
                }
            }
        }

        Err(AnalyzerError::invalid_data(
            "Failed to calculate percentile using enhanced fallback method".to_string(),
        ))
    }

    /// Simple percentile calculation for small datasets
    async fn calculate_percentile_simple(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        column_name: &str,
        percentile: f64,
        total_count: u64,
    ) -> ProfilerResult<f64> {
        let position = ((total_count as f64 * percentile).ceil() as u64).max(1);
        let offset = position - 1;

        let percentile_sql = format!(
            "SELECT CAST({column_name} AS DOUBLE) as value 
             FROM {table_name} 
             WHERE {column_name} IS NOT NULL 
             ORDER BY {column_name} 
             LIMIT 1 OFFSET {offset}"
        );

        let percentile_df = ctx
            .sql(&percentile_sql)
            .await
            .map_err(|e| AnalyzerError::execution(e.to_string()))?;
        let percentile_batches = percentile_df
            .collect()
            .await
            .map_err(|e| AnalyzerError::execution(e.to_string()))?;

        if !percentile_batches.is_empty() && percentile_batches[0].num_rows() > 0 {
            let batch = &percentile_batches[0];
            if let Some(value) = self.extract_optional_f64(batch, 0)? {
                return Ok(value);
            }
        }

        Err(AnalyzerError::invalid_data(
            "Failed to calculate percentile using simple method".to_string(),
        ))
    }

    fn calculate_entropy(&self, buckets: &[CategoricalBucket], total_count: u64) -> f64 {
        if total_count == 0 {
            return 0.0;
        }

        let mut entropy = 0.0;
        for bucket in buckets {
            if bucket.count > 0 {
                let probability = bucket.count as f64 / total_count as f64;
                entropy -= probability * probability.log2();
            }
        }
        entropy
    }
}

impl Default for ColumnProfiler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_profiler_config_builder() {
        let profiler = ColumnProfiler::builder()
            .cardinality_threshold(200)
            .sample_size(5000)
            .max_memory_bytes(1024 * 1024 * 1024) // 1GB
            .enable_parallel(false)
            .datafusion_memory_pool_size(256 * 1024 * 1024) // 256MB
            .build();

        assert_eq!(profiler.config.cardinality_threshold, 200);
        assert_eq!(profiler.config.sample_size, 5000);
        assert_eq!(profiler.config.max_memory_bytes, 1024 * 1024 * 1024);
        assert!(!profiler.config.enable_parallel);
        assert_eq!(profiler.config.memory_pool.size, Some(256 * 1024 * 1024));

        // Test memory pool configuration methods
        let profiler_advanced = ColumnProfiler::builder()
            .memory_pool_type(MemoryPoolType::Fair)
            .max_partitions(4)
            .enable_memory_manager(false)
            .build();

        assert_eq!(
            profiler_advanced.config.memory_pool.pool_type,
            MemoryPoolType::Fair
        );
        assert_eq!(profiler_advanced.config.memory_pool.max_partitions, Some(4));
        assert!(!profiler_advanced.config.memory_pool.enable_memory_manager);

        // Test custom memory pool config
        let custom_config = MemoryPoolConfig {
            size: Some(1024 * 1024 * 1024), // 1GB
            pool_type: MemoryPoolType::Unbounded,
            max_partitions: Some(8),
            enable_memory_manager: true,
        };

        let profiler_custom = ColumnProfiler::builder()
            .memory_pool_config(custom_config.clone())
            .build();

        assert_eq!(
            profiler_custom.config.memory_pool.size,
            Some(1024 * 1024 * 1024)
        );
        assert_eq!(
            profiler_custom.config.memory_pool.pool_type,
            MemoryPoolType::Unbounded
        );
        assert_eq!(profiler_custom.config.memory_pool.max_partitions, Some(8));
        assert!(profiler_custom.config.memory_pool.enable_memory_manager);
    }

    #[tokio::test]
    async fn test_data_type_detection() {
        let profiler = ColumnProfiler::new();

        assert_eq!(profiler.classify_value("123"), DetectedDataType::Integer);
        assert_eq!(profiler.classify_value("123.45"), DetectedDataType::Double);
        assert_eq!(profiler.classify_value("true"), DetectedDataType::Boolean);
        assert_eq!(profiler.classify_value("hello"), DetectedDataType::String);
    }

    #[tokio::test]
    async fn test_progress_callback() {
        use std::sync::{Arc, Mutex};

        let progress_calls = Arc::new(Mutex::new(Vec::new()));
        let progress_calls_clone = progress_calls.clone();

        let _profiler = ColumnProfiler::builder()
            .progress_callback(move |progress| {
                progress_calls_clone.lock().unwrap().push(progress);
            })
            .build();

        // Progress callback functionality will be tested in integration tests
    }

    #[tokio::test]
    async fn test_memory_pool_configuration() {
        let profiler = ColumnProfiler::builder()
            .memory_pool_type(MemoryPoolType::Fair)
            .max_partitions(8)
            .datafusion_memory_pool_size(1024 * 1024 * 1024) // 1GB
            .enable_memory_manager(false)
            .build();

        assert_eq!(profiler.config.memory_pool.pool_type, MemoryPoolType::Fair);
        assert_eq!(profiler.config.memory_pool.max_partitions, Some(8));
        assert_eq!(profiler.config.memory_pool.size, Some(1024 * 1024 * 1024));
        assert!(!profiler.config.memory_pool.enable_memory_manager);

        // Test optimized session context creation
        let ctx = profiler.create_optimized_session_context();
        assert!(ctx.is_ok());
    }

    #[tokio::test]
    async fn test_percentile_calculation_methods() {
        // This test would need actual data - placeholder for integration tests
        let _profiler = ColumnProfiler::new();

        // Test simple percentile calculation logic
        let total_count = 100u64;
        let percentile = 0.95;
        let position = percentile * (total_count - 1) as f64;
        let lower_index = position.floor() as u64;
        let upper_index = position.ceil() as u64;

        // Position should be 94.05, so floor=94, ceil=95
        assert_eq!(lower_index, 94);
        assert_eq!(upper_index, 95); // Different values, interpolation needed
        assert!(position > 94.0 && position < 95.0); // Rough range check

        // Test interpolation case
        let total_count_small = 10u64;
        let position_small = percentile * (total_count_small - 1) as f64;
        let lower_small = position_small.floor() as u64;
        let upper_small = position_small.ceil() as u64;

        // Position should be 8.55, so lower=8, upper=9
        assert_eq!(lower_small, 8);
        assert_eq!(upper_small, 9); // Different values, interpolation needed
        assert!(position_small > 8.0 && position_small < 9.0);
    }

    #[tokio::test]
    async fn test_enhanced_data_type_classification() {
        let profiler = ColumnProfiler::new();

        // Test various data type classifications
        assert_eq!(profiler.classify_value("true"), DetectedDataType::Boolean);
        assert_eq!(profiler.classify_value("false"), DetectedDataType::Boolean);
        assert_eq!(profiler.classify_value("123"), DetectedDataType::Integer);
        assert_eq!(profiler.classify_value("123.45"), DetectedDataType::Double);
        assert_eq!(
            profiler.classify_value("2023-12-25"),
            DetectedDataType::Date
        );
        assert_eq!(
            profiler.classify_value("2023-12-25T10:30:00"),
            DetectedDataType::Timestamp
        );
        assert_eq!(
            profiler.classify_value("hello world"),
            DetectedDataType::String
        );

        // Test edge cases
        assert_eq!(profiler.classify_value("0"), DetectedDataType::Integer);
        assert_eq!(profiler.classify_value("0.0"), DetectedDataType::Double);
        assert_eq!(profiler.classify_value(" "), DetectedDataType::String);
    }

    #[tokio::test]
    async fn test_memory_pool_type_enum() {
        // Test enum functionality
        let greedy = MemoryPoolType::Greedy;
        let fair = MemoryPoolType::Fair;
        let unbounded = MemoryPoolType::Unbounded;

        assert_ne!(greedy, fair);
        assert_ne!(fair, unbounded);
        assert_ne!(greedy, unbounded);

        // Test debug formatting
        assert_eq!(format!("{greedy:?}"), "Greedy");
        assert_eq!(format!("{fair:?}"), "Fair");
        assert_eq!(format!("{unbounded:?}"), "Unbounded");
    }

    #[tokio::test]
    async fn test_memory_pool_config_defaults() {
        let default_config = MemoryPoolConfig::default();

        assert_eq!(default_config.pool_type, MemoryPoolType::Greedy);
        assert_eq!(default_config.size, None);
        assert_eq!(default_config.max_partitions, None);
        assert!(default_config.enable_memory_manager);
    }

    #[tokio::test]
    async fn test_profiler_config_builder_chain() {
        let profiler = ColumnProfiler::builder()
            .cardinality_threshold(500)
            .sample_size(20000)
            .max_memory_bytes(2 * 1024 * 1024 * 1024) // 2GB
            .enable_parallel(false)
            .memory_pool_type(MemoryPoolType::Unbounded)
            .max_partitions(16)
            .enable_memory_manager(true)
            .datafusion_memory_pool_size(512 * 1024 * 1024) // 512MB
            .build();

        assert_eq!(profiler.config.cardinality_threshold, 500);
        assert_eq!(profiler.config.sample_size, 20000);
        assert_eq!(profiler.config.max_memory_bytes, 2 * 1024 * 1024 * 1024);
        assert!(!profiler.config.enable_parallel);
        assert_eq!(
            profiler.config.memory_pool.pool_type,
            MemoryPoolType::Unbounded
        );
        assert_eq!(profiler.config.memory_pool.max_partitions, Some(16));
        assert!(profiler.config.memory_pool.enable_memory_manager);
        assert_eq!(profiler.config.memory_pool.size, Some(512 * 1024 * 1024));
    }
}
