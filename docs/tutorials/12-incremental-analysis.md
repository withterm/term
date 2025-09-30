# Tutorial: Incremental Analysis for Efficient Processing

## Introduction

In this tutorial, you'll learn how to use Term's incremental analysis framework to efficiently process growing datasets without recomputing metrics from scratch. This is essential for handling append-only data, partitioned datasets, and continuous data streams.

## What You'll Learn

- How incremental analysis works with algebraic states
- How to process partitioned datasets efficiently
- How to merge analyzer states across partitions
- How to implement stateful metric computation
- How to build incremental validation pipelines

## Prerequisites

Before starting, you should:
- Understand Term's analyzer framework
- Have completed the [Understanding Analyzers tutorial](./03-understanding-analyzers.md)
- Have partitioned or time-series data available

## Step 1: Understanding Incremental Analysis

Incremental analysis works by storing intermediate algebraic states that can be merged:

```rust
use term_guard::analyzers::incremental::{IncrementalAnalysisRunner, StateStore};

// Incremental analysis flow:
// 1. Compute state for each partition independently
// 2. Store states in a persistent state store
// 3. Merge states to get overall metrics
// 4. Add new partitions without reprocessing old data
```

## Step 2: Your First Incremental Analysis

Let's process daily partitions incrementally:

```rust
use term_guard::prelude::*;
use term_guard::analyzers::incremental::{
    IncrementalAnalysisRunner,
    FileSystemStateStore,
    IncrementalConfig
};
use term_guard::analyzers::basic::{SizeAnalyzer, CompletenessAnalyzer, MeanAnalyzer};
use datafusion::prelude::*;
use chrono::{Duration, Utc};

#[tokio::main]
async fn main() -> Result<()> {
    // Create state store for persistence
    let state_store = FileSystemStateStore::new("./analyzer_states")?;
    
    // Configure incremental runner
    let config = IncrementalConfig {
        enable_checkpointing: true,
        checkpoint_interval: Duration::hours(1),
        max_partitions_per_run: 10,
        parallel_processing: true,
    };
    
    // Create incremental runner with analyzers
    let mut runner = IncrementalAnalysisRunner::new(Box::new(state_store))
        .with_config(config)
        .add_analyzer(Box::new(SizeAnalyzer::new()))
        .add_analyzer(Box::new(CompletenessAnalyzer::new("order_id")))
        .add_analyzer(Box::new(MeanAnalyzer::new("amount")));
    
    // Process daily partitions
    for day in 1..=7 {
        println!("\n📅 Processing day {}", day);
        
        // Load partition data
        let ctx = SessionContext::new();
        ctx.register_parquet(
            "orders",
            &format!("data/orders/date=2024-01-{:02}/", day),
            ParquetReadOptions::default()
        ).await?;
        
        // Analyze partition and update state
        let partition_key = format!("2024-01-{:02}", day);
        let results = runner.analyze_partition(&ctx, &partition_key).await?;
        
        // Get cumulative metrics
        let cumulative = runner.get_cumulative_results().await?;
        
        println!("Partition metrics:");
        println!("  Rows: {}", results.get_metric("row_count").unwrap());
        println!("  Completeness: {:.2}%", 
                 results.get_metric("order_id_completeness").unwrap() * 100.0);
        
        println!("Cumulative metrics (days 1-{}):", day);
        println!("  Total rows: {}", cumulative.get_metric("row_count").unwrap());
        println!("  Avg completeness: {:.2}%", 
                 cumulative.get_metric("order_id_completeness").unwrap() * 100.0);
        println!("  Overall mean: ${:.2}", 
                 cumulative.get_metric("amount_mean").unwrap());
    }
    
    Ok(())
}
```

## Step 3: Understanding Algebraic States

Learn how different analyzers maintain mergeable states:

```rust
use term_guard::analyzers::traits::AnalyzerState;

// Example: How Size analyzer maintains state
#[derive(Clone, Serialize, Deserialize)]
struct SizeState {
    count: i64,
}

impl AnalyzerState for SizeState {
    fn merge(&mut self, other: &Self) -> Result<()> {
        self.count += other.count;
        Ok(())
    }
    
    fn to_metric(&self) -> MetricValue {
        MetricValue::Count(self.count)
    }
}

// Example: How Mean analyzer maintains state
#[derive(Clone, Serialize, Deserialize)]
struct MeanState {
    sum: f64,
    count: i64,
}

impl AnalyzerState for MeanState {
    fn merge(&mut self, other: &Self) -> Result<()> {
        self.sum += other.sum;
        self.count += other.count;
        Ok(())
    }
    
    fn to_metric(&self) -> MetricValue {
        MetricValue::Float(self.sum / self.count as f64)
    }
}

// Complex example: Standard deviation state
#[derive(Clone, Serialize, Deserialize)]
struct StdDevState {
    count: i64,
    sum: f64,
    sum_squares: f64,
}

impl AnalyzerState for StdDevState {
    fn merge(&mut self, other: &Self) -> Result<()> {
        self.count += other.count;
        self.sum += other.sum;
        self.sum_squares += other.sum_squares;
        Ok(())
    }
    
    fn to_metric(&self) -> MetricValue {
        let mean = self.sum / self.count as f64;
        let variance = (self.sum_squares / self.count as f64) - mean.powi(2);
        MetricValue::Float(variance.sqrt())
    }
}
```

## Step 4: Processing Large Partitioned Datasets

Handle massive datasets with thousands of partitions:

```rust
async fn process_large_partitioned_dataset() -> Result<()> {
    // Create runner with optimized configuration
    let state_store = FileSystemStateStore::new("./states")?;
    let mut runner = IncrementalAnalysisRunner::new(Box::new(state_store))
        .with_config(IncrementalConfig {
            enable_checkpointing: true,
            checkpoint_interval: Duration::minutes(5),
            max_partitions_per_run: 100,
            parallel_processing: true,
        });
    
    // Add analyzers that support incremental computation
    runner
        .add_analyzer(Box::new(SizeAnalyzer::new()))
        .add_analyzer(Box::new(CompletenessAnalyzer::new("id")))
        .add_analyzer(Box::new(ApproxCountDistinctAnalyzer::new("user_id")))
        .add_analyzer(Box::new(KLLSketchAnalyzer::builder()
            .column("value")
            .build()));
    
    // Process partitions in batches
    let partitions = discover_partitions("s3://bucket/data/").await?;
    let total_partitions = partitions.len();
    
    for (batch_idx, partition_batch) in partitions.chunks(100).enumerate() {
        println!("\n📦 Processing batch {}/{}", 
                 batch_idx + 1, 
                 (total_partitions + 99) / 100);
        
        // Process batch in parallel
        let batch_results = process_partition_batch_parallel(
            &mut runner,
            partition_batch
        ).await?;
        
        // Checkpoint after each batch
        runner.checkpoint().await?;
        
        // Report progress
        let cumulative = runner.get_cumulative_results().await?;
        let processed = (batch_idx + 1) * 100;
        
        println!("Progress: {}/{} partitions", 
                 processed.min(total_partitions), 
                 total_partitions);
        println!("Cumulative rows: {}", 
                 cumulative.get_metric("row_count").unwrap());
        
        // Memory management
        if batch_idx % 10 == 0 {
            runner.compact_states().await?;
        }
    }
    
    // Final results
    let final_results = runner.get_cumulative_results().await?;
    generate_report(&final_results)?;
    
    Ok(())
}

async fn process_partition_batch_parallel(
    runner: &mut IncrementalAnalysisRunner,
    partitions: &[String]
) -> Result<Vec<AnalyzerContext>> {
    use futures::future::join_all;
    
    let tasks: Vec<_> = partitions.iter().map(|partition| {
        let partition = partition.clone();
        async move {
            // Create context for partition
            let ctx = SessionContext::new();
            ctx.register_parquet(
                "data",
                &partition,
                ParquetReadOptions::default()
            ).await?;
            
            // Return context with partition key
            Ok::<_, TermError>((partition, ctx))
        }
    }).collect();
    
    let contexts = join_all(tasks).await;
    
    let mut results = Vec::new();
    for context_result in contexts {
        let (partition_key, ctx) = context_result?;
        let result = runner.analyze_partition(&ctx, &partition_key).await?;
        results.push(result);
    }
    
    Ok(results)
}
```

## Step 5: Stateful Stream Processing

Process streaming data with incremental updates:

```rust
use tokio::time::{interval, Duration as TokioDuration};

async fn incremental_stream_processing() -> Result<()> {
    let state_store = FileSystemStateStore::new("./stream_states")?;
    let mut runner = IncrementalAnalysisRunner::new(Box::new(state_store))
        .add_analyzer(Box::new(SizeAnalyzer::new()))
        .add_analyzer(Box::new(CompletenessAnalyzer::new("event_id")))
        .add_analyzer(Box::new(MeanAnalyzer::new("latency_ms")));
    
    // Process micro-batches every minute
    let mut ticker = interval(TokioDuration::from_secs(60));
    let mut batch_id = 0;
    
    loop {
        ticker.tick().await;
        batch_id += 1;
        
        // Get latest micro-batch
        let ctx = SessionContext::new();
        let batch_data = fetch_latest_events().await?;
        
        if batch_data.is_empty() {
            continue;
        }
        
        // Register as temporary table
        ctx.register_batch("events", batch_data).await?;
        
        // Process incrementally
        let partition_key = format!("batch_{}", batch_id);
        let batch_results = runner.analyze_partition(&ctx, &partition_key).await?;
        
        // Get sliding window metrics (last hour)
        let window_results = runner.get_window_results(
            Duration::hours(1),
            WindowType::Sliding
        ).await?;
        
        // Check for anomalies in streaming metrics
        check_streaming_anomalies(&window_results).await?;
        
        // Clean up old states (keep last 24 hours)
        if batch_id % 60 == 0 {  // Every hour
            runner.prune_states_older_than(Duration::hours(24)).await?;
        }
        
        // Log metrics
        println!("Batch {}: {} events, {:.2}% complete, {:.2}ms avg latency",
                 batch_id,
                 batch_results.get_metric("row_count").unwrap(),
                 batch_results.get_metric("event_id_completeness").unwrap() * 100.0,
                 batch_results.get_metric("latency_ms_mean").unwrap());
    }
}
```

## Step 6: Handling Schema Evolution

Manage schema changes in incremental analysis:

```rust
async fn handle_schema_evolution() -> Result<()> {
    let state_store = FileSystemStateStore::new("./evolving_states")?;
    
    // Create schema-aware runner
    let mut runner = IncrementalAnalysisRunner::new(Box::new(state_store))
        .with_schema_evolution(SchemaEvolutionStrategy::Adaptive);
    
    // Version 1: Original schema
    let ctx_v1 = SessionContext::new();
    ctx_v1.register_csv(
        "data",
        "data/v1/orders.csv",  // columns: id, amount
        CsvReadOptions::default()
    ).await?;
    
    runner
        .add_analyzer(Box::new(CompletenessAnalyzer::new("id")))
        .add_analyzer(Box::new(MeanAnalyzer::new("amount")));
    
    let v1_results = runner.analyze_partition(&ctx_v1, "v1").await?;
    
    // Version 2: Schema with new column
    let ctx_v2 = SessionContext::new();
    ctx_v2.register_csv(
        "data",
        "data/v2/orders.csv",  // columns: id, amount, customer_id (new)
        CsvReadOptions::default()
    ).await?;
    
    // Add analyzer for new column
    runner.add_analyzer(Box::new(CompletenessAnalyzer::new("customer_id")));
    
    // Runner handles schema change gracefully
    let v2_results = runner.analyze_partition(&ctx_v2, "v2").await?;
    
    // Get merged results (handles missing columns)
    let merged = runner.get_cumulative_results().await?;
    
    println!("Schema evolution handled:");
    println!("  V1 columns: {:?}", v1_results.get_column_names());
    println!("  V2 columns: {:?}", v2_results.get_column_names());
    println!("  Merged metrics available: {:?}", merged.get_metric_names());
    
    Ok(())
}
```

## Step 7: Custom State Store Implementation

Create a custom state store for specific requirements:

```rust
use term_guard::analyzers::incremental::{StateStore, StateMap};
use async_trait::async_trait;

// Custom state store using Redis
struct RedisStateStore {
    client: redis::Client,
    ttl_seconds: usize,
}

#[async_trait]
impl StateStore for RedisStateStore {
    async fn save_state(
        &self,
        partition_key: &str,
        states: StateMap
    ) -> Result<()> {
        let mut conn = self.client.get_async_connection().await?;
        
        // Serialize states
        let serialized = serde_json::to_string(&states)?;
        
        // Store with TTL
        redis::cmd("SETEX")
            .arg(format!("state:{}", partition_key))
            .arg(self.ttl_seconds)
            .arg(serialized)
            .query_async(&mut conn)
            .await?;
        
        Ok(())
    }
    
    async fn load_state(&self, partition_key: &str) -> Result<Option<StateMap>> {
        let mut conn = self.client.get_async_connection().await?;
        
        let data: Option<String> = redis::cmd("GET")
            .arg(format!("state:{}", partition_key))
            .query_async(&mut conn)
            .await?;
        
        match data {
            Some(json) => Ok(Some(serde_json::from_str(&json)?)),
            None => Ok(None),
        }
    }
    
    async fn list_partitions(&self) -> Result<Vec<String>> {
        let mut conn = self.client.get_async_connection().await?;
        
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg("state:*")
            .query_async(&mut conn)
            .await?;
        
        Ok(keys.into_iter()
            .map(|k| k.strip_prefix("state:").unwrap().to_string())
            .collect())
    }
    
    async fn delete_state(&self, partition_key: &str) -> Result<()> {
        let mut conn = self.client.get_async_connection().await?;
        
        redis::cmd("DEL")
            .arg(format!("state:{}", partition_key))
            .query_async(&mut conn)
            .await?;
        
        Ok(())
    }
    
    async fn compact(&self) -> Result<()> {
        // Redis handles memory management
        Ok(())
    }
}

// Use custom state store
async fn use_redis_state_store() -> Result<()> {
    let redis_store = RedisStateStore {
        client: redis::Client::open("redis://127.0.0.1/")?,
        ttl_seconds: 86400,  // 24 hours
    };
    
    let mut runner = IncrementalAnalysisRunner::new(Box::new(redis_store))
        .add_analyzer(Box::new(SizeAnalyzer::new()));
    
    // Process partitions as usual
    // States are now stored in Redis
    
    Ok(())
}
```

## Step 8: Exercise - Building an Incremental Pipeline

Create a complete incremental analysis pipeline:

```rust
struct IncrementalPipeline {
    runner: IncrementalAnalysisRunner,
    config: PipelineConfig,
    metrics: Arc<RwLock<PipelineMetrics>>,
}

struct PipelineConfig {
    source_path: String,
    partition_pattern: String,
    analyzers: Vec<Box<dyn Analyzer>>,
    validation_checks: Vec<Check>,
    alert_thresholds: HashMap<String, f64>,
}

struct PipelineMetrics {
    partitions_processed: usize,
    total_rows: i64,
    processing_time_ms: u64,
    last_updated: DateTime<Utc>,
}

impl IncrementalPipeline {
    async fn run(&mut self) -> Result<()> {
        loop {
            // Discover new partitions
            let new_partitions = self.discover_new_partitions().await?;
            
            if new_partitions.is_empty() {
                tokio::time::sleep(TokioDuration::from_secs(60)).await;
                continue;
            }
            
            println!("📊 Found {} new partitions", new_partitions.len());
            
            for partition in new_partitions {
                let start = Instant::now();
                
                // Process partition
                let results = self.process_partition(&partition).await?;
                
                // Run validation
                let validation_passed = self.validate_results(&results).await?;
                
                if !validation_passed {
                    self.handle_validation_failure(&partition).await?;
                    continue;
                }
                
                // Check for alerts
                self.check_alerts(&results).await?;
                
                // Update metrics
                self.update_metrics(results, start.elapsed()).await;
                
                // Checkpoint periodically
                if self.metrics.read().await.partitions_processed % 10 == 0 {
                    self.runner.checkpoint().await?;
                }
            }
            
            // Generate summary
            self.generate_summary().await?;
        }
    }
    
    async fn process_partition(&mut self, partition: &str) -> Result<AnalyzerContext> {
        // Load partition data
        let ctx = SessionContext::new();
        ctx.register_parquet(
            "data",
            &format!("{}/{}", self.config.source_path, partition),
            ParquetReadOptions::default()
        ).await?;
        
        // Run incremental analysis
        self.runner.analyze_partition(&ctx, partition).await
    }
    
    async fn validate_results(&self, results: &AnalyzerContext) -> bool {
        // Create validation suite
        let mut suite_builder = ValidationSuite::builder("incremental_validation");
        
        for check in &self.config.validation_checks {
            suite_builder = suite_builder.add_check(check.clone());
        }
        
        let suite = suite_builder.build();
        
        // Convert analyzer results to validation context
        let ctx = results.to_validation_context();
        let validation_results = suite.validate(&ctx).await.unwrap();
        
        validation_results.is_success()
    }
    
    async fn check_alerts(&self, results: &AnalyzerContext) -> Result<()> {
        for (metric_name, threshold) in &self.config.alert_thresholds {
            if let Some(value) = results.get_metric(metric_name) {
                if value > *threshold {
                    self.send_alert(metric_name, value, *threshold).await?;
                }
            }
        }
        Ok(())
    }
    
    async fn generate_summary(&self) -> Result<()> {
        let cumulative = self.runner.get_cumulative_results().await?;
        let metrics = self.metrics.read().await;
        
        println!("\n📈 Incremental Pipeline Summary");
        println!("{'='*50}");
        println!("Partitions processed: {}", metrics.partitions_processed);
        println!("Total rows analyzed: {}", metrics.total_rows);
        println!("Average processing time: {:.2}s", 
                 metrics.processing_time_ms as f64 / 1000.0 / metrics.partitions_processed as f64);
        println!("Last updated: {}", metrics.last_updated);
        
        println!("\n📊 Cumulative Metrics:");
        for (name, value) in cumulative.all_metrics() {
            println!("  {}: {:.2}", name, value);
        }
        
        Ok(())
    }
}

// Run the pipeline
#[tokio::main]
async fn main() -> Result<()> {
    let config = PipelineConfig {
        source_path: "s3://data-lake/events".to_string(),
        partition_pattern: "year=*/month=*/day=*".to_string(),
        analyzers: vec![
            Box::new(SizeAnalyzer::new()),
            Box::new(CompletenessAnalyzer::new("event_id")),
            Box::new(MeanAnalyzer::new("duration_ms")),
            Box::new(ApproxQuantileAnalyzer::new("duration_ms", 0.99)),
        ],
        validation_checks: vec![
            Check::builder("quality_checks")
                .has_size(Assertion::GreaterThan(100))
                .has_completeness("event_id", 0.99)
                .build(),
        ],
        alert_thresholds: HashMap::from([
            ("duration_ms_p99".to_string(), 1000.0),
        ]),
    };
    
    let mut pipeline = IncrementalPipeline::new(config).await?;
    pipeline.run().await
}
```

## Summary

You've learned how to:
- ✅ Set up incremental analysis with state stores
- ✅ Process partitioned datasets efficiently
- ✅ Understand and use algebraic states
- ✅ Handle streaming data incrementally
- ✅ Manage schema evolution
- ✅ Build complete incremental pipelines

## Next Steps

- Read the [Incremental Analysis Reference](../reference/incremental-analysis.md)
- Learn about [Performance Optimization](../how-to/optimize-performance.md)
- Explore [Custom Analyzer Development](../how-to/write-custom-analyzers.md)

## Troubleshooting

**Q: States getting too large?**
A: Use compression, implement state compaction, or reduce retention period.

**Q: Merge operations failing?**
A: Ensure all analyzers implement proper merge semantics for their states.

**Q: Performance degradation with many partitions?**
A: Use parallel processing, implement checkpointing, and consider distributed state stores.

## Exercises

1. Implement incremental analysis for your production data pipeline
2. Create a custom state store using your preferred database
3. Build a dashboard showing incremental processing metrics
4. Implement automatic partition discovery and processing