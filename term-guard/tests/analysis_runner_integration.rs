//! Integration tests for the AnalysisRunner.

#[cfg(feature = "test-utils")]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use term_guard::analyzers::{advanced::*, basic::*, AnalysisRunner, Analyzer, AnalyzerContext};
    use term_guard::test_utils::{create_tpc_h_context, ScaleFactor};

    #[tokio::test]
    async fn test_analysis_runner_with_tpc_h() {
        let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

        // Create a view named "data" that points to lineitem table
        ctx.sql("CREATE VIEW data AS SELECT * FROM lineitem")
            .await
            .unwrap();

        let runner = AnalysisRunner::new()
            .add(SizeAnalyzer::new())
            .add(CompletenessAnalyzer::new("l_orderkey"))
            .add(CompletenessAnalyzer::new("l_partkey"))
            .add(CompletenessAnalyzer::new("l_discount"))
            .add(DistinctnessAnalyzer::new("l_orderkey"))
            .add(MeanAnalyzer::new("l_quantity"))
            .add(MinAnalyzer::new("l_extendedprice"))
            .add(MaxAnalyzer::new("l_extendedprice"))
            .add(SumAnalyzer::new("l_quantity"));

        let context = runner.run(&ctx).await.unwrap();

        // Verify all metrics were computed
        assert!(context.get_metric("size").is_some());
        assert!(context.get_metric("completeness.l_orderkey").is_some());
        assert!(context.get_metric("completeness.l_partkey").is_some());
        assert!(context.get_metric("completeness.l_discount").is_some());
        assert!(context.get_metric("distinctness.l_orderkey").is_some());
        assert!(context.get_metric("mean.l_quantity").is_some());
        assert!(context.get_metric("min.l_extendedprice").is_some());
        assert!(context.get_metric("max.l_extendedprice").is_some());
        assert!(context.get_metric("sum.l_quantity").is_some());

        // Check for no errors
        assert!(!context.has_errors());
    }

    #[tokio::test]
    async fn test_mixed_basic_and_advanced_analyzers() {
        let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

        // Create a view named "data" that points to lineitem table
        ctx.sql("CREATE VIEW data AS SELECT * FROM lineitem")
            .await
            .unwrap();

        let runner = AnalysisRunner::new()
            // Basic analyzers
            .add(SizeAnalyzer::new())
            .add(CompletenessAnalyzer::new("l_quantity"))
            .add(MeanAnalyzer::new("l_quantity"))
            // Advanced analyzers
            .add(ApproxCountDistinctAnalyzer::new("l_orderkey"))
            .add(StandardDeviationAnalyzer::new("l_quantity"))
            .add(DataTypeAnalyzer::new("l_shipdate"))
            .add(HistogramAnalyzer::new("l_discount", 10));

        let context = runner.run(&ctx).await.unwrap();

        // Verify metrics from both basic and advanced analyzers
        assert!(context.get_metric("size").is_some());
        assert!(context.get_metric("completeness.l_quantity").is_some());
        assert!(context.get_metric("mean.l_quantity").is_some());
        assert!(context
            .get_metric("approx_count_distinct.l_orderkey")
            .is_some());
        assert!(context.get_metric("standard_deviation").is_some());
        assert!(context.get_metric("data_type").is_some());
        assert!(context.get_metric("histogram").is_some());

        assert!(!context.has_errors());
    }

    #[tokio::test]
    async fn test_progress_reporting_with_many_analyzers() {
        let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

        // Create a view named "data" that points to lineitem table
        ctx.sql("CREATE VIEW data AS SELECT * FROM lineitem")
            .await
            .unwrap();

        let progress_count = Arc::new(AtomicUsize::new(0));
        let progress_clone = progress_count.clone();

        let runner = AnalysisRunner::new()
            // Add 15 analyzers
            .add(SizeAnalyzer::new())
            .add(CompletenessAnalyzer::new("l_orderkey"))
            .add(CompletenessAnalyzer::new("l_partkey"))
            .add(CompletenessAnalyzer::new("l_suppkey"))
            .add(CompletenessAnalyzer::new("l_linenumber"))
            .add(CompletenessAnalyzer::new("l_quantity"))
            .add(CompletenessAnalyzer::new("l_extendedprice"))
            .add(CompletenessAnalyzer::new("l_discount"))
            .add(CompletenessAnalyzer::new("l_tax"))
            .add(DistinctnessAnalyzer::new("l_orderkey"))
            .add(DistinctnessAnalyzer::new("l_partkey"))
            .add(MeanAnalyzer::new("l_quantity"))
            .add(MinAnalyzer::new("l_extendedprice"))
            .add(MaxAnalyzer::new("l_extendedprice"))
            .add(SumAnalyzer::new("l_quantity"))
            .on_progress(move |_progress| {
                progress_clone.fetch_add(1, Ordering::SeqCst);
            });

        let _context = runner.run(&ctx).await.unwrap();

        // Should have received progress updates for each analyzer
        assert_eq!(progress_count.load(Ordering::SeqCst), 15);
    }

    #[tokio::test]
    async fn test_performance_comparison() {
        use std::time::Instant;

        let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

        // Create a view named "data" that points to lineitem table
        ctx.sql("CREATE VIEW data AS SELECT * FROM lineitem")
            .await
            .unwrap();

        // Run analyzers individually
        let start_individual = Instant::now();
        let mut individual_context = AnalyzerContext::new();

        // Run each analyzer type separately
        let size_analyzer = SizeAnalyzer::new();
        let state = size_analyzer.compute_state_from_data(&ctx).await.unwrap();
        let metric = size_analyzer.compute_metric_from_state(&state).unwrap();
        individual_context.store_metric(size_analyzer.name(), metric);

        for column in ["l_quantity", "l_extendedprice"] {
            let analyzer = CompletenessAnalyzer::new(column);
            let state = analyzer.compute_state_from_data(&ctx).await.unwrap();
            let metric = analyzer.compute_metric_from_state(&state).unwrap();
            individual_context.store_analyzer_metric(analyzer.name(), column, metric);
        }

        let distinctness = DistinctnessAnalyzer::new("l_orderkey");
        let state = distinctness.compute_state_from_data(&ctx).await.unwrap();
        let metric = distinctness.compute_metric_from_state(&state).unwrap();
        individual_context.store_analyzer_metric(distinctness.name(), "l_orderkey", metric);

        let mean = MeanAnalyzer::new("l_quantity");
        let state = mean.compute_state_from_data(&ctx).await.unwrap();
        let metric = mean.compute_metric_from_state(&state).unwrap();
        individual_context.store_analyzer_metric(mean.name(), "l_quantity", metric);

        let min = MinAnalyzer::new("l_extendedprice");
        let state = min.compute_state_from_data(&ctx).await.unwrap();
        let metric = min.compute_metric_from_state(&state).unwrap();
        individual_context.store_analyzer_metric(min.name(), "l_extendedprice", metric);

        let max = MaxAnalyzer::new("l_extendedprice");
        let state = max.compute_state_from_data(&ctx).await.unwrap();
        let metric = max.compute_metric_from_state(&state).unwrap();
        individual_context.store_analyzer_metric(max.name(), "l_extendedprice", metric);

        let sum = SumAnalyzer::new("l_quantity");
        let state = sum.compute_state_from_data(&ctx).await.unwrap();
        let metric = sum.compute_metric_from_state(&state).unwrap();
        individual_context.store_analyzer_metric(sum.name(), "l_quantity", metric);

        let individual_duration = start_individual.elapsed();

        // Run analyzers using AnalysisRunner
        let start_runner = Instant::now();
        let runner = AnalysisRunner::new()
            .add(SizeAnalyzer::new())
            .add(CompletenessAnalyzer::new("l_quantity"))
            .add(CompletenessAnalyzer::new("l_extendedprice"))
            .add(DistinctnessAnalyzer::new("l_orderkey"))
            .add(MeanAnalyzer::new("l_quantity"))
            .add(MinAnalyzer::new("l_extendedprice"))
            .add(MaxAnalyzer::new("l_extendedprice"))
            .add(SumAnalyzer::new("l_quantity"));
        let _runner_context = runner.run(&ctx).await.unwrap();
        let runner_duration = start_runner.elapsed();

        println!("Individual execution: {individual_duration:?}");
        println!("AnalysisRunner execution: {runner_duration:?}");

        // The runner should complete in reasonable time
        // Note: We're not asserting runner is faster because current implementation
        // doesn't yet optimize for shared computation
        assert!(runner_duration.as_secs() < 10);
    }

    #[tokio::test]
    async fn test_error_recovery_with_multiple_analyzers() {
        let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

        // Create a view named "data" that points to lineitem table
        ctx.sql("CREATE VIEW data AS SELECT * FROM lineitem")
            .await
            .unwrap();

        // Create a runner with some analyzers that will fail (non-existent columns)
        let runner = AnalysisRunner::new()
            .add(SizeAnalyzer::new())
            .add(CompletenessAnalyzer::new("non_existent_column1"))
            .add(MeanAnalyzer::new("l_quantity"))
            .add(CompletenessAnalyzer::new("non_existent_column2"))
            .add(SumAnalyzer::new("l_quantity"))
            .continue_on_error(true);

        let context = runner.run(&ctx).await.unwrap();

        // Should have some successful metrics
        assert!(context.get_metric("size").is_some());
        assert!(context.get_metric("mean.l_quantity").is_some());
        assert!(context.get_metric("sum.l_quantity").is_some());

        // Should have recorded errors for non-existent columns
        assert!(context.has_errors());
        assert_eq!(context.errors().len(), 2);

        // Verify error messages contain the analyzer names
        let errors = context.errors();
        assert!(errors
            .iter()
            .any(|e| e.analyzer_name.contains("completeness")));
    }
}
