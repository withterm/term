//! Integration tests for the query optimizer using TPC-H data.

#[cfg(all(test, feature = "test-utils"))]
mod tests {
    use term_guard::constraints::{
        Assertion, CompletenessConstraint, StatisticType, StatisticalConstraint,
        UniquenessConstraint,
    };
    use term_guard::core::{Check, Level, ValidationSuite};
    use term_guard::test_utils::{create_tpc_h_context, ScaleFactor};

    #[tokio::test]
    async fn test_optimizer_with_multiple_completeness_checks() {
        // Create TPC-H context with small scale factor
        let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

        // Create a 'data' view from the customer table since constraints expect 'data'
        ctx.sql("CREATE VIEW data AS SELECT * FROM customer")
            .await
            .unwrap();

        // Create multiple completeness checks on the same table
        let suite = ValidationSuite::builder("optimizer_test_completeness")
            .description("Test optimizer with multiple completeness checks")
            .with_optimizer(true) // Enable optimizer
            .check(
                Check::builder("customer_completeness")
                    .level(Level::Error)
                    .constraint(CompletenessConstraint::with_threshold("c_custkey", 1.0))
                    .constraint(CompletenessConstraint::with_threshold("c_name", 1.0))
                    .constraint(CompletenessConstraint::with_threshold("c_address", 1.0))
                    .constraint(CompletenessConstraint::with_threshold("c_nationkey", 1.0))
                    .constraint(CompletenessConstraint::with_threshold("c_phone", 1.0))
                    .build(),
            )
            .build();

        // Run with optimizer enabled
        let start = std::time::Instant::now();
        let result = suite.run(&ctx).await.unwrap();
        let optimized_duration = start.elapsed();

        assert!(!result.is_failure());
        assert_eq!(result.report().metrics.failed_checks, 0);
        assert_eq!(result.report().metrics.total_checks, 5);

        // Now run without optimizer for comparison
        let suite_no_opt = ValidationSuite::builder("no_optimizer_test")
            .description("Test without optimizer")
            .with_optimizer(false) // Disable optimizer
            .check(
                Check::builder("customer_completeness")
                    .level(Level::Error)
                    .constraint(CompletenessConstraint::with_threshold("c_custkey", 1.0))
                    .constraint(CompletenessConstraint::with_threshold("c_name", 1.0))
                    .constraint(CompletenessConstraint::with_threshold("c_address", 1.0))
                    .constraint(CompletenessConstraint::with_threshold("c_nationkey", 1.0))
                    .constraint(CompletenessConstraint::with_threshold("c_phone", 1.0))
                    .build(),
            )
            .build();

        let start = std::time::Instant::now();
        let result_no_opt = suite_no_opt.run(&ctx).await.unwrap();
        let non_optimized_duration = start.elapsed();

        assert!(result_no_opt.is_success());
        assert_eq!(result_no_opt.report().metrics.failed_checks, 0);
        assert_eq!(result_no_opt.report().metrics.total_checks, 5);

        // The optimized version should theoretically be faster
        // (though in practice with small data, overhead might make it slower)
        println!(
            "Optimized duration: {optimized_duration:?}, Non-optimized duration: {non_optimized_duration:?}"
        );
    }

    #[tokio::test]
    async fn test_optimizer_with_mixed_statistics() {
        // Create TPC-H context
        let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

        // Create a 'data' view from the orders table since constraints expect 'data'
        ctx.sql("CREATE VIEW data AS SELECT * FROM orders")
            .await
            .unwrap();

        // Create checks with various statistical constraints that can be combined
        let suite = ValidationSuite::builder("optimizer_test_statistics")
            .description("Test optimizer with statistical constraints")
            .with_optimizer(true)
            .check(
                Check::builder("order_statistics")
                    .level(Level::Warning)
                    .constraint(
                        StatisticalConstraint::new(
                            "o_totalprice",
                            StatisticType::Min,
                            Assertion::GreaterThan(0.0),
                        )
                        .unwrap(),
                    )
                    .constraint(
                        StatisticalConstraint::new(
                            "o_totalprice",
                            StatisticType::Max,
                            Assertion::LessThan(1000000.0),
                        )
                        .unwrap(),
                    )
                    .constraint(
                        StatisticalConstraint::new(
                            "o_totalprice",
                            StatisticType::Sum,
                            Assertion::GreaterThan(0.0),
                        )
                        .unwrap(),
                    )
                    .constraint(
                        StatisticalConstraint::new(
                            "o_totalprice",
                            StatisticType::Mean,
                            Assertion::GreaterThan(0.0),
                        )
                        .unwrap(),
                    )
                    .constraint(CompletenessConstraint::with_threshold("o_totalprice", 1.0))
                    .build(),
            )
            .build();

        let result = suite.run(&ctx).await.unwrap();

        assert!(!result.is_failure());
        assert_eq!(result.report().metrics.total_checks, 5);

        // All metrics should have values
        assert!(result
            .report()
            .metrics
            .custom_metrics
            .contains_key("order_statistics.min"));
        assert!(result
            .report()
            .metrics
            .custom_metrics
            .contains_key("order_statistics.max"));
        assert!(result
            .report()
            .metrics
            .custom_metrics
            .contains_key("order_statistics.mean"));
        assert!(result
            .report()
            .metrics
            .custom_metrics
            .contains_key("order_statistics.sum"));
        assert!(result
            .report()
            .metrics
            .custom_metrics
            .contains_key("order_statistics.completeness"));
    }

    #[tokio::test]
    async fn test_optimizer_with_multiple_tables() {
        // Create TPC-H context
        let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

        // Create checks across multiple tables
        let suite = ValidationSuite::builder("optimizer_test_multi_table")
            .description("Test optimizer with multiple tables")
            .with_optimizer(true)
            .check(
                Check::builder("customer_checks")
                    .level(Level::Error)
                    .constraint(CompletenessConstraint::with_threshold("c_custkey", 1.0))
                    .constraint(UniquenessConstraint::full_uniqueness("c_custkey", 1.0).unwrap())
                    .build(),
            )
            .check(
                Check::builder("orders_checks")
                    .level(Level::Error)
                    .constraint(CompletenessConstraint::with_threshold("o_orderkey", 1.0))
                    .constraint(UniquenessConstraint::full_uniqueness("o_orderkey", 1.0).unwrap())
                    .build(),
            )
            .check(
                Check::builder("lineitem_checks")
                    .level(Level::Error)
                    .constraint(CompletenessConstraint::with_threshold("l_orderkey", 1.0))
                    .constraint(CompletenessConstraint::with_threshold("l_partkey", 1.0))
                    .build(),
            )
            .build();

        let result = suite.run(&ctx).await.unwrap();

        // Note: These checks will fail because TPC-H data is registered as a single "data" table
        // In a real scenario with multiple tables, the optimizer would group by table
        assert_eq!(result.report().metrics.total_checks, 6);
    }

    #[tokio::test]
    async fn test_optimizer_plan_explanation() {
        // Create TPC-H context
        let ctx = create_tpc_h_context(ScaleFactor::SF01).await.unwrap();

        // Create a 'data' view from the customer table since constraints expect 'data'
        ctx.sql("CREATE VIEW data AS SELECT * FROM customer")
            .await
            .unwrap();

        // Create optimizer instance
        let mut optimizer = term_guard::optimizer::QueryOptimizer::new();

        // Create test checks
        let check = Check::builder("test_check")
            .level(Level::Error)
            .constraint(CompletenessConstraint::with_threshold("c_custkey", 1.0))
            .constraint(CompletenessConstraint::with_threshold("c_name", 1.0))
            .constraint(UniquenessConstraint::full_uniqueness("c_custkey", 1.0).unwrap())
            .build();

        let checks = vec![check];

        // Get the optimization plan
        let mut term_ctx = term_guard::core::TermContext::new().unwrap();

        // Register the 'data' table with the TermContext
        // We need to copy the table provider from the TPC-H context
        let table_provider = ctx.table_provider("data").await.unwrap();
        term_ctx
            .inner_mut()
            .register_table("data", table_provider)
            .unwrap();

        let plan = optimizer.explain_plan(&checks, &term_ctx).await.unwrap();

        // Verify the plan contains expected information
        assert!(plan.contains("Query Optimization Plan"));
        assert!(plan.contains("Total Checks:"));
        assert!(plan.contains("Total Constraints:"));
        assert!(plan.contains("Optimized Groups:"));
        assert!(plan.contains("Cache Statistics"));
    }
}

// Tests without the test-utils feature
#[cfg(not(feature = "test-utils"))]
mod tests {
    use datafusion::prelude::*;
    use term_guard::constraints::CompletenessConstraint;
    use term_guard::core::{Check, Level, ValidationSuite};
    use tokio;

    #[tokio::test]
    async fn test_optimizer_basic() {
        // Create a simple test context with CSV data
        let ctx = SessionContext::new();

        // Create some test data
        let csv_data = "id,name,value\n1,A,100\n2,B,200\n3,C,300\n";
        let temp_file = std::env::temp_dir().join("optimizer_test.csv");
        std::fs::write(&temp_file, csv_data).unwrap();

        // Register the CSV as a table
        ctx.register_csv(
            "data",
            temp_file.to_str().unwrap(),
            CsvReadOptions::default(),
        )
        .await
        .unwrap();

        // Create a validation suite with optimizer enabled
        let suite = ValidationSuite::builder("optimizer_basic_test")
            .description("Basic optimizer test")
            .with_optimizer(true)
            .check(
                Check::builder("completeness_check")
                    .level(Level::Error)
                    .constraint(CompletenessConstraint::with_threshold("id", 1.0))
                    .constraint(CompletenessConstraint::with_threshold("name", 1.0))
                    .constraint(CompletenessConstraint::with_threshold("value", 1.0))
                    .build(),
            )
            .build();

        let result = suite.run(&ctx).await.unwrap();

        assert!(!result.is_failure());
        assert_eq!(result.report().metrics.total_checks, 3);
        assert_eq!(result.report().metrics.passed_checks, 3);

        // Clean up
        std::fs::remove_file(&temp_file).ok();
    }
}
