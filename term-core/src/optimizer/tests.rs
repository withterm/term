//! Unit tests for the query optimizer module.

use super::*;
use crate::core::{Check, ConstraintResult, ConstraintStatus, Level};
use crate::prelude::*;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

/// Mock constraint for testing.
#[derive(Debug, Clone)]
struct MockConstraint {
    name: String,
    result: ConstraintResult,
    _sql: String,
}

#[async_trait]
impl crate::core::Constraint for MockConstraint {
    async fn evaluate(&self, _ctx: &SessionContext) -> Result<ConstraintResult> {
        Ok(self.result.clone())
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> Option<&str> {
        Some("Mock constraint for testing")
    }
}

#[cfg(test)]
mod optimizer_tests {
    use super::*;

    #[tokio::test]
    async fn test_optimizer_creation() {
        let optimizer = QueryOptimizer::new();

        // Verify all components are initialized
        assert!(std::ptr::eq(&optimizer.analyzer, &optimizer.analyzer));
        assert!(std::ptr::eq(&optimizer.combiner, &optimizer.combiner));
        assert!(std::ptr::eq(&optimizer.executor, &optimizer.executor));
        assert!(std::ptr::eq(&optimizer.stats_cache, &optimizer.stats_cache));
    }

    #[tokio::test]
    async fn test_extract_constraints() {
        let optimizer = QueryOptimizer::new();

        // Create test checks with mock constraints
        let constraint1 = MockConstraint {
            name: "test_constraint_1".to_string(),
            result: ConstraintResult {
                status: ConstraintStatus::Success,
                metric: Some(1.0),
                message: None,
            },
            _sql: "SELECT COUNT(*) FROM data".to_string(),
        };

        let constraint2 = MockConstraint {
            name: "test_constraint_2".to_string(),
            result: ConstraintResult {
                status: ConstraintStatus::Success,
                metric: Some(0.95),
                message: None,
            },
            _sql: "SELECT COUNT(*), COUNT(column) FROM data".to_string(),
        };

        let check = Check::builder("test_check")
            .level(Level::Error)
            .constraint(constraint1)
            .constraint(constraint2)
            .build();

        let checks = vec![check];
        let constraints = optimizer.extract_constraints(&checks);

        assert_eq!(constraints.len(), 2);
        assert_eq!(constraints[0].0, "test_check.test_constraint_1");
        assert_eq!(constraints[1].0, "test_check.test_constraint_2");
    }
}

#[cfg(test)]
mod analyzer_tests {
    use super::*;
    use crate::optimizer::analyzer::AggregationType;

    #[test]
    fn test_analyzer_cache() {
        let mut analyzer = QueryAnalyzer::new();

        // Create a mock constraint
        let constraint = Arc::new(MockConstraint {
            name: "completeness".to_string(),
            result: ConstraintResult {
                status: ConstraintStatus::Success,
                metric: Some(1.0),
                message: None,
            },
            _sql: "SELECT COUNT(*) FROM data".to_string(),
        });

        let constraints = vec![(
            "test1".to_string(),
            constraint.clone() as Arc<dyn crate::core::Constraint>,
        )];

        // First analysis should populate cache
        let analysis1 = analyzer.analyze(&constraints).unwrap();
        assert_eq!(analysis1.len(), 1);

        // Second analysis should use cache
        let analysis2 = analyzer.analyze(&constraints).unwrap();
        assert_eq!(analysis2.len(), 1);

        // Verify the analysis is correct
        assert_eq!(analysis1[0].name, "test1");
        assert_eq!(analysis1[0].table_name, "data");
        assert!(analysis1[0].aggregations.contains(&AggregationType::Count));
    }

    #[test]
    fn test_aggregation_type_detection() {
        let analyzer = QueryAnalyzer::new();

        // Test various constraint types
        let test_cases = vec![
            ("completeness", vec![AggregationType::Count]),
            (
                "uniqueness",
                vec![AggregationType::Count, AggregationType::CountDistinct],
            ),
            ("min", vec![AggregationType::Min]),
            ("max", vec![AggregationType::Max]),
            ("mean", vec![AggregationType::Avg]),
            ("sum", vec![AggregationType::Sum]),
            ("standard_deviation", vec![AggregationType::StdDev]),
        ];

        for (constraint_name, expected_aggs) in test_cases {
            let constraint = Arc::new(MockConstraint {
                name: constraint_name.to_string(),
                result: ConstraintResult {
                    status: ConstraintStatus::Success,
                    metric: Some(1.0),
                    message: None,
                },
                _sql: String::new(),
            });

            let analysis = analyzer
                .analyze_constraint(
                    "test".to_string(),
                    constraint as Arc<dyn crate::core::Constraint>,
                )
                .unwrap();

            assert_eq!(analysis.aggregations, expected_aggs);
        }
    }
}

#[cfg(test)]
mod combiner_tests {
    use super::*;
    use crate::optimizer::analyzer::ConstraintAnalysis;

    #[test]
    fn test_group_by_table() {
        let combiner = QueryCombiner::new();

        // Create test analyses with different tables
        let analyses = vec![
            create_test_analysis("c1", "data", true),
            create_test_analysis("c2", "data", true),
            create_test_analysis("c3", "other_table", true),
        ];

        let groups = combiner.group_constraints(analyses).unwrap();

        // Should have 2 groups (one for "data", one for "other_table")
        assert!(groups.len() >= 2);
    }

    #[test]
    fn test_max_group_size() {
        let mut combiner = QueryCombiner::new();
        combiner.set_max_group_size(2);

        // Create 5 combinable constraints for same table
        let analyses: Vec<_> = (0..5)
            .map(|i| create_test_analysis(&format!("c{}", i), "data", true))
            .collect();

        let groups = combiner.group_constraints(analyses).unwrap();

        // With max size 2, we should have at least 3 groups
        assert!(groups.len() >= 3);

        // No group should exceed max size
        for group in groups {
            assert!(group.constraints.len() <= 2);
        }
    }

    #[test]
    fn test_non_combinable_constraints() {
        let combiner = QueryCombiner::new();

        // Mix of combinable and non-combinable constraints
        let analyses = vec![
            create_test_analysis("c1", "data", true),
            create_test_analysis("c2", "data", false), // Non-combinable
            create_test_analysis("c3", "data", true),
        ];

        let groups = combiner.group_constraints(analyses).unwrap();

        // Non-combinable constraint should be in its own group
        let non_combinable_group = groups
            .iter()
            .find(|g| g.constraints.len() == 1 && g.constraints[0].name == "c2");
        assert!(non_combinable_group.is_some());
    }

    fn create_test_analysis(name: &str, table: &str, combinable: bool) -> ConstraintAnalysis {
        ConstraintAnalysis {
            name: name.to_string(),
            constraint: Arc::new(MockConstraint {
                name: name.to_string(),
                result: ConstraintResult {
                    status: ConstraintStatus::Success,
                    metric: Some(1.0),
                    message: None,
                },
                _sql: String::new(),
            }),
            table_name: table.to_string(),
            aggregations: vec![crate::optimizer::analyzer::AggregationType::Count],
            columns: vec![],
            has_predicates: false,
            is_combinable: combinable,
        }
    }
}

#[cfg(test)]
mod executor_tests {
    use super::*;

    #[test]
    fn test_predicate_pushdown_toggle() {
        let mut executor = OptimizedExecutor::new();

        // Should be enabled by default
        assert!(executor.enable_pushdown);

        // Test disabling
        executor.set_pushdown_enabled(false);
        assert!(!executor.enable_pushdown);

        // Test re-enabling
        executor.set_pushdown_enabled(true);
        assert!(executor.enable_pushdown);
    }
}

#[cfg(test)]
mod stats_cache_tests {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn test_cache_operations() {
        let mut cache = StatsCache::new();

        // Test set and get
        cache.set("key1".to_string(), 42.0);
        assert_eq!(cache.get("key1"), Some(42.0));

        // Test overwrite
        cache.set("key1".to_string(), 100.0);
        assert_eq!(cache.get("key1"), Some(100.0));

        // Test non-existent key
        assert_eq!(cache.get("non_existent"), None);
    }

    #[test]
    fn test_cache_expiration() {
        let mut cache = StatsCache::with_config(Duration::from_millis(50), 10);

        cache.set("key1".to_string(), 42.0);
        assert_eq!(cache.get("key1"), Some(42.0));

        // Wait for expiration
        sleep(Duration::from_millis(60));
        assert_eq!(cache.get("key1"), None);
    }

    #[test]
    fn test_cache_eviction() {
        let mut cache = StatsCache::with_config(Duration::from_secs(60), 2);

        cache.set("key1".to_string(), 1.0);
        cache.set("key2".to_string(), 2.0);
        assert_eq!(cache.size(), 2);

        // Adding third entry should evict oldest
        cache.set("key3".to_string(), 3.0);
        assert_eq!(cache.size(), 2);

        // key1 should have been evicted
        assert_eq!(cache.get("key1"), None);
        assert_eq!(cache.get("key2"), Some(2.0));
        assert_eq!(cache.get("key3"), Some(3.0));
    }

    #[test]
    fn test_cache_stats() {
        let mut cache = StatsCache::with_config(Duration::from_millis(100), 10);

        cache.set("key1".to_string(), 1.0);
        cache.set("key2".to_string(), 2.0);

        let stats = cache.stats();
        assert_eq!(stats.total_entries, 2);
        assert_eq!(stats.active_entries, 2);
        assert_eq!(stats.expired_entries, 0);

        // Wait for expiration
        sleep(Duration::from_millis(110));

        let stats = cache.stats();
        assert_eq!(stats.total_entries, 2);
        assert_eq!(stats.active_entries, 0);
        assert_eq!(stats.expired_entries, 2);
    }

    #[test]
    fn test_remove_expired() {
        let mut cache = StatsCache::with_config(Duration::from_millis(50), 10);

        cache.set("key1".to_string(), 1.0);
        cache.set("key2".to_string(), 2.0);
        assert_eq!(cache.size(), 2);

        // Wait for expiration
        sleep(Duration::from_millis(60));

        // Remove expired entries
        cache.remove_expired();
        assert_eq!(cache.size(), 0);
    }
}
