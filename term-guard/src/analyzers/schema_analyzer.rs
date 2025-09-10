//! Schema analyzer for automatic constraint suggestion.
//!
//! This module provides intelligent constraint suggestions based on schema analysis,
//! foreign key detection, and naming conventions. Part of Phase 3: UX & Integration
//! for the Term joined data sources feature.
//!
//! # Features
//!
//! - Automatic foreign key detection based on naming patterns
//! - Temporal column identification
//! - Sum consistency detection for financial data
//! - Join coverage recommendations
//! - Intelligent constraint prioritization
//!
//! # Example
//!
//! ```rust,ignore
//! use term_guard::analyzers::SchemaAnalyzer;
//! use datafusion::prelude::*;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let ctx = SessionContext::new();
//! // Register your tables...
//!
//! let analyzer = SchemaAnalyzer::new(&ctx);
//! let suggestions = analyzer.analyze_all_tables().await?;
//!
//! for suggestion in suggestions {
//!     println!("Suggested: {} between {} (confidence: {:.2})",
//!              suggestion.constraint_type,
//!              suggestion.tables.join(" and "),
//!              suggestion.confidence);
//! }
//! # Ok(())
//! # }
//! ```

use crate::analyzers::suggestions::{ConstraintParameter, SuggestionPriority};
use crate::core::Check;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, instrument};

/// Schema analyzer for automatic constraint suggestion.
///
/// This analyzer examines table schemas and relationships to suggest
/// appropriate multi-table constraints automatically.
pub struct SchemaAnalyzer<'a> {
    ctx: &'a SessionContext,
    naming_patterns: NamingPatterns,
}

/// Naming patterns for identifying relationships and special columns.
#[derive(Debug, Clone)]
struct NamingPatterns {
    /// Patterns that indicate foreign key relationships
    foreign_key_suffixes: Vec<String>,
    /// Patterns that indicate temporal columns
    temporal_patterns: Vec<String>,
    /// Patterns that indicate financial/amount columns
    amount_patterns: Vec<String>,
    /// Patterns that indicate quantity columns
    quantity_patterns: Vec<String>,
}

impl Default for NamingPatterns {
    fn default() -> Self {
        Self {
            foreign_key_suffixes: vec![
                "_id".to_string(),
                "_key".to_string(),
                "_fk".to_string(),
                "_ref".to_string(),
            ],
            temporal_patterns: vec![
                "_at".to_string(),
                "_date".to_string(),
                "_time".to_string(),
                "_timestamp".to_string(),
                "created".to_string(),
                "updated".to_string(),
                "modified".to_string(),
                "processed".to_string(),
                "completed".to_string(),
            ],
            amount_patterns: vec![
                "amount".to_string(),
                "total".to_string(),
                "price".to_string(),
                "cost".to_string(),
                "payment".to_string(),
                "revenue".to_string(),
                "balance".to_string(),
            ],
            quantity_patterns: vec![
                "quantity".to_string(),
                "qty".to_string(),
                "count".to_string(),
                "units".to_string(),
                "items".to_string(),
            ],
        }
    }
}

/// A suggested cross-table constraint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossTableSuggestion {
    /// Type of constraint (e.g., "foreign_key", "cross_table_sum")
    pub constraint_type: String,
    /// Tables involved in the constraint
    pub tables: Vec<String>,
    /// Columns involved in the constraint
    pub columns: HashMap<String, Vec<String>>,
    /// Confidence score (0.0 to 1.0)
    pub confidence: f64,
    /// Human-readable rationale
    pub rationale: String,
    /// Priority level
    pub priority: SuggestionPriority,
    /// Additional parameters for the constraint
    pub parameters: HashMap<String, ConstraintParameter>,
}

impl<'a> SchemaAnalyzer<'a> {
    /// Create a new schema analyzer.
    pub fn new(ctx: &'a SessionContext) -> Self {
        Self {
            ctx,
            naming_patterns: NamingPatterns::default(),
        }
    }

    /// Analyze all registered tables and suggest constraints.
    #[instrument(skip(self))]
    pub async fn analyze_all_tables(&self) -> crate::error::Result<Vec<CrossTableSuggestion>> {
        let mut suggestions = Vec::new();

        // Get all table names
        let catalog = self.ctx.catalog("datafusion").unwrap();
        let schema = catalog.schema("public").unwrap();
        let table_names: Vec<String> = schema.table_names();

        info!(
            "Analyzing {} tables for constraint suggestions",
            table_names.len()
        );

        // Collect all table schemas
        let mut table_schemas = HashMap::new();
        for table_name in &table_names {
            if let Ok(Some(table)) = schema.table(table_name).await {
                let schema = table.schema();
                table_schemas.insert(table_name.clone(), schema);
            }
        }

        // Analyze foreign key relationships
        suggestions.extend(self.analyze_foreign_keys(&table_schemas));

        // Analyze temporal relationships
        suggestions.extend(self.analyze_temporal_constraints(&table_schemas));

        // Analyze financial consistency
        suggestions.extend(self.analyze_financial_consistency(&table_schemas));

        // Analyze join coverage
        suggestions.extend(self.analyze_join_coverage(&table_schemas));

        // Sort by priority and confidence
        suggestions.sort_by(|a, b| match (&a.priority, &b.priority) {
            (SuggestionPriority::Critical, SuggestionPriority::Critical) => {
                b.confidence.partial_cmp(&a.confidence).unwrap()
            }
            (SuggestionPriority::Critical, _) => std::cmp::Ordering::Less,
            (_, SuggestionPriority::Critical) => std::cmp::Ordering::Greater,
            _ => b.confidence.partial_cmp(&a.confidence).unwrap(),
        });

        Ok(suggestions)
    }

    /// Analyze schemas for foreign key relationships.
    fn analyze_foreign_keys(
        &self,
        schemas: &HashMap<String, Arc<Schema>>,
    ) -> Vec<CrossTableSuggestion> {
        let mut suggestions = Vec::new();

        for (table_name, schema) in schemas {
            for field in schema.fields() {
                // Check if this field looks like a foreign key
                if let Some(referenced_table) = self.detect_foreign_key(field.name(), schemas) {
                    // Check if the referenced table and column exist
                    if let Some(ref_schema) = schemas.get(&referenced_table) {
                        let ref_column =
                            self.infer_primary_key_column(&referenced_table, ref_schema);

                        let mut columns = HashMap::new();
                        columns.insert(table_name.clone(), vec![field.name().to_string()]);
                        columns.insert(referenced_table.clone(), vec![ref_column.clone()]);

                        suggestions.push(CrossTableSuggestion {
                            constraint_type: "foreign_key".to_string(),
                            tables: vec![table_name.clone(), referenced_table.clone()],
                            columns,
                            confidence: self.calculate_fk_confidence(field.name(), &referenced_table),
                            rationale: format!(
                                "Column '{}' in '{table_name}' appears to reference '{referenced_table}' based on naming convention",
                                field.name()
                            ),
                            priority: SuggestionPriority::High,
                            parameters: HashMap::new(),
                        });
                    }
                }
            }
        }

        suggestions
    }

    /// Detect if a column name suggests a foreign key relationship.
    fn detect_foreign_key(
        &self,
        column_name: &str,
        schemas: &HashMap<String, Arc<Schema>>,
    ) -> Option<String> {
        // Check if column ends with foreign key suffix
        for suffix in &self.naming_patterns.foreign_key_suffixes {
            if column_name.ends_with(suffix) {
                // Extract potential table name
                let base_name = &column_name[..column_name.len() - suffix.len()];

                // Try to find matching table (with pluralization handling)
                for table_name in schemas.keys() {
                    if self.matches_table_name(base_name, table_name) {
                        return Some(table_name.clone());
                    }
                }
            }
        }

        None
    }

    /// Check if a base name matches a table name (handling pluralization).
    fn matches_table_name(&self, base_name: &str, table_name: &str) -> bool {
        // Exact match
        if base_name == table_name {
            return true;
        }

        // Singular to plural
        if format!("{base_name}s") == table_name {
            return true;
        }

        // Plural to singular
        if base_name == format!("{table_name}s") {
            return true;
        }

        // Handle 'ies' pluralization (e.g., category -> categories)
        if base_name.ends_with('y')
            && table_name == format!("{}ies", &base_name[..base_name.len() - 1])
        {
            return true;
        }

        false
    }

    /// Infer the primary key column for a table.
    fn infer_primary_key_column(&self, table_name: &str, schema: &Arc<Schema>) -> String {
        // Look for common primary key patterns
        let table_id = format!("{table_name}_id");
        let table_key = format!("{table_name}_key");
        let common_pk_names = vec!["id", table_id.as_str(), "key", table_key.as_str()];

        for field in schema.fields() {
            for pk_name in &common_pk_names {
                if field.name().to_lowercase() == pk_name.to_lowercase() {
                    return field.name().to_string();
                }
            }
        }

        // Default to "id" if not found
        "id".to_string()
    }

    /// Calculate confidence score for foreign key detection.
    fn calculate_fk_confidence(&self, column_name: &str, referenced_table: &str) -> f64 {
        let mut confidence: f64 = 0.5; // Base confidence

        // Higher confidence if column name closely matches table name
        if column_name.contains(referenced_table)
            || column_name.contains(&referenced_table[..referenced_table.len().saturating_sub(1)])
        {
            confidence += 0.3;
        }

        // Higher confidence for common patterns
        if column_name.ends_with("_id") {
            confidence += 0.2;
        }

        confidence.min(1.0)
    }

    /// Analyze schemas for temporal constraints.
    fn analyze_temporal_constraints(
        &self,
        schemas: &HashMap<String, Arc<Schema>>,
    ) -> Vec<CrossTableSuggestion> {
        let mut suggestions = Vec::new();

        for (table_name, schema) in schemas {
            let temporal_columns = self.find_temporal_columns(schema);

            // Suggest temporal ordering for pairs of temporal columns
            if temporal_columns.len() >= 2 {
                for i in 0..temporal_columns.len() {
                    for j in i + 1..temporal_columns.len() {
                        let col1 = &temporal_columns[i];
                        let col2 = &temporal_columns[j];

                        // Determine likely ordering based on names
                        let (before, after) = self.infer_temporal_order(col1, col2);

                        let mut columns = HashMap::new();
                        columns.insert(table_name.clone(), vec![before.clone(), after.clone()]);

                        let mut parameters = HashMap::new();
                        parameters.insert(
                            "validation_type".to_string(),
                            ConstraintParameter::String("before_after".to_string()),
                        );

                        suggestions.push(CrossTableSuggestion {
                            constraint_type: "temporal_ordering".to_string(),
                            tables: vec![table_name.clone()],
                            columns,
                            confidence: 0.8,
                            rationale: format!(
                                "Columns '{before}' and '{after}' appear to have a temporal relationship"
                            ),
                            priority: SuggestionPriority::Medium,
                            parameters,
                        });
                    }
                }
            }

            // Suggest business hours validation for timestamp columns
            for col in &temporal_columns {
                if col.contains("transaction") || col.contains("order") || col.contains("payment") {
                    let mut columns = HashMap::new();
                    columns.insert(table_name.clone(), vec![col.clone()]);

                    let mut parameters = HashMap::new();
                    parameters.insert(
                        "start_time".to_string(),
                        ConstraintParameter::String("09:00".to_string()),
                    );
                    parameters.insert(
                        "end_time".to_string(),
                        ConstraintParameter::String("17:00".to_string()),
                    );

                    suggestions.push(CrossTableSuggestion {
                        constraint_type: "business_hours".to_string(),
                        tables: vec![table_name.clone()],
                        columns,
                        confidence: 0.6,
                        rationale: format!(
                            "Column '{col}' may benefit from business hours validation"
                        ),
                        priority: SuggestionPriority::Low,
                        parameters,
                    });
                }
            }
        }

        suggestions
    }

    /// Find temporal columns in a schema.
    fn find_temporal_columns(&self, schema: &Arc<Schema>) -> Vec<String> {
        let mut temporal_columns = Vec::new();

        for field in schema.fields() {
            // Check data type
            let is_temporal_type = matches!(
                field.data_type(),
                DataType::Date32
                    | DataType::Date64
                    | DataType::Timestamp(_, _)
                    | DataType::Time32(_)
                    | DataType::Time64(_)
            );

            // Check naming pattern
            let matches_pattern = self
                .naming_patterns
                .temporal_patterns
                .iter()
                .any(|pattern| field.name().to_lowercase().contains(pattern));

            if is_temporal_type || matches_pattern {
                temporal_columns.push(field.name().to_string());
            }
        }

        temporal_columns
    }

    /// Infer temporal ordering between two columns based on their names.
    fn infer_temporal_order(&self, col1: &str, col2: &str) -> (String, String) {
        let order_keywords = vec![
            ("created", 0),
            ("started", 1),
            ("updated", 2),
            ("modified", 2),
            ("processed", 3),
            ("completed", 4),
            ("finished", 4),
            ("ended", 5),
        ];

        let get_order = |col: &str| -> i32 {
            for (keyword, order) in &order_keywords {
                if col.to_lowercase().contains(keyword) {
                    return *order;
                }
            }
            100 // Default order for unknown
        };

        let order1 = get_order(col1);
        let order2 = get_order(col2);

        if order1 <= order2 {
            (col1.to_string(), col2.to_string())
        } else {
            (col2.to_string(), col1.to_string())
        }
    }

    /// Analyze schemas for financial consistency constraints.
    fn analyze_financial_consistency(
        &self,
        schemas: &HashMap<String, Arc<Schema>>,
    ) -> Vec<CrossTableSuggestion> {
        let mut suggestions = Vec::new();

        // Find all amount/quantity columns across tables
        let mut amount_columns: HashMap<String, Vec<String>> = HashMap::new();
        let mut quantity_columns: HashMap<String, Vec<String>> = HashMap::new();

        for (table_name, schema) in schemas {
            for field in schema.fields() {
                if self.is_amount_column(field.name(), field.data_type()) {
                    amount_columns
                        .entry(table_name.clone())
                        .or_default()
                        .push(field.name().to_string());
                }
                if self.is_quantity_column(field.name(), field.data_type()) {
                    quantity_columns
                        .entry(table_name.clone())
                        .or_default()
                        .push(field.name().to_string());
                }
            }
        }

        // Suggest cross-table sum constraints for related tables
        for (table1, cols1) in &amount_columns {
            for (table2, cols2) in &amount_columns {
                if table1 < table2 && self.are_tables_related(table1, table2, schemas) {
                    for col1 in cols1 {
                        for col2 in cols2 {
                            if self.are_columns_likely_related(col1, col2) {
                                let mut columns = HashMap::new();
                                columns.insert(table1.clone(), vec![col1.clone()]);
                                columns.insert(table2.clone(), vec![col2.clone()]);

                                let mut parameters = HashMap::new();
                                parameters.insert(
                                    "tolerance".to_string(),
                                    ConstraintParameter::Float(0.01),
                                );

                                suggestions.push(CrossTableSuggestion {
                                    constraint_type: "cross_table_sum".to_string(),
                                    tables: vec![table1.clone(), table2.clone()],
                                    columns,
                                    confidence: 0.7,
                                    rationale: format!(
                                        "Financial columns '{table1}.{col1}' and '{table2}.{col2}' may need sum consistency validation"
                                    ),
                                    priority: SuggestionPriority::High,
                                    parameters,
                                });
                            }
                        }
                    }
                }
            }
        }

        suggestions
    }

    /// Check if a column appears to contain amount/financial data.
    fn is_amount_column(&self, name: &str, data_type: &DataType) -> bool {
        // Check if it's a numeric type
        let is_numeric = matches!(
            data_type,
            DataType::Float32
                | DataType::Float64
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
        );

        if !is_numeric {
            return false;
        }

        // Check naming patterns
        self.naming_patterns
            .amount_patterns
            .iter()
            .any(|pattern| name.to_lowercase().contains(pattern))
    }

    /// Check if a column appears to contain quantity data.
    fn is_quantity_column(&self, name: &str, data_type: &DataType) -> bool {
        // Check if it's a numeric type
        let is_numeric = matches!(
            data_type,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float32
                | DataType::Float64
        );

        if !is_numeric {
            return false;
        }

        // Check naming patterns
        self.naming_patterns
            .quantity_patterns
            .iter()
            .any(|pattern| name.to_lowercase().contains(pattern))
    }

    /// Check if two tables appear to be related.
    fn are_tables_related(
        &self,
        table1: &str,
        table2: &str,
        schemas: &HashMap<String, Arc<Schema>>,
    ) -> bool {
        // Check if either table has a foreign key to the other
        if let Some(schema1) = schemas.get(table1) {
            for field in schema1.fields() {
                if let Some(ref_table) = self.detect_foreign_key(field.name(), schemas) {
                    if ref_table == table2 {
                        return true;
                    }
                }
            }
        }

        if let Some(schema2) = schemas.get(table2) {
            for field in schema2.fields() {
                if let Some(ref_table) = self.detect_foreign_key(field.name(), schemas) {
                    if ref_table == table1 {
                        return true;
                    }
                }
            }
        }

        // Check if table names suggest a relationship
        table1.contains(table2) || table2.contains(table1)
    }

    /// Check if two column names suggest they are related.
    fn are_columns_likely_related(&self, col1: &str, col2: &str) -> bool {
        // Same column name
        if col1 == col2 {
            return true;
        }

        // Check for common keywords
        let keywords = vec!["total", "amount", "sum", "payment", "cost", "price"];
        for keyword in keywords {
            if col1.contains(keyword) && col2.contains(keyword) {
                return true;
            }
        }

        false
    }

    /// Analyze schemas for join coverage recommendations.
    fn analyze_join_coverage(
        &self,
        schemas: &HashMap<String, Arc<Schema>>,
    ) -> Vec<CrossTableSuggestion> {
        let mut suggestions = Vec::new();

        // For each detected foreign key relationship, suggest join coverage validation
        for (table_name, schema) in schemas {
            for field in schema.fields() {
                if let Some(referenced_table) = self.detect_foreign_key(field.name(), schemas) {
                    let mut columns = HashMap::new();
                    columns.insert(table_name.clone(), vec![field.name().to_string()]);
                    columns.insert(referenced_table.clone(), vec!["id".to_string()]); // Assume id column

                    let mut parameters = HashMap::new();
                    parameters.insert(
                        "expected_coverage".to_string(),
                        ConstraintParameter::Float(0.95),
                    );

                    suggestions.push(CrossTableSuggestion {
                        constraint_type: "join_coverage".to_string(),
                        tables: vec![table_name.clone(), referenced_table.clone()],
                        columns,
                        confidence: 0.75,
                        rationale: format!(
                            "Join between '{table_name}' and '{referenced_table}' should have high coverage for data quality"
                        ),
                        priority: SuggestionPriority::Medium,
                        parameters,
                    });
                }
            }
        }

        suggestions
    }

    /// Convert suggestions to a validation Check.
    pub fn suggestions_to_check(
        &self,
        suggestions: &[CrossTableSuggestion],
        check_name: &str,
    ) -> Check {
        let mut builder = Check::builder(check_name);

        for suggestion in suggestions {
            match suggestion.constraint_type.as_str() {
                "foreign_key" => {
                    if suggestion.tables.len() == 2 {
                        let child_col = format!(
                            "{}.{}",
                            suggestion.tables[0], suggestion.columns[&suggestion.tables[0]][0]
                        );
                        let parent_col = format!(
                            "{}.{}",
                            suggestion.tables[1], suggestion.columns[&suggestion.tables[1]][0]
                        );
                        builder = builder.foreign_key(child_col, parent_col);
                    }
                }
                "cross_table_sum" => {
                    if suggestion.tables.len() == 2 {
                        let left_col = format!(
                            "{}.{}",
                            suggestion.tables[0], suggestion.columns[&suggestion.tables[0]][0]
                        );
                        let right_col = format!(
                            "{}.{}",
                            suggestion.tables[1], suggestion.columns[&suggestion.tables[1]][0]
                        );
                        builder = builder.cross_table_sum(left_col, right_col);
                    }
                }
                "join_coverage" => {
                    if suggestion.tables.len() == 2 {
                        builder =
                            builder.join_coverage(&suggestion.tables[0], &suggestion.tables[1]);
                    }
                }
                "temporal_ordering" => {
                    if !suggestion.tables.is_empty() {
                        builder = builder.temporal_ordering(&suggestion.tables[0]);
                    }
                }
                _ => {}
            }
        }

        builder.build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema};

    #[test]
    fn test_foreign_key_detection() {
        let ctx = SessionContext::new();
        let analyzer = SchemaAnalyzer::new(&ctx);
        let mut schemas = HashMap::new();

        // Create orders table with customer_id
        let orders_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("customer_id", DataType::Int64, false),
            Field::new("total", DataType::Float64, false),
        ]));
        schemas.insert("orders".to_string(), orders_schema);

        // Create customers table
        let customers_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        schemas.insert("customers".to_string(), customers_schema);

        let suggestions = analyzer.analyze_foreign_keys(&schemas);

        assert!(!suggestions.is_empty());
        assert_eq!(suggestions[0].constraint_type, "foreign_key");
        assert!(suggestions[0].tables.contains(&"orders".to_string()));
        assert!(suggestions[0].tables.contains(&"customers".to_string()));
    }

    #[test]
    fn test_temporal_column_detection() {
        let ctx = SessionContext::new();
        let analyzer = SchemaAnalyzer::new(&ctx);

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "created_at",
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None),
                false,
            ),
            Field::new(
                "updated_at",
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("name", DataType::Utf8, false),
        ]));

        let temporal_cols = analyzer.find_temporal_columns(&schema);

        assert_eq!(temporal_cols.len(), 2);
        assert!(temporal_cols.contains(&"created_at".to_string()));
        assert!(temporal_cols.contains(&"updated_at".to_string()));
    }

    #[test]
    fn test_temporal_ordering() {
        let ctx = SessionContext::new();
        let analyzer = SchemaAnalyzer::new(&ctx);

        let (before, after) = analyzer.infer_temporal_order("created_at", "updated_at");
        assert_eq!(before, "created_at");
        assert_eq!(after, "updated_at");

        let (before, after) = analyzer.infer_temporal_order("processed_at", "created_at");
        assert_eq!(before, "created_at");
        assert_eq!(after, "processed_at");
    }

    #[test]
    fn test_amount_column_detection() {
        let ctx = SessionContext::new();
        let analyzer = SchemaAnalyzer::new(&ctx);

        assert!(analyzer.is_amount_column("total_amount", &DataType::Float64));
        assert!(analyzer.is_amount_column("price", &DataType::Decimal128(10, 2)));
        assert!(!analyzer.is_amount_column("customer_id", &DataType::Int64));
        assert!(!analyzer.is_amount_column("total", &DataType::Utf8));
    }
}
