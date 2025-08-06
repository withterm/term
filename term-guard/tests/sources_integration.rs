//! Integration tests for data sources.

use arrow::array::{Array, Int32Array, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::*;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use tempfile::TempDir;
use term_guard::prelude::*;
use term_guard::sources::{CsvSource, DataSource, JsonSource, ParquetSource};

// Database features disabled until properly implemented
// #[cfg(feature = "database")]
// use term_guard::sources::{DatabaseConfig, DatabaseSource};

// #[cfg(all(feature = "database", feature = "postgres"))]
// use term_guard::sources::PostgresSource;

// #[cfg(all(feature = "database", feature = "mysql"))]
// use term_guard::sources::MySqlSource;

// #[cfg(all(feature = "database", feature = "sqlite"))]
// use term_guard::sources::SqliteSource;

/// Creates a test directory with sample CSV files.
fn create_csv_test_data() -> TempDir {
    let dir = TempDir::new().unwrap();

    // Create first CSV file
    let mut file1 = File::create(dir.path().join("data1.csv")).unwrap();
    writeln!(file1, "id,name,value").unwrap();
    writeln!(file1, "1,Alice,100").unwrap();
    writeln!(file1, "2,Bob,200").unwrap();
    file1.flush().unwrap();

    // Create second CSV file
    let mut file2 = File::create(dir.path().join("data2.csv")).unwrap();
    writeln!(file2, "id,name,value").unwrap();
    writeln!(file2, "3,Charlie,300").unwrap();
    writeln!(file2, "4,David,400").unwrap();
    file2.flush().unwrap();

    // Create TSV file
    let mut file3 = File::create(dir.path().join("data.tsv")).unwrap();
    writeln!(file3, "id\tname\tvalue").unwrap();
    writeln!(file3, "5\tEve\t500").unwrap();
    file3.flush().unwrap();

    dir
}

/// Creates a test directory with sample Parquet files.
fn create_parquet_test_data() -> TempDir {
    let dir = TempDir::new().unwrap();

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    // Create first Parquet file
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            Arc::new(Int32Array::from(vec![100, 200])),
        ],
    )
    .unwrap();

    let file1 = File::create(dir.path().join("data1.parquet")).unwrap();
    let mut writer1 = ArrowWriter::try_new(file1, schema.clone(), Default::default()).unwrap();
    writer1.write(&batch1).unwrap();
    writer1.close().unwrap();

    // Create second Parquet file
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![3, 4])),
            Arc::new(StringArray::from(vec!["Charlie", "David"])),
            Arc::new(Int32Array::from(vec![300, 400])),
        ],
    )
    .unwrap();

    let file2 = File::create(dir.path().join("data2.parquet")).unwrap();
    let mut writer2 = ArrowWriter::try_new(file2, schema, Default::default()).unwrap();
    writer2.write(&batch2).unwrap();
    writer2.close().unwrap();

    dir
}

/// Creates a test directory with sample JSON files.
fn create_json_test_data() -> TempDir {
    let dir = TempDir::new().unwrap();

    // Create NDJSON file
    let mut file1 = File::create(dir.path().join("data1.ndjson")).unwrap();
    writeln!(file1, r#"{{"id": 1, "name": "Alice", "value": 100}}"#).unwrap();
    writeln!(file1, r#"{{"id": 2, "name": "Bob", "value": 200}}"#).unwrap();
    file1.flush().unwrap();

    // Create another NDJSON file
    let mut file2 = File::create(dir.path().join("data2.jsonl")).unwrap();
    writeln!(file2, r#"{{"id": 3, "name": "Charlie", "value": 300}}"#).unwrap();
    writeln!(file2, r#"{{"id": 4, "name": "David", "value": 400}}"#).unwrap();
    file2.flush().unwrap();

    // Create regular JSON file
    let mut file3 = File::create(dir.path().join("array.json")).unwrap();
    writeln!(
        file3,
        r#"[
        {{"id": 5, "name": "Eve", "value": 500}},
        {{"id": 6, "name": "Frank", "value": 600}}
    ]"#
    )
    .unwrap();
    file3.flush().unwrap();

    dir
}

#[tokio::test]
async fn test_csv_source_glob_pattern() {
    let dir = create_csv_test_data();
    let pattern = format!("{}/*.csv", dir.path().display());

    let source = CsvSource::from_glob(&pattern).await.unwrap();
    let ctx = SessionContext::new();
    source.register(&ctx, "csv_data").await.unwrap();

    // Query the data
    let df = ctx
        .sql("SELECT COUNT(*) as count, SUM(value) as total FROM csv_data")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    let total = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);

    assert_eq!(count, 4); // 2 files with 2 rows each
    assert_eq!(total, 1000); // 100 + 200 + 300 + 400
}

#[tokio::test]
async fn test_parquet_source_multiple_files() {
    let dir = create_parquet_test_data();
    let pattern = format!("{}/*.parquet", dir.path().display());

    let source = ParquetSource::from_glob(&pattern).await.unwrap();
    let ctx = SessionContext::new();
    source.register(&ctx, "parquet_data").await.unwrap();

    // Query the data
    let df = ctx
        .sql("SELECT name FROM parquet_data ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let column = batches[0].column(0);

    // Handle both StringArray and StringViewArray types
    let names_len;
    let name_0;
    let name_3;

    if let Some(names) = column.as_any().downcast_ref::<StringViewArray>() {
        names_len = names.len();
        name_0 = names.value(0);
        name_3 = names.value(3);
    } else if let Some(names) = column.as_any().downcast_ref::<StringArray>() {
        names_len = names.len();
        name_0 = names.value(0);
        name_3 = names.value(3);
    } else {
        panic!("Unexpected column type: {:?}", column.data_type());
    }

    assert_eq!(names_len, 4);
    assert_eq!(name_0, "Alice");
    assert_eq!(name_3, "David");
}

#[tokio::test]
async fn test_json_source_ndjson_files() {
    use term_guard::sources::JsonOptions;

    let dir = create_json_test_data();
    let patterns = vec![
        format!("{}/*.ndjson", dir.path().display()),
        format!("{}/*.jsonl", dir.path().display()),
    ];

    // Provide explicit schema to avoid inference issues
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let mut source = JsonSource::from_globs(patterns).await.unwrap();
    source = source.with_custom_options(JsonOptions {
        schema: Some(schema),
        ..Default::default()
    });

    let ctx = SessionContext::new();
    source.register(&ctx, "json_data").await.unwrap();

    // Query the data
    let df = ctx
        .sql("SELECT MAX(value) as max_value FROM json_data")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let max_value = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);

    assert_eq!(max_value, 400);
}

#[tokio::test]
async fn test_sources_with_term_context() {
    use term_guard::core::TermContext;

    let dir = create_csv_test_data();
    let csv_path = dir.path().join("data1.csv");

    // Create TermContext and register CSV
    let mut ctx = TermContext::new().unwrap();
    ctx.register_csv("test_data", csv_path.to_str().unwrap())
        .await
        .unwrap();

    // Verify we can query it
    let df = ctx
        .inner()
        .sql("SELECT name, value FROM test_data WHERE value > 150")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches[0].num_rows(), 1);
}

#[tokio::test]
async fn test_csv_custom_delimiter() {
    use term_guard::sources::CsvOptions;

    let dir = create_csv_test_data();
    let tsv_path = format!("{}/data.tsv", dir.path().display());

    let options = CsvOptions {
        delimiter: b'\t',
        ..Default::default()
    };

    let source = CsvSource::with_options(tsv_path, options).unwrap();
    let ctx = SessionContext::new();
    source.register(&ctx, "tsv_data").await.unwrap();

    // Query the data
    let df = ctx.sql("SELECT name FROM tsv_data").await.unwrap();
    let batches = df.collect().await.unwrap();

    let column = batches[0].column(0);
    let name = if let Some(names) = column.as_any().downcast_ref::<StringViewArray>() {
        names.value(0)
    } else if let Some(names) = column.as_any().downcast_ref::<StringArray>() {
        names.value(0)
    } else {
        panic!("Unexpected column type: {:?}", column.data_type());
    };

    assert_eq!(name, "Eve");
}

#[tokio::test]
async fn test_empty_glob_pattern() {
    let dir = TempDir::new().unwrap();
    let pattern = format!("{}/*.csv", dir.path().display());

    let result = CsvSource::from_glob(&pattern).await;
    assert!(result.is_err());

    if let Err(e) = result {
        match e {
            TermError::DataSource { message, .. } => {
                assert!(message.contains("No files found"));
            }
            _ => panic!("Expected DataSource error"),
        }
    }
}

// Database integration tests
// Database features not implemented yet, so we disable these tests
#[cfg(test)]
#[allow(dead_code, unused_imports, unexpected_cfgs)]
mod database_tests {
    #[allow(unused_imports)]
    use super::*;

    #[cfg(feature = "database")]
    use term_guard::sources::{DatabaseConfig, DatabaseSource};

    #[cfg(feature = "postgres")]
    use term_guard::sources::PostgresSource;

    #[cfg(feature = "mysql")]
    use term_guard::sources::MySqlSource;

    #[cfg(feature = "sqlite")]
    use term_guard::sources::SqliteSource;

    #[tokio::test]
    #[allow(unexpected_cfgs)]
    async fn test_database_config_creation() {
        #[cfg(feature = "postgres")]
        {
            let config = DatabaseConfig::PostgreSQL {
                host: "localhost".to_string(),
                port: 5432,
                database: "test_db".to_string(),
                username: "user".to_string(),
                password: "pass".into(),
                sslmode: Some("disable".to_string()),
            };
            assert_eq!(config.database_type(), "PostgreSQL");
        }

        #[cfg(feature = "mysql")]
        {
            let config = DatabaseConfig::MySQL {
                host: "localhost".to_string(),
                port: 3306,
                database: "test_db".to_string(),
                username: "user".to_string(),
                password: "pass".into(),
            };
            assert_eq!(config.database_type(), "MySQL");
        }

        #[cfg(feature = "sqlite")]
        {
            let config = DatabaseConfig::SQLite("test.db".to_string());
            assert_eq!(config.database_type(), "SQLite");
        }
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    #[allow(unexpected_cfgs)]
    async fn test_postgres_source_creation() {
        let source =
            PostgresSource::new("localhost", 5432, "test_db", "user", "pass", "test_table");

        assert!(source.is_ok());
        let source = source.unwrap();

        assert!(source.description().contains("PostgreSQL"));
        assert!(source.description().contains("test_table"));
        assert!(source.description().contains("localhost:5432/test_db"));
    }

    #[cfg(feature = "mysql")]
    #[tokio::test]
    #[allow(unexpected_cfgs)]
    async fn test_mysql_source_creation() {
        let source = MySqlSource::new("localhost", 3306, "test_db", "user", "pass", "test_table");

        assert!(source.is_ok());
        let source = source.unwrap();

        assert!(source.description().contains("MySQL"));
        assert!(source.description().contains("test_table"));
        assert!(source.description().contains("localhost:3306/test_db"));
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    #[allow(unexpected_cfgs)]
    async fn test_sqlite_source_creation() {
        let source = SqliteSource::new("test.db", "test_table");

        assert!(source.is_ok());
        let source = source.unwrap();

        assert!(source.description().contains("SQLite"));
        assert!(source.description().contains("test_table"));
        assert!(source.description().contains("test.db"));
    }

    #[tokio::test]
    #[allow(unexpected_cfgs)]
    async fn test_generic_database_source() {
        #[cfg(feature = "postgres")]
        {
            let config = DatabaseConfig::PostgreSQL {
                host: "localhost".to_string(),
                port: 5432,
                database: "test_db".to_string(),
                username: "user".to_string(),
                password: "pass".into(),
                sslmode: None,
            };
            let source = DatabaseSource::new(config, "test_table");

            assert!(source.is_ok());
            let source = source.unwrap();

            assert!(source.description().contains("PostgreSQL"));
            assert!(source.description().contains("test_table"));
        }

        #[cfg(feature = "sqlite")]
        {
            let config = DatabaseConfig::SQLite("test.db".to_string());
            let source = DatabaseSource::new(config, "test_table");

            assert!(source.is_ok());
            let source = source.unwrap();

            assert!(source.description().contains("SQLite"));
            assert!(source.description().contains("test_table"));
        }
    }

    // Note: These tests require actual database connections to work fully.
    // In a real CI/CD environment, you would set up test databases or use
    // integration test frameworks like testcontainers.

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    #[allow(unexpected_cfgs)]
    async fn test_sqlite_in_memory_connection() {
        use tempfile::NamedTempFile;

        // Create an in-memory SQLite database would require setting up the schema
        // For now, we test that the source can be created without errors
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap();

        let source = SqliteSource::new(db_path, "test_table");
        assert!(source.is_ok());

        let source = source.unwrap();
        assert!(source.description().contains("SQLite"));
        assert!(source.description().contains("test_table"));

        // Note: To fully test registration, we would need to:
        // 1. Create the SQLite database file
        // 2. Create the test_table with some data
        // 3. Then test source.register() with a SessionContext
        // This is left as an exercise for more comprehensive integration testing
    }
}
