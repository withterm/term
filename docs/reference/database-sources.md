# Database Sources Reference

<!-- 
This is a REFERENCE document following Diátaxis principles.
Information-oriented, comprehensive technical description.
-->

## Overview

Database sources provide native connectivity to PostgreSQL, MySQL, and SQLite databases through DataFusion table providers. They enable direct validation of database tables without requiring data export or transformation.

## Synopsis

```rust
use term_guard::sources::{DatabaseConfig, DatabaseSource, PostgresSource, MySqlSource, SqliteSource};
use datafusion::prelude::SessionContext;

// Generic database source
let config = DatabaseConfig::PostgreSQL { /* ... */ };
let source = DatabaseSource::new(config, "table_name")?;

// Database-specific sources
let postgres = PostgresSource::new("host", 5432, "db", "user", "pass", "table")?;
let mysql = MySqlSource::new("host", 3306, "db", "user", "pass", "table")?;
let sqlite = SqliteSource::new("path/to/db.sqlite", "table")?;

// Register with DataFusion
let ctx = SessionContext::new();
source.register(&ctx, "table_alias").await?;
```

## Description

Database sources integrate with DataFusion's query engine to provide native database connectivity. They handle connection pooling, query pushdown optimization, and schema inference automatically. All database sources implement the `DataSource` trait and can be used interchangeably in Term's validation framework.

## API Reference

### Types

#### `DatabaseConfig`

```rust
pub enum DatabaseConfig {
    #[cfg(feature = "postgres")]
    PostgreSQL {
        host: String,
        port: u16,
        database: String,
        username: String,
        password: SecureString,
        sslmode: Option<String>,
    },
    #[cfg(feature = "mysql")]
    MySQL {
        host: String,
        port: u16,
        database: String,
        username: String,
        password: SecureString,
    },
    #[cfg(feature = "sqlite")]
    SQLite(String),
}
```

Configuration enum defining connection parameters for supported database types.

##### Variants

- **`PostgreSQL`** - PostgreSQL database connection parameters
  - **`host`**: `String` - Server hostname or IP address
  - **`port`**: `u16` - Server port (typically 5432)
  - **`database`**: `String` - Database name
  - **`username`**: `String` - Database username
  - **`password`**: `SecureString` - Database password (securely stored)
  - **`sslmode`**: `Option<String>` - SSL connection mode ("disable", "prefer", "require")

- **`MySQL`** - MySQL database connection parameters
  - **`host`**: `String` - Server hostname or IP address
  - **`port`**: `u16` - Server port (typically 3306)
  - **`database`**: `String` - Database name
  - **`username`**: `String` - Database username
  - **`password`**: `SecureString` - Database password (securely stored)

- **`SQLite`** - SQLite database file path
  - **`String`** - Absolute or relative path to SQLite database file

##### Example

```rust
let postgres_config = DatabaseConfig::PostgreSQL {
    host: "localhost".to_string(),
    port: 5432,
    database: "mydb".to_string(),
    username: "user".to_string(),
    password: SecureString::new("password"),
    sslmode: Some("require".to_string()),
};
```

#### `DatabaseSource`

```rust
pub struct DatabaseSource {
    config: DatabaseConfig,
    table_name: String,
    schema: Option<Arc<Schema>>,
}
```

Generic database source that can connect to any supported database type.

##### Fields

- **`config`**: `DatabaseConfig` - Database connection configuration
- **`table_name`**: `String` - Name of the database table to access
- **`schema`**: `Option<Arc<Schema>>` - Cached Arrow schema (populated after connection)

##### Example

```rust
let config = DatabaseConfig::SQLite("data.db".to_string());
let source = DatabaseSource::new(config, "customers")?;
```

#### `PostgresSource`

```rust
#[cfg(feature = "postgres")]
pub struct PostgresSource {
    inner: DatabaseSource,
}
```

PostgreSQL-specific database source providing ergonomic API for PostgreSQL connections.

##### Example

```rust
let source = PostgresSource::new("localhost", 5432, "mydb", "user", "pass", "table")?;
```

#### `MySqlSource`

```rust
#[cfg(feature = "mysql")]
pub struct MySqlSource {
    inner: DatabaseSource,
}
```

MySQL-specific database source providing ergonomic API for MySQL connections.

##### Example

```rust
let source = MySqlSource::new("localhost", 3306, "mydb", "user", "pass", "table")?;
```

#### `SqliteSource`

```rust
#[cfg(feature = "sqlite")]
pub struct SqliteSource {
    inner: DatabaseSource,
}
```

SQLite-specific database source providing ergonomic API for SQLite connections.

##### Example

```rust
let source = SqliteSource::new("path/to/database.db", "table")?;
```

### Functions

#### `DatabaseSource::new`

```rust
pub fn new(config: DatabaseConfig, table_name: impl Into<String>) -> Result<Self>
```

Creates a new database source with the given configuration and table name.

##### Parameters

- **`config`**: `DatabaseConfig` - Database connection configuration
- **`table_name`**: `impl Into<String>` - Name of the table to access

##### Returns

- `Result<DatabaseSource>` - New database source instance

##### Errors

- `TermError::DataSource` - If configuration is invalid

##### Example

```rust
let config = DatabaseConfig::PostgreSQL { /* ... */ };
let source = DatabaseSource::new(config, "my_table")?;
```

#### `PostgresSource::new`

```rust
#[cfg(feature = "postgres")]
pub fn new(
    host: impl Into<String>,
    port: u16,
    database: impl Into<String>,
    username: impl Into<String>,
    password: impl Into<String>,
    table_name: impl Into<String>,
) -> Result<Self>
```

Creates a new PostgreSQL source with SSL disabled.

##### Parameters

- **`host`**: `impl Into<String>` - PostgreSQL server hostname
- **`port`**: `u16` - PostgreSQL server port
- **`database`**: `impl Into<String>` - Database name
- **`username`**: `impl Into<String>` - Database username
- **`password`**: `impl Into<String>` - Database password
- **`table_name`**: `impl Into<String>` - Name of the table to access

##### Returns

- `Result<PostgresSource>` - New PostgreSQL source instance

##### Example

```rust
let source = PostgresSource::new("localhost", 5432, "mydb", "user", "pass", "customers")?;
```

#### `PostgresSource::new_with_ssl`

```rust
#[cfg(feature = "postgres")]
pub fn new_with_ssl(
    host: impl Into<String>,
    port: u16,
    database: impl Into<String>,
    username: impl Into<String>,
    password: impl Into<String>,
    table_name: impl Into<String>,
    sslmode: impl Into<String>,
) -> Result<Self>
```

Creates a new PostgreSQL source with SSL configuration.

##### Parameters

- **`host`**: `impl Into<String>` - PostgreSQL server hostname
- **`port`**: `u16` - PostgreSQL server port
- **`database`**: `impl Into<String>` - Database name
- **`username`**: `impl Into<String>` - Database username
- **`password`**: `impl Into<String>` - Database password
- **`table_name`**: `impl Into<String>` - Name of the table to access
- **`sslmode`**: `impl Into<String>` - SSL mode ("disable", "prefer", "require")

##### Returns

- `Result<PostgresSource>` - New PostgreSQL source instance

##### Example

```rust
let source = PostgresSource::new_with_ssl(
    "db.example.com", 5432, "prod", "app", "secret", "users", "require"
)?;
```

#### `MySqlSource::new`

```rust
#[cfg(feature = "mysql")]
pub fn new(
    host: impl Into<String>,
    port: u16,
    database: impl Into<String>,
    username: impl Into<String>,
    password: impl Into<String>,
    table_name: impl Into<String>,
) -> Result<Self>
```

Creates a new MySQL source.

##### Parameters

- **`host`**: `impl Into<String>` - MySQL server hostname
- **`port`**: `u16` - MySQL server port
- **`database`**: `impl Into<String>` - Database name
- **`username`**: `impl Into<String>` - Database username
- **`password`**: `impl Into<String>` - Database password
- **`table_name`**: `impl Into<String>` - Name of the table to access

##### Returns

- `Result<MySqlSource>` - New MySQL source instance

##### Example

```rust
let source = MySqlSource::new("localhost", 3306, "mydb", "user", "pass", "orders")?;
```

#### `SqliteSource::new`

```rust
#[cfg(feature = "sqlite")]
pub fn new(path: impl Into<String>, table_name: impl Into<String>) -> Result<Self>
```

Creates a new SQLite source.

##### Parameters

- **`path`**: `impl Into<String>` - Path to the SQLite database file
- **`table_name`**: `impl Into<String>` - Name of the table to access

##### Returns

- `Result<SqliteSource>` - New SQLite source instance

##### Example

```rust
let source = SqliteSource::new("data/app.db", "events")?;
```

### Traits

#### `DataSource`

```rust
#[async_trait]
pub trait DataSource {
    async fn register_with_telemetry(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        telemetry: Option<&Arc<TermTelemetry>>,
    ) -> Result<()>;

    fn schema(&self) -> Option<&Arc<Schema>>;
    fn description(&self) -> String;
}
```

Trait implemented by all database sources for registering tables with DataFusion.

##### Required Methods

- **`register_with_telemetry`** - Registers the data source with a DataFusion context
- **`schema`** - Returns the Arrow schema of the data source (if available)
- **`description`** - Returns a human-readable description of the data source

##### Provided Methods

- **`register`** - Convenience method that calls `register_with_telemetry` with no telemetry

### Configuration

#### Connection Pooling

Database sources automatically manage connection pools with the following defaults:

- **Pool Size**: Automatically determined based on system resources
- **Connection Timeout**: 5 seconds for SQLite, 30 seconds for PostgreSQL/MySQL
- **Idle Timeout**: Connections are recycled after period of inactivity
- **Max Lifetime**: Connections are recreated periodically for security

#### SSL Configuration (PostgreSQL)

- **`disable`** - No SSL connection
- **`prefer`** - SSL if available, fallback to non-SSL
- **`require`** - SSL required, fail if unavailable
- **`verify-ca`** - SSL required with CA verification
- **`verify-full`** - SSL required with full verification

#### Environment Variables

Database sources respect the following environment variables:

- **`POSTGRES_HOST`** - Default PostgreSQL host
- **`POSTGRES_PORT`** - Default PostgreSQL port
- **`POSTGRES_USER`** - Default PostgreSQL username
- **`POSTGRES_PASSWORD`** - Default PostgreSQL password
- **`POSTGRES_DB`** - Default PostgreSQL database
- **`MYSQL_HOST`** - Default MySQL host
- **`MYSQL_PORT`** - Default MySQL port
- **`MYSQL_USER`** - Default MySQL username
- **`MYSQL_PASSWORD`** - Default MySQL password
- **`MYSQL_DATABASE`** - Default MySQL database

### Constants

#### Connection Timeouts

- **`SQLITE_CONNECTION_TIMEOUT`**: `Duration::from_millis(5000)` - SQLite connection timeout
- **`DEFAULT_CONNECTION_TIMEOUT`**: `Duration::from_secs(30)` - Default connection timeout for networked databases

## Behavior

### Connection Management

Database sources use connection pooling to efficiently manage database connections:

1. **Pool Creation**: Connection pools are created lazily on first use
2. **Connection Reuse**: Connections are reused across multiple validation runs
3. **Automatic Cleanup**: Idle connections are automatically closed
4. **Error Recovery**: Failed connections are automatically retried

### Query Pushdown

Database sources leverage DataFusion's query pushdown optimization:

- **Filter Pushdown**: WHERE clauses are executed on the database server
- **Projection Pushdown**: Only required columns are fetched
- **Aggregation Pushdown**: COUNT, SUM, and other aggregations run on the server
- **Limit Pushdown**: LIMIT clauses reduce data transfer

### Schema Inference

Database sources automatically infer Arrow schemas from database tables:

- **Type Mapping**: Database types are mapped to appropriate Arrow types
- **Nullable Handling**: NULL constraints are preserved in the Arrow schema
- **Schema Caching**: Schemas are cached to avoid repeated metadata queries

### Error Handling

Database sources provide detailed error information:

- **Connection Errors**: Network issues, authentication failures
- **Schema Errors**: Table not found, permission denied
- **Query Errors**: SQL syntax errors, constraint violations
- **Timeout Errors**: Connection or query timeouts

## Performance Characteristics

- **Connection Overhead**: O(1) - Connection pooling amortizes connection costs
- **Query Performance**: Database-dependent - leverages native query optimization
- **Memory Usage**: O(result_set_size) - Results are streamed when possible
- **Network Efficiency**: Optimized through query pushdown and result streaming

## Version History

| Version | Changes |
|---------|---------|
| 0.0.1   | Initial implementation with PostgreSQL, MySQL, SQLite support |
| 0.0.2   | Added SSL support for PostgreSQL |
| 0.0.3   | Added connection pooling optimization |
| 0.0.4   | Added ValidationContext integration |

## Examples

### Basic Example

```rust
use term_guard::sources::{DatabaseConfig, DatabaseSource};
use term_guard::prelude::*;
use datafusion::prelude::SessionContext;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = DatabaseConfig::SQLite("data.db".to_string());
    let source = DatabaseSource::new(config, "customers")?;
    
    let ctx = SessionContext::new();
    source.register(&ctx, "customers").await?;
    
    let suite = ValidationSuite::builder("customer_validation")
        .table_name("customers")
        .add_check(Check::new("Basic validation").is_complete("id"))
        .build();
    
    let result = suite.run(&ctx).await?;
    println!("Success: {}", result.is_success());
    
    Ok(())
}
```

### Advanced Example with Multiple Databases

```rust
use term_guard::sources::{PostgresSource, MySqlSource, SqliteSource};
use term_guard::prelude::*;
use datafusion::prelude::SessionContext;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    
    // Connect to multiple databases
    let postgres = PostgresSource::new_with_ssl(
        "db1.example.com", 5432, "prod", "reader", "pass", "users", "require"
    )?;
    let mysql = MySqlSource::new(
        "db2.example.com", 3306, "analytics", "reader", "pass", "events"
    )?;
    let sqlite = SqliteSource::new("local_cache.db", "cache")?;
    
    // Register all tables
    postgres.register(&ctx, "users").await?;
    mysql.register(&ctx, "events").await?;
    sqlite.register(&ctx, "cache").await?;
    
    // Validate each source
    let sources = vec![
        ("users", "User data validation"),
        ("events", "Event data validation"),
        ("cache", "Cache data validation"),
    ];
    
    for (table, description) in sources {
        let suite = ValidationSuite::builder(description)
            .table_name(table)
            .add_check(Check::new("Data integrity").is_complete("id"))
            .build();
        
        let result = suite.run(&ctx).await?;
        println!("{}: {}", description, if result.is_success() { "✅" } else { "❌" });
    }
    
    Ok(())
}
```

## See Also

- [`ValidationContext`](validation-context.md) - Runtime context for dynamic table names
- [`DataSource`](../reference/constraints.md#datasource) - Base trait for all data sources
- [How to Connect to PostgreSQL](../how-to/connect-postgresql.md)
- [How to Connect to MySQL](../how-to/connect-mysql.md)
- [How to Connect to SQLite](../how-to/connect-sqlite.md)
- [Understanding Database Connectors](../explanation/database-connectors.md)

---