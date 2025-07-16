//! Database connectivity for Term validation library.
//!
//! This module provides native database connectors using datafusion-table-providers
//! for PostgreSQL, MySQL, and SQLite. Features include connection pooling, query
//! pushdown optimization, and native Rust implementations for better performance.

use crate::prelude::*;
use crate::security::SecureString;
use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use std::sync::Arc;
use tracing::instrument;

#[cfg(feature = "postgres")]
use datafusion_table_providers::{
    postgres::PostgresTableFactory, sql::db_connection_pool::postgrespool::PostgresConnectionPool,
};

#[cfg(feature = "mysql")]
use datafusion_table_providers::{
    mysql::MySQLTableFactory, sql::db_connection_pool::mysqlpool::MySQLConnectionPool,
};

#[cfg(any(feature = "postgres", feature = "mysql"))]
use datafusion_table_providers::util::secrets::to_secret_map;

#[cfg(feature = "sqlite")]
use datafusion_table_providers::{
    sql::db_connection_pool::{sqlitepool::SqliteConnectionPoolFactory, Mode},
    sqlite::SqliteTableFactory,
};

/// Database connection configuration.
///
/// This enum defines the supported database types and their connection parameters.
/// Each variant contains the connection string or path required to connect to the
/// respective database.
///
/// # Examples
///
/// ```rust,ignore
/// use term_guard::sources::DatabaseConfig;
///
/// let postgres_config = DatabaseConfig::PostgreSQL {
///     host: "localhost".to_string(),
///     port: 5432,
///     database: "mydb".to_string(),
///     username: "user".to_string(),
///     password: "pass".to_string(),
///     sslmode: Some("disable".to_string()),
/// };
///
/// let mysql_config = DatabaseConfig::MySQL {
///     host: "localhost".to_string(),
///     port: 3306,
///     database: "mydb".to_string(),
///     username: "user".to_string(),
///     password: "pass".to_string(),
/// };
///
/// let sqlite_config = DatabaseConfig::SQLite("path/to/database.db".to_string());
/// ```
#[derive(Debug, Clone)]
pub enum DatabaseConfig {
    /// PostgreSQL database connection parameters
    #[cfg(feature = "postgres")]
    PostgreSQL {
        host: String,
        port: u16,
        database: String,
        username: String,
        password: SecureString,
        sslmode: Option<String>,
    },
    /// MySQL database connection parameters
    #[cfg(feature = "mysql")]
    MySQL {
        host: String,
        port: u16,
        database: String,
        username: String,
        password: SecureString,
    },
    /// SQLite database file path
    #[cfg(feature = "sqlite")]
    SQLite(String),
}

impl DatabaseConfig {
    /// Returns a human-readable description of the database type.
    pub fn database_type(&self) -> &'static str {
        match self {
            #[cfg(feature = "postgres")]
            DatabaseConfig::PostgreSQL { .. } => "PostgreSQL",
            #[cfg(feature = "mysql")]
            DatabaseConfig::MySQL { .. } => "MySQL",
            #[cfg(feature = "sqlite")]
            DatabaseConfig::SQLite(_) => "SQLite",
        }
    }
}

/// Generic database source that can connect to any supported database type.
///
/// This source provides a unified interface for database connectivity with support
/// for connection pooling, query pushdown optimization, and schema inference.
///
/// # Examples
///
/// ```rust,ignore
/// use term_guard::sources::{DatabaseSource, DatabaseConfig};
/// use datafusion::prelude::SessionContext;
///
/// # async fn example() -> Result<()> {
/// let config = DatabaseConfig::PostgreSQL {
///     host: "localhost".to_string(),
///     port: 5432,
///     database: "mydb".to_string(),
///     username: "user".to_string(),
///     password: "pass".to_string(),
///     sslmode: Some("disable".to_string()),
/// };
///
/// let source = DatabaseSource::new(config, "users_table")?;
/// let ctx = SessionContext::new();
/// source.register(&ctx, "users").await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct DatabaseSource {
    config: DatabaseConfig,
    table_name: String,
    schema: Option<Arc<Schema>>,
}

impl DatabaseSource {
    /// Creates a new database source with the given configuration and table name.
    ///
    /// # Arguments
    ///
    /// * `config` - Database configuration containing connection details
    /// * `table_name` - Name of the table to access in the database
    ///
    /// # Returns
    ///
    /// A new `DatabaseSource` instance
    pub fn new(config: DatabaseConfig, table_name: impl Into<String>) -> Result<Self> {
        Ok(Self {
            config,
            table_name: table_name.into(),
            schema: None,
        })
    }

    /// Creates a table provider for the configured database type.
    #[instrument(skip(self), fields(db_type = %self.config.database_type()))]
    async fn create_table_provider(&self) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
        match &self.config {
            #[cfg(feature = "postgres")]
            DatabaseConfig::PostgreSQL {
                host,
                port,
                database,
                username,
                password,
                sslmode,
            } => {
                let mut params = std::collections::HashMap::new();
                params.insert("host".to_string(), host.clone());
                params.insert("port".to_string(), port.to_string());
                params.insert("db".to_string(), database.clone());
                params.insert("user".to_string(), username.clone());
                params.insert("pass".to_string(), password.expose().to_string());
                if let Some(ssl) = sslmode {
                    params.insert("sslmode".to_string(), ssl.clone());
                }

                let postgres_params = to_secret_map(params);
                let postgres_pool = Arc::new(
                    PostgresConnectionPool::new(postgres_params)
                        .await
                        .map_err(|e| TermError::DataSource {
                            source_type: "PostgreSQL".to_string(),
                            message: format!("Failed to create PostgreSQL connection pool: {e}"),
                            source: Some(Box::new(e)),
                        })?,
                );

                let table_factory = PostgresTableFactory::new(postgres_pool);

                table_factory
                    .table_provider(TableReference::bare(self.table_name.as_str()))
                    .await
                    .map_err(|e| TermError::DataSource {
                        source_type: "PostgreSQL".to_string(),
                        message: format!(
                            "Failed to create table provider for '{}': {e}",
                            self.table_name
                        ),
                        source: None,
                    })
            }
            #[cfg(feature = "mysql")]
            DatabaseConfig::MySQL {
                host,
                port,
                database,
                username,
                password,
            } => {
                let password_str = password.expose();
                let connection_string =
                    format!("mysql://{username}:{password_str}@{host}:{port}/{database}");
                let mut params = std::collections::HashMap::new();
                params.insert("connection_string".to_string(), connection_string);
                params.insert("sslmode".to_string(), "disabled".to_string());

                let mysql_params = to_secret_map(params);
                let mysql_pool =
                    Arc::new(MySQLConnectionPool::new(mysql_params).await.map_err(|e| {
                        TermError::DataSource {
                            source_type: "MySQL".to_string(),
                            message: format!("Failed to create MySQL connection pool: {e}"),
                            source: Some(Box::new(e)),
                        }
                    })?);

                let table_factory = MySQLTableFactory::new(mysql_pool);

                table_factory
                    .table_provider(TableReference::bare(self.table_name.as_str()))
                    .await
                    .map_err(|e| TermError::DataSource {
                        source_type: "MySQL".to_string(),
                        message: format!(
                            "Failed to create table provider for '{}': {e}",
                            self.table_name
                        ),
                        source: None,
                    })
            }
            #[cfg(feature = "sqlite")]
            DatabaseConfig::SQLite(path) => {
                let sqlite_pool = Arc::new(
                    SqliteConnectionPoolFactory::new(
                        path,
                        Mode::File,
                        std::time::Duration::from_millis(5000),
                    )
                    .build()
                    .await
                    .map_err(|e| TermError::DataSource {
                        source_type: "SQLite".to_string(),
                        message: format!("Failed to create SQLite connection pool: {e}"),
                        source: None,
                    })?,
                );

                let table_factory = SqliteTableFactory::new(sqlite_pool);

                table_factory
                    .table_provider(TableReference::bare(self.table_name.as_str()))
                    .await
                    .map_err(|e| TermError::DataSource {
                        source_type: "SQLite".to_string(),
                        message: format!(
                            "Failed to create table provider for '{}': {e}",
                            self.table_name
                        ),
                        source: None,
                    })
            }
        }
    }
}

#[async_trait]
impl super::DataSource for DatabaseSource {
    #[instrument(skip(self, ctx, telemetry), fields(db_type = %self.config.database_type(), table = %self.table_name, table_name = %table_name))]
    async fn register_with_telemetry(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        telemetry: Option<&Arc<TermTelemetry>>,
    ) -> Result<()> {
        // Create telemetry span for database source loading
        let mut _datasource_span = if let Some(tel) = telemetry {
            tel.start_datasource_span(self.config.database_type(), table_name)
        } else {
            TermSpan::noop()
        };

        let provider = self.create_table_provider().await?;

        ctx.register_table(table_name, provider)
            .map_err(|e| TermError::DataSource {
                source_type: self.config.database_type().to_string(),
                message: format!("Failed to register table '{table_name}': {e}"),
                source: Some(Box::new(e)),
            })?;

        Ok(())
    }

    fn schema(&self) -> Option<&Arc<Schema>> {
        self.schema.as_ref()
    }

    fn description(&self) -> String {
        match &self.config {
            #[cfg(feature = "postgres")]
            DatabaseConfig::PostgreSQL {
                host,
                port,
                database,
                ..
            } => {
                let table_name = &self.table_name;
                format!("PostgreSQL table '{table_name}' at {host}:{port}/{database}")
            }
            #[cfg(feature = "mysql")]
            DatabaseConfig::MySQL {
                host,
                port,
                database,
                ..
            } => {
                let table_name = &self.table_name;
                format!("MySQL table '{table_name}' at {host}:{port}/{database}")
            }
            #[cfg(feature = "sqlite")]
            DatabaseConfig::SQLite(path) => {
                let table_name = &self.table_name;
                format!("SQLite table '{table_name}' at {path}")
            }
        }
    }
}

/// PostgreSQL-specific database source.
///
/// This is a convenience wrapper around `DatabaseSource` for PostgreSQL connections.
/// It provides a more ergonomic API for PostgreSQL-specific use cases.
///
/// # Examples
///
/// ```rust,ignore
/// use term_guard::sources::PostgresSource;
/// use datafusion::prelude::SessionContext;
///
/// # async fn example() -> Result<()> {
/// let source = PostgresSource::new(
///     "localhost",
///     5432,
///     "mydb",
///     "user",
///     "pass",
///     "users_table"
/// )?;
///
/// let ctx = SessionContext::new();
/// source.register(&ctx, "users").await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "postgres")]
#[derive(Debug, Clone)]
pub struct PostgresSource {
    inner: DatabaseSource,
}

#[cfg(feature = "postgres")]
impl PostgresSource {
    /// Creates a new PostgreSQL source.
    ///
    /// # Arguments
    ///
    /// * `host` - PostgreSQL server hostname
    /// * `port` - PostgreSQL server port
    /// * `database` - Database name
    /// * `username` - Database username
    /// * `password` - Database password
    /// * `table_name` - Name of the table to access
    ///
    /// # Returns
    ///
    /// A new `PostgresSource` instance
    pub fn new(
        host: impl Into<String>,
        port: u16,
        database: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Result<Self> {
        let config = DatabaseConfig::PostgreSQL {
            host: host.into(),
            port,
            database: database.into(),
            username: username.into(),
            password: SecureString::new(password.into()),
            sslmode: Some("disable".to_string()),
        };
        let inner = DatabaseSource::new(config, table_name)?;
        Ok(Self { inner })
    }

    /// Creates a new PostgreSQL source with SSL mode.
    pub fn new_with_ssl(
        host: impl Into<String>,
        port: u16,
        database: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
        table_name: impl Into<String>,
        sslmode: impl Into<String>,
    ) -> Result<Self> {
        let config = DatabaseConfig::PostgreSQL {
            host: host.into(),
            port,
            database: database.into(),
            username: username.into(),
            password: SecureString::new(password.into()),
            sslmode: Some(sslmode.into()),
        };
        let inner = DatabaseSource::new(config, table_name)?;
        Ok(Self { inner })
    }
}

#[cfg(feature = "postgres")]
#[async_trait]
impl super::DataSource for PostgresSource {
    async fn register_with_telemetry(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        telemetry: Option<&Arc<TermTelemetry>>,
    ) -> Result<()> {
        self.inner
            .register_with_telemetry(ctx, table_name, telemetry)
            .await
    }

    fn schema(&self) -> Option<&Arc<Schema>> {
        self.inner.schema()
    }

    fn description(&self) -> String {
        self.inner.description()
    }
}

/// MySQL-specific database source.
///
/// This is a convenience wrapper around `DatabaseSource` for MySQL connections.
/// It provides a more ergonomic API for MySQL-specific use cases.
///
/// # Examples
///
/// ```rust,ignore
/// use term_guard::sources::MySqlSource;
/// use datafusion::prelude::SessionContext;
///
/// # async fn example() -> Result<()> {
/// let source = MySqlSource::new(
///     "localhost",
///     3306,
///     "mydb",
///     "user",
///     "pass",
///     "users_table"
/// )?;
///
/// let ctx = SessionContext::new();
/// source.register(&ctx, "users").await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "mysql")]
#[derive(Debug, Clone)]
pub struct MySqlSource {
    inner: DatabaseSource,
}

#[cfg(feature = "mysql")]
impl MySqlSource {
    /// Creates a new MySQL source.
    ///
    /// # Arguments
    ///
    /// * `host` - MySQL server hostname
    /// * `port` - MySQL server port
    /// * `database` - Database name
    /// * `username` - Database username
    /// * `password` - Database password
    /// * `table_name` - Name of the table to access
    ///
    /// # Returns
    ///
    /// A new `MySqlSource` instance
    pub fn new(
        host: impl Into<String>,
        port: u16,
        database: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Result<Self> {
        let config = DatabaseConfig::MySQL {
            host: host.into(),
            port,
            database: database.into(),
            username: username.into(),
            password: SecureString::new(password.into()),
        };
        let inner = DatabaseSource::new(config, table_name)?;
        Ok(Self { inner })
    }
}

#[cfg(feature = "mysql")]
#[async_trait]
impl super::DataSource for MySqlSource {
    async fn register_with_telemetry(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        telemetry: Option<&Arc<TermTelemetry>>,
    ) -> Result<()> {
        self.inner
            .register_with_telemetry(ctx, table_name, telemetry)
            .await
    }

    fn schema(&self) -> Option<&Arc<Schema>> {
        self.inner.schema()
    }

    fn description(&self) -> String {
        self.inner.description()
    }
}

/// SQLite-specific database source.
///
/// This is a convenience wrapper around `DatabaseSource` for SQLite connections.
/// It provides a more ergonomic API for SQLite-specific use cases.
///
/// # Examples
///
/// ```rust,ignore
/// use term_guard::sources::SqliteSource;
/// use datafusion::prelude::SessionContext;
///
/// # async fn example() -> Result<()> {
/// let source = SqliteSource::new("path/to/database.db", "users_table")?;
///
/// let ctx = SessionContext::new();
/// source.register(&ctx, "users").await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "sqlite")]
#[derive(Debug, Clone)]
pub struct SqliteSource {
    inner: DatabaseSource,
}

#[cfg(feature = "sqlite")]
impl SqliteSource {
    /// Creates a new SQLite source.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the SQLite database file
    /// * `table_name` - Name of the table to access
    ///
    /// # Returns
    ///
    /// A new `SqliteSource` instance
    pub fn new(path: impl Into<String>, table_name: impl Into<String>) -> Result<Self> {
        let config = DatabaseConfig::SQLite(path.into());
        let inner = DatabaseSource::new(config, table_name)?;
        Ok(Self { inner })
    }
}

#[cfg(feature = "sqlite")]
#[async_trait]
impl super::DataSource for SqliteSource {
    async fn register_with_telemetry(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        telemetry: Option<&Arc<TermTelemetry>>,
    ) -> Result<()> {
        self.inner
            .register_with_telemetry(ctx, table_name, telemetry)
            .await
    }

    fn schema(&self) -> Option<&Arc<Schema>> {
        self.inner.schema()
    }

    fn description(&self) -> String {
        self.inner.description()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::DataSource;

    #[test]
    fn test_database_config_description() {
        #[cfg(feature = "postgres")]
        {
            let config = DatabaseConfig::PostgreSQL {
                host: "localhost".to_string(),
                port: 5432,
                database: "mydb".to_string(),
                username: "user".to_string(),
                password: SecureString::new("pass"),
                sslmode: None,
            };
            assert_eq!(config.database_type(), "PostgreSQL");
        }

        #[cfg(feature = "mysql")]
        {
            let config = DatabaseConfig::MySQL {
                host: "localhost".to_string(),
                port: 3306,
                database: "mydb".to_string(),
                username: "user".to_string(),
                password: SecureString::new("pass"),
            };
            assert_eq!(config.database_type(), "MySQL");
        }

        #[cfg(feature = "sqlite")]
        {
            let config = DatabaseConfig::SQLite("path/to/database.db".to_string());
            assert_eq!(config.database_type(), "SQLite");
        }
    }

    #[test]
    fn test_database_source_creation() {
        #[cfg(feature = "postgres")]
        {
            let config = DatabaseConfig::PostgreSQL {
                host: "localhost".to_string(),
                port: 5432,
                database: "mydb".to_string(),
                username: "user".to_string(),
                password: SecureString::new("pass"),
                sslmode: None,
            };
            let source = DatabaseSource::new(config, "test_table").unwrap();
            assert_eq!(source.table_name, "test_table");
            assert!(source.description().contains("PostgreSQL"));
            assert!(source.description().contains("test_table"));
        }

        #[cfg(feature = "sqlite")]
        {
            let config = DatabaseConfig::SQLite("test.db".to_string());
            let source = DatabaseSource::new(config, "test_table").unwrap();
            assert_eq!(source.table_name, "test_table");
            assert!(source.description().contains("SQLite"));
            assert!(source.description().contains("test_table"));
        }
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn test_postgres_source_creation() {
        let source =
            PostgresSource::new("localhost", 5432, "mydb", "user", "pass", "test_table").unwrap();
        assert!(source.description().contains("PostgreSQL"));
        assert!(source.description().contains("test_table"));
    }

    #[cfg(feature = "mysql")]
    #[test]
    fn test_mysql_source_creation() {
        let source =
            MySqlSource::new("localhost", 3306, "mydb", "user", "pass", "test_table").unwrap();
        assert!(source.description().contains("MySQL"));
        assert!(source.description().contains("test_table"));
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn test_sqlite_source_creation() {
        let source = SqliteSource::new("test.db", "test_table").unwrap();
        assert!(source.description().contains("SQLite"));
        assert!(source.description().contains("test_table"));
    }
}
