use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use etl::types::{ColumnSchema, Type};
use mysql_async::prelude::*;
use mysql_async::{Conn, OptsBuilder, Pool, PoolConstraints, PoolOpts};
use tracing::{debug, info};

/// Trace identifier for ETL operations in Doris client.
const ETL_TRACE_ID: &str = "ETL DorisClient";

/// Special column name for Change Data Capture operations in Doris.
pub(crate) const DORIS_CDC_SPECIAL_COLUMN: &str = "_change_type";

/// Special column name for Change Data Capture sequence ordering in Doris.
pub(crate) const DORIS_CDC_SEQUENCE_COLUMN: &str = "_change_sequence_number";

/// Doris table identifier (fully qualified: database.table).
pub type DorisTableId = String;

/// Client for interacting with Apache Doris.
///
/// Provides methods for table management, data insertion via StreamLoad,
/// and query execution against Doris databases with connection pooling.
#[derive(Clone, Debug)]
pub struct DorisClient {
    database: String,
    host: String,
    http_port: u16,
    pool: Pool,
    username: String,
    password: String,
}

impl DorisClient {
    /// Creates a new [`DorisClient`] with connection pooling.
    ///
    /// # Arguments
    /// * `host` - The Doris frontend host
    /// * `query_port` - The MySQL protocol port (typically 9030)
    /// * `http_port` - The HTTP port for StreamLoad (typically 8030)
    /// * `database` - The database name
    /// * `username` - The username for authentication
    /// * `password` - The password for authentication
    pub async fn new(
        host: String,
        query_port: u16,
        http_port: u16,
        database: String,
        username: String,
        password: String,
    ) -> EtlResult<Self> {
        let opts = OptsBuilder::default()
            .ip_or_hostname(&host)
            .tcp_port(query_port)
            .db_name(Some(&database))
            .user(Some(&username))
            .pass(Some(&password))
            .pool_opts(PoolOpts::default().with_constraints(PoolConstraints::new(5, 30).unwrap()));

        let pool = Pool::new(opts);

        // Test the connection
        let mut conn = pool.get_conn().await.map_err(|e| {
            etl_error!(
                ErrorKind::DestinationConnectionFailed,
                "Failed to connect to Doris",
                e.to_string()
            )
        })?;

        // Verify database exists
        conn.query_drop(format!("USE `{}`", database))
            .await
            .map_err(|e| {
                etl_error!(
                    ErrorKind::DestinationConnectionFailed,
                    "Failed to use database",
                    format!("Database '{}': {}", database, e)
                )
            })?;

        drop(conn);

        info!(target: ETL_TRACE_ID, "Connected to Doris at {}:{} database: {}", host, query_port, database);

        Ok(Self {
            database,
            host,
            http_port,
            pool,
            username,
            password,
        })
    }

    /// Gets a connection from the pool.
    async fn get_conn(&self) -> EtlResult<Conn> {
        self.pool.get_conn().await.map_err(|e| {
            etl_error!(
                ErrorKind::DestinationConnectionFailed,
                "Failed to get connection from pool",
                e.to_string()
            )
        })
    }

    /// Creates a table in Doris if it doesn't already exist.
    ///
    /// Creates a Unique Key table model with CDC columns (`_change_type`, `_change_sequence_number`)
    /// for change tracking. The table uses HASH distribution and is configured for single-replica storage.
    ///
    /// If primary key columns are specified in the schema, they become the Unique Key.
    /// If no primary keys are specified, all columns are used as the Unique Key.
    ///
    /// # Arguments
    /// * `table_name` - The name of the table to create
    /// * `schema` - Column schemas defining the table structure
    ///
    /// # Returns
    /// * `Ok(())` if the table was created or already exists
    /// * `Err` if table creation failed
    pub async fn create_table_if_not_exists(
        &self,
        table_name: &str,
        schema: &[ColumnSchema],
    ) -> EtlResult<()> {
        let mut conn = self.get_conn().await?;

        // Build column definitions
        let mut column_defs = Vec::new();
        let mut key_columns = Vec::new();

        // Add CDC columns
        column_defs.push(format!("`{}` VARCHAR(10)", DORIS_CDC_SPECIAL_COLUMN));
        column_defs.push(format!("`{}` BIGINT", DORIS_CDC_SEQUENCE_COLUMN));

        for col in schema {
            let doris_type = type_to_doris_type(&col.typ)?;
            column_defs.push(format!("`{}` {}", col.name, doris_type));

            // Primary key columns become key columns in Doris
            if col.primary {
                key_columns.push(format!("`{}`", col.name));
            }
        }

        // If no primary keys specified, use all columns as keys
        if key_columns.is_empty() {
            for col in schema {
                key_columns.push(format!("`{}`", col.name));
            }
        }

        let create_table_sql = format!(
            "CREATE TABLE IF NOT EXISTS `{}`.`{}` ({}) UNIQUE KEY({}) DISTRIBUTED BY HASH({}) BUCKETS 10 PROPERTIES (\"replication_num\" = \"1\")",
            self.database,
            table_name,
            column_defs.join(", "),
            key_columns.join(", "),
            key_columns.first().unwrap_or(&"`_change_type`".to_string())
        );

        debug!(target: ETL_TRACE_ID, "Creating table with SQL: {}", create_table_sql);

        conn.query_drop(&create_table_sql).await.map_err(|e| {
            etl_error!(
                ErrorKind::DestinationError,
                "Failed to create table",
                format!("Table '{}': {}", table_name, e)
            )
        })?;

        info!(target: ETL_TRACE_ID, "Created table: {}.{}", self.database, table_name);

        Ok(())
    }

    /// Truncates all data from a Doris table.
    ///
    /// Executes a TRUNCATE TABLE statement to remove all rows while preserving the table structure and schema.
    ///
    /// # Arguments
    /// * `table_name` - The name of the table to truncate
    ///
    /// # Returns
    /// * `Ok(())` if the table was successfully truncated
    /// * `Err` if the truncate operation failed
    pub async fn truncate_table(&self, table_name: &str) -> EtlResult<()> {
        let mut conn = self.get_conn().await?;

        let truncate_sql = format!("TRUNCATE TABLE `{}`.`{}`", self.database, table_name);

        conn.query_drop(&truncate_sql).await.map_err(|e| {
            etl_error!(
                ErrorKind::DestinationError,
                "Failed to truncate table",
                format!("Table '{}': {}", table_name, e)
            )
        })?;

        info!(target: ETL_TRACE_ID, "Truncated table: {}.{}", self.database, table_name);

        Ok(())
    }

    /// Writes rows to a Doris table using the StreamLoad HTTP API.
    ///
    /// StreamLoad is the recommended way to load bulk data into Doris efficiently.
    /// This method serializes rows to JSON format and submits them via HTTP PUT request.
    /// The StreamLoad API handles the data ingestion asynchronously and returns immediately.
    ///
    /// # Arguments
    /// * `table_name` - The name of the target table
    /// * `rows` - Vector of tuples containing (column_names, values) for each row
    ///
    /// # Returns
    /// * `Ok(())` if all rows were successfully loaded
    /// * `Err` if serialization or HTTP request failed
    pub async fn write_rows(
        &self,
        table_name: &str,
        rows: &[(Vec<String>, Vec<serde_json::Value>)],
    ) -> EtlResult<()> {
        if rows.is_empty() {
            return Ok(());
        }

        // Convert rows to JSON format for StreamLoad
        let json_data: Vec<serde_json::Value> = rows
            .iter()
            .map(|(col_names, values)| {
                let mut json_obj = serde_json::Map::new();
                for (col_name, value) in col_names.iter().zip(values.iter()) {
                    json_obj.insert(col_name.clone(), value.clone());
                }
                serde_json::Value::Object(json_obj)
            })
            .collect();

        let payload = serde_json::to_string(&json_data).map_err(|e| {
            etl_error!(
                ErrorKind::DestinationError,
                "Failed to serialize rows to JSON",
                e.to_string()
            )
        })?;

        // Use StreamLoad HTTP API
        let url = format!(
            "http://{}:{}/api/{}/{}/_stream_load",
            self.host, self.http_port, self.database, table_name
        );

        let client = reqwest::Client::new();
        let response = client
            .put(&url)
            .basic_auth(&self.username, Some(&self.password))
            .header("format", "json")
            .header("strip_outer_array", "true")
            .body(payload)
            .send()
            .await
            .map_err(|e| {
                etl_error!(
                    ErrorKind::DestinationError,
                    "Failed to send StreamLoad request",
                    e.to_string()
                )
            })?;

        let status = response.status();
        let response_text = response.text().await.unwrap_or_default();

        if !status.is_success() {
            return Err(etl_error!(
                ErrorKind::DestinationError,
                "StreamLoad failed",
                format!("Status {}: {}", status, response_text)
            ));
        }

        debug!(target: ETL_TRACE_ID, "StreamLoad response: {}", response_text);
        info!(target: ETL_TRACE_ID, "Wrote {} rows to {}.{}", rows.len(), self.database, table_name);

        Ok(())
    }
}

/// Converts a Postgres [`Type`] to a Doris SQL type string.
fn type_to_doris_type(ty: &Type) -> EtlResult<String> {
    let type_name = ty.name();

    Ok(match type_name {
        "bool" => "BOOLEAN".to_string(),
        "int2" => "SMALLINT".to_string(),
        "int4" => "INT".to_string(),
        "int8" => "BIGINT".to_string(),
        "float4" => "FLOAT".to_string(),
        "float8" => "DOUBLE".to_string(),
        "text" | "varchar" | "char" | "bpchar" => "STRING".to_string(),
        "bytea" => "STRING".to_string(),
        "date" => "DATE".to_string(),
        "time" => "STRING".to_string(),
        "timestamp" => "DATETIME".to_string(),
        "timestamptz" => "DATETIME".to_string(),
        "json" | "jsonb" => "JSON".to_string(),
        "uuid" => "VARCHAR(36)".to_string(),
        "numeric" | "decimal" => "DECIMAL(38, 9)".to_string(),
        // Arrays and other complex types stored as JSON
        _ if type_name.ends_with("[]") => "JSON".to_string(),
        _ => "STRING".to_string(), // Default fallback
    })
}
