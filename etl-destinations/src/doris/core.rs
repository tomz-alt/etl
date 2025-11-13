use etl::destination::Destination;
use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::{Event, TableId, TableName, TableRow, generate_sequence_number};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info};

use crate::doris::client::{
    DORIS_CDC_SEQUENCE_COLUMN, DORIS_CDC_SPECIAL_COLUMN, DorisClient, DorisTableId,
};
use crate::doris::encoding::cell_to_json;

/// Returns the [`DorisTableId`] for a supplied [`TableName`].
///
/// In Doris, we use the format `schema_table` where underscores in the original
/// names are escaped to double underscores to prevent collisions.
pub fn table_name_to_doris_table_id(table_name: &TableName) -> DorisTableId {
    let escaped_schema = table_name.schema.replace('_', "__");
    let escaped_table = table_name.name.replace('_', "__");
    format!("{}_{}", escaped_schema, escaped_table)
}

/// Internal state for [`DorisDestination`] wrapped in `Arc<Mutex<>>`.
///
/// Contains caches for created tables to avoid redundant operations.
#[derive(Debug)]
struct Inner {
    /// Cache of table IDs that have been successfully created.
    created_tables: HashSet<DorisTableId>,
}

/// A Doris destination that implements the ETL [`Destination`] trait.
///
/// Provides data pipeline functionality including table creation, bulk loading,
/// and CDC event handling for Apache Doris.
#[derive(Debug, Clone)]
pub struct DorisDestination<S> {
    client: DorisClient,
    max_concurrent_streams: usize,
    store: S,
    inner: Arc<Mutex<Inner>>,
}

impl<S> DorisDestination<S>
where
    S: StateStore + SchemaStore,
{
    /// Creates a new [`DorisDestination`] with connection pooling.
    ///
    /// # Arguments
    /// * `host` - The Doris frontend host
    /// * `query_port` - The MySQL protocol port (typically 9030)
    /// * `http_port` - The HTTP port for StreamLoad (typically 8030)
    /// * `database` - The database name
    /// * `username` - The username for authentication
    /// * `password` - The password for authentication
    /// * `max_concurrent_streams` - Maximum concurrent write streams
    /// * `store` - State and schema store
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        host: String,
        query_port: u16,
        http_port: u16,
        database: String,
        username: String,
        password: String,
        max_concurrent_streams: usize,
        store: S,
    ) -> EtlResult<Self> {
        let client = DorisClient::new(
            host,
            query_port,
            http_port,
            database.clone(),
            username,
            password,
        )
        .await?;

        let inner = Inner {
            created_tables: HashSet::new(),
        };

        Ok(Self {
            client,
            max_concurrent_streams,
            store,
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    /// Ensures a table exists in Doris, creating it if necessary.
    async fn ensure_table_exists(&self, table_id: &TableId) -> EtlResult<DorisTableId> {
        let Some(table_schema) = self.store.get_table_schema(table_id).await? else {
            return Err(etl_error!(
                ErrorKind::MissingTableSchema,
                "Table schema not found",
                format!("No schema found for table_id {}", table_id)
            ));
        };

        let table_name = &table_schema.name;
        let doris_table_id = table_name_to_doris_table_id(table_name);

        // Check cache first
        {
            let inner = self.inner.lock().await;
            if inner.created_tables.contains(&doris_table_id) {
                return Ok(doris_table_id);
            }
        }

        // Create table
        self.client
            .create_table_if_not_exists(&doris_table_id, &table_schema.column_schemas)
            .await?;

        // Update cache
        {
            let mut inner = self.inner.lock().await;
            inner.created_tables.insert(doris_table_id.clone());
        }

        Ok(doris_table_id)
    }

    /// Converts table rows to the format expected by the Doris client.
    fn table_rows_to_json_rows(
        &self,
        table_rows: Vec<TableRow>,
        column_names: &[String],
    ) -> Vec<(Vec<String>, Vec<serde_json::Value>)> {
        table_rows
            .into_iter()
            .map(|row| {
                let values = row.values.iter().map(cell_to_json).collect();
                (column_names.to_vec(), values)
            })
            .collect()
    }
}

impl<S> Destination for DorisDestination<S>
where
    S: StateStore + SchemaStore + Send + Sync,
{
    fn name() -> &'static str {
        "doris"
    }

    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        let doris_table_id = self.ensure_table_exists(&table_id).await?;
        self.client.truncate_table(&doris_table_id).await?;
        Ok(())
    }

    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        let doris_table_id = self.ensure_table_exists(&table_id).await?;

        if table_rows.is_empty() {
            info!("No rows to write for table {}", doris_table_id);
            return Ok(());
        }

        let Some(schema) = self.store.get_table_schema(&table_id).await? else {
            return Err(etl_error!(
                ErrorKind::MissingTableSchema,
                "Table schema not found",
                format!("No schema found for table_id {}", table_id)
            ));
        };

        // Build column names list (CDC columns + schema columns)
        let mut column_names = vec![
            DORIS_CDC_SPECIAL_COLUMN.to_string(),
            DORIS_CDC_SEQUENCE_COLUMN.to_string(),
        ];
        for col in &schema.column_schemas {
            column_names.push(col.name.clone());
        }

        // Add CDC columns to each row
        let mut rows_with_cdc: Vec<TableRow> = Vec::new();

        for (sequence, row) in table_rows.into_iter().enumerate() {
            let mut new_values = vec![
                etl::types::Cell::String("INSERT".to_string()),
                etl::types::Cell::I64(sequence as i64),
            ];
            new_values.extend(row.values);

            rows_with_cdc.push(TableRow::new(new_values));
        }

        // Convert to JSON format
        let json_rows = self.table_rows_to_json_rows(rows_with_cdc, &column_names);

        // Split into batches for concurrent processing
        let batches: Vec<Vec<_>> = {
            if json_rows.is_empty() {
                vec![]
            } else {
                let batch_size = json_rows.len().div_ceil(self.max_concurrent_streams);
                let batch_size = batch_size.max(1);

                json_rows
                    .chunks(batch_size)
                    .map(|chunk| chunk.to_vec())
                    .collect()
            }
        };

        // Write batches concurrently
        let mut tasks = Vec::new();
        for batch in batches {
            let client = self.client.clone();
            let table_id_clone = doris_table_id.clone();

            let task =
                tokio::spawn(async move { client.write_rows(&table_id_clone, &batch).await });

            tasks.push(task);
        }

        // Wait for all tasks to complete
        for task in tasks {
            task.await.map_err(|e| {
                etl_error!(
                    ErrorKind::DestinationError,
                    "Failed to join write task",
                    e.to_string()
                )
            })??;
        }

        Ok(())
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        // Process events by type
        for event in events {
            match event {
                Event::Insert(insert_event) => {
                    let doris_table_id = self.ensure_table_exists(&insert_event.table_id).await?;
                    let Some(schema) = self.store.get_table_schema(&insert_event.table_id).await?
                    else {
                        return Err(etl_error!(
                            ErrorKind::MissingTableSchema,
                            "Table schema not found",
                            format!("No schema found for table_id {}", insert_event.table_id)
                        ));
                    };

                    // Build column names
                    let mut column_names = vec![
                        DORIS_CDC_SPECIAL_COLUMN.to_string(),
                        DORIS_CDC_SEQUENCE_COLUMN.to_string(),
                    ];
                    for col in &schema.column_schemas {
                        column_names.push(col.name.clone());
                    }

                    let sequence_number =
                        generate_sequence_number(insert_event.start_lsn, insert_event.commit_lsn);

                    // Add CDC columns
                    let mut new_values = vec![
                        etl::types::Cell::String("INSERT".to_string()),
                        etl::types::Cell::String(sequence_number),
                    ];
                    new_values.extend(insert_event.table_row.values);

                    let row_with_cdc = TableRow::new(new_values);
                    let json_rows = self.table_rows_to_json_rows(vec![row_with_cdc], &column_names);

                    self.client.write_rows(&doris_table_id, &json_rows).await?;
                }
                Event::Update(update_event) => {
                    let doris_table_id = self.ensure_table_exists(&update_event.table_id).await?;
                    let Some(schema) = self.store.get_table_schema(&update_event.table_id).await?
                    else {
                        return Err(etl_error!(
                            ErrorKind::MissingTableSchema,
                            "Table schema not found",
                            format!("No schema found for table_id {}", update_event.table_id)
                        ));
                    };

                    // Build column names
                    let mut column_names = vec![
                        DORIS_CDC_SPECIAL_COLUMN.to_string(),
                        DORIS_CDC_SEQUENCE_COLUMN.to_string(),
                    ];
                    for col in &schema.column_schemas {
                        column_names.push(col.name.clone());
                    }

                    let sequence_number =
                        generate_sequence_number(update_event.start_lsn, update_event.commit_lsn);

                    // Add CDC columns
                    let mut new_values = vec![
                        etl::types::Cell::String("UPDATE".to_string()),
                        etl::types::Cell::String(sequence_number),
                    ];
                    new_values.extend(update_event.table_row.values);

                    let row_with_cdc = TableRow::new(new_values);
                    let json_rows = self.table_rows_to_json_rows(vec![row_with_cdc], &column_names);

                    self.client.write_rows(&doris_table_id, &json_rows).await?;
                }
                Event::Delete(delete_event) => {
                    let doris_table_id = self.ensure_table_exists(&delete_event.table_id).await?;

                    // For delete events, we might only have the key columns depending on REPLICA IDENTITY
                    if let Some((_, old_row)) = delete_event.old_table_row {
                        let Some(schema) =
                            self.store.get_table_schema(&delete_event.table_id).await?
                        else {
                            return Err(etl_error!(
                                ErrorKind::MissingTableSchema,
                                "Table schema not found",
                                format!("No schema found for table_id {}", delete_event.table_id)
                            ));
                        };

                        // Build column names
                        let mut column_names = vec![
                            DORIS_CDC_SPECIAL_COLUMN.to_string(),
                            DORIS_CDC_SEQUENCE_COLUMN.to_string(),
                        ];
                        for col in &schema.column_schemas {
                            column_names.push(col.name.clone());
                        }

                        let sequence_number = generate_sequence_number(
                            delete_event.start_lsn,
                            delete_event.commit_lsn,
                        );

                        // Add CDC columns
                        let mut new_values = vec![
                            etl::types::Cell::String("DELETE".to_string()),
                            etl::types::Cell::String(sequence_number),
                        ];
                        new_values.extend(old_row.values);

                        let row_with_cdc = TableRow::new(new_values);
                        let json_rows =
                            self.table_rows_to_json_rows(vec![row_with_cdc], &column_names);

                        self.client.write_rows(&doris_table_id, &json_rows).await?;
                    }
                }
                Event::Truncate(truncate_event) => {
                    // Handle truncate for each affected table
                    for rel_id in truncate_event.rel_ids {
                        let table_id = TableId::new(rel_id);
                        let doris_table_id = self.ensure_table_exists(&table_id).await?;
                        self.client.truncate_table(&doris_table_id).await?;
                    }
                }
                Event::Begin(_) | Event::Commit(_) | Event::Relation(_) | Event::Unsupported => {
                    // These events don't require data writes
                    debug!("Skipping non-data event: {:?}", event.event_type());
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_name_to_doris_table_id() {
        let table_name = TableName {
            schema: "public".to_string(),
            name: "users".to_string(),
        };
        assert_eq!(table_name_to_doris_table_id(&table_name), "public_users");

        // Test escaping
        let table_name = TableName {
            schema: "my_schema".to_string(),
            name: "my_table".to_string(),
        };
        assert_eq!(
            table_name_to_doris_table_id(&table_name),
            "my__schema_my__table"
        );
    }
}
