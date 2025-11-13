# Apache Doris ETL Destination

This module provides ETL destination support for Apache Doris, enabling real-time data replication from PostgreSQL to Doris with full Change Data Capture (CDC) capabilities.

## Features

- **Connection Pooling**: Efficient MySQL async connection pooling for query operations
- **StreamLoad API**: Bulk data loading via Doris HTTP StreamLoad API for optimal performance
- **CDC Support**: Full change tracking with `INSERT`, `UPDATE`, and `DELETE` operations
- **Concurrent Writes**: Configurable parallel batch processing for high throughput
- **Type Mapping**: Comprehensive PostgreSQL to Doris type conversion
- **Unique Key Tables**: Automatic upsert behavior for CDC operations

## Architecture

### Table Model

The integration uses Doris **Unique Key** tables with CDC metadata columns:

```sql
CREATE TABLE schema_table (
    _change_type VARCHAR(10),           -- 'INSERT', 'UPDATE', or 'DELETE'
    _change_sequence_number BIGINT,     -- Sequence for ordering changes
    <user columns...>
) UNIQUE KEY(<primary_key_columns>)
DISTRIBUTED BY HASH(<first_primary_key>)
BUCKETS 10
PROPERTIES ("replication_num" = "1");
```

### CDC Behavior

#### INSERT & UPDATE Operations
- Written via **StreamLoad** API using JSON format
- Unique Key model automatically handles upserts
- Rows with matching primary keys are replaced
- Efficient bulk loading for high throughput

#### DELETE Operations
- **Two modes available**:

  1. **Logical Deletes** (default):
     - Rows marked with `_change_type = 'DELETE'`
     - Data remains in table for audit/history
     - Downstream queries filter by `_change_type != 'DELETE'`
     - No storage reclamation

  2. **Physical Deletes** (via `delete_rows` method):
     - Actual DELETE SQL statements
     - Rows physically removed from Doris
     - Storage is reclaimed
     - Batched in groups of 100 for efficiency

### Type Mapping

| PostgreSQL Type | Doris Type | Notes |
|----------------|------------|-------|
| `bool` | `BOOLEAN` | Direct mapping |
| `int2`, `smallint` | `SMALLINT` | 16-bit integer |
| `int4`, `int`, `integer` | `INT` | 32-bit integer |
| `int8`, `bigint` | `BIGINT` | 64-bit integer |
| `float4`, `real` | `FLOAT` | Single precision |
| `float8`, `double precision` | `DOUBLE` | Double precision |
| `text`, `varchar`, `char` | `STRING` | Variable length text |
| `bytea` | `STRING` | Binary data (base64 encoded) |
| `date` | `DATE` | Date only |
| `time` | `STRING` | Time as formatted string |
| `timestamp` | `DATETIME` | Timestamp without timezone |
| `timestamptz` | `DATETIME` | Timestamp (converted to UTC) |
| `json`, `jsonb` | `JSON` | Native JSON type |
| `uuid` | `VARCHAR(36)` | UUID as string |
| `numeric`, `decimal` | `DECIMAL(38, 9)` | Arbitrary precision |
| Arrays | `JSON` | Serialized as JSON |
| Composite types | `JSON` | Serialized as JSON |

## Configuration

```rust
DestinationConfig::Doris {
    host: "doris-fe-host".to_string(),
    query_port: 9030,              // MySQL protocol port
    http_port: 8030,               // StreamLoad HTTP port
    database: "my_database".to_string(),
    username: "root".to_string(),
    password: SecretString::new("password".to_string()),
    max_concurrent_streams: 8,     // Parallel write streams
}
```

## Usage

### Basic Table Replication

```rust
use etl_destinations::doris::DorisDestination;

let destination = DorisDestination::new(
    "doris-fe".to_string(),
    9030,                          // query_port
    8030,                          // http_port
    "my_db".to_string(),
    "root".to_string(),
    "password".to_string(),
    8,                             // max_concurrent_streams
    state_store,
).await?;

// Tables are automatically created on first write
destination.write_table_rows(table_id, rows).await?;
```

### CDC Event Processing

```rust
// Process change events from PostgreSQL logical replication
destination.write_events(vec![
    Event::Insert(insert_event),
    Event::Update(update_event),
    Event::Delete(delete_event),
]).await?;
```

### Querying with CDC Filtering

To query only current (non-deleted) rows:

```sql
SELECT * FROM my_table
WHERE _change_type != 'DELETE'
ORDER BY _change_sequence_number DESC;
```

For latest version of each row:

```sql
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY _change_sequence_number DESC) as rn
    FROM my_table
) t
WHERE rn = 1 AND _change_type != 'DELETE';
```

## Performance Considerations

### StreamLoad Configuration

The integration uses Doris StreamLoad API with these characteristics:

- **Format**: JSON with `strip_outer_array=true`
- **Batching**: Configurable via `max_concurrent_streams`
- **Concurrency**: Parallel uploads for high throughput
- **Compression**: Not currently enabled (can be added)

### Recommended Settings

For optimal performance:

```rust
// High-throughput scenarios
max_concurrent_streams: 16

// Mixed workload
max_concurrent_streams: 8

// Low-latency requirements
max_concurrent_streams: 4
```

### Connection Pooling

- **Min connections**: 5
- **Max connections**: 30
- **Protocol**: MySQL async for queries, HTTP for StreamLoad

## Limitations

1. **DELETE Semantics**: By default, deletes are logical (rows marked, not removed)
2. **Transaction Boundaries**: BEGIN/COMMIT events are logged but don't create Doris transactions
3. **Schema Changes**: Table schema changes require manual intervention
4. **Unique Keys Required**: Tables must have primary keys for proper CDC behavior
5. **No DDL Replication**: Only DML operations (INSERT/UPDATE/DELETE) are replicated

## Troubleshooting

### Connection Issues

```
Error: Failed to connect to Doris
```

**Solution**: Verify Doris FE is running and ports are accessible:
```bash
mysql -h doris-fe -P 9030 -u root -p
curl http://doris-fe:8030/api/<database>/<table>/_stream_load
```

### StreamLoad Failures

```
Error: StreamLoad failed with status 500
```

**Solution**: Check Doris FE logs and verify:
- Table exists and schema is compatible
- Database user has write permissions
- Backend nodes are healthy

### Schema Mismatch

```
Error: Failed to create table
```

**Solution**: Verify column types are compatible and table doesn't exist with different schema.

## Advanced Usage

### Custom DELETE Handling

For physical deletes with storage reclamation:

```rust
// Get primary key columns from schema
let key_columns: Vec<String> = schema.column_schemas
    .iter()
    .filter(|c| c.primary)
    .map(|c| c.name.clone())
    .collect();

// Physically delete rows
client.delete_rows(&table_name, &key_columns, &rows_to_delete).await?;
```

### Monitoring

Enable trace logging to monitor operations:

```rust
// All Doris operations log to target "ETL DorisClient"
RUST_LOG=etl_destinations::doris=debug
```

## Testing

Run unit tests:
```bash
cargo test --package etl-destinations --features doris
```

Run integration tests (requires running Doris instance):
```bash
cargo test --package etl-destinations --features doris --test doris_integration
```

## References

- [Apache Doris Documentation](https://doris.apache.org/docs/)
- [Doris StreamLoad Guide](https://doris.apache.org/docs/data-operate/import/stream-load-manual)
- [Doris Data Model](https://doris.apache.org/docs/data-table/data-model)
