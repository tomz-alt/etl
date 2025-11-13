# Apache Doris ETL Integration - Quick Start Guide

This guide shows you how to set up and run the Apache Doris ETL integration.

## Prerequisites

- Rust 1.88.0 or later
- Docker and Docker Compose (for running Doris)
- PostgreSQL source database

## 1. Start Apache Doris

Use the provided Docker Compose setup for local development:

```bash
cd etl-destinations/tests
docker compose -f docker-compose-doris.yml up -d
```

This starts:
- Doris Frontend (FE) on ports 8030 (HTTP) and 9030 (MySQL)
- Doris Backend (BE) on port 8040

Wait ~30 seconds for Doris to initialize, then create a test database:

```bash
# Connect to Doris and create database
mysql -h 127.0.0.1 -P 9030 -u root -p
# Password: (leave empty, press Enter)

# In MySQL shell:
CREATE DATABASE IF NOT EXISTS test_db;
exit;
```

## 2. Configure the Doris Destination

Add Doris configuration to your ETL config file:

```toml
[destination]
type = "doris"
host = "127.0.0.1"
query_port = 9030
http_port = 8030
database = "test_db"
username = "root"
password = ""
max_concurrent_streams = 4
```

Or use environment variables:

```bash
export DORIS_HOST="127.0.0.1"
export DORIS_QUERY_PORT="9030"
export DORIS_HTTP_PORT="8030"
export DORIS_DATABASE="test_db"
export DORIS_USERNAME="root"
export DORIS_PASSWORD=""
```

## 3. Build with Doris Feature

```bash
# Build the ETL binary with Doris support
cargo build --release --features doris

# Or run directly
cargo run --features doris -- --config your-config.toml
```

## 4. Run the ETL Pipeline

```bash
# Start replication
./target/release/etl-replicator --config config.toml

# The pipeline will:
# 1. Connect to PostgreSQL source
# 2. Create tables in Doris with CDC columns
# 3. Copy initial data via StreamLoad
# 4. Stream changes in real-time
```

## 5. Query Your Data in Doris

```bash
# Connect to Doris
mysql -h 127.0.0.1 -P 9030 -u root

# Query replicated data
USE test_db;
SHOW TABLES;

# View data (including CDC columns)
SELECT * FROM schema_table LIMIT 10;

# Query only active records (exclude DELETEs)
SELECT * FROM schema_table
WHERE _change_type != 'DELETE'
LIMIT 10;
```

## 6. Running Tests

### Unit Tests

```bash
# Run all Doris unit tests
cargo test --package etl-destinations --features doris --lib
```

### Integration Tests

```bash
# Option 1: Use the automated test runner
cd etl-destinations/tests
./run-doris-integration-tests.sh

# Option 2: Manual testing
docker compose -f docker-compose-doris.yml up -d
sleep 30  # Wait for Doris to initialize

# Create test database
mysql -h 127.0.0.1 -P 9030 -u root -e "CREATE DATABASE IF NOT EXISTS test_db;"

# Run integration tests
cargo test --package etl-destinations --test doris_integration --features doris -- --ignored

# Cleanup
docker compose -f docker-compose-doris.yml down -v
```

### Environment Variables for Tests

```bash
export DORIS_TEST_HOST="127.0.0.1"
export DORIS_TEST_QUERY_PORT="9030"
export DORIS_TEST_HTTP_PORT="8030"
export DORIS_TEST_DATABASE="test_db"
export DORIS_TEST_USERNAME="root"
export DORIS_TEST_PASSWORD=""
```

## Configuration Reference

### Required Fields

| Field | Description | Example |
|-------|-------------|---------|
| `host` | Doris FE host | `"127.0.0.1"` |
| `query_port` | MySQL protocol port | `9030` |
| `http_port` | StreamLoad HTTP port | `8030` |
| `database` | Target database name | `"my_database"` |
| `username` | Doris username | `"root"` |
| `password` | Doris password | `"password"` |

### Optional Fields

| Field | Description | Default |
|-------|-------------|---------|
| `max_concurrent_streams` | Number of parallel write streams | `4` |

## Common Issues

### "Connection refused" on port 9030

**Solution**: Wait longer for Doris to initialize (can take 30-60 seconds).

```bash
# Check if Doris is ready
docker compose -f docker-compose-doris.yml logs fe | grep "transfer from UNKNOWN to MASTER"
```

### "Database does not exist"

**Solution**: Create the database first:

```bash
mysql -h 127.0.0.1 -P 9030 -u root -e "CREATE DATABASE your_database;"
```

### StreamLoad fails with "No backend available"

**Solution**: Ensure the Doris Backend (BE) is running and registered:

```bash
# Check backend status
mysql -h 127.0.0.1 -P 9030 -u root -e "SHOW BACKENDS\G"

# Should show Alive = true
```

### Tests fail immediately

**Solution**: Make sure Docker containers are running:

```bash
docker ps | grep doris
# Should show 2 containers: doris-fe and doris-be
```

## Next Steps

- Read the [detailed architecture documentation](etl-destinations/src/doris/README.md)
- Check the [testing guide](etl-destinations/tests/DORIS_TESTING.md)
- Review [type mappings](etl-destinations/src/doris/README.md#type-mapping) for data type compatibility
- Configure production settings (replication, buckets, etc.)

## Production Deployment

For production use, you'll want to:

1. **Deploy a proper Doris cluster** (not Docker Compose):
   - Multiple FE nodes for high availability
   - Multiple BE nodes for distributed storage
   - Proper replication settings (e.g., `replication_num = 3`)

2. **Tune performance settings**:
   - Adjust `max_concurrent_streams` based on workload
   - Configure bucket count based on data volume
   - Set up appropriate distribution keys

3. **Enable monitoring**:
   - Doris has built-in monitoring endpoints
   - Monitor StreamLoad success rates
   - Track query performance

4. **Security**:
   - Use strong passwords
   - Configure firewall rules
   - Enable SSL/TLS if needed

## Resources

- [Apache Doris Documentation](https://doris.apache.org/docs/get-starting/quick-start)
- [StreamLoad Documentation](https://doris.apache.org/docs/data-operate/import/stream-load-manual)
- [Unique Key Model](https://doris.apache.org/docs/table-design/data-model/unique)
