# Apache Doris Integration Testing Guide

This guide explains how to run both unit and integration tests for the Apache Doris ETL destination.

## Quick Start

### Run Unit Tests (No Infrastructure Required)

```bash
# From the repository root
cargo test --package etl-destinations --features doris --lib

# Expected output:
# running 13 tests
# test result: ok. 13 passed; 0 failed; 0 ignored
```

### Run Integration Tests (Requires Docker)

```bash
# Navigate to the tests directory
cd etl-destinations/tests

# Run the automated test script
./run-doris-integration-tests.sh
```

This script will:
1. ✅ Start Apache Doris cluster (FE + BE) using Docker Compose
2. ✅ Wait for services to be healthy
3. ✅ Create test database
4. ✅ Run all integration tests
5. ✅ Clean up containers automatically

## Test Coverage

### Unit Tests (13 tests)

**SQL Safety**
- `test_json_value_to_sql` - Conversion of JSON values to SQL literals
- `test_json_value_to_sql_escaping` - SQL injection prevention

**Data Encoding**
- `test_null_cell` - Null value encoding
- `test_primitive_cells` - Basic type encoding (bool, int, float, string)
- `test_array_cell` - Array encoding with nulls
- `test_nan_and_infinity` - Special float value handling

**Type Validation**
- `test_validate_numeric_nan` - NaN rejection
- `test_validate_numeric_infinity` - ±Infinity rejection
- `test_validate_numeric_valid` - Valid numeric acceptance
- `test_validate_float_nan` - Float NaN handling (converted to null)
- `test_validate_float_infinity` - Float ±Infinity handling
- `test_validate_normal_cells` - Normal cell validation

**Utilities**
- `test_table_name_to_doris_table_id` - Name escaping and collision prevention

### Integration Tests (5 tests, ignored by default)

**Connection**
- `test_client_connection` - Verify connection to Doris cluster

**Table Operations**
- `test_create_table` - Table creation and schema verification
- `test_write_and_read_rows` - StreamLoad write and query read

**CDC Operations**
- `test_delete_rows` - Physical DELETE operations
- `test_upsert_behavior` - Unique Key table upsert semantics

## Manual Setup (Alternative to Script)

If you prefer to manually manage the Doris instance:

### 1. Start Doris Cluster

```bash
cd etl-destinations/tests
docker-compose -f docker-compose-doris.yml up -d

# Wait for healthy status
docker-compose -f docker-compose-doris.yml ps

# Check logs if needed
docker-compose -f docker-compose-doris.yml logs -f doris-fe
```

### 2. Create Test Database

```bash
docker exec doris-fe-test mysql -h127.0.0.1 -P9030 -uroot -e "CREATE DATABASE IF NOT EXISTS test_db;"
```

### 3. Run Integration Tests

```bash
cd .. # back to etl root

# Set environment variables
export DORIS_HOST=localhost
export DORIS_QUERY_PORT=9030
export DORIS_HTTP_PORT=8030
export DORIS_DATABASE=test_db
export DORIS_USER=root
export DORIS_PASSWORD=""

# Run tests
cargo test --package etl-destinations --features doris --test doris_integration -- --ignored --test-threads=1
```

### 4. Cleanup

```bash
cd etl-destinations/tests
docker-compose -f docker-compose-doris.yml down -v
```

## Configuration

### Environment Variables

All integration tests can be configured via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `DORIS_HOST` | `localhost` | Doris FE hostname |
| `DORIS_QUERY_PORT` | `9030` | MySQL protocol port |
| `DORIS_HTTP_PORT` | `8030` | StreamLoad HTTP port |
| `DORIS_DATABASE` | `test_db` | Test database name |
| `DORIS_USER` | `root` | Database username |
| `DORIS_PASSWORD` | `` (empty) | Database password |

### Docker Compose Services

**doris-fe** (Frontend)
- Image: `apache/doris:2.1.0-fe-x86_64`
- Ports: `8030` (HTTP), `9030` (MySQL)
- Health check: HTTP endpoint at `/api/health`
- Volume: Persistent metadata storage

**doris-be** (Backend)
- Image: `apache/doris:2.1.0-be-x86_64`
- Port: `8040` (webserver)
- Depends on: FE service health
- Volume: Persistent data storage

## Troubleshooting

### Doris Takes Too Long to Start

The health check retries up to 60 times (2 minutes). If it fails:

```bash
# Check FE logs
docker-compose -f tests/docker-compose-doris.yml logs doris-fe

# Check BE logs
docker-compose -f tests/docker-compose-doris.yml logs doris-be

# Restart with fresh state
docker-compose -f tests/docker-compose-doris.yml down -v
docker-compose -f tests/docker-compose-doris.yml up -d
```

### Port Conflicts

If ports 8030, 9030, or 8040 are already in use:

```bash
# Check what's using the ports
lsof -i :8030
lsof -i :9030
lsof -i :8040

# Either stop the conflicting service or modify docker-compose-doris.yml
# to use different host ports
```

### Connection Refused

If tests fail with "connection refused":

1. Wait longer for Doris to fully initialize (can take 30-60 seconds)
2. Verify FE is healthy: `docker ps | grep doris-fe`
3. Test MySQL connection: `mysql -h127.0.0.1 -P9030 -uroot`
4. Test HTTP endpoint: `curl http://localhost:8030/api/health`

### Tests Fail Intermittently

Run tests serially to avoid race conditions:

```bash
cargo test --package etl-destinations --features doris --test doris_integration -- --ignored --test-threads=1
```

### Clean Slate

To completely reset the test environment:

```bash
# Remove all Doris containers and volumes
docker-compose -f tests/docker-compose-doris.yml down -v

# Remove any orphaned containers
docker ps -a | grep doris | awk '{print $1}' | xargs docker rm -f

# Remove volumes
docker volume ls | grep doris | awk '{print $2}' | xargs docker volume rm
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Doris Integration Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Install Rust
        uses: actions-rs/toolchain@v1
        with:
          toolchain: stable

      - name: Run unit tests
        run: cargo test --package etl-destinations --features doris --lib

      - name: Start Doris
        run: |
          cd etl-destinations/tests
          docker-compose -f docker-compose-doris.yml up -d

      - name: Wait for Doris
        run: |
          timeout 120 bash -c 'until docker-compose -f etl-destinations/tests/docker-compose-doris.yml ps | grep healthy; do sleep 2; done'
          sleep 10

      - name: Create test database
        run: |
          docker exec doris-fe-test mysql -h127.0.0.1 -P9030 -uroot -e "CREATE DATABASE IF NOT EXISTS test_db;"

      - name: Run integration tests
        env:
          DORIS_HOST: localhost
          DORIS_QUERY_PORT: 9030
          DORIS_HTTP_PORT: 8030
          DORIS_DATABASE: test_db
          DORIS_USER: root
          DORIS_PASSWORD: ""
        run: |
          cargo test --package etl-destinations --features doris --test doris_integration -- --ignored --test-threads=1

      - name: Cleanup
        if: always()
        run: |
          cd etl-destinations/tests
          docker-compose -f docker-compose-doris.yml down -v
```

## Test Development

### Adding New Tests

1. **Unit tests**: Add to the `#[cfg(test)]` modules in the source files
2. **Integration tests**: Add to `tests/doris_integration.rs`

```rust
#[tokio::test]
#[ignore] // Requires running Doris instance
async fn test_new_feature() {
    let (host, query_port, http_port, database, user, password) = get_doris_config();

    let client = DorisClient::new(
        host, query_port, http_port,
        database, user, password
    ).await.unwrap();

    // Your test code here
}
```

### Best Practices

1. **Use `#[ignore]`** for integration tests that require infrastructure
2. **Clean up**: Always truncate or drop test tables after tests
3. **Serial execution**: Use `--test-threads=1` to avoid race conditions
4. **Descriptive names**: Test names should clearly indicate what they test
5. **Assertions**: Include helpful error messages in assertions

## Performance Notes

- **Unit tests**: ~1.5 seconds
- **Integration tests**: ~15-30 seconds (including Doris startup)
- **First run**: May take longer due to Docker image downloads (~500MB)

## Resources

- [Apache Doris Documentation](https://doris.apache.org/docs/)
- [Doris Docker Images](https://hub.docker.com/r/apache/doris)
- [Rust Testing Guide](https://doc.rust-lang.org/book/ch11-00-testing.html)
