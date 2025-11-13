//! Integration tests for Apache Doris destination.
//!
//! These tests require a running Doris instance. Set environment variables:
//! - DORIS_HOST: Doris FE host (default: localhost)
//! - DORIS_QUERY_PORT: MySQL protocol port (default: 9030)
//! - DORIS_HTTP_PORT: StreamLoad HTTP port (default: 8030)
//! - DORIS_DATABASE: Test database name (default: test_db)
//! - DORIS_USER: Username (default: root)
//! - DORIS_PASSWORD: Password (default: empty)
//!
//! To run:
//! ```bash
//! docker run -d -p 9030:9030 -p 8030:8030 apache/doris:latest
//! cargo test --package etl-destinations --features doris --test doris_integration -- --ignored
//! ```

#[cfg(feature = "doris")]
mod doris_tests {
    use etl::types::{ColumnSchema, Type};
    use etl_destinations::doris::DorisClient;
    use std::env;

    fn get_doris_config() -> (String, u16, u16, String, String, String) {
        let host = env::var("DORIS_HOST").unwrap_or_else(|_| "localhost".to_string());
        let query_port = env::var("DORIS_QUERY_PORT")
            .unwrap_or_else(|_| "9030".to_string())
            .parse()
            .unwrap();
        let http_port = env::var("DORIS_HTTP_PORT")
            .unwrap_or_else(|_| "8030".to_string())
            .parse()
            .unwrap();
        let database = env::var("DORIS_DATABASE").unwrap_or_else(|_| "test_db".to_string());
        let user = env::var("DORIS_USER").unwrap_or_else(|_| "root".to_string());
        let password = env::var("DORIS_PASSWORD").unwrap_or_else(|_| "".to_string());

        (host, query_port, http_port, database, user, password)
    }

    #[tokio::test]
    #[ignore] // Requires running Doris instance
    async fn test_client_connection() {
        let (host, query_port, http_port, database, user, password) = get_doris_config();

        let client = DorisClient::new(host, query_port, http_port, database, user, password).await;

        assert!(
            client.is_ok(),
            "Failed to connect to Doris: {:?}",
            client.err()
        );
    }

    #[tokio::test]
    #[ignore] // Requires running Doris instance
    async fn test_create_table() {
        let (host, query_port, http_port, database, user, password) = get_doris_config();

        let client = DorisClient::new(host, query_port, http_port, database, user, password)
            .await
            .unwrap();

        // Define a simple schema
        let schema = vec![
            ColumnSchema::new("id".to_string(), Type::INT4, -1, false, true),
            ColumnSchema::new("name".to_string(), Type::TEXT, -1, true, false),
            ColumnSchema::new("age".to_string(), Type::INT2, -1, true, false),
        ];

        // Create table
        let result = client
            .create_table_if_not_exists("test_users", &schema)
            .await;

        assert!(result.is_ok(), "Failed to create table: {:?}", result.err());

        // Try creating again (should be idempotent)
        let result = client
            .create_table_if_not_exists("test_users", &schema)
            .await;

        assert!(
            result.is_ok(),
            "Failed to create table again: {:?}",
            result.err()
        );
    }
}
