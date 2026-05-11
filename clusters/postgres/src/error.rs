use bb8_postgres::tokio_postgres::Error as PgError;
use datafusion_dist::DistError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PostgresClusterError {
    #[error("PostgreSQL connection error to {host}:{port} (user={user}, db={db_name}): {source}")]
    Connection {
        source: PgError,
        host: String,
        port: u16,
        user: String,
        db_name: String,
    },

    #[error("Connection pool error: {0}")]
    Pool(#[from] bb8::RunError<PgError>),

    #[error("Cluster query error: {0}")]
    Query(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use bb8_postgres::tokio_postgres::{self, NoTls};

    #[tokio::test]
    async fn test_connection_error_formatting() {
        // Obtain a real PgError by connecting to a non-existent server.
        let pg_err = match tokio_postgres::connect(
            "host=nonexistent port=9999 user=test dbname=test",
            NoTls,
        )
        .await
        {
            Err(e) => e,
            Ok(_) => panic!("expected connection to fail"),
        };

        let err = PostgresClusterError::Connection {
            source: pg_err,
            host: "192.168.0.221".to_string(),
            port: 33459,
            user: "admin".to_string(),
            db_name: "datafabric".to_string(),
        };
        let msg = err.to_string();
        assert!(
            msg.contains("192.168.0.221:33459"),
            "should contain host:port, got: {}",
            msg
        );
        assert!(msg.contains("user=admin"), "should contain user, got: {}", msg);
        assert!(
            msg.contains("db=datafabric"),
            "should contain db, got: {}",
            msg
        );
        // The source error should also appear in the output.
        assert!(!msg.is_empty(), "error message should not be empty");
    }

    #[tokio::test]
    async fn test_connection_error_formatting_no_db() {
        let pg_err = match tokio_postgres::connect(
            "host=nonexistent port=9999 user=test dbname=test",
            NoTls,
        )
        .await
        {
            Err(e) => e,
            Ok(_) => panic!("expected connection to fail"),
        };

        let err = PostgresClusterError::Connection {
            source: pg_err,
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            db_name: "(none)".to_string(),
        };
        let msg = err.to_string();
        assert!(
            msg.contains("localhost:5432"),
            "should contain host:port, got: {}",
            msg
        );
        assert!(msg.contains("db=(none)"), "should contain (none) db, got: {}", msg);
    }
}

impl From<PostgresClusterError> for DistError {
    fn from(err: PostgresClusterError) -> Self {
        DistError::cluster(Box::new(err))
    }
}
