use bb8_postgres::tokio_postgres::Error as PgError;
use datafusion_dist::DistError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PostgresClusterError {
    #[error("PostgreSQL connection error: {0}")]
    Connection(#[from] PgError),

    #[error("Connection pool error: {0}")]
    Pool(#[from] bb8::RunError<PgError>),

    #[error("Cluster query error: {0}")]
    Query(String),

    #[error("Node registration error: {0}")]
    NodeRegistration(String),
}

impl From<PostgresClusterError> for DistError {
    fn from(err: PostgresClusterError) -> Self {
        DistError::cluster(Box::new(err))
    }
}
