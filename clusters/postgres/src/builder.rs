use std::time::Duration;

use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use bb8_postgres::tokio_postgres::NoTls;
use datafusion_dist::DistError;

use crate::{PostgresCluster, PostgresClusterError};

#[derive(Debug, derive_with::With)]
pub struct PostgresClusterBuilder {
    host: String,
    port: u16,
    user: String,
    password: String,
    dbname: Option<String>,
    table: String,
    pool_max_size: u32,
    pool_min_idle: Option<u32>,
    pool_idle_timeout: Option<Duration>,
    heartbeat_timeout_seconds: i32,
}

impl PostgresClusterBuilder {
    pub fn new(
        host: impl Into<String>,
        port: u16,
        user: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port,
            user: user.into(),
            password: password.into(),
            dbname: None,
            table: "cluster_nodes".to_string(),
            pool_max_size: 100,
            pool_min_idle: Some(5),
            pool_idle_timeout: Some(Duration::from_secs(60)),
            heartbeat_timeout_seconds: 60,
        }
    }

    pub async fn build(self) -> Result<PostgresCluster, DistError> {
        let mut config = bb8_postgres::tokio_postgres::config::Config::new();
        config
            .host(&self.host)
            .port(self.port)
            .user(&self.user)
            .password(&self.password);
        if let Some(dbname) = self.dbname {
            config.dbname(dbname);
        }
        let manager = PostgresConnectionManager::new(config, NoTls);
        let pool = Pool::builder()
            .max_size(self.pool_max_size)
            .min_idle(self.pool_min_idle)
            .idle_timeout(self.pool_idle_timeout)
            .build(manager)
            .await
            .map_err(|e| DistError::cluster(Box::new(PostgresClusterError::Connection(e))))?;

        let cluster = PostgresCluster {
            table: self.table,
            pool,
            heartbeat_timeout_seconds: self.heartbeat_timeout_seconds,
        };

        cluster.create_table_if_not_exists().await?;

        Ok(cluster)
    }
}
