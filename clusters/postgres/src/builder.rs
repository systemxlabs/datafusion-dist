use std::time::Duration;

use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use bb8_postgres::tokio_postgres::NoTls;
use datafusion_dist::{DistError, cluster::NodeId};

use crate::{PostgresCluster, PostgresClusterError};

pub struct PostgresClusterBuilder {
    host: String,
    port: u16,
    user: String,
    password: String,
    dbname: Option<String>,
    node_id: Option<NodeId>,
    pool_max_size: u32,
    pool_min_idle: Option<u32>,
    pool_idle_timeout: Option<Duration>,
}

impl PostgresClusterBuilder {
    pub fn new(
        host: impl Into<String>,
        port: u16,
        user: impl Into<String>,
        password: impl Into<String>,
        node_id: NodeId,
    ) -> Self {
        Self {
            host: host.into(),
            port,
            user: user.into(),
            password: password.into(),
            dbname: None,
            node_id: Some(node_id),
            pool_max_size: 100,
            pool_min_idle: Some(5),
            pool_idle_timeout: Some(Duration::from_secs(60)),
        }
    }

    pub fn dbname(mut self, dbname: impl Into<String>) -> Self {
        self.dbname = Some(dbname.into());
        self
    }

    pub fn pool_max_size(mut self, pool_max_size: u32) -> Self {
        self.pool_max_size = pool_max_size;
        self
    }

    pub fn pool_min_idle(mut self, pool_min_idle: Option<u32>) -> Self {
        self.pool_min_idle = pool_min_idle;
        self
    }

    pub fn pool_idle_timeout(mut self, pool_idle_timeout: Option<Duration>) -> Self {
        self.pool_idle_timeout = pool_idle_timeout;
        self
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

        let node_id = self.node_id.ok_or_else(|| {
            DistError::cluster(Box::new(PostgresClusterError::NodeRegistration(
                "NodeId is required".to_string(),
            )))
        })?;

        let cluster = PostgresCluster::new(node_id, pool);

        cluster.ensure_schema().await?;

        Ok(cluster)
    }
}
