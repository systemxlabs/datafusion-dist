use std::collections::HashMap;

use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use bb8_postgres::tokio_postgres::NoTls;
use datafusion_dist::{
    DistResult,
    cluster::{DistCluster, NodeId, NodeState},
};
use log::{debug, trace};

use crate::PostgresClusterError;

#[derive(Debug, Clone)]
pub struct PostgresCluster {
    pool: Pool<PostgresConnectionManager<NoTls>>,
    heartbeat_timeout_seconds: i32,
}

impl PostgresCluster {
    pub fn new(pool: Pool<PostgresConnectionManager<NoTls>>) -> Self {
        Self {
            pool,
            heartbeat_timeout_seconds: 60,
        }
    }

    pub fn with_heartbeat_timeout(mut self, timeout_seconds: i32) -> Self {
        self.heartbeat_timeout_seconds = timeout_seconds;
        self
    }

    pub async fn ensure_schema(&self) -> DistResult<()> {
        let client = self.pool.get().await.map_err(PostgresClusterError::Pool)?;

        client
            .execute(
                r#"
                     CREATE TABLE IF NOT EXISTS cluster_nodes (
                         host TEXT NOT NULL,
                         port INTEGER NOT NULL,
                         total_memory BIGINT,
                         used_memory BIGINT,
                         free_memory BIGINT,
                         available_memory BIGINT,
                         global_cpu_usage REAL,
                         num_running_tasks INTEGER,
                         last_heartbeat TIMESTAMPTZ DEFAULT NOW(),
                     )
                     "#,
                &[],
            )
            .await
            .map_err(|e| PostgresClusterError::Query(format!("Failed to create table: {}", e)))?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl DistCluster for PostgresCluster {
    // Send heartbeat
    async fn heartbeat(&self, node_id: NodeId, state: NodeState) -> DistResult<()> {
        trace!("Sending heartbeat for node");

        let client = self.pool.get().await.map_err(PostgresClusterError::Pool)?;

        let query = r#"
                   INSERT INTO cluster_nodes (
                       host, port, total_memory, used_memory, free_memory,
                       available_memory, global_cpu_usage, num_running_tasks, last_heartbeat
                   ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
                   ON CONFLICT (host, port)
                   DO UPDATE SET
                       total_memory = EXCLUDED.total_memory,
                       used_memory = EXCLUDED.used_memory,
                       free_memory = EXCLUDED.free_memory,
                       available_memory = EXCLUDED.available_memory,
                       global_cpu_usage = EXCLUDED.global_cpu_usage,
                       num_running_tasks = EXCLUDED.num_running_tasks,
                       last_heartbeat = NOW()
                   "#;

        client
            .execute(
                query,
                &[
                    &node_id.host,
                    &(node_id.port as i32),
                    &(state.total_memory as i64),
                    &(state.used_memory as i64),
                    &(state.free_memory as i64),
                    &(state.available_memory as i64),
                    &state.global_cpu_usage,
                    &state.num_running_tasks,
                ],
            )
            .await
            .map_err(|e| {
                PostgresClusterError::Query(format!("Failed to insert heartbeat: {}", e))
            })?;

        debug!("Heartbeat sent successfully");
        Ok(())
    }

    // Get alive nodes
    async fn alive_nodes(&self) -> DistResult<HashMap<NodeId, NodeState>> {
        trace!("Fetching alive nodes");

        let client = self.pool.get().await.map_err(PostgresClusterError::Pool)?;

        let cutoff_time = (chrono::Utc::now()
            - chrono::Duration::try_seconds(self.heartbeat_timeout_seconds as i64)
                .unwrap_or_else(|| chrono::Duration::seconds(60)))
        .naive_utc();

        let rows = client
                        .query(
                            r#"
                            SELECT host, port, total_memory, used_memory,
                                   free_memory, available_memory, global_cpu_usage, num_running_tasks
                            FROM cluster_nodes
                            WHERE last_heartbeat >= $1
                            "#,
                            &[&cutoff_time],
                        )
                        .await
                        .map_err(|e| PostgresClusterError::Query(format!("Failed to query alive nodes: {}", e)))?;

        let mut result = HashMap::new();
        for row in rows {
            let host: String = row
                .try_get(0)
                .map_err(|e| PostgresClusterError::Query(e.to_string()))?;
            let port: i32 = row
                .try_get(1)
                .map_err(|e| PostgresClusterError::Query(e.to_string()))?;

            let node_id = NodeId {
                host,
                port: port as u16,
            };

            let node_state = NodeState {
                total_memory: row
                    .try_get::<_, i64>(2)
                    .map_err(|e| PostgresClusterError::Query(e.to_string()))?
                    as u64,
                used_memory: row
                    .try_get::<_, i64>(3)
                    .map_err(|e| PostgresClusterError::Query(e.to_string()))?
                    as u64,
                free_memory: row
                    .try_get::<_, i64>(4)
                    .map_err(|e| PostgresClusterError::Query(e.to_string()))?
                    as u64,
                available_memory: row
                    .try_get::<_, i64>(5)
                    .map_err(|e| PostgresClusterError::Query(e.to_string()))?
                    as u64,
                global_cpu_usage: row
                    .try_get(6)
                    .map_err(|e| PostgresClusterError::Query(e.to_string()))?,
                num_running_tasks: row
                    .try_get(7)
                    .map_err(|e| PostgresClusterError::Query(e.to_string()))?,
            };

            result.insert(node_id, node_state);
        }

        debug!("Found {} alive nodes", result.len());
        Ok(result)
    }
}
