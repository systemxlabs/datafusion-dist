use std::{collections::HashMap, sync::LazyLock};

use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use bb8_postgres::tokio_postgres::NoTls;
use datafusion_dist::{
    DistResult,
    cluster::{DistCluster, NodeId, NodeState, NodeStatus},
    util::timestamp_ms,
};
use log::{debug, trace};

use crate::PostgresClusterError;

static DATAFUSION_DIST_CLUSTER_NODES_TABLE_NAME: LazyLock<String> = LazyLock::new(|| {
    std::env::var("DATAFUSION_DIST_CLUSTER_NODES_TABLE_NAME")
        .unwrap_or_else(|_| "cluster_nodes".to_string())
});

static ENSURE_SCHEMA_QUERY: LazyLock<String> = LazyLock::new(|| {
    format!(
        r#"
            CREATE TABLE IF NOT EXISTS {} (
                host TEXT NOT NULL,
                port INTEGER NOT NULL,
                status TEXT NOT NULL,
                total_memory BIGINT NOT NULL,
                used_memory BIGINT NOT NULL,
                free_memory BIGINT NOT NULL,
                available_memory BIGINT NOT NULL,
                global_cpu_usage FLOAT4 NOT NULL,
                num_running_tasks INTEGER NOT NULL,
                last_heartbeat BIGINT NOT NULL,
                UNIQUE(host, port)
            )
        "#,
        DATAFUSION_DIST_CLUSTER_NODES_TABLE_NAME.clone()
    )
});

static HEARTBEAT_QUERY: LazyLock<String> = LazyLock::new(|| {
    format!(
        r#"
            INSERT INTO {} (
                host, port, status, total_memory, used_memory, free_memory,
                available_memory, global_cpu_usage, num_running_tasks, last_heartbeat
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (host, port)
            DO UPDATE SET
                status = EXCLUDED.status,
                total_memory = EXCLUDED.total_memory,
                used_memory = EXCLUDED.used_memory,
                free_memory = EXCLUDED.free_memory,
                available_memory = EXCLUDED.available_memory,
                global_cpu_usage = EXCLUDED.global_cpu_usage,
                num_running_tasks = EXCLUDED.num_running_tasks,
                last_heartbeat = EXCLUDED.last_heartbeat
        "#,
        DATAFUSION_DIST_CLUSTER_NODES_TABLE_NAME.clone()
    )
});

static ALIVE_NODES_QUERY: LazyLock<String> = LazyLock::new(|| {
    format!(
        r#"SELECT host, port, status, total_memory, used_memory,
            free_memory, available_memory, global_cpu_usage, num_running_tasks
        FROM {}
        WHERE last_heartbeat >= $1
        "#,
        DATAFUSION_DIST_CLUSTER_NODES_TABLE_NAME.clone()
    )
});

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
            .execute(ENSURE_SCHEMA_QUERY.as_str(), &[])
            .await
            .map_err(|e| PostgresClusterError::Query(format!("Failed to create table: {e:?}")))?;

        Ok(())
    }

    fn calculate_cutoff_time(&self) -> i64 {
        let timeout_ms = i64::from(self.heartbeat_timeout_seconds)
            .checked_mul(1000)
            .unwrap_or(60_000);
        timestamp_ms() - timeout_ms
    }
}

#[async_trait::async_trait]
impl DistCluster for PostgresCluster {
    // Send heartbeat
    async fn heartbeat(&self, node_id: NodeId, state: NodeState) -> DistResult<()> {
        // Get current timestamp in milliseconds as i64
        let timestamp = timestamp_ms();

        let client = self.pool.get().await.map_err(PostgresClusterError::Pool)?;

        let query = HEARTBEAT_QUERY.as_str();

        client
            .execute(
                query,
                &[
                    &node_id.host,
                    &(node_id.port as i32),
                    &state.status.to_string(),
                    &(state.total_memory as i64),
                    &(state.used_memory as i64),
                    &(state.free_memory as i64),
                    &(state.available_memory as i64),
                    &state.global_cpu_usage,
                    &(state.num_running_tasks as i32),
                    &timestamp,
                ],
            )
            .await
            .map_err(|e| {
                PostgresClusterError::Query(format!("Failed to insert heartbeat: {e:?}"))
            })?;

        debug!("Heartbeat sent successfully");
        Ok(())
    }

    // Get alive nodes
    async fn alive_nodes(&self) -> DistResult<HashMap<NodeId, NodeState>> {
        trace!("Fetching alive nodes");

        let cutoff_time = self.calculate_cutoff_time();

        let client = self.pool.get().await.map_err(PostgresClusterError::Pool)?;

        let rows = client
            .query(ALIVE_NODES_QUERY.as_str(), &[&cutoff_time])
            .await
            .map_err(|e| {
                PostgresClusterError::Query(format!("Failed to query alive nodes: {}", e))
            })?;

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

            let status_str: String = row
                .try_get(2)
                .map_err(|e| PostgresClusterError::Query(e.to_string()))?;

            let status = status_str.parse::<NodeStatus>()?;

            let node_state = NodeState {
                status,
                total_memory: row
                    .try_get::<_, i64>(3)
                    .map_err(|e| PostgresClusterError::Query(e.to_string()))?
                    as u64,
                used_memory: row
                    .try_get::<_, i64>(4)
                    .map_err(|e| PostgresClusterError::Query(e.to_string()))?
                    as u64,
                free_memory: row
                    .try_get::<_, i64>(5)
                    .map_err(|e| PostgresClusterError::Query(e.to_string()))?
                    as u64,
                available_memory: row
                    .try_get::<_, i64>(6)
                    .map_err(|e| PostgresClusterError::Query(e.to_string()))?
                    as u64,
                global_cpu_usage: row
                    .try_get(7)
                    .map_err(|e| PostgresClusterError::Query(e.to_string()))?,
                num_running_tasks: row
                    .try_get::<_, i32>(8)
                    .map_err(|e| PostgresClusterError::Query(e.to_string()))?
                    as u32,
            };

            result.insert(node_id, node_state);
        }

        debug!("Found {} alive nodes", result.len());
        Ok(result)
    }
}
