use std::collections::HashMap;

use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use bb8_postgres::tokio_postgres::NoTls;
use datafusion_dist::{
    DistResult,
    cluster::{DistCluster, NodeId, NodeState},
};
use log::{debug, error, trace};

use crate::PostgresClusterError;

#[derive(Debug, Clone)]
pub struct PostgresCluster {
    id: NodeId,
    pool: Pool<PostgresConnectionManager<NoTls>>,
    heartbeat_timeout_seconds: i32,
}

impl PostgresCluster {
    pub fn new(id: NodeId, pool: Pool<PostgresConnectionManager<NoTls>>) -> Self {
        Self {
            id,
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
                         node_id TEXT PRIMARY KEY,
                         host TEXT NOT NULL,
                         port INTEGER NOT NULL,
                         total_memory BIGINT,
                         used_memory BIGINT,
                         free_memory BIGINT,
                         available_memory BIGINT,
                         global_cpu_usage REAL,
                         num_running_tasks INTEGER,
                         last_heartbeat TIMESTAMPTZ DEFAULT NOW(),
                         created_at TIMESTAMPTZ DEFAULT NOW(),
                         updated_at TIMESTAMPTZ DEFAULT NOW()
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
    async fn heartbeat(&self, state: NodeState) -> DistResult<()> {
        trace!("Sending heartbeat for node");

        let client = self.pool.get().await.map_err(PostgresClusterError::Pool)?;

        let node_addr = format!("{}:{}", self.id.host, self.id.port);

        let query = r#"
                   INSERT INTO cluster_nodes (
                       node_id, host, port, total_memory, used_memory, free_memory,
                       available_memory, global_cpu_usage, num_running_tasks, last_heartbeat
                   ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
                   ON CONFLICT (node_id)
                   DO UPDATE SET
                       total_memory = EXCLUDED.total_memory,
                       used_memory = EXCLUDED.used_memory,
                       free_memory = EXCLUDED.free_memory,
                       available_memory = EXCLUDED.available_memory,
                       global_cpu_usage = EXCLUDED.global_cpu_usage,
                       num_running_tasks = EXCLUDED.num_running_tasks,
                       last_heartbeat = NOW(),
                       updated_at = NOW()
                   "#;

        // 将u64转换为i64以适应PostgreSQL的BIGINT类型
        client.execute(
            query,
            &[
                &node_addr,
                &self.id.host,
                &(self.id.port as i32),
                &(state.total_memory as i64),
                &(state.used_memory as i64),
                &(state.free_memory as i64),
                &(state.available_memory as i64),
                &state.global_cpu_usage,
                &state.num_running_tasks,
            ],
        ).await.map_err(|e| {
            PostgresClusterError::Query(format!("Failed to insert heartbeat: {}", e))
        })?;

        debug!("Heartbeat sent successfully");
        Ok(())
    }
    // Get alive nodes
    async fn alive_nodes(&self) -> DistResult<HashMap<NodeId, NodeState>> {
        trace!("Fetching alive nodes");

        let client = self.pool.get().await.map_err(PostgresClusterError::Pool)?;

        // 计算活跃节点的时间阈值
        let cutoff_time = (chrono::Utc::now() - chrono::Duration::try_seconds(self.heartbeat_timeout_seconds as i64).unwrap_or_else(|| chrono::Duration::seconds(60))).naive_utc();

        let rows = client
                        .query(
                            r#"
                            SELECT node_id, host, port, total_memory, used_memory,
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
            let node_id_str: String = row.get("node_id");
            let parts: Vec<&str> = node_id_str.split(':').collect();

            if parts.len() != 2 {
                error!("Invalid node_id format: {}", node_id_str);
                continue;
            }

            let host = parts[0].to_string();
            let port: i32 = match parts[1].parse() {
                Ok(port) => port,
                Err(_) => {
                    error!("Invalid port in node_id: {}", node_id_str);
                    continue;
                }
            };

            let node_id = NodeId {
                host,
                port: port as u16,
            };

            let node_state = NodeState {
                total_memory: row.get::<_, i64>("total_memory") as u64,
                used_memory: row.get::<_, i64>("used_memory") as u64,
                free_memory: row.get::<_, i64>("free_memory") as u64,
                available_memory: row.get::<_, i64>("available_memory") as u64,
                global_cpu_usage: row.get("global_cpu_usage"),
                num_running_tasks: row.get("num_running_tasks"),
            };

            result.insert(node_id, node_state);
        }

        debug!("Found {} alive nodes", result.len());
        Ok(result)
    }
}
