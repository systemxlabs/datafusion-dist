use std::{collections::HashMap, fmt::Debug};

use serde::{Deserialize, Serialize};

use crate::DistResult;

#[async_trait::async_trait]
pub trait DistCluster: Debug + Send + Sync {
    // Send heartbeat
    async fn heartbeat(&self, state: NodeState) -> DistResult<()>;
    // Get alive nodes
    async fn alive_nodes(&self) -> DistResult<HashMap<NodeId, NodeState>>;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeState {
    pub total_memory: u64,
    pub used_memory: u64,
    pub free_memory: u64,
    pub available_memory: u64,
    pub global_cpu_usage: f32,
    pub num_running_tasks: u32,
}
