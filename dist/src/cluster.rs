use std::{
    collections::HashMap,
    fmt::{Debug, Display},
};

use serde::{Deserialize, Serialize};

use crate::DistResult;

#[async_trait::async_trait]
pub trait DistCluster: Debug + Send + Sync {
    // Send heartbeat
    async fn heartbeat(&self, node_id: NodeId, state: NodeState) -> DistResult<()>;
    // Get alive nodes
    async fn alive_nodes(&self) -> DistResult<HashMap<NodeId, NodeState>>;
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId {
    pub host: String,
    pub port: u16,
}

impl Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum NodeStatus {
    #[default]
    Available,
    Draining,
    Exited,
}

impl NodeStatus {
    pub fn to_str(self) -> &'static str {
        match self {
            NodeStatus::Available => "Available",
            NodeStatus::Draining => "Draining",
            NodeStatus::Exited => "Exited",
        }
    }
}

impl TryFrom<&str> for NodeStatus {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "Available" => Ok(NodeStatus::Available),
            "Draining" => Ok(NodeStatus::Draining),
            "Exited" => Ok(NodeStatus::Exited),
            _ => Err(format!("Unknown NodeStatus: {}", value)),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeState {
    pub status: NodeStatus,
    pub total_memory: u64,
    pub used_memory: u64,
    pub free_memory: u64,
    pub available_memory: u64,
    pub global_cpu_usage: f32,
    pub num_running_tasks: u32,
}
