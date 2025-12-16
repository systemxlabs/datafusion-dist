use std::{fmt::Debug, sync::Arc};

use datafusion::physical_plan::ExecutionPlan;

use crate::{RecordBatchStream, error::DistResult};

#[async_trait::async_trait]
pub trait DistNetwork: Debug + Send + Sync {
    // Send task plan
    async fn send_plan(&self, node_id: NodeId, task_id: TaskId, plan: Arc<dyn ExecutionPlan>) -> DistResult<()>;
    // Execute task plan
    async fn execute_task(&self, node_id: NodeId, task_id: TaskId) -> DistResult<RecordBatchStream>;
    
    async fn node_info(&self, node_id: NodeId) -> DistResult<NodeInfo>;
}

pub struct TaskId {
    pub job_id: String,
    pub stage_id: u32,
    pub partition_id: u32,
}

pub struct NodeId {
    pub host: String,
    pub port: u16,
}

pub struct NodeInfo {
    pub memory: u64,
}