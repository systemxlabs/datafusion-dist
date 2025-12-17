use std::{collections::HashMap, fmt::Debug, sync::Arc};

use datafusion::physical_plan::ExecutionPlan;
use serde::{Deserialize, Serialize};

use crate::{RecordBatchStream, cluster::NodeId, error::DistResult};

#[async_trait::async_trait]
pub trait DistNetwork: Debug + Send + Sync {
    // Send task plan
    async fn send_plan(
        &self,
        node_id: NodeId,
        stage_plans: HashMap<StageId, Arc<dyn ExecutionPlan>>,
    ) -> DistResult<()>;

    // Execute task plan
    async fn execute_task(&self, node_id: NodeId, task_id: TaskId)
    -> DistResult<RecordBatchStream>;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StageId {
    pub job_id: String,
    pub stage_id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskId {
    pub job_id: String,
    pub stage_id: u32,
    pub partition_id: u32,
}
