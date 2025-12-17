use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    sync::Arc,
};

use datafusion::physical_plan::ExecutionPlan;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{DistResult, RecordBatchStream, cluster::NodeId};

#[async_trait::async_trait]
pub trait DistNetwork: Debug + Send + Sync {
    fn local_node(&self) -> DistResult<NodeId>;

    // Send task plan
    async fn send_plans(
        &self,
        node_id: NodeId,
        stage_plans: HashMap<StageId, Arc<dyn ExecutionPlan>>,
    ) -> DistResult<()>;

    // Execute task plan
    async fn execute_task(&self, node_id: NodeId, task_id: TaskId)
    -> DistResult<RecordBatchStream>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StageId {
    pub job_id: Uuid,
    pub stage: u32,
}

impl StageId {
    pub fn task_id(&self, partition: u32) -> TaskId {
        TaskId {
            job_id: self.job_id,
            stage: self.stage,
            partition,
        }
    }
}

impl Display for StageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.job_id, self.stage)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskId {
    pub job_id: Uuid,
    pub stage: u32,
    pub partition: u32,
}

impl TaskId {
    pub fn stage_id(&self) -> StageId {
        StageId {
            job_id: self.job_id,
            stage: self.stage,
        }
    }
}

impl Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}/{}", self.job_id, self.stage, self.partition)
    }
}
