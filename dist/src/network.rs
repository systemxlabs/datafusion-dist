use std::{collections::HashMap, fmt::Debug, sync::Arc};

use datafusion::physical_plan::ExecutionPlan;

use crate::{
    DistResult, RecordBatchStream,
    cluster::NodeId,
    planner::{StageId, TaskId},
};

#[async_trait::async_trait]
pub trait DistNetwork: Debug + Send + Sync {
    fn local_node(&self) -> NodeId;

    // Send task plan
    async fn send_tasks(&self, node_id: NodeId, scheduled_tasks: ScheduledTasks) -> DistResult<()>;

    // Execute task plan
    async fn execute_task(&self, node_id: NodeId, task_id: TaskId)
    -> DistResult<RecordBatchStream>;
}

pub struct ScheduledTasks {
    pub stage_plans: HashMap<StageId, Arc<dyn ExecutionPlan>>,
    pub task_ids: Vec<TaskId>,
}

impl ScheduledTasks {
    pub fn new(
        stage_plans: HashMap<StageId, Arc<dyn ExecutionPlan>>,
        task_ids: Vec<TaskId>,
    ) -> Self {
        ScheduledTasks {
            stage_plans,
            task_ids,
        }
    }
}
