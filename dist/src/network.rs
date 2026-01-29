use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::Arc,
};

use datafusion_execution::SendableRecordBatchStream;
use datafusion_physical_plan::ExecutionPlan;
use serde::{Deserialize, Serialize};

use crate::{
    DistError, DistResult, JobId,
    cluster::NodeId,
    planner::{StageId, TaskId},
    runtime::{StageState, TaskMetrics, TaskSet},
};

#[async_trait::async_trait]
pub trait DistNetwork: Debug + Send + Sync {
    fn local_node(&self) -> NodeId;

    async fn send_tasks(&self, node_id: NodeId, scheduled_tasks: ScheduledTasks) -> DistResult<()>;

    async fn execute_task(
        &self,
        node_id: NodeId,
        task_id: TaskId,
    ) -> DistResult<SendableRecordBatchStream>;

    async fn get_job_status(
        &self,
        node_id: NodeId,
        job_id: Option<JobId>,
    ) -> DistResult<HashMap<StageId, StageInfo>>;

    async fn cleanup_job(&self, node_id: NodeId, job_id: JobId) -> DistResult<()>;
}

pub struct ScheduledTasks {
    pub stage_plans: HashMap<StageId, Arc<dyn ExecutionPlan>>,
    pub task_ids: Vec<TaskId>,
    pub job_task_distribution: Arc<HashMap<TaskId, NodeId>>,
    pub job_meta: Arc<HashMap<String, String>>,
}

impl ScheduledTasks {
    pub fn new(
        stage_plans: HashMap<StageId, Arc<dyn ExecutionPlan>>,
        task_ids: Vec<TaskId>,
        job_task_distribution: Arc<HashMap<TaskId, NodeId>>,
        job_meta: Arc<HashMap<String, String>>,
    ) -> Self {
        ScheduledTasks {
            stage_plans,
            task_ids,
            job_task_distribution,
            job_meta,
        }
    }

    pub fn job_id(&self) -> DistResult<JobId> {
        self.task_ids
            .first()
            .map(|task_id| task_id.job_id.clone())
            .ok_or_else(|| DistError::internal("ScheduledTasks has no task_ids".to_string()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageInfo {
    pub create_at_ms: i64,
    pub assigned_partitions: HashSet<usize>,
    pub task_set_infos: Vec<TaskSetInfo>,
    pub job_meta: Arc<HashMap<String, String>>,
}

impl StageInfo {
    pub fn from_stage_state(stage_state: &StageState) -> Self {
        let task_set_infos = stage_state
            .task_sets
            .iter()
            .map(TaskSetInfo::from_task_set)
            .collect();

        StageInfo {
            create_at_ms: stage_state.create_at_ms,
            assigned_partitions: stage_state.assigned_partitions.clone(),
            task_set_infos,
            job_meta: stage_state.job_meta.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSetInfo {
    pub running_partitions: HashSet<usize>,
    pub dropped_partitions: HashMap<usize, TaskMetrics>,
}

impl TaskSetInfo {
    pub fn from_task_set(task_set: &TaskSet) -> Self {
        TaskSetInfo {
            running_partitions: task_set.running_partitions.clone(),
            dropped_partitions: task_set.dropped_partitions.clone(),
        }
    }
}
