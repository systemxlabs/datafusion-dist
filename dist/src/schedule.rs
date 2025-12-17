use std::{collections::HashMap, fmt::Debug, sync::Arc};

use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};

use crate::{
    DistError, DistResult,
    cluster::{NodeId, NodeState},
    network::{StageId, TaskId},
};

#[async_trait::async_trait]
pub trait DistSchedule: Debug + Send + Sync {
    async fn schedule(
        &self,
        node_states: HashMap<NodeId, NodeState>,
        stage_plans: &HashMap<StageId, Arc<dyn ExecutionPlan>>,
    ) -> DistResult<HashMap<TaskId, NodeId>>;
}

#[derive(Debug)]
pub struct RoundRobinScheduler;

#[async_trait::async_trait]
impl DistSchedule for RoundRobinScheduler {
    async fn schedule(
        &self,
        node_states: HashMap<NodeId, NodeState>,
        stage_plans: &HashMap<StageId, Arc<dyn ExecutionPlan>>,
    ) -> DistResult<HashMap<TaskId, NodeId>> {
        if node_states.is_empty() {
            return Err(DistError::schedule("No nodes available for scheduling"));
        }

        let mut assignments = HashMap::new();
        let mut index = 0;

        for (stage_id, plan) in stage_plans.iter() {
            let partition_count = plan.output_partitioning().partition_count();

            for partition in 0..partition_count {
                let task_id = stage_id.task_id(partition as u32);

                let node_id = node_states
                    .keys()
                    .nth(index % node_states.len())
                    .expect("index should be within bounds");
                assignments.insert(task_id, node_id.clone());

                index += 1;
            }
        }
        Ok(assignments)
    }
}
