use std::{collections::HashMap, fmt::Debug, sync::Arc};

use datafusion::physical_plan::ExecutionPlan;

use crate::{
    cluster::{NodeId, NodeState},
    error::{DistError, DistResult},
    network::TaskId,
};

#[async_trait::async_trait]
pub trait DistSchedule: Debug + Send + Sync {
    async fn schedule(
        &self,
        node_states: HashMap<NodeId, NodeState>,
        task_plans: HashMap<TaskId, Arc<dyn ExecutionPlan>>,
    ) -> DistResult<HashMap<TaskId, NodeId>>;
}

#[derive(Debug)]
pub struct RoundRobinScheduler;

#[async_trait::async_trait]
impl DistSchedule for RoundRobinScheduler {
    async fn schedule(
        &self,
        node_states: HashMap<NodeId, NodeState>,
        task_plans: HashMap<TaskId, Arc<dyn ExecutionPlan>>,
    ) -> DistResult<HashMap<TaskId, NodeId>> {
        if node_states.is_empty() {
            return Err(DistError::schedule("No nodes available for scheduling"));
        }
        let mut assignments = HashMap::new();
        for (index, (task_id, _)) in task_plans.into_iter().enumerate() {
            let node_id = node_states
                .keys()
                .nth(index % node_states.len())
                .expect("index should be within bounds");
            assignments.insert(task_id, node_id.clone());
        }
        Ok(assignments)
    }
}
