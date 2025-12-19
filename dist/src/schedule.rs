use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    sync::Arc,
};

use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};

use crate::{
    DistError, DistResult,
    cluster::{NodeId, NodeState},
    planner::{StageId, TaskId},
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

pub struct DisplayableTaskDistribution<'a>(pub &'a HashMap<TaskId, NodeId>);

impl Display for DisplayableTaskDistribution<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut node_tasks = HashMap::new();

        for (task_id, node_id) in self.0.iter() {
            node_tasks
                .entry(node_id)
                .or_insert_with(Vec::new)
                .push(task_id);
        }

        let mut node_dist = Vec::new();
        for (node_id, tasks) in node_tasks.iter() {
            node_dist.push(format!(
                "{}->{node_id}",
                tasks
                    .iter()
                    .map(|t| format!("{}/{}", t.stage, t.partition))
                    .collect::<Vec<String>>()
                    .join(",")
            ));
        }

        write!(f, "{}", node_dist.join(", "))
    }
}
