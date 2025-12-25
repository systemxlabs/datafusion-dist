use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    sync::Arc,
};

use datafusion::{
    common::tree_node::{TreeNode, TreeNodeRecursion},
    physical_plan::{
        ExecutionPlan, ExecutionPlanProperties,
        coalesce_partitions::CoalescePartitionsExec, repartition::RepartitionExec,
    },
};

use crate::{
    DistError, DistResult,
    cluster::{NodeId, NodeState},
    planner::{StageId, TaskId},
};

#[async_trait::async_trait]
pub trait DistSchedule: Debug + Send + Sync {
    async fn schedule(
        &self,
        node_states: &HashMap<NodeId, NodeState>,
        stage_plans: &HashMap<StageId, Arc<dyn ExecutionPlan>>,
    ) -> DistResult<HashMap<TaskId, NodeId>>;
}

#[derive(Debug)]
pub struct DefaultScheduler;

#[async_trait::async_trait]
impl DistSchedule for DefaultScheduler {
    async fn schedule(
        &self,
        node_states: &HashMap<NodeId, NodeState>,
        stage_plans: &HashMap<StageId, Arc<dyn ExecutionPlan>>,
    ) -> DistResult<HashMap<TaskId, NodeId>> {
        if node_states.is_empty() {
            return Err(DistError::schedule("No nodes available for scheduling"));
        }

        let mut assignments = HashMap::new();

        let mut stage_index = 0;
        let mut task_index = 0;

        for (stage_id, plan) in stage_plans.iter() {
            if is_plan_fully_pipelined(plan) {
                let assignment =
                    assign_stage_tasks_to_all_nodes(*stage_id, plan, node_states, &mut task_index);
                assignments.extend(assignment);
            } else {
                let assignment =
                    assign_stage_all_tasks_to_node(*stage_id, plan, node_states, &mut stage_index);
                assignments.extend(assignment);
                stage_index += 1;
            }
        }
        Ok(assignments)
    }
}

pub fn is_plan_fully_pipelined(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let mut fully_pipelined = true;
    plan.apply(|node| {
        let any = node.as_any();
        if any.is::<RepartitionExec>() || any.is::<CoalescePartitionsExec>() {
            fully_pipelined = false;
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("plan traversal should not fail");

    fully_pipelined
}

fn assign_stage_tasks_to_all_nodes(
    stage_id: StageId,
    plan: &Arc<dyn ExecutionPlan>,
    node_states: &HashMap<NodeId, NodeState>,
    task_index: &mut usize,
) -> HashMap<TaskId, NodeId> {
    let mut assignments = HashMap::new();
    let partition_count = plan.output_partitioning().partition_count();

    for partition in 0..partition_count {
        let task_id = stage_id.task_id(partition as u32);
        assignments.insert(
            task_id,
            node_states
                .keys()
                .nth(*task_index % node_states.len())
                .expect("index should be within bounds")
                .clone(),
        );
        *task_index += 1;
    }

    assignments
}

fn assign_stage_all_tasks_to_node(
    stage_id: StageId,
    plan: &Arc<dyn ExecutionPlan>,
    node_states: &HashMap<NodeId, NodeState>,
    stage_index: &mut usize,
) -> HashMap<TaskId, NodeId> {
    let node_id = node_states
        .keys()
        .nth(*stage_index % node_states.len())
        .expect("index should be within bounds");

    let mut assignments = HashMap::new();
    let partition_count = plan.output_partitioning().partition_count();

    for partition in 0..partition_count {
        let task_id = stage_id.task_id(partition as u32);
        assignments.insert(task_id, node_id.clone());
    }

    *stage_index += 1;

    assignments
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
